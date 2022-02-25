from flask import current_app as app
from sqlalchemy import func

# the satyrn configs...
# db = current_app.satConf.db
# TARGET_MODEL = current_app.satConf.targetModel
# SEARCH_SPACE = current_app.satConf.searchSpace
# formatResult = current_app.satConf.formatResult
# PREFILTERS = getattr(current_app.satConf, "preFilters")

cache = app.cache
CACHE_TIMEOUT=6000

#
#
# Helper functions for searching/results
@cache.memoize(timeout=CACHE_TIMEOUT)
def getResults(opts, ring, ringExtractor, targetEntity, page=0, batchSize=10):
    # takes a dictionary of key->vals that power a set of searchs downstream...
    # also takes a page + slice value to power pagination on the UI (and caching per page)
    # defers to another memoized function getResultSet to preload results in batches of 10x the slice
    targetRange = getCacheRange(page, batchSize)
    payload = getResultSet(opts, ring, ringExtractor, targetEntity, targetRange)
    relativeStart = page * batchSize - targetRange[0]
    relativeStop = relativeStart + batchSize
    return {
        "totalCount": payload["totalCount"], # the total count based on query
        "page": page, # the page this is
        "batchSize": batchSize, # the batch size of page (if more than count, we're at end)
        "activeCacheRange": targetRange, # the range of this batch's cache
        "results": payload["results"][relativeStart:relativeStop] # the list of cases
    }

def getCacheRange(page, batchSize):
    # work out the size of the slice to pass to getResultSet for a reasonable caching range
    window = batchSize*10
    targetTop = window
    while not (page*batchSize) < targetTop:
        targetTop += window
    return [targetTop-window, targetTop]

@cache.memoize(timeout=CACHE_TIMEOUT)
def getResultSet(opts, ring, ringExtractor, targetEntity, targetRange=[0,100]):
    return rawGetResultSet(opts, ring, ringExtractor, targetEntity, targetRange)

def rawGetResultSet(opts, ring, ringExtractor, targetEntity, targetRange=None, simpleResults=True, just_query=False, sess=None, query=None):
    db = ring.db
    targetInfo = ringExtractor.resolveEntity(targetEntity)[1]
    targetModel = getattr(db, targetInfo.table)
    searchSpace = ringExtractor.getSearchSpace(targetEntity)
    formatResult = ringExtractor.formatResult
    # takes a dictionary of key->vals that power a set of searchs downstream...
    # also takes a ring, ringExtractor and targetEntity name
    # and a range value to memoize a broader set than current page view
    # returns a dict with two keys: results and totalCount
    if not sess:
        sess = db.Session()
        query = sess.query(targetModel)

    '''
    TODO: This would need to be modified,
    example query
    {
        Query: {
            And: [
                {
                    Or: [
                        [{Judge, name}, Donna, contains] 
                        [{Judge, name}, Andong, contains] 
                    ]
                },
                [{Case, date, month}, October, equals] 
            ]
        }
        Relationships: [CaseToJudge]
    }

    {
        Query: {
            And: [
                {
                    Not: [{Judge, name}, Andrew, contains] 
                },
                {
                    Not: [{Judge, name}, Andong, contains] 
                }
                ]
        }
        Relationships: [CaseToJudge]
    }


    before it wouldve been something like
    {judgename: [andong, donna]}
    {judgenam: andong, year:1020}

    the general breakdwon of the code:
        - Will have to be some kind of recursive function


    makequery(the_d):

        if the_d has operation (not, and ,or) (i.e. if it a dict):
            if and or or:
                return and/or(makequery for each thing  in the ands list)
            if not:
                return not of the makequery of thing that the_d has
        else (it a list, really a len 3 tuple):
            return a formed filter with the stuff in the_d
            the steps for this are
                use the get function to get the field
                use a defined "MEETS FILTER" funcition that takes a field and a value
                    e.g. makefilter(field, val, filter_type="exact")

    outline of code from here onward

    1. do the makequery code
    2. do joins
    3. do sorting
    4. return results, or not, i aint your mom


    # PENDING: do we add reference to the query (line 58)

    '''


    # Make query function
    # query, tables = make_query(query_dct, tables=[])
    # NOTE: we mighta ctually not need to get all tables here
    # bc tables were used in the case that we didnt have access to
    # the main filtereable table in analytics. i dont think
    # this would be the case? since we will always be
    # querying the main table, thereby it should always
    # be possible to join to it



    for needleType, needle in opts.items():
        if needleType in ["sortBy", "sortDir"]:
            continue
        # get the info from searchSpace
        details = searchSpace[needleType]
        # we care about the type, model and fields of the detail
        if details["allowMultiple"]:
            for subneedle in needle:
                query = bindQuery(sess, targetModel, query, needleType, subneedle, details)
        else:
            query = bindQuery(sess, targetModel, query, needleType, needle, details)



    # DO joins
    # NOTE: move analytics join to utils.py. then resuse
    # NOTE: also move _get to utils.py (and similar functions)

    # Do prefilters
    # TODO: bring this back?
    # for field in PREFILTERS:
    #     for filt in PREFILTERS[field]:
    #         query = query.filter(filt)

    if "sortBy" in opts and opts["sortBy"] is not None:
        details = searchSpace[opts["sortBy"]]
        query = sortQuery(sess, targetModel, query, opts["sortBy"], opts["sortDir"], details)
    if just_query:
        return query
    return bundleQueryResults(query, targetRange, targetEntity, formatResult, simpleResults)



# TODO: repurpose bindQuery so that it does the "unit" thingy for one formed filter
# might be able to reuse the join functions from engine and just bring it here
# might be able to reuse the _get functions and whatnot from engine
# should account for different datatypes and types of searches
# so far: range, exact, contains
# range will have the second value of the tuple be a list
# joins taken out of here, not needed in details i believe
def bindQuery(sess, targetModel, query, needleType, needle, details):
    # breakpoint()
    if details["model"] == targetModel:
        # don't have to worry about joins on this one...just filter
        targetField = createTargetFieldSet(details["model"], details["fields"])
        if details["type"] == "date":
            if needle[0]: query = query.filter(targetField >= needle[0])
            if needle[1]: query = query.filter(targetField <= needle[1])
        else:
            query = query.filter(func.lower(targetField).contains(func.lower(needle)))
    # elif "specialCase" not in details:
    elif type(details["model"]) != list:
        # TODO: work out the date range thing here too
        # this is a generic single-join situation
        targetField = createTargetFieldSet(details["model"], details["fields"])
        pathToModel = getattr(targetModel, details["fromTargetModel"])
        query = query.filter(
            pathToModel.any(func.lower(targetField).contains(func.lower(needle)))
        )
    elif type(details["model"]) == list:
        # TODO: work out the date range thing here too
        # this currently only supports a double hop
        # (e.g.: for SCALES, db.Case->db.JudgeOnCase->db.Judge)
        # TODO: expand this to take arbitrary paths (as necessary)
        # see "judgeName" in SCALES searchSpace.py for how this works from the config side
        # (note that details["model"] list and details["fromTargetModel"] are parallel lists off by 1 because of TARGET_MODEL (in SCALES, that's db.Case) being the implicit starter model)

        path_list = []
        curr_model = targetModel
        for idx in range(len(details["model"])):
            path_list.append(getattr(curr_model, details["fromTargetModel"][idx]))
            curr_model = details["model"][idx]

        terminalModel = curr_model

        # pathStepA = getattr(TARGET_MODEL, details["fromTargetModel"][0])
        # pathStepB = getattr(details["model"][0], details["fromTargetModel"][1])
        # terminalModel = details["model"][1]
        fieldSet = createTargetFieldSet(terminalModel, details["fields"])

        # filter_path = pathStepA.any(pathStepB.any( func.lower(func.concat(fieldSet)).contains(func.lower(needle))  ) )

        filter_path = func.lower(func.concat(fieldSet)).contains(func.lower(needle))
        for path in reversed(path_list):
            filter_path = path.any(filter_path)

        query = query.filter(filter_path)
        # for reference, on the "judgeName" in SCALES config, the above is equivalent to:
        # query = query.filter(
        #     db.Case.judges.any(db.JudgeOnCase.judge.any(func.lower(
        #       func.concat(createTargetFieldSet(db.Judge, details["fields"]))) \
        #         .contains(func.lower(needle))))
        # )
    else:
        # TODO: there will prob be more here...
        # perhaps this is the space for plugins?
        pass
    return query

def sortQuery(sess, targetModel, query, sortBy, sortDir, details):
    sortKey = "sortField" if "sortField" in details else "fields"
    # breakpoint()
    if details["model"] == targetModel:
        targetField = createTargetFieldSet(targetModel, details[sortKey])
        # targetField = targetField if sortDir == "asc" else targetField.desc()
        if sortDir == "desc":
            return query.order_by(targetField.desc())
        return query.order_by(targetField)
    else:
        # TODO: set it up so that the system can sort by relationships
        return query

def bundleQueryResults(query, targetRange, targetEntity, formatResult, simpleResults=True):
    totalCount = query.count()
    if targetRange is not None:
        results = query.slice(targetRange[0], targetRange[1]).all()
    else:
        results = query.all()

    if simpleResults:
        results = [formatResult(result, targetEntity) for result in results]

        return {
            "results": results,
            "totalCount": totalCount,
            "resultRange": targetRange
        }
    return results

def createTargetFieldSet(model, fields):
    field = [getattr(model, field) for field in fields]
    if len(field) > 1:
        fieldSet = []
        for fs in field:
            fieldSet += [fs, " "]
        field = func.concat(*fieldSet)
    else:
        field = field[0]
    return field
