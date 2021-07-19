from flask import current_app
from sqlalchemy import func

# the satyrn configs...
db = current_app.satConf.db
TARGET_MODEL = current_app.satConf.targetModel
SEARCH_SPACE = current_app.satConf.searchSpace
formatResult = current_app.satConf.formatResult
PREFILTERS = getattr(current_app.satConf, "preFilters")

cache = current_app.cache
CACHE_TIMEOUT=6000

#
#
# Helper functions for searching/results
@cache.memoize(timeout=CACHE_TIMEOUT)
def getResults(opts, page=0, batchSize=10):
    # takes a dictionary of key->vals that power a set of searchs downstream...
    # also takes a page + slice value to power pagination on the UI (and caching per page)
    # defers to another memoized function getResultSet to preload results in batches of 10x the slice
    targetRange = getCacheRange(page, batchSize)
    payload = getResultSet(opts, targetRange)
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
def getResultSet(opts, targetRange=[0,100]):
    return rawGetResultSet(opts, targetRange)

def rawGetResultSet(opts, targetRange=None, simpleResults=True, just_query=False, sess=None, query=None):
    # breakpoint()
    # takes a dictionary of key->vals that power a set of searchs downstream...
    # also takes a range value to memoize a broader set than current page view
    # returns a dict with two keys: results and totalCount
    # TODO: should this leverage ids from autocomplete?
    # sticking to simple search in v0.1
    if not sess:
        sess = db.Session()
        query = sess.query(TARGET_MODEL)
    for needleType, needle in opts.items():
        if needleType in ["sortBy", "sortDir"]:
            continue
        # get the info from SEARCH_SPACE
        details = SEARCH_SPACE[needleType]
        # we care about the type, model and fields of the detail
        if details["allowMultiple"]:
            for subneedle in needle:
                query = bindQuery(sess, query, needleType, subneedle, details)
        else:
            query = bindQuery(sess, query, needleType, needle, details)

    # Do prefilters
    for field in PREFILTERS:
        for filt in PREFILTERS[field]:
            query = query.filter(filt)

    if "sortBy" in opts and opts["sortBy"] is not None:
        details = SEARCH_SPACE[opts["sortBy"]]
        query = sortQuery(sess, query, opts["sortBy"], opts["sortDir"], details)
    if just_query:
        return query
    return bundleQueryResults(query, targetRange, simpleResults)

def bindQuery(sess, query, needleType, needle, details):
    # breakpoint()
    if details["model"] == TARGET_MODEL:
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
        pathToModel = getattr(TARGET_MODEL, details["fromTargetModel"])
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
        curr_model = TARGET_MODEL
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

def sortQuery(sess, query, sortBy, sortDir, details):
    sortKey = "sortField" if "sortField" in details else "fields"
    # breakpoint()
    if details["model"] == TARGET_MODEL:
        targetField = createTargetFieldSet(TARGET_MODEL, details[sortKey])
        # targetField = targetField if sortDir == "asc" else targetField.desc()
        if sortDir == "desc":
            return query.order_by(targetField.desc())
        return query.order_by(targetField)
    else:
        # TODO: set it up so that the system can sort by relationships
        return query

def bundleQueryResults(query, targetRange, simpleResults=True):
    totalCount = query.count()
    if targetRange is not None:
        results = query.slice(targetRange[0], targetRange[1]).all()
    else:
        results = query.all()

    if simpleResults:
        results = [formatResult(result) for result in results]

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
