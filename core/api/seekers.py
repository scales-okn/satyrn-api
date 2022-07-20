from flask import current_app as app
from sqlalchemy import func
from sqlalchemy import and_, or_

from . import utils
from . import sql_func


cache = app.cache
CACHE_TIMEOUT=6000

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


def rawGetResultSet(opts, ring, ringExtractor, targetEntity, targetRange=None, simpleResults=True, just_query=False, sess=None, query=None, make_joins=True):
    db = ring.db
    targetInfo = ringExtractor.resolveEntity(targetEntity)[1]
    targetModel = getattr(db, targetInfo.table)
    searchSpace = ringExtractor.getSearchSpace(targetEntity)
    formatResult = ringExtractor.formatResult
    # takes a dictionary of key->vals that power a set of searchs downstream...
    # also takes a ring, ringExtractor and targetEntity name
    # and a range value to memoize a broader set than current page view
    # returns a dict with two keys: results and totalCount
    joins_todo = []
    if not sess:
        sess = db.Session()
        ##query = sess.query(targetModel).distinct(targetInfo.id[0]).group_by(targetInfo.id[0])
        query = sess.query(targetModel)
        ##PROBLEM FOR LATER
            ##Query and join the 'derived' table for the entity --> multitable entity issue
                ## Don't rejoin again afterwards! 
    if opts["query"]:
        que, joins_todo = makeFilters(query, ringExtractor, db, opts["query"], [])
        query = query.filter(que)

    # DO joins
    if make_joins:
        relationships = opts["relationships"]
        query, joined_tables = utils._do_joins(query, [targetInfo.table], relationships, ringExtractor, targetEntity, db, [],joins_todo)
        query, joined_tables = utils.do_multitable_joins(query, joins_todo, ringExtractor, targetEntity, db, joined_tables, [targetInfo.table])

    # Do prefilters
    # TODO: bring this back?
    # for field in PREFILTERS:
    #     for filt in PREFILTERS[field]:
    #         query = query.filter(filt)

    # query = query.order_by(targetInfo.id[0])
    if "sortBy" in opts and opts["sortBy"] is not None:
        details = searchSpace[opts["sortBy"]]
        query = sortQuery(sess, targetModel, query, opts["sortBy"], opts["sortDir"], details)
    if just_query:
        return query, joins_todo
        
    return bundleQueryResults(query, targetRange, targetEntity, formatResult, simpleResults)

def makeFilters(query, extractor, db, opts, joins_todo):
    # check if just a condition
    if type(opts) == list:
        # this is just a condition for filtering
        query, new_joins = addFilter(query, extractor, db, opts)
        for item in new_joins:
            if item not in joins_todo:
                joins_todo.append(item)
        return query, joins_todo

    else:
        # This is a dictionary, will need to do a boolean
        if len(opts.keys()) != 1:
            print("opts has more than one key or is empty")
            return None, joins_todo

        if "AND" in opts:
            flters = [] 
            for opt in opts["AND"]:
                que, new_joins = makeFilters(query, extractor, db, opt, joins_todo)
                flters.append(que)
            for item in new_joins:
                if item not in joins_todo:
                    joins_todo.append(item)
            return and_(*flters), joins_todo

        elif "OR" in opts:
            flters = [] 
            for opt in opts["OR"]:
                que, new_joins = makeFilters(query, extractor, db, opt, joins_todo)
                flters.append(que)
            for item in new_joins:
                if item not in joins_todo:
                    joins_todo.append(item)
            return or_(*flters), joins_todo

        elif "NOT" in opts:
            flter, new_joins = makeFilters(query, extractor, db, opts["NOT"], joins_todo)
            for item in new_joins:
                if item not in joins_todo:
                    joins_todo.append(item)
            return ~flter, joins_todo

        else:
            print("opts does not have AND, OR, or NOT")
            print(opts)
            return None, joins_todo
    '''
    '''


def addFilter(query, extractor, db, opts):
    dct = opts[0]
    vals = opts[1]
    filter_type = opts[2]
    field, _, joins_todo = utils._get(extractor, dct["entity"], dct["field"], db)
    if filter_type == "exact":
        return field == vals, joins_todo
    elif filter_type == "range":
        return and_(field >= vals[0], field <= vals[1]), joins_todo
    elif filter_type == "contains":
        return func.lower(field).contains(func.lower(vals)), joins_todo
    elif filter_type in ["lessthan", "greaterthan", "lessthan_eq", "greaterthan_eq"]:
        comparator_dict = {
            "lessthan": lambda a,b: a < b,
            "greaterthan": lambda a,b: a > b,
            "lessthan_eq": lambda a,b: a <= b,
            "greaterthan_eq": lambda a,b: a >= b,
        }
        return comparator_dict[filter_type](field, vals), joins_todo
    else:
        print("unacceptable/non-implemented filter type")
        print("technically this hould never be reached bc we checked filters in api")
        return None, joins_todo

    return query, joins_todo


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
