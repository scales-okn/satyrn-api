from sqlalchemy import func

def runAutocomplete(db, theType, config, opts={"query": None}):
    # opts["type"] = config["acType"] if "acType" in config else theType
    # TODO: come back and make this work with multi hop joins
    opts["model"] = config["model"]
    opts["vals"] = config["fields"] if "fields" in config else None
    if not opts["query"] or not opts["vals"]:
        return None
    opts["limit"] = opts["limit"] if "limit" in opts else 1000
    opts["format"] = opts["format"] if "format" in config else ("{} "*len(opts["vals"])).strip()
    sess = db.Session()
    return getDedupedBundle(db, opts, sess)

# a helper for deduping types that have same names
# this helper is not memoized, though the functions that call into should be
def getDedupedBundle(db, opts={"query": None}, sess=None):
    if not sess:
        return None
    # targetModel = getattr(db, opts["type"])
    targetModel = opts["model"]
    # put together the query
    theSet = sess.query(targetModel)
    targetVals = [getattr(targetModel, val) for val in opts["vals"]]
    if opts["query"]:
        hayVals = targetVals[0]
        for tv in targetVals[1:]:
            hayVals = hayVals.concat(tv)
        theSet = theSet.filter(func.lower(hayVals) \
                        .contains(func.lower(opts["query"])))
    theSet = theSet.with_entities(*targetVals).limit(opts["limit"])

    # TODO: deal with the fact that stuff gets GORY when you just blindly join the strings in a result set
    # Need some representation of how to format each result/entity (like judge name formatting, but in a template string format)
    # in the meantime, just return the first item in the list for the sake of keeping the demo path good
    # later, come back and improve this next line with the template string:
    # output = list(set([" ".join(item) for item in theSet.all()]))
    output = list(set([item[0] for item in theSet.all()]))
    output.sort()
    return output
