import json

from flask import Blueprint, current_app, jsonify, request, send_from_directory
from flask_cors import cross_origin
from flask_security import login_required

# from .analysisSpace import ANALYSIS_MODEL_SPACE as ANALYSIS_SPACE
from .engine import run_analysis
from .seekers import getResults
from .autocomplete import runAutocomplete

from .viewHelpers import CLEAN_OPS, apiKeyCheck, errorGen, organizeFilters, cleanDate, getOrCreateRing, getRing, getRingFromService, convertFilters, organizeFilters2
from .viewHelpers import organizeAnalysis
# # some "local globals"
app = current_app # this is now the same app instance as defined in appBundler.py
api = Blueprint("api", __name__)
# SATCONF = current_app.satConf
# db = SATCONF.db
cache = app.cache

# One cache enabled helper function...
@cache.memoize(timeout=1000)
def cachedAutocomplete(db, theType, searchSpace, opts):
    # TODO: make this work with the new DB setup!
    return json.dumps(runAutocomplete(db, theType, searchSpace, opts))

#
# THE ROUTES
# base route as a pseudo health check
# no login necessary to check this
@api.route("/")
@apiKeyCheck
def base():
    return json.dumps({
        "status": "API is up and running"
    })

@api.route("/rings/", methods=["GET"]) #, "POST"])
@apiKeyCheck
def getAPIInfo():
    return json.dumps({
        # "success": success,
        # "message": msg,
        "rings": [{
            "id": rid,
            "versions": {
                version: {
                    "name": ringInfo.name,
                    "description": ringInfo.description
                }
                for version, ringInfo in app.rings.get(rid).items()
            }
        }
        for rid in app.rings.keys()]
    })


@api.route("/rings/<ringId>/", methods=["GET"])
@apiKeyCheck
def getRingInfo(ringId):
    # THIS IS GOING TO ASSUME THE REQUESTING PROXY
    # IS MANAGING WHETHER THE USER HAS THE RIGHT TO DO THIS OR NOT
    # Also, if no version set, "latest" is implied (see next endpoint for explicitly set version)
    ring, ringExtractor = getOrCreateRing(ringId)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    ringInfo = ringExtractor.generateInfo()
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)

@api.route("/rings/<ringId>/<version>/", methods=["GET"])
@apiKeyCheck
def getRingInfoWithVersion(ringId, version):
    # THIS IS GOING TO ASSUME THE REQUESTING PROXY
    # IS MANAGING WHETHER THE USER HAS THE RIGHT TO DO THIS OR NOT
    ring, ringExtractor = getOrCreateRing(ringId, version=version)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    ringInfo = ringExtractor.generateInfo()
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)

@api.route("/rings/<ringId>/<version>/<targetEntity>/")
@apiKeyCheck
def getEntityInfo(ringId, version, targetEntity):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    ringInfo = ringExtractor.generateInfo(targetEntity)
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)

@api.route("/autocomplete/<ringId>/<version>/<targetEntity>/<theType>/")
@apiKeyCheck
def getAutocompletes(ringId, version, targetEntity, theType):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    limit = request.args.get("limit", 1000)
    opts = {"query": request.args.get("query", None), "limit": limit}
    searchSpace = ringExtractor.getSearchSpace(targetEntity)
    if theType in searchSpace \
      and "autocomplete" in searchSpace[theType] \
      and searchSpace[theType]["autocomplete"]:
        return cachedAutocomplete(ring.db, theType, searchSpace[theType], opts)
    return json.dumps({"success": False, "message": "Unknown autocomplete type"})


@api.route("/results/<ringId>/<version>/<targetEntity>/", methods=["GET","POST"])
@cross_origin(supports_credentials=True)
@apiKeyCheck
def searchDB(ringId, version, targetEntity):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    # takes a list of args that match to top-level keys in SEARCH_SPACE
    # or None and it'll return the full set (in batches of limit)
    # set up some args

    # NOTE ON DATES
    # dates always expect a range, either:
    # [2018-01-01, 2018-01-01] for single day
    # [2018-01-01, 2018-06-15] for everything between two dates
    # [null, 2018-01-01] for everything up to a date (and inverse for after)

    batchSize = int(request.args.get("batchSize", 10))
    page = int(request.args.get("page", 0))
    # bundle search terms
    searchSpace = ringExtractor.getSearchSpace(targetEntity)
    sortables = ringExtractor.getSortables(targetEntity)

    # left in case there is stuff in the request
    opts = organizeFilters(request, searchSpace, targetEntity)
    if not opts:
        opts = request.json if request.json else opts
        if "page" in opts:
            page = int(opts["page"])
        if "batchSize" in opts:
            batchSize = int(opts["batchSize"])
    else:
        # we will use the filters in the url/get rather than in the json
        query = convertFilters(targetEntity, searchSpace, opts)
        opts = {"query": query, "relationships": []}

    opts = organizeFilters2(opts, searchSpace)

    # and manage sorting
    # TODO: move this next line to config
    # TODO2: add judges and other stuff?
    sortBy = request.args.get("sortBy", None)
    sortDir = request.args.get("sortDirection", "desc")
    opts["sortBy"] = sortBy if sortBy in sortables else None
    opts["sortDir"] = sortDir if sortDir in ["asc", "desc"] else "desc"

    # now go hunting
    results = getResults(opts, ring, ringExtractor, targetEntity, page=page, batchSize=batchSize)
    return json.dumps(results, default=str)


# PENDING: Add some check here that analysis opts are valid
# PENDING: Use fieldTypes?
@api.route("/analysis/<ringId>/<version>/<targetEntity>/", methods=["GET","POST"])
@cross_origin(supports_credentials=True)
@apiKeyCheck
def runAnalysis(ringId, version, targetEntity):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    # takes a list of args that match to top-level keys in SEARCH_SPACE (or None)
    # and keys related to analysis with analysisType defining the "frame" (matching a key in analysisSpace.py)
    # The analysis parameters come in via a JSON body thingy

    # then get the analysis stuff:
    # operation = request.args.get("op", None)
    analysisOpts = request.json

    # first, get the search/filter stuff:
    searchSpace = ringExtractor.getSearchSpace(targetEntity)
    searchOpts = organizeFilters(request, searchSpace, targetEntity)
    if searchOpts:
        query = convertFilters(targetEntity, searchSpace, searchOpts)
        analysisOpts["query"] = query
    if "query" not in analysisOpts:
        analysisOpts["query"] = {}

    searchOpts = analysisOpts

    searchOpts = organizeFilters2(searchOpts, searchSpace)

    # analysisOpts = organizeAnalysis(analysisOpts, ringExtractor.getAnalysisSpace(targetEntity))

    if not analysisOpts:
        print("ill formed analysis opts")
        return jsonify({})

    # searchOpts = organizeFilters(request, searchSpace)
    # TODO: write bit of code to obtain from the json the filters and whatnot
    # TODO: rview getseachspace and organize filters to make sure weget the crucial parts of it
    raw_results = run_analysis(s_opts=searchOpts, a_opts=analysisOpts, targetEntity=targetEntity, ring=ring, extractor=ringExtractor)

    results = {
        "length": len(raw_results["results"]),
        "results": raw_results["results"],
        "units": raw_results["units"],
        "counts": raw_results["entity_counts"] if "entity_counts" in raw_results else {},
        "fieldNames": raw_results["field_names"],
    }
    if "score" in raw_results:
        results["score"] = raw_results["score"]
    # this next line is a bit of a hack to deal with un-jsonable things by coercing them
    # to strings without having to write quick managers for every possible type (date, datetime, int64, etc)
    results = json.loads(json.dumps(results, default=str))
    # doing jsonify here manages the mimetype
    return jsonify(results)


@api.route("/result/<id>/")
@apiKeyCheck
def getResultHTML(id):
    sess = db.Session()
    target = sess.query(SATCONF.targetModel).get(id)
    extraStuff = "<script type='application/javascript' src='/static/highlighter.js'></script>"
    extraStuff += "<link href='/static/highlighter.css' rel='stylesheet'>"
    caseHTML = target.get_clean_html() \
                   .replace("</body></html>", extraStuff+"</body></html>")
    return caseHTML

@api.route("/download/<payloadName>/")
@apiKeyCheck
def downloadDocketSet(payloadName):
    # TODO: this is just a demo stub -- in the future, this needs to take a GET param
    # and bundle the results from the db, zip them and return the resulting file
    # maybe also store the file locally for subsequent downloads for a period of time?
    # return json.dumps({"testo": 1})
    return send_from_directory(app.downloadDir, "DocketCollection.zip", as_attachment=True)
