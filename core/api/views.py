from datetime import datetime
from functools import wraps
import json
import secrets

from flask import current_app, Blueprint, request, send_file, send_from_directory
from flask_security import login_required
from sqlalchemy import func

# from .analysisSpace import ANALYSIS_MODEL_SPACE as ANALYSIS_SPACE
from .engine import AnalyticsEngine
from .operations import OPERATION_SPACE
from .seekers import getResults
from .autocomplete import runAutocomplete

# Clean up the globals
CLEAN_OPS = {k: {k1: v1 for k1, v1 in v.items() if type(v1) in [int, float, str, list, dict]} for k, v in OPERATION_SPACE.items()}


# from the satyrn configs...
# SEARCH_SPACE = current_app.satConf.searchSpace
# ANALYSIS_SPACE = current_app.satConf.analysisSpace

# static globals for /info/ endpoint
# COLUMNS_INFO = current_app.satConf.columns
# SORT_INFO = current_app.satConf.defaultSort
# SORTABLES = [col["key"] for col in COLUMNS_INFO if col["sortable"] is True]

# a decorator for checking API keys
# API key set flatfootedly via env in appBundler.py for now
# requires that every call to the API has a get param of key=(apikey) appended to it
# basic implementation -- most use cases will require this is updated to pass via request header
def apiKeyCheck(innerfunc):
    @wraps(innerfunc)
    def decfunc(*args, **kwargs):
        if "ENV" in app.config and app.config["ENV"] in ["development", "dev"]:
            # we can bypass when running locally for ease of dev
            pass
        elif not request.headers.get("x-api-key"):
            return errorGen("API key required")
        elif request.headers.get("x-api-key") != app.config["API_KEY"]:
            return errorGen("Incorrect API key")
        return innerfunc(*args, **kwargs)
    return decfunc


# a simple error messenger for standardized updates
# augment as necessary
def errorGen(msg):
    return json.dumps({
        "success": False,
        "message": str(msg)
    })

# FIELD_UNITS = {k: v["unit"] for k, v in current_app.satConf.analysisSpace.items() if "unit" in v}
#
# # some "local globals"
app = current_app # this is now the same app instance as defined in appBundler.py
api = Blueprint("api", __name__)
# SATCONF = current_app.satConf
# db = SATCONF.db
cache = app.cache

# a generic filter-prep function
def organizeFilters(request, searchSpace):
    opts = {}
    for k in searchSpace.keys():
        setting = request.args.get(k, None)
        if setting:
            if searchSpace[k]["type"] == "date":
                dateRange = setting.strip('][').split(",")
                opts[k] = [cleanDate(dte) for dte in dateRange]
            elif searchSpace[k]["allowMultiple"]:
                opts[k] = request.args.getlist(k, None)
            else:
                opts[k] = setting
    return opts

def cleanDate(dte):
    return datetime.strptime(dte, '%Y-%m-%d') if dte != "null" else None


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

@api.route("/info/")
@apiKeyCheck
def getAPIInfo():
    return json.dumps({
        "rings": {rr.name: rr.id for rr in app.rings.values()}
    })

@api.route("/info/<ringId>/")
@apiKeyCheck
def getRingInfo(ringId):
    ringInfo = app.ringExtractors[ringId].generateInfo()
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)

@api.route("/info/<ringId>/<targetEntity>/")
@apiKeyCheck
def getEntityInfo(ringId, targetEntity):
    ringInfo = app.ringExtractors[ringId].generateInfo(targetEntity)
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)

@cache.memoize(timeout=1000)
def cachedAutocomplete(db, theType, searchSpace, opts):
    # TODO: make this work with the new DB setup!
    return json.dumps(runAutocomplete(db, theType, searchSpace, opts))

@api.route("/autocomplete/<ringId>/<targetEntity>/<theType>/")
@apiKeyCheck
def getAutocompletes(ringId, targetEntity, theType):
    limit = request.args.get("limit", 1000)
    opts = {"query": request.args.get("query", None), "limit": limit}
    searchSpace = app.ringExtractors[ringId].getSearchSpace(targetEntity)

    if theType in searchSpace \
      and "autocomplete" in searchSpace[theType] \
      and searchSpace[theType]["autocomplete"]:
        return cachedAutocomplete(app.rings[ringId].db, theType, searchSpace[theType], opts)
    return json.dumps({"success": False, "message": "Unknown autocomplete type"})

@api.route("/results/<ringId>/<targetEntity>/")
@apiKeyCheck
def searchDB(ringId, targetEntity):
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
    searchSpace = app.ringExtractors[ringId].getSearchSpace(targetEntity)
    sortables = app.ringExtractors[ringId].getSortables(targetEntity)
    opts = organizeFilters(request, searchSpace)
    # and manage sorting
    # TODO: move this next line to config
    # TODO2: add judges and other stuff?
    sortBy = request.args.get("sortBy", None)
    sortDir = request.args.get("sortDirection", "desc")
    opts["sortBy"] = sortBy if sortBy in sortables else None
    opts["sortDir"] = sortDir if sortDir in ["asc", "desc"] else "desc"
    # now go hunting
    results = getResults(opts, ringId, targetEntity, page=page, batchSize=batchSize)
    return json.dumps(results, default=str)

@api.route("/analysis/")
@apiKeyCheck
def runAnalysis():
    # takes a list of args that match to top-level keys in SEARCH_SPACE (or None)
    # and keys related to analysis with analysisType defining the "frame" (matching a key in analysisSpace.py)

    # this sort of url works: /api/analysis/?operation=count&groupBy=judge&targetField=case&timeSeries=year

    # first, get the search/filter stuff:
    searchOpts = organizeFilters(request)

    # then get the analysis stuff:
    analysisOpts = {}
    operation = request.args.get("operation", None)
    if operation in OPERATION_SPACE.keys():
        analysisOpts["operation"] = operation
    for entry in ["groupBy", "targetField", "perField", "timeSeries"]:
        entryVal = request.args.get(entry, None)
        if entryVal in ANALYSIS_SPACE.keys():
            analysisOpts[entry] = entryVal

    if analysisOpts["operation"] == "percentage":
        # PATCH: This is a non sustainable solution for percentage operation
        if analysisOpts["targetField"] == "feeWaiver":
            analysisOpts["numeratorField"] = ["grant", "term"]

        elif analysisOpts["targetField"] == "proSe":
            analysisOpts["numeratorField"] = [True]

    ae = AnalyticsEngine(searchOpts=searchOpts, analysisOpts=analysisOpts)
    results = ae.run()

    # TODO PATCH: This is a non sustainable solution for percentage operation
    if analysisOpts["operation"] == "percentage":
        for idx, x in enumerate(results["results"]):
            print(x)
            # results["results"][idx][-1] *= 10
    results = {
        "length": len(results["results"]),
        "results": results["results"],
        "units": results["units"],
        "counts": results["counts"] if "counts" in results else []
    }
    return json.dumps(results, default=str)

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
