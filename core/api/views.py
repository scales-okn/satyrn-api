from datetime import datetime
from functools import wraps
import json
import os
import secrets

from flask import current_app, Blueprint, request, send_file, send_from_directory
from flask_security import login_required
import requests
from sqlalchemy import func

# from .analysisSpace import ANALYSIS_MODEL_SPACE as ANALYSIS_SPACE
from .engine import run_analysis
from .operations import OPERATION_SPACE
from .seekers import getResults
from .autocomplete import runAutocomplete
from ..compiler import compile_ring
from ..extractors import RingConfigExtractor

# Clean up the globals
CLEAN_OPS = {k: {k1: v1 for k1, v1 in v.items() if type(v1) in [int, float, str, list, dict] and k1 not in ["pandaFunc", "funcDict", "pandasFunc"]} for k, v in OPERATION_SPACE.items()}

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
# RING HELPERS
# to get or create ring as necessary
def getOrCreateRing(ringId, version=None, forceRefresh=False):
    # breakpoint()
    if (ringId not in app.rings) or (version and version not in app.rings.get(ringId, {})) or forceRefresh:
        getRingFromService(ringId, version)
    if not version:
        # get the highest version number available (mirrors behavior of the get)
        versions = sorted(app.rings[ringId].keys())
        version = versions[-1:][0]
    return app.rings[ringId][version], app.ringExtractors[ringId][version]

def getRing(ringId, version=None):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    return ring

def getRingFromService(ringId, version=None):
    # TODO: go get ring config and hydrate and append to app.rings / app.ringExtractors
    headers = {"x-api-key": app.config["UX_SERVICE_API_KEY"]}
    if version:
        request = requests.get(os.path.join(app.uxServiceAPI, "rings", ringId, version), headers=headers)
    else:
        # get the latest...
        request = requests.get(os.path.join(app.uxServiceAPI, "rings", ringId), headers=headers)
    requestJSON = request.json()
    ringConfig = requestJSON["data"]["ring"]
    # breakpoint()
    if type(ringConfig) == str:
        ringConfig = json.loads(ringConfig)
    ring = compile_ring(ringConfig, in_type="json")
    if not ring.id in app.rings:
        app.rings[ring.id] = {}
        app.ringExtractors[ring.id] = {}
    if not version:
        version = ring.version
    app.rings[ring.id][version] = ring
    app.ringExtractors[ring.id][version] = RingConfigExtractor(ring)


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
    ringInfo = ringExtractor.generateInfo()
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)

@api.route("/rings/<ringId>/<version>/", methods=["GET"])
@apiKeyCheck
def getRingInfoWithVersion(ringId, version):
    # THIS IS GOING TO ASSUME THE REQUESTING PROXY
    # IS MANAGING WHETHER THE USER HAS THE RIGHT TO DO THIS OR NOT
    ring, ringExtractor = getOrCreateRing(ringId, version=version)
    ringInfo = ringExtractor.generateInfo()
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)

@api.route("/rings/<ringId>/<version>/<targetEntity>/")
@apiKeyCheck
def getEntityInfo(ringId, version, targetEntity):
    ringInfo = app.ringExtractors[ringId][version].generateInfo(targetEntity)
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)

@cache.memoize(timeout=1000)
def cachedAutocomplete(db, theType, searchSpace, opts):
    # TODO: make this work with the new DB setup!
    return json.dumps(runAutocomplete(db, theType, searchSpace, opts))

@api.route("/autocomplete/<ringId>/<version>/<targetEntity>/<theType>/")
@apiKeyCheck
def getAutocompletes(ringId, version, targetEntity, theType):
    limit = request.args.get("limit", 1000)
    opts = {"query": request.args.get("query", None), "limit": limit}
    searchSpace = app.ringExtractors[ringId][version].getSearchSpace(targetEntity)

    if theType in searchSpace \
      and "autocomplete" in searchSpace[theType] \
      and searchSpace[theType]["autocomplete"]:
        return cachedAutocomplete(app.rings[ringId][version].db, theType, searchSpace[theType], opts)
    return json.dumps({"success": False, "message": "Unknown autocomplete type"})

@api.route("/results/<ringId>/<version>/<targetEntity>/")
@apiKeyCheck
def searchDB(ringId, version, targetEntity):
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
    searchSpace = app.ringExtractors[ringId][version].getSearchSpace(targetEntity)
    sortables = app.ringExtractors[ringId][version].getSortables(targetEntity)
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

@api.route("/analysis/<ringId>/<version>/<targetEntity>/")
@apiKeyCheck
def runAnalysis(ringId, version, targetEntity):
    # takes a list of args that match to top-level keys in SEARCH_SPACE (or None)
    # and keys related to analysis with analysisType defining the "frame" (matching a key in analysisSpace.py)
    # The analysis parameters come in via a JSON body thingy

    # then get the analysis stuff:
    operation = request.args.get("op", None)
    analysisOpts = request.json

    # first, get the search/filter stuff:

    searchSpace = app.ringExtractors[ringId][version].getSearchSpace(targetEntity)
    searchOpts = organizeFilters(request, searchSpace)
    raw_results = run_analysis(s_opts=searchOpts, a_opts=analysisOpts, targetEntity=targetEntity)

    results = {
        "length": len(raw_results["results"]),
        "results": raw_results["results"],
        "units": raw_results["units"],
        "counts": raw_results["entity_counts"] if "entity_counts" in raw_results else {}
    }
    if "score" in raw_results:
        results["score"] = raw_results["score"]
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
