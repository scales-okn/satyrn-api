from datetime import datetime
from functools import wraps
import json
import os

from flask import current_app, Blueprint, request
import requests

from .operations import OPERATION_SPACE
from ..compiler import compile_ring
from ..extractors import RingConfigExtractor

app = current_app # this is now the same app instance as defined in appBundler.py

# Clean up the globals
CLEAN_OPS = {k: {k1: v1 for k1, v1 in v.items() if type(v1) in [int, float, str, list, dict] and k1 not in ["pandaFunc", "funcDict", "pandasFunc"]} for k, v in OPERATION_SPACE.items()}

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

def errorGen(msg):
    return json.dumps({
        "success": False,
        "message": str(msg)
    })

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
    version = int(version) if version else version
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
