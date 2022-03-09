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
def organizeFilters(request, searchSpace, targetEntity):

    opts = {}
    # iterables = {k[1]: val for k, val in searchSpace if k[0] == targetEntity}
    iterables = searchSpace[None]["attributes"]

    for k in iterables.keys():
        setting = request.args.get(k, None)
        if setting:
            if iterables[k]["type"] == "date":
                dateRange = setting.strip('][').split(",")
                opts[k] = [cleanDate(dte) for dte in dateRange]
            elif searchSpace[None]["allowMultiple"]:
                opts[k] = request.args.getlist(k, None)
            else:
                opts[k] = setting
    return opts

# new generic filter prep function
def organizeFilters2(opts, searchSpace):

    query = traverseFilters(opts["query"], searchSpace)
    opts["query"] = query if query else {}

    # TODO: relationship check
    opts["relationships"] = opts["relationships"]

    return opts


def traverseFilters(opts, searchSpace):

    if type(opts) == list:
        # this is just a condition for filtering
        return checkFilter(opts, searchSpace)

    else:
        # This is a dictionary
        if len(opts.keys()) != 1:
            return None

        if "AND" in opts or "OR" in opts:
            key = "AND" if "AND" in opts else "OR"
            flters = [traverseFilters(opt, searchSpace) for opt in opts[key]]
            flters = list(filter(lambda x: x, flters))
            if flters:
                return {key: flters}
            else:
                return None

        elif "NOT" in opts:
            flter = traverseFilters(opts["NOT"], searchSpace)
            if flter:
                return {"NOT": flter}
            else:
                return None

        else:
            return None


# TODO: change this so that it can handle new format: [((ent, field), bigads dict)]
# filter to get the list of entries that have (ent, field) as first in the tuple
# if there, all good
# migth be ok if there ar emore than 1, might not need further checking or whatevs around relationships and stuff
# NOTE: we are assuming that regardless of relationship, some requirements remain the same
# i.e. filters fro the same entity regardless of relationship will format and meet same requirements
def checkFilter(filt, searchSpace):
    if len(filt) != 3:
        return None
    ent_dct, vals, filt_tpe = filt

    if not ("entity" in ent_dct and "field" in ent_dct):
        return None
    ent = ent_dct["entity"]
    field = ent_dct["field"]
    ent_lst = [val for key, val in searchSpace.items() if val["entity"] == ent and field in val["attributes"]]
    if len(ent_lst):
        # TODO: deeper check
        # types, values, etc
        # date cleaning, etc
        return filt
    else:
        return None


# convert filters from 2 to 2.1
def convertFilters(targetEntity, searchSpace, filter_dct):
    query = {"AND": []}
    for key, val in filter_dct.items():
        if type(val) == list:
            if searchSpace[key]["type"] == "date":
                tpl = _createSearchTuple(targetEntity, searchSpace, key, val, "range")
                query["AND"].append(tpl)
            else:
                for v in val:
                    tpl = _createSearchTuple(targetEntity, searchSpace, key, val)
                    query["AND"].append(tpl)
        else:
            tpl = _createSearchTuple(targetEntity, searchSpace, key, val)
            query["AND"].append(tpl)
    return query


# TODO: change this to work with new format: [((ent, field), bigads dict)]
# NOTE: this will be relationshipless bc its for 2.0
def _createSearchTuple(targetEntity, searchSpace, key, val, tpe="exact"):

    # att_dct = [y for x, y in searchSpace if x == (targetEntity, key) and not y["relationships"]][0]
    att_dct = searchSpace[None]["attributes"]
    if att_dct[key]["type"]  == "float":
        val = float(val)
    elif att_dct[key]["type"]  == "integer":
        val = int(val)
    return [{"entity": targetEntity,
                    "field": key}, val, "exact"]


def cleanDate(dte):
    return datetime.strptime(dte, '%Y-%m-%d') if dte != "null" else None


def organizeAnalysis(opts, analysisSpace):
    # TODO: do it
    '''
    vibe:
    0: check if "op", and "relationships" in dct
    1. grab requirements for operation
    2. see if we have all the fields that are required
    3. See if we all the fields are proper types and whatno


    PENDING: check relationships?


    NOTE: we might need to leave some leeway for paramters?
    unclear if we wanna do that check here OR if we wanna
    leave it for the frontend
    '''


#
# RING HELPERS
# to get or create ring as necessary
def getOrCreateRing(ringId, version=None, forceRefresh=False):
    # breakpoint()
    version = int(version) if version else version
    if (ringId not in app.rings) or (version and version not in app.rings.get(ringId, {})) or forceRefresh:
        try:
            getRingFromService(ringId, version)
        except:
            msg = "Ring with id {} ".format(ringId)
            msg += "and version number {} ".format(version) if version is not None else "at any version number "
            msg += "could not be loaded from service. This is likely either because a ring with this ID/version number can't be found or because the asset service is down."
            return {"success": False, "message": msg}, None
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
        request = requests.get(os.path.join(app.uxServiceAPI, "rings", ringId, str(version)), headers=headers)
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
