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
        # TODO: Consider cases where ent_lst length > 1.
        # rn the hunch is that it shouldnt matter
        # since different relationships dont change
        # the underlying data type and such of the attribute/entity

        # TODO: deeper check
        # types, values, etc
        # date cleaning, etc

        ent = ent_lst[0]
        attr = ent["attributes"][field]

        if attr["type"] == "date":
            clean_func = cleanDate
        elif attr["type"] == "integer":
            clean_func = cleanInt
        elif attr["type"] == "float":
            clean_func = cleanFloat
        else:
            clean_func = lambda x: x

        if filt[2] == "range":
            if attr["type"] not in ["date", "integer", "float"]:
                return None

            if type(filt[1]) != list or len(filt[1]) != 2:
                return None

            filt[1] = [clean_func(val) for val in filt[1]]
            if None in filt[1]:
                return None

        elif filt[2] == "exact":
            filt[1] = clean_func(filt[1])

            if filt[1] == None:
                return None

        elif filt[2] == "contains":
            if attr["type"] != "string":
                return None
            filt[1] = clean_func(filt[1])

            if filt[1] == None:
                return None

        elif filt[2] in ["lessthan", "greaterthan", "lessthan_eq", "greaterthan_eq"]:
            if attr["type"] not in ["date", "integer", "float", "date:year"]:
                return None

            filt[1] = clean_func(filt[1])
            if filt[1] == None:
                return None

        else:
            print("rn no other options allowed")
            return None




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


# NOTE: this will be relationshipless bc its for 2.0
def _createSearchTuple(targetEntity, searchSpace, key, val, tpe="exact"):

    att_dct = searchSpace[None]["attributes"]
    if att_dct[key]["type"]  == "float":
        val = float(val)
    elif att_dct[key]["type"]  == "integer":
        val = int(val)
    return [{"entity": targetEntity,
                    "field": key}, val, "exact"]


def cleanDate(dte):
    return datetime.strptime(dte, '%Y-%m-%d') if dte != "null" else None

def cleanFloat(num):
    try:
        return float(num)
    except ValueError:
        return None

def cleanInt(num):
    try:
        return int(num)
    except ValueError:
        return None



def organizeAnalysis(opts, analysisSpace):
    '''
    vibe:
    0: check if "op", and "relationships" in dct
    1. grab requirements for operation
    2. see if we have all the fields that are required
    3. See if we all the fields are proper types and whatno

    4. if any of the required fields are missing, return indication that
    operation cannot go thru

    PENDING: check relationships?

    NOTE: we might need to leave some leeway for paramters?
    unclear if we wanna do that check here OR if we wanna
    leave it for the frontend
    '''

    new_opts = {}
    # check if op is valid
    if "op" not in opts or opts["op"] not in CLEAN_OPS:
        print("op key missing or not in operation space")
        return {}
    else:
        new_opts["op"] = opts["op"]

    # check if relationships exist
    # TODO: check if relationshisp valid
    new_opts["relationships"] = opts.get("relationships", [])

    op_dct = CLEAN_OPS[opts["op"]]
    for req_field, req_dct in op_dct["required"].items():

        if req_field not in opts:
            print(f"field {req_field} not in opts")
            return {}

        new_opt = _checkAnalysisField(opts[req_field], req_dct, analysisSpace)
        if not new_opt:
            print(f"invalid opts for field {req_field}")
            return {}
        else:
            new_opts[req_field] = new_opt


    for opt_field, opt_dct in op_dct.get("optional", {}).items():
        if opt_field not in opts:
            print(f"field {opt_field} not in opts")
            continue

        if opt_field == "groupBy" or opt_dct["maxDepth"] > 1:


            if type(opts[opt_field]) == dict:
                opts[opt_field] = [opts[opt_field]]
            elif type(opts[opt_field]) != list:
                print(f"wrong type, shouldve been list")
                continue

            if len(opts[opt_field]) > opt_dct["maxDepth"]:
                print(f"too many arguments for groupBy")
                return {}

            for opt in opts[opt_field]:
                new_opt = _checkAnalysisField(opt, opt_dct, analysisSpace)
                if not new_opt:
                    print(f"invalid opts for field {opt_field}")
                    return {}
                else:
                    if opt_field not in new_opts:
                        new_opts[opt_field] = []
                    new_opts[opt_field].append(new_opt)
        else:
            new_opt = _checkAnalysisField(opts[opt_field], opt_dct, analysisSpace)
            if not new_opt:
                print(f"invalid opts for field {opt_field}")
                return {}
            else:
                new_opts[opt_field] = new_opt


    return new_opts


def _checkAnalysisField(opt_dct, req_dct, analysisSpace):
    if type(opt_dct) != dict:
        return {}
    if "entity" not in opt_dct or "field" not in opt_dct:
        return {}
    ent = opt_dct["entity"]
    field = opt_dct["field"]
    ent_lst = [val for key, val in analysisSpace.items() if val["entity"] == ent and field in val["attributes"]]

    if not ent_lst:
        print(f"field {ent}, {field} not in analysis space")
        return {}

    else:
        datatype = ent_lst[0]["attributes"][field]["type"]
        # TODO: check data type validity w/what that req_dct requires
        if datatype not in req_dct["validInputs"]:
            print(f"field {ent},{field} datatype {datatype} does not match required datatype")
            # do not copy value into list, but can continue in process
            return {}

        # parameter check: see if they meet requirements
        params = req_dct.get("parameters")
        # PENDING: how to check if parameters properly done
        # if params:
        #     for param_dct in params:
        #         if datatype in param_dct["inputTypes"]:
        #             # PENDING: How
        #         else:
        #             if param_dct.get("required", False):
        #                 print(f"field {req_field} requires parameter but not given?")

        # else:
        #     # No parameters, can carry on per ush
        #     pass

    return opt_dct


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

    print("getting ring", flush=True)
    print("ring not found", flush=True)

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
