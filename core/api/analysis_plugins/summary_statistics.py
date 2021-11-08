
from copy import deepcopy
import pandas as pd
from core.api.utils import _name

def summaryQuery(s_opts, orig_a_opts, targetEntity):

    a_opts = deepcopy(orig_a_opts)
    target_dct = deepcopy(a_opts["target"])
    del a_opts["target"]
    for idx, op in enumerate(["min", "median", "max", "average", "mode"]):
        new_dct = deepcopy(target_dct)
        new_dct["op"] = op
        a_opts["target" + str(idx)] = new_dct

    return s_opts, a_opts, targetEntity

def pandasSummary(a_opts, results, group_args, field_names, col_names):
    return {"results": results}, field_names, col_names


def summaryUnits(a_opts, field_names, col_names, init_units):
    return {"results": init_units}


dct = {    
    "summaryStatistics": {
        "fields": {
            "target": {
                "types": ["string", "bool", "int", "float", "average", "count"],
                "fieldType": "target",
            },
            "target0": {
                "types": ["min"],
                "fieldType": "target",
                "spawned": True
            },
            "target1": {
                "types": ["median"],
                "fieldType": "target",
                "spawned": True
            },
            "target2": {
                "types": ["max"],
                "fieldType": "target",
                "spawned": True
            },
            "target3": {
                "types": ["average"],
                "fieldType": "target",
                "spawned": True
            },
            "target4": {
                "types": ["mode"],
                "fieldType": "target",
                "spawned": True
            },
        },
        "unitsPrep": summaryUnits, 
        "nicename": "Summary statistics of",
        "queryPrep": summaryQuery,
        "pandasFunc": {
            "op": pandasSummary,
        },
        "type": "complex",
        "groupingAllowed": {
            "groupType": ["groupBy", "timeseries"],
            "numberGroups": 2,
            "numberRequired": 0
        }
    },

}


'''
{
    "target": {
        "entity":"Contribution",
        "field": "amount"
    },
    "op": "summaryStatistics",
    "groupBy": [{
        "entity": "Contribution",
        "field": "inState"
    }],
    "rings": ["do-i-just-put-anything-here"],
    "relationships": []
}



{
    "target0": {
        "entity":"Contribution",
        "field": "amount"
        "op": "min"
    },
    "target1": {
        "entity":"Contribution",
        "field": "amount"
        "op": "median"
    },
    "target2": {
        "entity":"Contribution",
        "field": "amount"
        "op": "max"
    },
    "target3": {
        "entity":"Contribution",
        "field": "amount"
        "op": "average"
    },
    "target4": {
        "entity":"Contribution",
        "field": "amount"
        "op": "mode"
    },
    "op": "summaryStatistics",
    "groupBy": [{
        "entity": "Contribution",
        "field": "inState"
    }],
    "rings": ["do-i-just-put-anything-here"],
    "relationships": []
}


'''