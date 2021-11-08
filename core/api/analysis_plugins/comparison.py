
from copy import deepcopy
import pandas as pd
from core.api.utils import _name

def comparisonQuery(s_opts, orig_a_opts, targetEntity):

    a_opts = deepcopy(orig_a_opts)
    for targ, num in zip(["target", "target2"], ["numerator", "numerator2"]):
        num_name = "numerator"
        if num in a_opts:
            a_opts[targ].update({"op": a_opts[targ]["op"] if "op" in a_opts[targ] else "oneHot",
                            "extra": {num_name: a_opts[num]}
                        })
            #  del a_opts[num] 
        elif "op" not in a_opts[targ]:             
            a_opts[targ].update({
                            "op": "None",
                        })
        else:
            # a_opts[targ].update({
            #                 "extra": {}
            #             })
            pass
    return s_opts, a_opts, targetEntity

def pandasComparison(a_opts, results, group_args, field_names, col_names):
    return {"results": results}, field_names, col_names


def compareUnits(a_opts, field_names, col_names, init_units):
    return {"results": init_units}


dct = {    
    "comparison": {
        "fields": {
            "target": {
                "types": ["string", "bool", "int", "float", "average", "count"],
                "fieldType": "target",
            },
            "target2": {
                "types": ["string", "bool", "int", "float", "average", "count"],
                "fieldType": "target",
            },
            "numerator": {
                "types": ["== target"],
                "required": ["target == bool", "target == string"],
                "fieldType": "extra",
            },
            "numerator2": {
                "types": ["== target"],
                "required": ["target == bool", "target == string"],
                "fieldType": "extra",
            }
        },
        "unitsPrep": compareUnits, 
        "nicename": "comparison between",
        "queryPrep": comparisonQuery,
        "pandasFunc": {
            "op": pandasComparison,
        },
        "type": "complex",
        "groupingAllowed": {
            "groupType": ["groupBy", "timeseries"],
            "numberGroups": 2,
            "numberRequired": 1
        }
    },

}


'''
{
    "target": {
        "entity":"Contribution",
        "field": "amount"
    },
    "target2": {
        "entity":"Contribution",
        "field": "inState"
    },
    "numerator2": [true],
    "op": "comparison",
    "groupBy": [{
        "entity": "Contribution",
        "field": "id"
    }],
    "rings": ["do-i-just-put-anything-here"],
    "relationships": []
}


{
    'target': 
        {
            'entity': 'Contribution', 
            'field': 'amount', 
            'op': 'None', 
            'extra': {}
        }, 
    'target2': 
        {
            'entity': 'Contribution', 
            'field': 'inState', 
            'op': 'oneHot', 
            'extra': {
                'numerator2': [True], 
            }
        },
    'numerator2': [True], 
    'op': 'comparison', 
    'groupBy': [{'entity': 'Contribution', 'field': 'id'}], 
    'rings': ['do-i-just-put-anything-here'], 
    'relationships': []
}
'''