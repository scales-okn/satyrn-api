# PEnding about correlation
'''
TODO
questions like where there are different filters on each of the calculations
correlation groupby committee
    # of contributions given by people in alaska
    amount of money raised overall
'''

from copy import deepcopy
import pandas as pd
from core.api.utils import _name

def correlationQuery(s_opts, orig_a_opts, targetEntity):

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
                            "extra": {}
                        })
        else:
            a_opts[targ].update({
                            "extra": {}
                        })
    return s_opts, a_opts, targetEntity


def pandasCorrelation(a_opts, results, group_args, field_names, col_names):

    df = pd.DataFrame(results, columns=field_names)
    corr_matrix = df.corr("pearson")
    ring = a_opts["rings"][0]

    df_unique = df.nunique()

    col_1 = _name(ring, a_opts["target"]["entity"], a_opts["target"]["field"], a_opts["target"]["op"])
    col_2 = _name(ring, a_opts["target2"]["entity"], a_opts["target2"]["field"], a_opts["target2"]["op"])

    corr_val = corr_matrix[col_1][col_2]

    return {"results": results, "score": corr_val}, field_names, col_names


def corrUnits(a_opts, field_names, col_names, init_units):
    return {"results": init_units, "score": "no units"}

dct = {    
    "correlation": {
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
            },
            "grouping": {
                "types": ["id"],
                "fieldType": "group",
            }
        },
        "unitsPrep": corrUnits, 
        "nicename": "Correlation between",
        "queryPrep": correlationQuery,
        "pandasFunc": {
            "op": pandasCorrelation,
        },
        "type": "complex",
        "groupingAllowed": {
            "groupType": [],
            "numberGroups": 0
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
    "op": "correlation",
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
    'op': 'correlation', 
    'groupBy': [{'entity': 'Contribution', 'field': 'id'}], 
    'rings': ['do-i-just-put-anything-here'], 
    'relationships': []
}


'''