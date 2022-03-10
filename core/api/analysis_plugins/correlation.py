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

    print(orig_a_opts)

    a_opts = deepcopy(orig_a_opts)
    for targ in ["target1", "target2"]:
        if "numerator" in a_opts[targ]:
            a_opts[targ].update({"op": a_opts[targ]["op"] if "op" in a_opts[targ] else "oneHot",
                            "extra": {"numerator": a_opts[targ]["numerator"]}
                        })
        elif "op" not in a_opts[targ]:             
            a_opts[targ].update({
                            "op": "None",
                        })
        else:
            # a_opts[targ].update({
            #                 "extra": {}
            #             })
            pass

    print(a_opts)
    return s_opts, a_opts, targetEntity


def pandasCorrelation(a_opts, results, group_args, field_names, col_names):

    # print(a_opts)
    # print(results)
    # print(group_args)
    # print(field_names)
    # print(col_names)

    df = pd.DataFrame(results, columns=field_names)
    corr_matrix = df.corr("pearson")

    df_unique = df.nunique()

    col_1 = _name(a_opts["target1"]["entity"], a_opts["target1"]["field"], a_opts["target1"]["op"])
    col_2 = _name(a_opts["target2"]["entity"], a_opts["target2"]["field"], a_opts["target2"]["op"])


    if len(results):
        corr_val = corr_matrix[col_1][col_2]
    else:
        corr_val = 0

    # print({"results": results, "score": corr_val})
    # print(field_names)
    # print(col_names)

    return {"results": results, "score": corr_val}, field_names, col_names


def corrUnits(a_opts, field_names, col_names, init_units):
    # print(a_opts)
    # print(field_names)
    # print(col_names)
    # print(init_units)
    # print({"results": init_units, "score": "no units"})
    return {"results": init_units, "score": "no units"}


dct = { "correlation": {
      "required": {
          "target1": {
            "validInputs": ["string", "bool", "int", "float", "average", "count"],
            "fieldType": "target",
            "parameters": [
              {
                "question": "language to be asked goes here",
                "inputTypes": ["bool", "string"],
                "options": "any",
                "allowMultiple": False # to support multi-part numerators like "New York AND California"
              },
              {
                "question": "language to be asked goes here",
                "inputTypes": ["int", "float"],
                "options": "aggregation",
                "required": False,
                "allowMultiple": False
              }
            ]
          },
          "target2": {
            "validInputs": ["string", "bool", "int", "float", "average", "count"],
            "fieldType": "target",
            "parameters": [
              {
                "question": "language to be asked goes here",
                "inputTypes": ["bool", "string"],
                "options": "any",
                "allowMultiple": True
              },
              {
                "question": "language to be asked goes here",
                "inputTypes": ["int", "float"],
                "options": "aggregation",
                "required": False,
                "allowMultiple": False
              }
            ]
          },
          "group": {
            "internalId": "group",
            "fieldType": "group",
            "validInputs": ["id"],
            "parameters": None
          }
      },
      "optional": {
        "groupBy": {
          "allowed": True,
          "maxDepth": 1,
          # "validInputs": ["id", "int", "float"], # optional to override defaults
          # "parameters": [ # optional to override defaults
          #   "inputTypes": ["int", "float"],
          #   "options": ["percentile", "threshold"],
          #   "allowMultiple": False
          # ]
        },
        "timeSeries": {
          "allowed": True,
          "maxDepth": 1
        }
      },
      "unitsPrep": corrUnits, # move to a standard method name on PluginClass
      "template": "Correlation between {group}'s {target1} and {target2}",
      "queryPrep": correlationQuery, # move to a standard method name on PluginClass
      "pandasFunc": pandasCorrelation, # move to a standard method name on PluginClass
      "type": "complex",
  }
}

# dct = {    
#     "correlation": {
#         "fields": {
#             "target": {
#                 "types": ["string", "bool", "int", "float", "average", "count"],
#                 "fieldType": "target",
#                 "extra": {
#                     "numerator": {
#                         "types": ["== target"],
#                         "required": ["target == bool", "target == string"],             
#                     }
#                 }
#             },
#             "target2": {
#                 "types": ["string", "bool", "int", "float", "average", "count"],
#                 "fieldType": "target",
#                 "extra": {
#                     "numerator": {
#                         "types": ["== target"],
#                         "required": ["target == bool", "target == string"],             
#                     }
#                 }
#             },
#             "grouping": {
#                 "types": ["id"],
#                 "fieldType": "group",
#             }
#         },
#         "unitsPrep": corrUnits, 
#         "nicename": "Correlation between",
#         "queryPrep": correlationQuery,
#         "pandasFunc": {
#             "op": pandasCorrelation,
#         },
#         "type": "complex",
#         "groupingAllowed": {
#             "groupType": [],
#             "numberGroups": 0
#         }
#     },

# }


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