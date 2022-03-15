
from copy import deepcopy
import pandas as pd
from core.api.utils import _name

def comparisonQuery(s_opts, orig_a_opts, targetEntity):

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
    return s_opts, a_opts, targetEntity

def pandasComparison(a_opts, results, group_args, field_names, col_names):
    return {"results": results}, field_names, col_names


def compareUnits(a_opts, field_names, col_names, init_units):
    return {"results": init_units}


dct = { "comparison": {
      "required": {
          "target1": {
            "validInputs": ["string", "boolean", "integer", "float", "id"],
            "fieldType": "target",
            "parameters": [
              {
                "question": "language to be asked goes here",
                "inputTypes": ["boolean", "string"],
                "options": "any",
                "allowMultiple": False # to support multi-part numerators like "New York AND California"
              },
              {
                "question": "language to be asked goes here",
                "inputTypes": ["int", "float"],
                "options": "aggregation",
                "required": False,
                "allowMultiple": False
              },
              {
                "question": "language to be asked goes here",
                "inputTypes": ["id"],
                "options": "aggregation",
                "required": False,
                "allowMultiple": False
              }
            ]
          },
          "target2": {
            "validInputs": ["string", "boolean", "integer", "float", "id"],
            "fieldType": "target",
            "parameters": [
              {
                "question": "language to be asked goes here",
                "inputTypes": ["boolean", "string"],
                "options": "any",
                "allowMultiple": True
              },
              {
                "question": "language to be asked goes here",
                "inputTypes": ["int", "float"],
                "options": "aggregation",
                "required": False,
                "allowMultiple": False
              },
              {
                "question": "language to be asked goes here",
                "inputTypes": ["id"],
                "options": "aggregation",
                "required": False,
                "allowMultiple": False
              }
            ]
          },
          "group": {
            "internalId": "group",
            "fieldType": "group",
            "validInputs": ["id", "boolean"],
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
      # "unitsPrep": compareUnits, # move to a standard method name on PluginClass
      "template": "Comparison between {group}'s {target1} and {target2}",
      "queryPrep": comparisonQuery, # move to a standard method name on PluginClass
      "pandasFunc": pandasComparison, # move to a standard method name on PluginClass
      "type": "complex",
  }
}

# dct = {    
#     "comparison": {
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
#             }
#         },
#         "unitsPrep": compareUnits, 
#         "nicename": "comparison between",
#         "queryPrep": comparisonQuery,
#         "pandasFunc": {
#             "op": pandasComparison,
#         },
#         "type": "complex",
#         "groupingAllowed": {
#             "groupType": ["groupBy", "timeseries"],
#             "numberGroups": 2,
#             "numberRequired": 1
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