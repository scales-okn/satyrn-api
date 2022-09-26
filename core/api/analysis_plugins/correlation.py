'''
Pending correlation development:
- questions like where there are different filters on each of the calculations
    e.g. correlation groupby committee
        # of contributions given by people in alaska
        amount of money raised overall
'''

from copy import deepcopy
import pandas as pd
from core.api.utils import _name

def correlationQuery(s_opts, orig_a_opts, targetEntity):

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
            pass

    return s_opts, a_opts, targetEntity


def pandasCorrelation(a_opts, results, group_args, field_names, col_names):

    df = pd.DataFrame(results, columns=field_names)
    corr_matrix = df.corr("pearson")

    df_unique = df.nunique()

    col_1 = _name(a_opts["target1"]["entity"], a_opts["target1"]["field"], a_opts["target1"]["op"])
    col_2 = _name(a_opts["target2"]["entity"], a_opts["target2"]["field"], a_opts["target2"]["op"])

    if len(results):
        corr_val = corr_matrix[col_1][col_2]
    else:
        corr_val = 0

    return {"results": results, "score": corr_val}, field_names, col_names


def corrUnits(a_opts, field_names, col_names, init_units):
    return {"results": init_units, "score": "no units"}


dct = { "correlation": {
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
                "inputTypes": ["integer", "float"],
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
      "unitsPrep": corrUnits, # move to a standard method name on PluginClass
      "template": "Correlation between {group}'s {target1} and {target2}",
      "queryPrep": correlationQuery, # move to a standard method name on PluginClass
      "pandasFunc": pandasCorrelation, # move to a standard method name on PluginClass
      "type": "complex",
  }
}
