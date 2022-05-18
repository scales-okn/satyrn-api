
from copy import deepcopy
import pandas as pd
from core.api.utils import _name

def summaryQuery(s_opts, orig_a_opts, targetEntity):

    a_opts = deepcopy(orig_a_opts)
    target_dct = deepcopy(a_opts["target"])
    del a_opts["target"]
    for idx, op in enumerate(["min", "median", "max", "average"]):
        new_dct = deepcopy(target_dct)
        new_dct["op"] = op
        a_opts["target" + str(idx)] = new_dct

    return s_opts, a_opts, targetEntity

def pandasSummary(a_opts, results, group_args, field_names, col_names):
    return {"results": results}, field_names, col_names


def summaryUnits(a_opts, field_names, col_names, init_units):
    return {"results": init_units}



dct = { "summaryStatistics": {
      "required": {
          "target": {
            "validInputs": ["integer", "float"],
            "fieldType": "target",
            "parameters": [
            ]
          },
      },
      "optional": {
        "groupBy": {
          "allowed": True,
          "maxDepth": 2,
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
      "spawned": {
        "target0": {
            "spawnOf": "target",
            "fieldType": "target",
        },
        "target1": {
            "spawnOf": "target",
            "fieldType": "target",
        },
        "target2": {
            "spawnOf": "target",
            "fieldType": "target",
        },
        "target3": {
            "spawnOf": "target",
            "fieldType": "target",
        }     
      },
      # "unitsPrep": compareUnits, # move to a standard method name on PluginClass
      "template": "Summary Statistics of {target}",
      "queryPrep": summaryQuery, # move to a standard method name on PluginClass
      "pandasFunc": pandasSummary, # move to a standard method name on PluginClass
      "type": "complex",
  }
}

