'''
This file is part of Satyrn.
Satyrn is free software: you can redistribute it and/or modify it under 
the terms of the GNU General Public License as published by the Free Software Foundation, 
either version 3 of the License, or (at your option) any later version.
Satyrn is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with Satyrn. 
If not, see <https://www.gnu.org/licenses/>.
'''

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
