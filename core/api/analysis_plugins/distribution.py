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
from functools import reduce
# PENDING: How to handle units with fields defined here
import numpy as np

def distributionQuery(s_opts, orig_a_opts, targetEntity):

    a_opts = deepcopy(orig_a_opts)
    return s_opts, a_opts, targetEntity


def pandasDistribution(a_opts, results, group_args, field_names, col_names):

    df = pd.DataFrame(results, columns=field_names)
    target_arg = _name(a_opts["target"]["entity"], a_opts["target"]["field"], a_opts["target"]["op"])

    # remove the "over" attributes from the group_args
    group_args = [group for group in group_args if not col_names[field_names.index(group)].startswith("over") ]

    if group_args:
        counts = df.groupby(group_args)[target_arg].sum()
        for value in counts.index:
            if type(value) != list:
                conditions = [(df[group_args[0]] == value)]
            else:
                conditions = [(df[arg] == v) for v,arg in zip(value, group_args)]
            condition = reduce(np.logical_and, conditions)
            df.loc[condition, target_arg] = df.loc[condition, target_arg] / df.loc[condition, target_arg].sum()
            
    else:
        df[target_arg] = df[target_arg] / df[target_arg].sum()

    tuples = [tuple(x) for x in df.to_numpy()]

    return {"results": tuples}, field_names, col_names


def distributionUnits(a_opts, field_names, col_names, init_units):
    idx = col_names.index("target")
    init_units[idx] = ["percentage of " + init_units[idx][0], "percentages of " + init_units[idx][0]]

    return {"results": init_units}

dct = { "distribution": {
      "required": {
          "target": {
            "validInputs": ["integer", "float", "average", "count"],
            "fieldType": "target",
            "parameters": [
              {
                "question": "language to be asked goes here",
                "inputTypes": ["integer", "float"],
                "options": "aggregation",
                "required": False,
                "allowMultiple": False
              }
            ]
          },
          "over": {
            "fieldType": "group",
            "validInputs": ["id", "boolean", "string"],
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
      "unitsPrep": distributionUnits, # move to a standard method name on PluginClass
      "template": "Distribution of {target} over {over}",
      "queryPrep": distributionQuery, # move to a standard method name on PluginClass
      "pandasFunc": pandasDistribution, # move to a standard method name on PluginClass
      "type": "complex",
  }
}
