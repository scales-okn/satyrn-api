
from copy import deepcopy
import pandas as pd
from core.api.utils import _name
from functools import reduce
# PENDING: How to handle units with fields defined here
import numpy as np

def distributionQuery(s_opts, orig_a_opts, targetEntity):


    a_opts = deepcopy(orig_a_opts)
    # a_opts["target"]["extra"] = {}
    return s_opts, a_opts, targetEntity


def pandasDistribution(a_opts, results, group_args, field_names, col_names):

    df = pd.DataFrame(results, columns=field_names)
    ring = a_opts["rings"][0]

    target_arg = _name(a_opts["target"]["entity"], a_opts["target"]["field"], a_opts["target"]["op"])
    group_args.pop()

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

    tuples = [tuple(x) for x in df.to_numpy()]\

    return {"results": tuples}, field_names, col_names


def distributionUnits(a_opts, field_names, col_names, init_units):
    idx = col_names.index("target")
    init_units[idx] = "percentage of " + init_units[idx]

    return {"results": init_units}



dct = {
    "distribution": {
        "fields": {
            "target": {
                "types": ["int", "float", "average", "count"],
                "fieldType": "target"
            },
            "over": {
                "types": ["string", "bool"],
                "fieldType": "group"
            }
        },
        "type": "complex",
        "unitsPrep": distributionUnits,
        "nicename": "Distribution of",
        "queryPrep": distributionQuery,
        "pandasFunc": {
            "op": pandasDistribution,
        },
        "groupingAllowed": {
            "groupType": ["groupBy", "timeseries"],
            "numberGroups": 1,
            "numberRequired": 0
        }
    }
}
