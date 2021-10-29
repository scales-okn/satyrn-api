
from sqlalchemy import func
from sqlalchemy import distinct
from sqlalchemy.sql.expression import case

import pandas as pd
from copy import deepcopy
from .utils import _name


import numpy as np

def onehot_processing(model_field, pos_values):
    # print(model_field)
    # print(pos_values)
    '''
    - Add parameter that indicates that we are gonna be doing some range stuff (or decide here)
    - Add the if case for that
    - Change how this is called in whenever onehot processing is called (if needed)
    '''
    def equal_lambd(val): return lambda x: x == val
    my_cases = [(equal_lambd(val), 1) for val in pos_values]

    case_list = [(k(model_field), v) for k, v in my_cases]
    case_ex = case(case_list, else_=0)
    return case_ex

def renamer(agg_func,desired_name):
    def return_func(x):
        return agg_func(x)
    return_func.__name__ = desired_name
    return return_func


def pandasSum(df, col, group_cols):
    # df = df.groupby(group_cols)[col].sum() if len(group_cols) else df
    # df[col] = df[col].sum()
    # return df
    return df.groupby(group_cols, as_index=False).sum() if len(group_cols) else df.sum()

def pandasMin(df, col, group_cols):
    return df.groupby(group_cols, as_index=False).min() if len(group_cols) else df.min()

def pandasMax(df, col, group_cols):
    return df.groupby(group_cols, as_index=False).max() if len(group_cols) else df.max()

def pandasAvg(df, col, group_cols):
    return df.groupby(group_cols, as_index=False).mean() if len(group_cols) else df.mean()

def pandasCount(df, col, group_cols):
    return df.groupby(group_cols, as_index=False)[col].nunique()

def pandasAvgCount(df, col, per, group_cols=[]):

    grouped_df = df.groupby(per)

    grouping = False
    if len(group_cols):
        grouping = True
    group_cols.append(per)


    # Count per group/perfield
    df = df.groupby(group_cols).agg(
            col=(col, "nunique"),
        )

    if grouping:
        group_cols.pop()
        df = df.groupby(group_cols)["col"]

    df = df.mean()

    return df

def pandasAvgSum(df, col, per, group_cols=[]):

    grouping = False
    if len(group_cols):
        grouping = True
    group_cols.append(per)

    # Count per group/perfield
    df = df.groupby(group_cols).agg(
            col=(col, "sum"),
        )

    if grouping:
        group_cols.pop()
        df = df.groupby(group_cols)["col"]

    df = df.mean()

    return df

def pandasPercentage(df, col, numerator, group_cols=[]):
    return


def pandasOneHot(df, col, numerator):
    df[col] = df[col].apply(lambda x: 1 if x == numerator else 0)
    return df


def base_query_prep(s_opts, orig_a_opts, targetEntity):

    a_opts = deepcopy(orig_a_opts)
    a_opts["target"].update({"op": a_opts["op"],
                    "extra": {"numerator": a_opts["numerator"]} if "numerator" in a_opts else {}
                })
    return s_opts, a_opts, targetEntity


OPERATION_SPACE = {
    "average": {
        "fields": {
            "target": {
                "types": ["float", "int"]
            }
        },
        "units": "unchanged", 
        "nicename": "Average",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra: func.avg(field),
        },
        "pandasFunc": {
            "op": pandasAvg
        },
        "type": "simple"
    },
    "count": {
        "fields": {
            "target": {
                "types": ["id"]
            }
        },
        "units": "unchanged", 
        "nicename": "Count of",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra: func.count(distinct(field)),
            # "processing": distinct
        },
        "pandasFunc": {
            "op": pandasCount
        },
        "type": "simple"
    },
    "sum": {
        "fields": {
            "target": {
                "types": ["float", "int"]
            }
        },
        "units": "unchanged", 
        "nicename": "Total",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra: func.sum(field),
        },
        "pandasFunc": {
            "op": pandasSum
        },
        "type": "simple"
    },
    "min": {
        "fields": {
            "target": {
                "types": ["float", "int"]
            }
        },
        "units": "unchanged", 
        "nicename": "Minimum",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra: func.min(field),
        },
        "pandasFunc": {
            "op": pandasMin
        },
        "type": "simple"
    },
    "max": {
        "fields": {
            "target": {
                "types": ["float", "int"]
            }
        },
        "units": "unchanged", 
        "nicename": "Maximum",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra: func.max(field),
        },
        "pandasFunc": {
            "op": pandasMax
        },
        "type": "simple"
    },
    "mode": {
        "fields": {
            "target": {
                "types": ["float", "int", "string", "bool"]
            }
        },
        "units": "unchanged",
        "nicename": "Mode",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra: func.mode().within_group(field.asc()),
        },
        "type": "simple"
    },
    "median": {
        "fields": {
            "target": {
                "types": ["float", "int"]
            },
        },
        "units": "unchanged", 
        "nicename": "Median",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra: func.percentile_disc(0.5).within_group(field.asc()),
        },
        "type": "simple"
    },

    "averageCount": {
        "fields": {
            "target": {
                "types": ["id"]
            },
            "per": {
                "types": ["id"]
            }
        },
        "units": "target/per", 
        "nicename": "Average Count of",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra: func.count(distinct(field)),
            "outerOp": lambda field, extra: func.avg(field),
        },
        "pandasFunc": {
            "op": pandasAvgCount,
        },
        "type": "recursive"
    },

    "averageSum": {
        "fields": {
            "target": {
                "types": ["float", "int"]
            },
            "per": {
                "types": ["id"]
            }
        },
        "units": "target/per", 
        "nicename": "Average Total of",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra: func.sum(field),
            "outerOp": lambda field, avg: func.sum(field),
        },
        "pandasFunc": {
            "op": pandasAvgSum,
        },
        "type": "recursive"
    },

    "percentage": {
        "fields": {
            "target": {
                "types": ["string", "bool"]
            },
            "numerator": {
                "types": ["== target"]
            }
        },
        "units": "percentage", 
        "nicename": "Percentage of",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra:  func.avg(onehot_processing(field, extra["numerator"])),
        },
        "pandasFunc": {
            "op": pandasPercentage,
        },
        "type": "simple"
    },

    "oneHot": {
        "fields": {
            "target": {
                "types": ["string", "bool"]
            },
            "numerator": {
                "types": ["== target"]
            }
        },
        "units": "none", 
        # "nicename": "Percentage of",
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra:  onehot_processing(field, extra["numerator"]),
            # "processing": percentage_processing
        },
        "pandasFunc": {
            "op": pandasOneHot,
        },
        "type": "simple"
    },
    "None": {
        "fields": {
            "target": {
                "types": ["string", "bool"]
            },
        },
        "queryPrep": base_query_prep,
        "funcDict": {
            "op": lambda field, extra: field,
        },
        "units": "unchanged",
        "type": "simple"
    },
    "summaryStatistics": {
        "fields": {
            "target": {
                "types": ["int, float"]
            },
        },
        "units": "unchanged", 
        "nicename": "Summary Statistics",
        "funcDict": {
            "summaryTableFunction": "fdjkfdjkjd",
        }
    },

}



import importlib.util

import os
directory = os.path.join(os.getcwd(), os.path.join("core","api","analysis_plugins"))
for filename in os.listdir(directory):
    if filename != "__init__.py" and filename.endswith(".py"):
        name = ".".join(["core", "api", "analysis_plugins", filename[:-3]])
        mod = importlib.import_module(name)
        op = getattr(mod, "dct")
        OPERATION_SPACE.update(op)
