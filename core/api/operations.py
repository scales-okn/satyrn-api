
from sqlalchemy import func
from sqlalchemy import distinct
from sqlalchemy.sql.expression import case

import pandas as pd
from copy import deepcopy
from .sql_func import sql_median

import numpy as np

def onehot_processing(model_field, pos_values):
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


def base_query_prep(s_opts, orig_a_opts, targetEntity):

    a_opts = deepcopy(orig_a_opts)
    a_opts["target"].update({"op": a_opts["op"],
                    "extra": {"numerator": a_opts["numerator"]} if "numerator" in a_opts["target"] else {}
                })
    
    return s_opts, a_opts, targetEntity


OPERATION_SPACE = {
    "average": {
        "required": {
            "target": {
                "validInputs": ["integer", "float"],
                "fieldType": "target",
            },
        },

        "funcDict": {
            "op": lambda field, dbtype, extra: func.avg(field),
        },
        "template": "Average of {target}",
        "units": "unchanged",
        "type": "simple",
    },
    "count": {
        "required": {
            "target": {
                "validInputs": ["id"],
                "fieldType": "target",
            },
        },
        "funcDict": {
            "op": lambda field, dbtype, extra: func.count(distinct(field)),
        },
        "template": "Count of unique {target}",
        "units": "unchanged",
        "type": "simple",
    },
    "sum": {
        "required": {
            "target": {
                "validInputs": ["integer", "float"],
                "fieldType": "target",
            },
        },

        "funcDict": {
            "op": lambda field, dbtype, extra: func.sum(field),
        },
        "template": "Total {target}",
        "units": "unchanged",
        "type": "simple",
    },
    "min": {
        "required": {
            "target": {
                "validInputs": ["integer", "float"],
                "fieldType": "target",
            },
        },

        "funcDict": {
            "op": lambda field, dbtype, extra: func.min(field),
        },
        "template": "Min of {target}",
        "units": "unchanged",
        "type": "simple",
    },
    "max": {
        "required": {
            "target": {
                "validInputs": ["integer", "float"],
                "fieldType": "target",
            },
        },

        "funcDict": {
            "op": lambda field, dbtype, extra: func.max(field),
        },
        "template": "Max of {target}",
        "units": "unchanged",
        "type": "simple",
    },
    # "mode": {
    #     "required": {
    #         "target": {
    #             "validInputs": ["int", "float"],
    #             "fieldType": "target",
    #         },
    #     },
    #     "funcDict": {
    #         "op": lambda field, dbtype, extra: func.mode().within_group(field.asc()),
    #     },
    #     "template": "Mode of {target}",
    #     "units": "unchanged"
    #     "type": "simple",
    # },

    "median": {
        "required": {
            "target": {
                "validInputs": ["integer", "float"],
                "fieldType": "target",
            },
        },
        "funcDict": {
            "op": lambda field, dbtype, extra: sql_median(field, dbtype),
        },
        "template": "Median of {target}",
        "units": "unchanged",
        "type": "simple",
    },

    "averageCount": {
        "required": {
            "target": {
                "validInputs": ["id"],
                "fieldType": "target",
            },
            "per": {
                "validInputs": ["id"],
                "fieldType": "group",
            }
        },
        "funcDict": {
            "op": lambda field, dbtype, extra: func.count(distinct(field)),
            "outerOp": lambda field, dbtype, extra: func.avg(field),
        },
        "template": "Average Count of {target} per {per}",
        "units": "target/per",
        "type": "recursive",
    },

    "averageSum": {
        "required": {
            "target": {
                "validInputs": ["float", "integer"],
                "fieldType": "target",
            },
            "per": {
                "validInputs": ["id"],
                "fieldType": "group",
            }
        },
        "funcDict": {
            "op": lambda field, dbtype, extra: func.sum(field),
            "outerOp": lambda field, dbtype, extra: func.avg(field),
        },
        "template": "Average Sum of {target} per {per}",
        "units": "target/per",
        "type": "recursive",
    },


    "percentage": {
        "required": {
            "target": {
                "validInputs": ["string", "boolean"],
                "fieldType": "target",
                "parameters": [
                  {
                    "question": "language to be asked goes here",
                    "inputTypes": ["boolean", "string"],
                    "options": "any",
                    "allowMultiple": True
                  },
                ]
            },
        },
        "funcDict": {
            "op": lambda field, dbtype, extra:  func.avg(onehot_processing(field, extra["numerator"])),
        },
        "template": "Percentage of {target}",
        "units": "percentage",
        "type": "simple",
    },

    "oneHot": {
        "required": {
            "target": {
                "validInputs": ["string", "boolean"],
                "fieldType": "target",
                "parameters": [
                  {
                    "question": "language to be asked goes here",
                    "inputTypes": ["boolean", "string"],
                    "options": "any",
                    "allowMultiple": True
                  },
                ]
            },
        },
        "funcDict": {
            "op": lambda field, dbtype, extra:  onehot_processing(field, extra["numerator"]),
        },
        # "template": "OneHot of {target}",
        "units": "none",
        "type": "simple",
        "statementManager": False
    },

    "None": {
        "required": {
            "target": {
                "validInputs": ["integer", "float", "boolean", "string"],
                "fieldType": "target",
            },
        },
        "funcDict": {
            "op": lambda field, dbtype, extra: field,
        },
        # "template": "{target}",
        "units": "unchanged",
        "type": "simple",
        "statementManager": False
    },

}

for key in OPERATION_SPACE.keys():
    OPERATION_SPACE[key]["queryPrep"] = base_query_prep
    OPERATION_SPACE[key]["optional"] = {
        "groupBy": {
            "allowed": True,
            "maxDepth": 2,
            "validInputs": ["id", "integer", "float", "string", "boolean"],
            "parameters": [{ # optional to override defaults
                "inputTypes": ["integer", "float"],
                "options": ["percentile", "threshold"],
                "allowMultiple": False
            }]
        },
        "timeSeries": {
            "allowed": True,
            "maxDepth": 1,
            "validInputs": ["date", "datetime"]
        }
    }




# def pandasSum(df, col, group_cols):
#     # df = df.groupby(group_cols)[col].sum() if len(group_cols) else df
#     # df[col] = df[col].sum()
#     # return df
#     return df.groupby(group_cols, as_index=False).sum() if len(group_cols) else df.sum()

# def pandasMin(df, col, group_cols):
#     return df.groupby(group_cols, as_index=False).min() if len(group_cols) else df.min()

# def pandasMax(df, col, group_cols):
#     return df.groupby(group_cols, as_index=False).max() if len(group_cols) else df.max()

# def pandasAvg(df, col, group_cols):
#     return df.groupby(group_cols, as_index=False).mean() if len(group_cols) else df.mean()

# def pandasCount(df, col, group_cols):
#     return df.groupby(group_cols, as_index=False)[col].nunique()

# def pandasAvgCount(df, col, per, group_cols=[]):

#     grouped_df = df.groupby(per)

#     grouping = False
#     if len(group_cols):
#         grouping = True
#     group_cols.append(per)


#     # Count per group/perfield
#     df = df.groupby(group_cols).agg(
#             col=(col, "nunique"),
#         )

#     if grouping:
#         group_cols.pop()
#         df = df.groupby(group_cols)["col"]

#     df = df.mean()

#     return df

# def pandasAvgSum(df, col, per, group_cols=[]):

#     grouping = False
#     if len(group_cols):
#         grouping = True
#     group_cols.append(per)

#     # Count per group/perfield
#     df = df.groupby(group_cols).agg(
#             col=(col, "sum"),
#         )

#     if grouping:
#         group_cols.pop()
#         df = df.groupby(group_cols)["col"]

#     df = df.mean()

#     return df

# def pandasPercentage(df, col, numerator, group_cols=[]):
#     return


# def pandasOneHot(df, col, numerator):
#     df[col] = df[col].apply(lambda x: 1 if x == numerator else 0)
#     return df



import importlib.util
import os

base_dir = os.environ.get("SATYRN_ROOT_DIR", os.getcwd())
directory = os.path.join(base_dir, os.path.join("core","api","analysis_plugins"))
for filename in os.listdir(directory):
    if filename != "__init__.py" and filename.endswith(".py"):
        name = ".".join(["core", "api", "analysis_plugins", filename[:-3]])
        mod = importlib.import_module(name)
        op = getattr(mod, "dct")
        # add defaults for optionals
        op_name = list(op.keys())[0]
        if "optional" not in op[op_name]:
            op[op_name]["optional"] = {
                "groupBy": {
                    "allowed": True,
                    "maxDepth": 2,
                    "validInputs": ["id", "integer", "float", "string", "boolean"],
                    "parameters": [{ # optional to override defaults
                        "inputTypes": ["integer", "float"],
                        "options": ["percentile", "threshold"],
                        "allowMultiple": False
                    }]
                },
                "timeSeries": {
                    "allowed": True,
                    "maxDepth": 1,
                    "validInputs": ["date", "datetime"]
                }
            }
        else:
            # defaults for groupby
            group = op[op_name]["optional"].get("groupBy", {})
            if group:
                if "validInputs" not in group:
                    group["validInputs"] = ["id", "integer", "float", "string","boolean"]
                if "parameters" not in group:
                    group["parameters"] = [{ # optional to override defaults
                        "inputTypes": ["integer", "float"],
                        "options": ["percentile", "threshold"],
                        "allowMultiple": False
                    }]
                else:
                    # check if parameters already defined for int and float
                    tpes = []
                    for tpe in ["integer", "float"]:
                        init = [param for param in group["parameters"] if tpe in param["inputTypes"]]
                        if not init:
                            tpes.append(tpe)

                    if tpes:
                        group["parameters"].append({ # optional to override defaults
                            "inputTypes": tpes,
                            "options": ["percentile", "threshold"],
                            "allowMultiple": False
                        })


            # defaults for timeseries
            timeSeries = op[op_name]["optional"].get("timeSeries")
            if timeSeries:
                if "validInputs" not in timeSeries:
                    timeSeries["validInputs"] = ["date", "datetime"]

        OPERATION_SPACE.update(op)
