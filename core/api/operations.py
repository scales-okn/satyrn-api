
from sqlalchemy import func
from sqlalchemy import distinct
from sqlalchemy.sql.expression import case

import pandas as pd

from .utils import _name
from functools import reduce
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

def pandasCorrelation(a_opts, results, group_args, field_names):

    df = pd.DataFrame(results, columns=field_names)
    corr_matrix = df.corr("pearson")
    ring = a_opts["rings"][0]



    df_unique = df.nunique()
    print(df_unique)

    print(field_names)
    if "numerator" in a_opts and "op" not in a_opts["target"]:
        col_1 =  _name(ring, a_opts["target"]["entity"], a_opts["target"]["field"], "oneHot")
    else:
        col_1 = _name(ring, a_opts["target"]["entity"], a_opts["target"]["field"], a_opts["target"].get("op", "None"))

    if "numerator2" in a_opts and "op" not in a_opts["target2"]:
        col_2 =  _name(ring, a_opts["target2"]["entity"], a_opts["target2"]["field"], "oneHot")
    else:
        col_2 = _name(ring, a_opts["target2"]["entity"], a_opts["target2"]["field"], a_opts["target2"].get("op", "None"))
    # col_2 = _name(ring, a_opts["target2"]["entity"], a_opts["target2"]["field"], a_opts["target2"].get("op", None))

    corr_val = corr_matrix[col_1][col_2]

    # PEnding about correlation
    '''
    TODO
    questions like where there are different filters on each of the calculations
    correlation groupby committee
        # of contributions given by people in alaska
        amount of money raised overall
    '''
    return {"results": results, "score": corr_val}


def pandasComparison(a_opts, results, group_args, field_names):

    return {"results": results}


def pandasDistribution(a_opts, results, group_args, field_names):

    df = pd.DataFrame(results, columns=field_names)
    ring = a_opts["rings"][0]

    target_arg = _name(ring, a_opts["target"]["entity"], a_opts["target"]["field"], a_opts["target"]["op"])
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

    tuples = [tuple(x) for x in df.to_numpy()]
    return {"results": tuples}



OPERATION_SPACE = {
    "average": {
        "fields": {
            "target": {
                "types": ["float", "int"]
            }
        },
        "units": "unchanged", 
        "nicename": "Average",
        "funcDict": {
            "op": func.avg,
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
        "funcDict": {
            "op": lambda field: func.count(distinct(field)),
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
        "funcDict": {
            "op": func.sum,
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
        "funcDict": {
            "op": func.min,
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
        "funcDict": {
            "op": func.max,
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
        "funcDict": {
            "op": func.mode,
            "processing": func.OrderedSetAgg,
            # ""
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
        "funcDict": {
            "op": func.percentile_disc,
            "processing": func.within_group,
            "val": 0.50
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
        "funcDict": {
            "op": lambda field: func.count(distinct(field)),
            "outerOp": func.avg
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
        "funcDict": {
            "op": func.sum,
            "outerOp": func.avg,
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
        "funcDict": {
            "op": lambda field, numer:  func.avg(onehot_processing(field, numer)),
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
        # "units": "percentage", 
        # "nicename": "Percentage of",
        "funcDict": {
            "op": onehot_processing,
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
        "funcDict": {
            "op": lambda field: field,
        },
        "type": "simple"
    },
    "correlation": {
        "fields": {
            "target": {
                "types": ["string", "bool", "int", "float", "average", "count"]
            },
            "target2": {
                "types": ["string", "bool", "int", "float", "average", "count"]
            },
            "numerator": {
                "types": ["== target"],
                "required": ["target == bool", "target == string"]
            },
            "numerator2": {
                "types": ["== target"],
                "required": ["target == bool", "target == string"]
            },
            "groupBy": {
                "types": ["id"]
            }
        },
        "units": "none", 
        "nicename": "Correlation between",
        # "funcDict": {
        #     "op": func.avg,
        #     "processing": onehot_processing
        # },
        "pandasFunc": {
            "op": pandasCorrelation,
        },
        "type": "complex"
    },

    "comparison": {
        "fields": {
            "target": {
                "types": ["string", "bool", "int", "float", "average", "count", "percentage"]
            },
            "target2": {
                "types": ["string", "bool", "int", "float", "average", "count", "percentage"]
            },
            "numerator": {
                "types": ["== target"],
                "required": ["target == bool", "target == string"]
            },
            "numerator2": {
                "types": ["== target"],
                "required": ["target == bool", "target == string"]
            },
            "groupBy": {
                "types": ["id"]
            }
        },
        "units": "unchanged", 
        "nicename": "Comparison between",
        # "funcDict": {
        #     "op": func.avg,
        #     "processing": onehot_processing
        # },
        "pandasFunc": {
            "op": pandasComparison,
        },
        "type": "complex"
    },


    "distribution": {
        "fields": {
            "target": {
                "types": ["int", "float", "average", "count"]
            },
            "over": {
                "types": ["string", "bool"]
            }
        },
        "type": "complex",
        "units": "distribution",
        "nicename": "Distribution of",
        # "funcDict": {
        #     "op": "holis"
        # },
        "pandasFunc": {
            "op": pandasDistribution,
        },
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


