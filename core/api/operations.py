from flask import current_app

# local globals
# db = current_app.jdb.noacri

# a global constant that is leveraged by the analysis engine/API
# similar to searchSpace.py -- config-driven analysis interface

from sqlalchemy import func
from sqlalchemy import distinct
from sqlalchemy.sql.expression import case

#BIG TODO PENDING: When chaining operations, how does that change units? Can we codify that somehow?


def percentage_processing(model_field, pos_values):

    def equal_lambd(val): return lambda x: x == val
    my_cases = [(equal_lambd(val), 1) for val in pos_values]

    case_list = [(k(model_field), v) for k, v in my_cases]
    case_ex = case(case_list, else_=0)
    return case_ex


OPERATION_SPACE = {
    "average": {
        "dataTypes": ["float", "int"],
        "neededFields": ["targetField"],
        "operation": func.avg,
        "units": "unchanged", 
        "nicename": "Average"
    },
    "count": {
        "dataTypes": ["id"],
        "neededFields": ["targetField"],
        "operation": func.count,
        "units": "unchanged", 
        "processing": distinct,
        "nicename": "Count of"
    },
    "averageCount": {
        "dataTypes": ["id"],
        "neededFields": ["targetField", "perField"],
        "operation": func.count,
        "units": "target/per", 
        "processing": distinct,
        "nicename": "Average Count of"
    },
    "sum": {
        "dataTypes": ["float", "int"],
        "neededFields": ["targetField"],
        "operation": func.sum,
        "units": "unchanged", 
        "nicename": "Total"
    },
    "min": {
        "dataTypes": ["float", "int"],
        "neededFields": ["targetField"],
        "operation": func.min,
        "units": "unchanged", 
        "nicename": "Minimum"
    },
    "max": {
        "dataTypes": ["float", "int"],
        "neededFields": ["targetField"],
        "operation": func.max,
        "units": "unchanged", 
        "nicename": "Maximum"
    },
    "percentage": {
        "dataTypes": ["string", "bool"],
        "neededFields": ["targetField", "numeratorField"],
        "operation": func.avg,
        "units": "percentage", 
        "processing": percentage_processing,
        "nicename": "Percentage of"
    }
}
