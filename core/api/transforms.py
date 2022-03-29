from flask import current_app

# local globals
# db = current_app.jdb.noacri

# a global constant that is leveraged by the analysis engine/API
# similar to searchSpace.py -- config-driven analysis interface

from sqlalchemy.sql.expression import case, extract
from sqlalchemy.sql.functions import concat
from sqlalchemy import func

def make_case_expression(model_field, transform_dict, else_val):
    # print(transform_dict)
    case_list = [(v(model_field), k) for k, v in transform_dict.items()]
    case_ex = case(case_list, else_=else_val) if else_val else case(case_list)
    return case_ex


comparator_dict = {
    "<": lambda a,b: a < b,
    ">": lambda a,b: a > b,
    "<=": lambda a,b: a <= b,
    ">=": lambda a,b: a >= b,
    "==": lambda a,b: a == b,
}

def lambda_func(string):
    split = string.split(" ")
    if split[0] == "x":
        value = float(split[2])
        comp = comparator_dict[split[1]]
        return lambda x: comp(x, value)
    if split[-1] == "x":
        value = float(split[0])
        comp = comparator_dict[split[1]]
        return lambda x: comp(value, x)
    value_1 = float(split[0])
    value_2 = float(split[4])
    comp_1 = comparator_dict[split[1]]
    comp_2 = comparator_dict[split[3]]
    return lambda x: (comp_1(value_1, x)) & (comp_2(x, value_2))


def inequalities_processing(model_field, db_type, extra):
    '''
    Returns a sqlalchemy case expression for building the string_list
    Examples:
        - ["x < 5", "5 <= x < 10", "10 < x"]
        - ["0 <= x <= 10", "10 < x <=  30"]
        - ["10 < x <= 30", "0 <= x <= 10"]
    NOTES: 
    - Each string in string_list should be mutually exclusive. It'll run even if it is not,
    but might cause unexpected behavior
    - It'll expect a format as above. Intervals don't need to be ordered. The order determines
    the order in which we will check for membership in each range
    - The space should be used between each "elements" and "operands". 
    - The notation follows mathematical set notation. e.g. (0,10): 0 < x < 10,
    [0,10]: 0 <= x <= 10, [0,10): 0 <= x < 10

    There is also an option of having a list of tuples, where the first string is 
    the same as above, and the second string is the "user-friendly" reading of that
    string
    "transformCases": [
        ("x <= 5", "< 5 years"),
        ("5 < x <= 10", "5 to 10 years"),
        ("10 < x <= 15", "10 to 15 years"),
        ("15 < x <= 20", "15 to 20 years"),
        ("x > 20", "> 20 years")
    ],
    '''
    extra = extra if extra else {}
    transform_dict = {}
    string_list = extra.get("string_list", ["x < 200", "200 <= x < 500", "500 <= x"])
    if type(string_list[0]) == str:
        for string in string_list:
            transform_dict[string] = lambda_func(string)
    else:
        for string1, string2 in string_list:
            transform_dict[string2] = lambda_func(string1)        
    return make_case_expression(model_field, transform_dict, else_val=extra.get("else_val", None))



def month_processing(model_field, string_list=None, else_val=None):
    '''
    obtains a datetime and then from that, maps them to datetime

    '''
    return concat(extract('year', model_field), "/", extract('month', model_field))


def year_processing(model_field, string_list=None, else_val=None):
    '''
    obtains a datetime and then from that, maps them to datetime

    '''
    return extract('year', model_field)



# def substr_processing(model_field, string_list=None, else_val=None):
#     return func.substr(model_field, 0, 10)


# def substr_processing(model_field, string_list=None, else_val=None):
#     return func.substr(model_field, 0, (func.length(model_field) - func.instr(model_field, " ")))

TRANSFORMS_SPACE = {
    "threshold": {
        "dataType": ["float", "int"],
        "newType": "string",
        "processor": inequalities_processing,

    },

    # "month_transform": {
    #     "dataType": ["datetime"],
    #     "newType": "date",
    #     "processor": month_processing,

    # },
    # "year_transform": {
    #     "dataType": ["datetime"],
    #     "newType": "date",
    #     "processor": year_processing,

    # },
    # "substr_transform": {
    #     "dataType": ["string"],
    #     "newType": "string",
    #     "processor": substr_processing,
    # }
}