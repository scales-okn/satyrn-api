
from sqlalchemy import func
# MEthods for databases

from pandas import DataFrame

from functools import reduce

from sqlalchemy.sql.expression import case

def sql_right(field, db_type, char_n=2):
    # Given a sqlalchemy string field, return the last char_n chars
    if db_type == "sqlite":
        return func.substr(field, -2, 2)
    elif db_type == "postgres":
        return func.right(field, 2)

def sql_concat(field_lst, db_type):
    # For now, not implemented, will do sop if needed in the future
    # since or sqlite and postgres + works for both
    if len(field_lst) == 1:
        return field_lst[0]

    if db_type == "sqlite":
        return reduce(lambda a, b: a + b, field_lst, "")

    elif db_type == "postgres":
        return reduce(lambda a, b: a + b, field_lst, "")
        # return func.concatenate(field_lst)

    pass

def sql_median(field, db_type):
    # Given a sqlalchemy string field, return the last char_n chars
    if db_type == "sqlite":
        return func.median(field)
    elif db_type == "postgres":
        return func.percentile_disc(0.5).within_group(field.asc())

# def sql_percent_rank(field, db_type):
#     # NOTE: DOes not currently work
#     if db_type == "sqlite":
#         return func.percent_rank(field)
#     elif db_type == "postgres":
#         return func.percent_rank().within_group(field.asc())


def count_entities(query, entity_ids, field_names, db_type):

    entity_counts = {}
    if db_type == "sqlite":
        df = DataFrame(query.all(), columns=field_names).nunique()
        for entity in entity_ids:
            entity_counts[entity] = int(df[entity])
    elif db_type == "postgres":
        for entity in entity_ids:
            entity_counts[entity] = query.distinct(entity).count()

    return entity_counts

def _nan_cast(field, cast_val):
    '''
    Casts a field in case it is a null value
    '''
    return case([(field == None, cast_val)], else_=field)



