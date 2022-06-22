# SQL functions to be able to work in postres and sqlite

from sqlalchemy import func
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
    # concatenating strings together
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

def count_entities(query, entity_ids, field_names, db_type):
    # Count the unique entity_ids in a given query
    entity_counts = {}
    if db_type == "sqlite":
        df = DataFrame(query.all(), columns=field_names).nunique()
        for entity in entity_ids:
            entity_counts[entity] = int(df[entity])
    elif db_type == "postgres":
        print("========== entity_ids: ", entity_ids)
        for entity in entity_ids:
            entity_counts[entity] = query.distinct(entity).count()
    elif db_type == "postgresql":
        print("++++++++ entity_ids: ", entity_ids)
        for entity in entity_ids:
            entity_counts[entity] = query.distinct(entity).count()
    return entity_counts

def _nan_cast(field, cast_val):
    # Casts a field in case it is a null value
    return case([(field == None, cast_val)], else_=field)



# # Unused, for future development
# def sql_percent_rank(field, db_type):
#     # NOTE: DOes not currently work
#     if db_type == "sqlite":
#         return func.percent_rank(field)
#     elif db_type == "postgres":
#         return func.percent_rank().within_group(field.asc())