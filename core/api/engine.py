# the placeholder for the analytics engine
# might delegate to seekers.py for search/return of results? analysis optimizations TBD
from copy import deepcopy

from flask import current_app
import numpy as np
import pandas as pd
from sqlalchemy import func
from sqlalchemy.sql.expression import case

from .operations import OPERATION_SPACE as OPS
from .seekers import rawGetResultSet
from .transforms import TRANSFORMS_SPACE as TRS

# satyrn configs...
# TARGET_MODEL = current_app.satConf.targetModel
# AMS = current_app.satConf.analysisSpace
# db = current_app.satConf.db
# PREFILTERS = current_app.satConf.preFilters

cache = current_app.cache
CACHE_TIMEOUT=3000


# Method to change the AMS so that it has the defaults in it

def add_defaults(ams):
    # PENDING: id should never be cast i thinks
    nulldefault = {
            "string": ("cast", "No value"),
            "id": ("ignore", "0"),
            "float": ("ignore", 0.0),
            "int": ("ignore", 0),
            "bool": ("ignore", False),
            "date": ("ignore", None),
            "datetime": ("ignore", None),
            "stringplaceholder": ("ignore", "No value")
        }
    for key in ams.keys():
        if "nulls" not in ams[key]:
            ams[key]["nulls"] = nulldefault[ams[key]["type"]][0]
        if ams[key]["nulls"] == "cast" and "nullCast" not in ams[key]:
            ams[key]["nullCast"] = nulldefault[ams[key]["type"]][1]




def run_analysis(s_opts, a_opts, targetEntity):

    def _get_db_sess(ringId):
        db = current_app.rings[ringId].db
        sess = db.Session()
        return db, sess

    # If across two rings, right now just do a different path
    print(a_opts)
    print(a_opts["rings"])

    if len(a_opts["rings"]) > 1:
        print("multiple ring")
        analysis_across_two_rings(s_opts, a_opts)

    else:
        db, sess  = _get_db_sess(a_opts["rings"][0])
        return single_ring_analysis(s_opts, a_opts, a_opts["rings"][0], targetEntity, sess, db)

    # formatting

    # Converting names into legible stuff

    # Getting units

    # Sorting

    # Row count (???????)

    '''
    Results:
    {
        results: []
        column names: []
        units: []
        Counts:{
            entity: {
                initial (after search filter)
                null/invalid filter
                after joins??
            }
        }
        
    }
    '''


def single_ring_analysis(s_opts, a_opts, ringId, targetEntity, sess, db):
    # depending on operation, prep necessary queries

    op = OPS[a_opts["op"]]["type"]
    if op == "simple":
        query, groupby_args = simple_query(s_opts, a_opts, ringId, targetEntity, sess, db)
        query = query.group_by(*groupby_args) if len(groupby_args) else query
        results = query.all()

    elif op == "recursive":

        query = recursive_query(s_opts, a_opts, ringId, sess, db)
        results = query.all()

    elif op == "complex":

        if a_opts["op"] == "correlation":
            results, corr_matrix = correlation(s_opts, a_opts, session, db)
            return corr_matrix
        elif a_opts["op"] == "comparison":
            return comparison(s_opts, a_opts, session, db)
        elif a_opts["op"] == "distribution":
            return distribution(s_opts, a_opts, session, db)

    else:
        print("unclear what operation you chose")

    # Formatting
    return results

def recursive_query(s_opts, a_opts, ringId, targetEntity, session, db):
    copy_opts = deepcopy(a_opts)
    copy_opts["op"] = "count" if a_opts["op"] == "averageCount" else "sum"
    query, query_args = simple_query(s_opts, copy_opts, ringId, targetEntity, session, db)

    groupby_args = deepcopy(query_args)
    groupby_args.append(_name(ringId, a_opts["per"]["entity"], a_opts["per"]["field"]))
    query = query.group_by(*groupby_args)


    # Build new query arguments
    s_query = query.subquery()
    query_args = [s_query.c[arg] for arg in query_args]
    query_args.append(func.avg(s_query.c[_name(ringId, a_opts["target"]["entity"], a_opts["target"]["field"])]))
    query = session.query(*query_args)

    # Build new group args
    query_args.pop()
    query = query.group_by(*query_args) if query_args else query

    return query

def simple_query(s_opts, a_opts, ringId, targetEntity, session, db):
    # Query fields

    q_args, tables, entity_ids, field_names = _prep_query(a_opts, ringId, db)

    query = session.query(*q_args)

    # Do filtering
    query = _do_filters(query, s_opts, ringId, targetEntity, db)

    # Do joins
    query = _do_joins(query, tables, a_opts["relationships"] if "relationships" in a_opts else [], ringId, targetEntity, db)

    # ADD THE UNIQUE CONSTRAINT ON THE TUPLES OF THE IMPORTANT FIELDS IDS
    query = query.distinct(*entity_ids)

    group_args = [_name(ringId, d["entity"], d["field"]) for d in a_opts["groupBy"]] if "groupBy" in a_opts else []

    row_count_query(a_opts, a_opts, ringId, targetEntity, session, db)
    return query, group_args

def row_count_query(s_opts, a_opts, ringId, targetEntity, session, db):
    # Query fields

    a_opts = deepcopy(a_opts)

    a_opts["op"] = "None"
    for key in ["target", "target2"]:
        if key in a_opts:
            a_opts[key].pop('op', None)
    for key in ["numerator", "numerator2"]:
        a_opts.pop(key, None)


    q_args, tables, entity_ids, field_names = _prep_query(a_opts, ringId, db)

    query = session.query(*q_args)

    # Do filtering
    query = _do_filters(query, s_opts, ringId, targetEntity, db)

    # Do joins
    query = _do_joins(query, tables, a_opts["relationships"] if "relationships" in a_opts else [], ringId, targetEntity, db)

    # ADD THE UNIQUE CONSTRAINT ON THE TUPLES OF THE IMPORTANT FIELDS IDS
    for entity in entity_ids:
        query = query.distinct(entity)
        print(entity)
        print(query.count())

    return

def _prep_query(a_opts, ringId, db):
    # Called to query across a (or multiple) computed values

    # TODO: Add "nice" rerpesentation for grouopby values (for nice naming stuff)
    # TODO: Add stuff incase there are transforms

    q_args = []
    tables = []
    col_names = []
    unique_entities = []

    # Get groupby fields
    groupby_fields = a_opts["groupBy"] if "groupBy" in a_opts else []

    for field in ["over", "per"]:
        if field in a_opts:
            groupby_fields.append(a_opts[field])

    for group in groupby_fields:
        the_field, name = _get(ringId, group["entity"], group["field"], db)
        q_args.append(the_field)
        col_names.append(name)
        table = _get_table_name(ringId, group["entity"], group["field"])
        if table not in tables:
            tables.append(table)
        if group["entity"] not in unique_entities:
            unique_entities.append(group["entity"])

    # Get target field
    def target_op(a_opts, targ):
        if "op" in a_opts[targ]:
            return a_opts[targ]["op"]
        elif a_opts["op"] not in ["correlation", "distribution", "comparison"]:
            return a_opts["op"]
        else:
            return "None"

    target_fields = []
    for targ, num in zip(["target", "target2"],["numerator", "numerator2"]):
        if targ in a_opts:
            target = deepcopy(a_opts[targ])
            if num in a_opts:
                target.update({"op": a_opts[targ]["op"] if "op" in a_opts[targ] else "oneHot",
                                "extra": {num: a_opts[num] }
                            })
            else:
                target.update({"op": target_op(a_opts, targ),
                                "extra": {}
                            })                
            target_fields.append(target)        
    print(target_fields)
    for target in target_fields:
        the_field, field_name = _get(ringId, target["entity"], target["field"], db)
        q_args.append(_do_operation(target["op"], the_field, target["extra"], field_name))
        col_names.append(field_name)
        table = _get_table_name(ringId, target["entity"], target["field"])
        if table not in tables:
            tables.append(table)
        if target["entity"] not in unique_entities:
            unique_entities.append(target["entity"])
      
    # QUERY The IDs of the entities we care about (as well as the nice name stuff for them)
    entity_ids = []
    for entity in unique_entities:
        the_field, name = _get(ringId, entity, "id", db)
        entity_ids.append(the_field)
        if name not in col_names:
            q_args.append(the_field)
            col_names.append(name)

    print(col_names)
    return q_args, tables, entity_ids, col_names

def _do_filters(query, s_opts, ringId, targetEntity, db):
    # TODO: Fill this out

    # do prefilters

    query = query.filter(_get(ringId, targetEntity, "id", db) != None)

    # do normal filters

    # do nan filtering

    return query

def _do_joins(query, tables, relationships, ringId, targetEntity, db):
    # VEERY SHAKY
    # PENDING: in the config json we might need to specify which table we are joining to, or seomwhere in the code
    # Should be shared between seekers as well
    # PENDING: this ought to be smarter
    # For now, we will just do "greedy": join to primary table when possible
    # print("in the joins")

    joined_tables = []
    primary = any(table == _get_table_name(ringId, targetEntity, "id") for table in tables)
    if not primary:
        # might need to do a join to the primary table
        pass

    def do_join(query, path, added_tables=[]):
        # NOTE: which table to join to kinda changes depending on what the "primary" table is perceived to be
        the_table = path[0].split(".")[0] if path[0].split(".")[0] not in added_tables else path[1].split(".")[0]
        return query.join(getattr(db, the_table),
                            _get_join_field(ringId, path[0], db) == _get_join_field(ringId, path[1], db),
                            # isouter=True
                            ), the_table

    def find_rel(item, lst):
        items = [rel for rel in lst if rel["name"] == item]
        return items[0] if len(items) else None

    joined_tables.append(_get_table_name(ringId, targetEntity, "id"))
    for relationship in relationships:
        rel_item = current_app.ringExtractors[ringId].resolveRelationship(relationship)[1]
        if "join" in rel_item:
            join = current_app.ringExtractors[ringId].resolveJoin(rel_item["join"])[1]
            for path in join["path"]:
                query, add_table = do_join(query, path, joined_tables)
                joined_tables.append(add_table)

    return query


def _format_results():
    # make human legible

    '''
    Do the dictionary mapping if needed (no longer need to query stuff from original db hopefully)

    Do rounding stuff if needed

    Do any unit conversions if needed (e.g. currency, temperatures, distances)
    '''

    # ordering (if any)
    pass

    # 

def _get(ringId, entity, attribute, db, transform=None):
    # returns the field from entity in ring
    entity_dict = current_app.ringExtractors[ringId].resolveEntity(entity)[1]
    if attribute != "id":
        attr_obj = [attr for attr in entity_dict.attributes if attr.name == attribute][0]
        model_name = attr_obj.source_table
        field_name = attr_obj.source_columns[0]
    else:
        model_name = entity_dict.table
        field_name = entity_dict.id[0]

    # PENDING: handling multiple columns in the columns of source
    model = getattr(db, model_name)
    field = getattr(model, field_name)
    name = _name(ringId, entity, attribute)

    if transform:
        field = TRS[transform].processor(field)

    return field.label(name), name

def _get_table_name(ringId, entity, attribute):
    # returns the table corresponding to an entity in ring   
    entity_dict = current_app.ringExtractors[ringId].resolveEntity(entity)[1]
    if attribute != "id":
        attr_obj = [attr for attr in entity_dict.attributes if attr.name == attribute][0]
        model_name = attr_obj.source_table
    else:
        model_name = entity_dict.table
    return model_name

def _get_join_field(path_bit, db):
    model_name, field_name = path_bit.split(".")
    model = getattr(db, model_name)
    field = getattr(model, field_name)
    return field

def _name(ringId, entity, attribute):
    return ".".join([ringId, entity, attribute])

def _outerjoin_name(ringId, join_field):
    return ".".join([ringId, join_field])

def _nan_cast(field, cast_val):
    return case([(field == None, cast_val)], else_=field)


def _do_operation(op_name, field, extra_dict, field_name):
    # "simple" atomic operations
    # max, min, sum, average, count, median, mode, percentiles, percent
    # PENDING figure out median mode stuff
    op_dict = OPS[op_name]["funcDict"]
    if "numerator" in extra_dict:
        return op_dict["op"](field, extra_dict["numerator"]).label(field_name)
    else:
        return op_dict["op"](field).label(field_name)
        

def correlation(s_opts, a_opts, session, db):

    ring = a_opts["rings"][0]
    query, group_args = simple_query(s_opts, a_opts, ring, session, db)

    query = query.group_by(*group_args)

    results = query.all()

    df = pd.DataFrame(results)
    corr_matrix = df.corr("pearson")

    # PEnding about correlation
    '''
    TODO
    questions like where there are different filters on each of the calculations
    correlation groupby committee
        # of contributions given by people in alaska
        amount of money raised overall
    '''
    return results, corr_matrix


def comparison(s_opts, a_opts, session, db):

    ring = a_opts["rings"][0]
    query, group_args = simple_query(s_opts, a_opts, ring, session, db)

    query = query.group_by(*group_args)

    results = query.all()

    return results


def distribution(s_opts, a_opts, session, db):

    ring = a_opts["rings"][0]
    query, group_args = simple_query(s_opts, a_opts, ring, session, db)
    over_arg = _name(ring, a_opts["over"]["entity"], a_opts["over"]["field"])
    group_args.append(over_arg)
    query = query.group_by(*group_args)

    results = query.all()
    df = pd.DataFrame(results)

    group_args.pop()
    target_arg = _name(ring, a_opts["target"]["entity"], a_opts["target"]["field"])
    if group_args:
        counts = df.groupby(group_args)[target_arg].sum()# .reset_index().rename(columns={0:'denom'})
        for value in counts.index:
            if type(value) == str:
                conditions = [(df[group_args[0]] == value)]# for v,arg in zip(value, group_args)]
            else:
                conditions = [(df[arg] == v) for v,arg in zip(value, group_args)]
            condition = reduce(np.logical_and, conditions)
            df.loc[condition, target_arg] = df.loc[condition, target_arg] / df.loc[condition, target_arg].sum()
            
    else:
        df[target_arg] = df[target_arg] / df[target_arg].sum()

    print(df)

    return results



