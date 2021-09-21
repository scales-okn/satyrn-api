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
from functools import reduce

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
        query, groupby_args, _ = simple_query(s_opts, a_opts, ringId, targetEntity, sess, db)
        query = query.group_by(*groupby_args) if len(groupby_args) else query
        results = query.all()
    
    elif op == "recursive":

        query = recursive_query(s_opts, a_opts, ringId, targetEntity, sess, db)
        results = query.all()

    elif op == "complex":
        if a_opts["op"] == "correlation":
            results = correlation(s_opts, a_opts, targetEntity, sess, db)
        elif a_opts["op"] == "comparison":
            results = comparison(s_opts, a_opts, targetEntity, sess, db)
        elif a_opts["op"] == "distribution":
            results = distribution(s_opts, a_opts, targetEntity, sess, db)

    else:
        print("unclear what operation you chose")

    entity_counts = row_count_query(s_opts, a_opts, ringId, targetEntity, sess, db)
    units = get_units(a_opts, ringId)
    # Formatting
    res_dict = {
        "results": results,
        "entity_counts": entity_counts,
        "units": units
    }

    return res_dict

def recursive_query(s_opts, a_opts, ringId, targetEntity, session, db):
    copy_opts = deepcopy(a_opts)
    copy_opts["op"] = "count" if a_opts["op"] == "averageCount" else "sum"
    query, query_args, _ = simple_query(s_opts, copy_opts, ringId, targetEntity, session, db)

    groupby_args = deepcopy(query_args)
    query = query.group_by(*groupby_args)

    # Build new query arguments
    s_query = query.subquery()
    query_args = [s_query.c[arg] for arg in query_args]
    query_args.pop()
    query_args.append(func.avg(s_query.c[_name(ringId, a_opts["target"]["entity"], a_opts["target"]["field"], copy_opts["op"])]))
    query = session.query(*query_args)

    # Build new group args
    query_args.pop()
    query = query.group_by(*query_args) if query_args else query

    return query

def simple_query(s_opts, a_opts, ringId, targetEntity, session, db):
    # Query fields

    q_args, tables, entity_ids, entity_names, field_names = _prep_query(a_opts, ringId, db)

    query = session.query(*q_args)

    # Do filtering
    query = _do_filters(query, s_opts, ringId, targetEntity, session, db)

    # Do joins
    query = _do_joins(query, tables, a_opts["relationships"] if "relationships" in a_opts else [], ringId, targetEntity, db)

    # ADD THE UNIQUE CONSTRAINT ON THE TUPLES OF THE IMPORTANT FIELDS IDS
    # PENDING: this gives errors. right now will not add entity ids
    # query = query.distinct(*entity_ids)

    group_args = [_name(ringId, d["entity"], d["field"]) for d in a_opts["groupBy"]] if "groupBy" in a_opts else []
    for field in ["over", "per", "timeseries"]:
        if field in a_opts:
             group_args.append(_name(ringId, a_opts[field]["entity"], a_opts[field]["field"]))


    return query, group_args, field_names

def row_count_query(s_opts, a_opts, ringId, targetEntity, session, db):
    # Query fields

    a_opts = deepcopy(a_opts)

    a_opts["op"] = "None"
    for key in ["target", "target2"]:
        if key in a_opts:
            a_opts[key].pop('op', None)
    for key in ["numerator", "numerator2"]:
        a_opts.pop(key, None)

    q_args, tables, entity_ids, entity_names, field_names = _prep_query(a_opts, ringId, db, counts=True)
    query = session.query(*q_args)

    # Do filtering
    query = _do_filters(query, s_opts, ringId, targetEntity, session, db)

    # Do joins
    query = _do_joins(query, tables, a_opts["relationships"] if "relationships" in a_opts else [], ringId, targetEntity, db)

    # ADD THE UNIQUE CONSTRAINT ON THE TUPLES OF THE IMPORTANT FIELDS IDS
    # PENDING

    entity_counts = {}
    for entity, name in zip(entity_ids, entity_names):
        new_query = query.distinct(entity)
        entity_counts[name] = new_query.count()

    print(entity_counts)
    return entity_counts

def get_units(a_opts, ringId):

    def _get_field_units(entity, attribute, transform=None):
        # returns the field from entity in ring
        entity_dict = current_app.ringExtractors[ringId].resolveEntity(entity)[1]
        if attribute != "id":
            attr_obj = [attr for attr in entity_dict.attributes if attr.name == attribute][0]
            unit = attr_obj.units[0] if attr_obj.units else attr_obj.nicename[0]
        else:
            unit = entity

        return unit

    def _apply_op_units(target_unit, op):
        op_units = OPS[op]["units"]
        if op_units == "unchanged":
            return target_unit
        elif op_units == "percentage":
            return "percent"
        elif op_units == "undefined":
            return "undefined"
        return "unknown"


    units_dict = {key: None for key in ["target", "per", "target2", "groupBy", "over", "timeseries"] if key in a_opts}

    for key in ["per", "over", "timeseries"]:
        if key in units_dict:
            units_dict[key] = _get_field_units(a_opts[key]["entity"], a_opts[key]["field"], a_opts[key].get("transform", None))

    if "groupBy" in units_dict:
        units_dict["groupBy"] = [_get_field_units(group["entity"], group["field"], group.get("transform, None")) for group in a_opts["groupBy"]]


    target_keys = [key for key in ["target", "target2"] if key in units_dict]
    for key in target_keys:
        unit = _get_field_units(a_opts[key]["entity"], a_opts[key]["field"], a_opts[key].get("transform", None))
        if "op" in a_opts[key]:
            unit = _apply_op_units(unit, a_opts[key]["op"])
        units_dict[key] = unit

    # Final units
    units_dict["operation"] = "none"
    op = a_opts["op"]
    op_units = OPS[op]["units"]

    if op_units == "comparison":
        units_dict["operation"] = "see targets units"
    elif op_units == "none":
        units_dict["operation"] = "no units"
    elif op_units == "distribution":
        units_dict["operation"] = "percent"
    elif op_units == "target/per":
        units_dict["operation"] = units_dict["target"] + '/' + units_dict["per"]
    elif op_units in ["unchanged", "percentage", "undefined"]:
        units_dict["operation"] = _apply_op_units(units_dict["target"], op)
    else:
        print("unknown units")
    
    return units_dict




def _prep_query(a_opts, ringId, db, counts=False):
    # Called to query across a (or multiple) computed values

    # TODO: Add "nice" rerpesentation for grouopby values (for nice naming stuff)
    # TODO: Add stuff incase there are transforms

    q_args = []
    tables = []
    col_names = []
    unique_entities = []

    # Get groupby fields
    groupby_fields = deepcopy(a_opts["groupBy"]) if "groupBy" in a_opts else []

    for field in ["over", "per", "timeseries"]:
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

    for target in target_fields:
        the_field, field_name = _get(ringId, target["entity"], target["field"], db, op=target["op"])
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
        if name not in col_names and counts:
            q_args.append(the_field)
            col_names.append(name)

    return q_args, tables, entity_ids, unique_entities, col_names,

def _do_filters(query, s_opts, ringId, targetEntity, session, db):
    # TODO: Fill this out

    # do prefilters

    query = query.filter(_get(ringId, targetEntity, "id", db) != None)

    # do normal filters
    if s_opts:
        query = rawGetResultSet(s_opts, ringId, targetEntity, targetRange=None, simpleResults=True, just_query=True, sess=session, query=query)

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
        # the_table = _get_table_name(ringId, targetEntity, "id")
        return query.join(getattr(db, the_table),
                            _get_join_field(path[0], db) == _get_join_field(path[1], db),
                            isouter=True
                            ), the_table

    def find_rel(item, lst):
        items = [rel for rel in lst if rel["name"] == item]
        return items[0] if len(items) else None

    joined_tables.append(_get_table_name(ringId, targetEntity, "id"))
    for relationship in relationships:
        rel_item = current_app.ringExtractors[ringId].resolveRelationship(relationship)[1]
        if len(rel_item.join):
            join = current_app.ringExtractors[ringId].resolveJoin(rel_item.join[0])[1]
            for path in join.path:
                query, add_table = do_join(query, path, joined_tables)
                joined_tables.append(add_table)

    return query


def _format_results():
    # make human legible

    '''
    Do the dictionary mapping if needed (no longer need to query stuff from original db hopefully)

    Do rounding stuff if needed

    # TODO PATCH: This is a non sustainable solution for percentage operation
    # if analysisOpts["operation"] == "percentage":
        # for idx, x in enumerate(results["results"]):
            # print(x)
            # results["results"][idx][-1] *= 10
    Do anything you need to do for percentage stuff (multiplying by 100 i think)

    Do any unit conversions if needed (e.g. currency, temperatures, distances)
    '''

    # ordering (if any)
    pass

    # 

def _get(ringId, entity, attribute, db, transform=None, op=None):
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
    name = _name(ringId, entity, attribute, op)

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

def _name(ringId, entity, attribute, op=None):
    if op:
        return ".".join([entity, attribute, op])
    return ".".join([entity, attribute])
    # return ".".join([ringId, entity, attribute])

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
    elif "numerator2" in extra_dict:
        return op_dict["op"](field, extra_dict["numerator2"]).label(field_name)
    else:
        return op_dict["op"](field).label(field_name)
        

def correlation(s_opts, a_opts, targetEntity, session, db):

    ring = a_opts["rings"][0]

    query, group_args, field_names = simple_query(s_opts, a_opts, ring, targetEntity, session, db)
    query = query.group_by(*group_args)
    results = query.all()

    df = pd.DataFrame(results, columns=field_names)
    corr_matrix = df.corr("pearson")

    col_1 = _name(a_opts["rings"][0], a_opts["target"]["entity"], a_opts["target"]["field"], a_opts["target"].get("op", None))
    col_2 = _name(a_opts["rings"][0], a_opts["target2"]["entity"], a_opts["target2"]["field"], a_opts["target2"].get("op", None))

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


def comparison(s_opts, a_opts, targetEntity, session, db):

    ring = a_opts["rings"][0]
    query, group_args, _ = simple_query(s_opts, a_opts, ring, targetEntity, session, db)

    query = query.group_by(*group_args)
    results = query.all()

    return results


def distribution(s_opts, a_opts, targetEntity, session, db):

    ring = a_opts["rings"][0]
    query, group_args, field_names = simple_query(s_opts, a_opts, ring, targetEntity, session, db)
    query = query.group_by(*group_args)

    results = query.all()
    df = pd.DataFrame(results, columns=field_names)

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
    return tuples
