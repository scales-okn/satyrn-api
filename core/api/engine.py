# the placeholder for the analytics engine
# might delegate to seekers.py for search/return of results? analysis optimizations TBD
from copy import deepcopy

from flask import current_app
from sqlalchemy import func
from sqlalchemy.sql.expression import case

from pandas import DataFrame

from .operations import OPERATION_SPACE as OPS
from .seekers import rawGetResultSet
from .transforms import TRANSFORMS_SPACE as TRS
from .utils import _get_join_field, _name, _outerjoin_name, _entity_from_name, _remove_duplicate_vals, count_entities, sql_concat
from .utils import _make_recursive_name, _entity_from_recursive_name

# PREFILTERS = current_app.satConf.preFilters

cache = current_app.cache
CACHE_TIMEOUT=3000


# TODO: ponder on the stuff about the format in which we should expect values (e.g. list of one, wrapped single values, double nested lists)

def run_analysis(s_opts, a_opts, targetEntity, ring, extractor):
    '''
    Callable function for the views API
    Theoretically no other function should be called from here by views
    Takes:
        s_opts: searching opts
        a_opts: analysis opts
        targetEntity: searchable entity
    Returns the following:
    {
        results: {
            [(), ]
        }
        column names: []
        units: {}
        Counts:{
            entity: {
                initial (after search filter)
                null/invalid filter
                after joins??
            }
        },
        score: float (only for correlation)
    }
    '''
    def _get_db_sess(ring):
        db = ring.db
        sess = db.Session()
        return db, sess

    # If across two rings, do a different path
    # if len(a_opts["rings"]) > 1:
    #     # analysis_across_two_rings(s_opts, a_opts)
    #     pass
    # else:

    db, sess  = _get_db_sess(ring)
    return single_ring_analysis(s_opts, a_opts, ring, extractor, targetEntity, sess, db)


def _add_group_ref(a_opts, field_name, pos=None):
    # if given pos, it means the undelrying field was a list

    if pos != None:
        name = field_name + str(pos) + "_ref"
        orig_dct = a_opts[field_name][pos]
    else:
        name = field_name + "_ref"
        orig_dct = a_opts[field_name]

    if orig_dct["field"] == "id":
        new_dct = deepcopy(orig_dct)
        new_dct["field"] = "reference"
        a_opts[name] = new_dct
        return a_opts, name
    else:
        return a_opts, None



def _expand_grouping(a_opts, field_types):

    extra_fields = []

    # go thru the groupby args and expand as needed
    if "groupBy" in a_opts:
        for idx in range(len(a_opts["groupBy"])):
            a_opts, new_field = _add_group_ref(a_opts, "groupBy", idx)
            if new_field:
                extra_fields.append(new_field)

    # go thru the field_types[group] and expand as needed (ignore per and timesries)
    for field in field_types["group"]:
        if field not in ["per", "timeseries"]:
            a_opts, new_field = _add_group_ref(a_opts, field)
            if new_field:
                extra_fields.append(new_field)

    field_types["group"].extend(extra_fields)

    return a_opts, field_types



def single_ring_analysis(s_opts, a_opts, ring, extractor, targetEntity, sess, db):
    # Analysis over only one ring
    '''
    Returns results dictionary, format:
    {
        "results": ,
        "field_names": ,
        "field_types": ,
        "units": {
            "results": 
        }
        "entity_counts": {
                ""
        }
    }
    '''

    op = a_opts["op"]
    field_types = {
        "group": ["per", "timeseries"],
        "target": ["target"]
    }

    if OPS[op]["type"] == "complex":
        addit_groups = [field for field, dct in OPS[op]["fields"].items() if dct["fieldType"] == "group" and field not in field_types["group"]]
        addit_target = [field for field, dct in OPS[op]["fields"].items() if dct["fieldType"] == "target" and field not in field_types["target"]]
        field_types["group"].extend(addit_groups)
        field_types["target"].extend(addit_target)

        a_opts, field_types = _expand_grouping(a_opts, field_types)

        results, new_opts, field_names, col_names, units = complex_operation(s_opts, a_opts, ring, extractor, targetEntity, sess, db, field_types)
        a_opts = new_opts
        
        results.update({"field_names": field_names, "field_types": col_names, "units": units})

    else:
        a_opts, field_types = _expand_grouping(a_opts, field_types)
        if OPS[op]["type"] == "simple":
            new_a_opts = OPS[op]["queryPrep"](s_opts, a_opts, targetEntity)[1]
            query, groupby_args, field_names, col_names = simple_query(s_opts, new_a_opts, ring, extractor, targetEntity, sess, db, field_types)
            query = query.group_by(*groupby_args) if len(groupby_args) else query
        elif OPS[op]["type"] == "recursive":
            query, field_names, col_names = recursive_query(s_opts, a_opts, ring, extractor, targetEntity, sess, db, field_types)
        else:
            print("unclear waht op")
            exit()
        units = get_units(a_opts, extractor, field_types, field_names, col_names)
        results = {"results": [list(q) for q in query.all()], "field_names": field_names, "field_types": col_names, "units": {"results": units}}
        
    results.update({
        "entity_counts": row_count_query(s_opts, a_opts, ring, extractor, targetEntity, sess, db, field_types),
    })
    results["field_names"] = [_entity_from_name(col_name) for col_name in results["field_names"]]

    return results


def complex_operation(s_opts, a_opts, ring, extractor, targetEntity, session, db, field_types):
    '''
    Complex operation defined via analysis plugins
    More on these in the analysis_plugins folder
    
    '''
    op_name = a_opts["op"]

    # What to query/Query translation
    new_a_opts = OPS[op_name]["queryPrep"](s_opts, a_opts, targetEntity)[1]
    for key in field_types["target"]:
        if key in new_a_opts and "extra" not in new_a_opts[key]:
            new_a_opts[key]["extra"] = {}

    query, group_args, field_names, col_names = simple_query(s_opts, new_a_opts, ring, extractor, targetEntity, session, db, field_types)
    query = query.group_by(*group_args)
    results = [list(q) for q in query.all()]

    # What to do with the queries results and formatting
    results, new_field_names, new_col_names = OPS[op_name]["pandasFunc"]["op"](new_a_opts, results, group_args, field_names, col_names)

    # Do units
    units = get_units(new_a_opts, extractor, field_types, field_names, col_names)
    if "unitsPrep" in OPS[op_name]:
        units = OPS[op_name]["unitsPrep"](a_opts, field_names, col_names, units)

    return results, new_a_opts, new_field_names, new_col_names, units


def recursive_query(s_opts, a_opts, ring, extractor, targetEntity, session, db, field_types):
    '''
    averageCount and averageSum operations
    Could expand to other "recursive" operations (e.g. min average)
    requires a "per" field in a_opts
    '''

    copy_opts = deepcopy(a_opts)
    copy_opts["op"] = "count" if a_opts["op"] == "averageCount" else "sum"
    copy_opts = OPS[copy_opts["op"]]["queryPrep"](s_opts, copy_opts, targetEntity)[1]
    query, query_args, field_names, col_names = simple_query(s_opts, copy_opts, ring, extractor, targetEntity, session, db, field_types)

    query = query.group_by(*query_args)

    # Build new query arguments
    s_query = query.subquery()
    query_args = [s_query.c[arg] for arg in query_args]

    # Modifies col_names and field_names to account for new recursive field
    idx = col_names.index("target")
    per_idx = col_names.index("per")
    col_names[idx] = "target/per"
    field_names[idx] = _make_recursive_name(field_names[idx], field_names[per_idx], "average")
    print(field_names)
    # field_names[idx] + "/" + field_names[per_idx]

    # Removes the per field from the query and col/field list
    for lst in [query_args, col_names, field_names]:
        del lst[per_idx]

    # Run query
    avg = func.avg(s_query.c[_name(a_opts["target"]["entity"], a_opts["target"]["field"], copy_opts["op"])])
    if a_opts.get("extra",{}).get("rounding", extractor.getRounding()) != "False":
        field = func.round(avg, a_opts.get("extra",{}).get("sigfigs", extractor.getSigFigs()))

    query_args.append(field)
    query = session.query(*query_args)

    # Build new group args by removing the new target field
    query_args.pop() 
    query = query.group_by(*query_args) if query_args else query

    return query, field_names, col_names


def simple_query(s_opts, a_opts, ring, extractor, targetEntity, session, db, field_types):
    '''
    Main querying function use for simple, recursive, and complex analyses
    Queries from db, filters, and joins
    Returns runnable query, grouping arguments, and column/field names
    '''

    # Query fields
    q_args, tables, entity_ids, entity_names, field_names, col_names = _prep_query(a_opts, extractor, db, field_types=field_types)
    query = session.query(*q_args)

    # Do filtering
    query = _do_filters(query, s_opts, ring, extractor, targetEntity, field_names, session, db)

    # Do joins
    query = _do_joins(query, tables, a_opts["relationships"] if "relationships" in a_opts else [], extractor, targetEntity, db)

    # ADD THE UNIQUE CONSTRAINT ON THE TUPLES OF THE IMPORTANT FIELDS IDS
    # PENDING: this gives errors. right now will not add entity ids
    # query = query.distinct(*entity_ids)

    group_args = [_name(d["entity"], d["field"], transform=d.get("transform")) for d in a_opts["groupBy"]] if "groupBy" in a_opts else []
    for field in field_types["group"]:
        if field in a_opts:
             group_args.append(_name(a_opts[field]["entity"], a_opts[field]["field"], transform=a_opts[field].get("transform"), date_transform=a_opts[field].get("dateTransform")))

     # Modify field_names so that it also return type of field
    return query, group_args, field_names, col_names


def row_count_query(s_opts, a_opts, ring, extractor, targetEntity, session, db, field_types):
    '''
    Queries to count entities
    Basically like a simple query but for the purpose of counting entities
    '''    

    # Query fields
    a_opts = deepcopy(a_opts)
    a_opts["op"] = "None"
    for key in field_types["target"]:
        if key in a_opts:
            a_opts[key].pop('op', None)
            a_opts[key]["op"] = "None"
            if "extra" not in a_opts[key]:
                a_opts[key]["extra"] = {}

    a_opts, _ = _remove_duplicate_vals(a_opts)

    q_args, tables, entity_ids, entity_names, field_names, col_names = _prep_query(a_opts, extractor, db, field_types=field_types, counts=True)
    query = session.query(*q_args)

    # Do filtering
    query = _do_filters(query, s_opts, ring, extractor, targetEntity, field_names, session, db)

    # Do joins
    query = _do_joins(query, tables, a_opts["relationships"] if "relationships" in a_opts else [], extractor, targetEntity, db)

    # Count entities
    db_type = extractor.getDBType()
    entity_counts = count_entities(query, entity_ids, field_names, db_type)

    return entity_counts

def get_units(a_opts, extractor, field_types, field_names, col_names):
    '''
    Returns units for each field in field_names/col_names
    '''

    def _get_field_units(entity, attribute, transform=None):
        # returns the field from entity in ring
        entity_dict = extractor.resolveEntity(entity)[1]
        if attribute not in ["id", "reference"]:
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


    # Get units for each field type
    units_dict = {}
    for key in field_types["group"]:
        if key in a_opts:
            units_dict[key] = _get_field_units(a_opts[key]["entity"], a_opts[key]["field"], a_opts[key].get("transform", None))

    if "groupBy" in a_opts:
        units_dict["groupBy"] = [_get_field_units(group["entity"], group["field"], group.get("transform", None)) for group in a_opts["groupBy"]]

    for key in field_types["target"]:
        if key in a_opts:
            unit = _get_field_units(a_opts[key]["entity"], a_opts[key]["field"], a_opts[key].get("transform", None))
            if "op" in a_opts[key]:
                unit = _apply_op_units(unit, a_opts[key]["op"])
            units_dict[key] = unit


    # Create the final list
    units_lst = []
    for col in col_names:
        if col == "target/per":
            units_lst.append(units_dict["target"] + '/' + units_dict["per"])
        elif col[0:7] == "groupBy" and col[-3:] != "ref":
            idx = int(col[7:])
            units_lst.append(units_dict["groupBy"][idx])
        else:
            units_lst.append(units_dict[col])

    return units_lst


def _prep_query(a_opts, extractor, db, field_types, counts=False):
    # Called to query across a (or multiple) computed values
    print(a_opts)

    q_args = []
    tables = []
    field_names = []
    unique_entities = []
    col_names = []

    # Get groupby fields
    groupby_fields = deepcopy(a_opts["groupBy"]) if "groupBy" in a_opts else []

    col_names = ["groupBy" + str(idx) for idx, val in enumerate(groupby_fields)]


    # TODO: Modify this so it accounts for lists as well (eventually ,not needed now i think)
    for field in field_types["group"]:
        if field in a_opts:
            groupby_fields.append(a_opts[field])
            col_names.append(field)

    for group in groupby_fields:
        the_field, name = _get(extractor, group["entity"], group["field"], db,
                                transform=group.get("transform", None), date_transform=group.get("dateTransform", None))
        q_args.append(the_field)
        field_names.append(name)
        table = _get_table_name(extractor, group["entity"], group["field"])
        if table not in tables:
            tables.append(table)
        if group["entity"] not in unique_entities:
            unique_entities.append(group["entity"])

        
    target_fields = []
    for targ in field_types["target"]:
        if targ in a_opts:
            target = deepcopy(a_opts[targ])             
            target_fields.append(target)
            col_names.append(targ)

    print(a_opts)
    print(target_fields)
    for target in target_fields:
        print(target.get("op", "None"))

        the_field, field_name = _get(extractor, target["entity"], target["field"], db, op=target.get("op", "None"),
                                        transform=target.get("transform", None), extra=target["extra"])
        q_args.append(the_field.label(field_name))
        field_names.append(field_name)
        table = _get_table_name(extractor, target["entity"], target["field"])
        if table not in tables:
            tables.append(table)
        if target["entity"] not in unique_entities:
            unique_entities.append(target["entity"])
      
    # Query the IDs of the entities we care about (as well as the nice name stuff for them)
    entity_ids = []
    for entity in unique_entities:
        the_field, name = _get(extractor, entity, "id", db)
        entity_ids.append(name)
        if name not in field_names and counts:
            q_args.append(the_field)
            field_names.append(name)
            col_names.append(name + "_id")

    return q_args, tables, entity_ids, unique_entities, field_names, col_names


# PENDING: Finish filling this out for prefilters
# PENDING: do counts before/after filtering?
def _do_filters(query, s_opts, ring, extractor, targetEntity, col_names, session, db):
    '''
    Adding filters
    '''

    # do prefilters
    pass
    # query = query.filter(_get(extractor, targetEntity, "id", db) != None)

    # do normal filters
    if s_opts:
        query = rawGetResultSet(s_opts, ring, extractor, targetEntity, targetRange=None, simpleResults=True, just_query=True, sess=session, query=query)

    # do nan filtering
    for name in col_names:
        param_dct = _entity_from_name(name)
        entity_dict = extractor.resolveEntity(param_dct["entity"])[1]
        if param_dct["field"] not in  ["id", "reference"]:
            attr_obj = [attr for attr in entity_dict.attributes if attr.name == param_dct["field"]][0]
        else:
            attr_obj = None
        if attr_obj:
            if attr_obj.nullHandling and attr_obj.nullHandling == "ignore":
                
                # NOTE: we might need to do something fancy here in case 
                # e.g. if the queried field is avg(amount), i think we have to filter by amount != None
                # meaning we cant directly use the name of the object, so we have to
                # again get the "real" field and filter off of that
                # PENDING: might have issues since there could be multiple fields with same name
                field, name = _get(extractor, param_dct["entity"], param_dct["field"], db)
                query = query.filter(field != None)

    return query


# PENDING: Should be stress tested, tested more
# PENDING: Should try to share this with seekers as much as possible
# PENDING: have yet to test the case where single entity across multiple tables
# PENDING: have yet to test teh case where multiple entities in one table (tho it should be fine)
# PENDING: test the case with derived person entity
def _do_joins(query, tables, relationships, extractor, targetEntity, db):
    '''
    Do joins, if needed
    We have a list of SQL tables that we are using, as well as the needed relationships
    to conduct those joins.
    '''

    joined_tables = set()
    p_table = _get_table_name(extractor, targetEntity, "id")
    primary_bool = any(table == p_table for table in tables)


    def do_join(query, path, added_tables=set()):
        # Given a query, a path, and a 
        # For now we are assuming one and only one of the tables in the path is not in added_tables
        if path[0].split(".")[0] not in added_tables :
            the_table = path[0].split(".")[0]
            indices = [0,1]
        else:
            the_table = path[1].split(".")[0]
            indices = [1,0]
        return query.join(getattr(db, the_table),
                            _get_join_field(path[indices[0]], db) == _get_join_field(path[indices[1]], db),
                            isouter=True
                            ), the_table

    rel_items = [extractor.resolveRelationship(rel)[1] for rel in relationships]

    if not primary_bool:
        # If the primary table is not in the queried fields,
        # Then we will execute the joins that are connected from a queried table to the primary table
        
        def rel_contains_entity_table(rel_item, entity, table):
            # Check if relationship has a join, and has the entity and table given
            # (might be superfluous to check entity)
            if rel_item.fro != entity and rel_item.to != entity:
                return False
            if not len(rel_item.join):
                return False
            join = extractor.resolveJoin(rel_item.join[0])[1]
            if join.from_ != table and join.to != table:
                return False
            return True
        
        # Go over the relationships, find the one(s) that link back to the target table
        rels = [rel for rel in rel_items if rel_contains_entity_table(rel, targetEntity, p_table)]

        # TODO: change this in case there are multiple joins
        # Iterate thru these and join them
        for rel_item in rels:
            for join_item in rel_item.join:
                join = extractor.resolveJoin(join_item)[1]

                if join.from_ in tables:
                    joined_tables.add(join.from_)
                elif join.to in tables:
                    joined_tables.add(join.to)
                else:
                    print("uhhhh hey bruh, something happened")
                for path in join.path:
                    query, add_table = do_join(query, path, joined_tables)
                    joined_tables.add(add_table)

        # update the list of relations to join to to remove the ones we already joined to
        rel_items = [rel for rel in rel_items if not rel_contains_entity_table(rel, targetEntity, p_table)]

    else:
        joined_tables.add(_get_table_name(extractor, targetEntity, "id"))


    # for the pending relationships, join them
    for rel_item in rel_items:
        for join_item in rel_item.join:
            join = extractor.resolveJoin(join_item)[1]
            for path in join.path:
                print(path)
                query, add_table = do_join(query, path, joined_tables)
                joined_tables.add(add_table)

    return query

# PENDING: Do this
# PNEDING: See if we actually need to do this, not sure if we do
def _format_results():
    # make human legible

    '''
    Do the dictionary mapping if needed (no longer need to query stuff from original db hopefully)

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



def _parse_ref_string(ref_str):
    # right now we assume ref_str is properly formed
    # AND that there arent any other $, } in the string other than
    # for attributes

    val_lst = []
    type_lst = []

    idx = 0
    is_attr = False
    val_start_idx = 0

    while idx < len(ref_str):
        if ref_str[idx] == "$":
            if idx > val_start_idx:
                val_lst.append(ref_str[val_start_idx:idx])
                type_lst.append(is_attr)

            is_attr = True
            val_start_idx = idx
        elif ref_str[idx] == "}":

            val_lst.append(ref_str[val_start_idx:idx+1])
            type_lst.append(is_attr)
            is_attr = False
            val_start_idx = idx + 1

        else:
            pass
        idx += 1

    if val_start_idx < len(ref_str):
        val_lst.append(ref_str[val_start_idx:len(ref_str)])
        type_lst.append(is_attr)

    return val_lst, type_lst


# NOTE: we are assuming here that ALL fields in that reference
# will belong to the entity
def _parse_reference(extractor, entity, db):

    entity_dict = extractor.resolveEntity(entity)[1]
    ref = entity_dict.reference
    val_lst, type_lst = _parse_ref_string(ref)

    print(val_lst)
    print(type_lst)

    concatenable = []
    for val, tpe in zip(val_lst, type_lst):
        if tpe:
            field, _ = _get_helper(extractor, entity, val[2:-1], db, None, None, None, None)
            concatenable.append(field)
        else:
            concatenable.append(val)

    name = _name(entity, "reference", None, None)
    print(concatenable)
    print(sql_concat(concatenable, extractor.getDBType()))
    return sql_concat(concatenable, extractor.getDBType()).label(name), name



# PENDING: handling multiple columns in the columns of source
# PENDING: Add "Conversion" to pretty name here (e.g. True/False to other stuff)
def _get(extractor, entity, attribute, db, transform=None, date_transform=None, op=None, extra=None):
    '''
    Returns the field from entity in ring, and the label of the field
    '''
    if attribute == "reference":
        return _parse_reference(extractor, entity, db)
    else:
        return _get_helper(extractor, entity, attribute, db ,transform, date_transform, op, extra)

# def _get_and_query_append(flag, q_args, field_names, extractor, entity, attribute, db, transform=None, date_transform=None, op=None):
#     ## flag will either be "group, or the target that is passed"
#     the_field, field_name = _get_helper(extractor, entity, attribute, db ,transform, date_transform, op)
#     if flag == "group": 
#         q_args.append(the_field)
#         field_names.append(name)
#     else: 
#         target = flag
#         entity_dict = extractor.resolveEntity(entity)[1]
#         attr_obj = [attr for attr in entity_dict.attributes if attr.name == attribute][0]
        
#         if attr_obj.rounding == "True":
#             q_args.append(func.round(OPS[target["op"]]["funcDict"]["op"](the_field, extractor.getDBType(), target["extra"]).label(field_name), attr_obj.sig_figs))
#         else:
#             q_args.append(OPS[target["op"]]["funcDict"]["op"](the_field, extractor.getDBType(), target["extra"]).label(field_name))
        
#         field_names.append(field_name)
#     return q_args, field_names

def _get_helper(extractor, entity, attribute, db, transform, date_transform, op, extra):
    '''
    Returns the field from entity in ring, and the label of the field
    '''
    entity_dict = extractor.resolveEntity(entity)[1]
    if attribute != "id":
        attr_obj = [attr for attr in entity_dict.attributes if attr.name == attribute][0]
        model_name = attr_obj.source_table
        field_name = attr_obj.source_columns[0]
        if date_transform:
            field_name = field_name + "_" + date_transform
    else:
        model_name = entity_dict.table
        field_name = entity_dict.id[0]
        attr_obj = None

    model = getattr(db, model_name)
    field = getattr(model, field_name)
    name = _name(entity, attribute, op, transform, date_transform)

    # Get null handling
    if attr_obj and attr_obj.nullHandling and attr_obj.nullHandling == "cast":
        field = _nan_cast(field, attr_obj.nullValue)

    # Do operation if it is available
    if op:
        print("op true")
        field = OPS[op]["funcDict"]["op"](field, extractor.getDBType(), extra)

    if attr_obj:
        # Round if object has rounding
        if attr_obj.rounding == "True":
            print("rounding true")
            field = func.round(field, attr_obj.sig_figs)

        if op == "percentage" and extra.get("rounding", extractor.getRounding()) != "False":
            field = func.round(field, extra.get("sigfigs", extractor.getSigFigs()))


    if transform:
        field = TRS[transform]["processor"](field)

    return field.label(name), name


def _get_table_name(extractor, entity, attribute):
    '''
    Returns the table name correponding to an entity's attribute in the ring
    '''
    # returns the table corresponding to an entity in ring   
    entity_dict = extractor.resolveEntity(entity)[1]
    if attribute not in  ["id", "reference"]:
        attr_obj = [attr for attr in entity_dict.attributes if attr.name == attribute][0]
        model_name = attr_obj.source_table
    else:
        model_name = entity_dict.table
    return model_name

def _nan_cast(field, cast_val):
    '''
    Casts a field in case it is a null value
    '''
    return case([(field == None, cast_val)], else_=field)

'''

Pending Stuff (1/18/2022):



- Querying niceName from id columns when grouping by id column

- Querying multiple columns for one attribute
    e.g. date: year, month, day
    e.g. name: first, middle, last

- Defining JSON with default behaviors (all of these can be added to config attributes)
    - Rounding figures
    - Date and datetime granularities
    - True/False conversion/format
    - Null dropping/converting behavior
    - Multiple column behavior
        - concatenate by default for all
            - Might need to cast all to string first if not already string
        
- Implement stuff to utilize the JSON
    - Might just use it at compile time and add it to compiled ring?



Next steps (11/26/2021)

Pending stuff
- Analysis plugins
    - Restructure so that each analysis plugin has their own folder
        - In each folder, brainstorm how to do it such that
            - We have test cases
            - We have some way of communicating what format we want to do
            - We communicate what kind of visualization we want to do
    - Create a pipy or whatevs repo for our own analysis plugins
    - Figure out how to import that repo to our own satyrn stuff
- Handling the case better of whether we have already built the file for CSV/Kafarella
- Distinct key checking for when multiple joins
- Transform
    - Add test case where in config we have ae defined inequalities thingy



Querying "Nice Name"
e.g. suppose you want to query "average contribution grouped by contributor"
The database is gonna get it by contributor.id, but in reality we want to have it by contributor.name
- QFA: 
    would we just use the "reference" field?
        I think for contribution its "amount" which might be weird
    are we committing to the format?
        Not a huge issue, we can change it later, just wanted to know
    would we still want to return the "ugly" rep? (i.e. contribution.id)
        I'm leaning towards yes


Querying multiple columns
e.g. "count of cases grouped by judge name"
e.g. "count of cases timeseries by date filed"
- For judge name
    - I guess we would just query everything as a concatenation?
- For date filed example (e.g. year, month, day)
    - Here I think we would just use the methods we created in the compiler
- QFA:
    - Above sound good?
    - NOTE: if there are some attributes that have multiple fields that would NOT work as a simple concatenation
    then I think we might need to rethink this
        - Some potential examples
            - Nature of Suit: has multiple categories/granularities. Would we always wanna show all of them?


Transform stuff into "nice name"
e.g. "average contribution by in state status"
It'll return grouped by True/False, but maybe we wanted it formatted differently
- One option is to list out in the config what each value should transform to
- A default migth be to just have strings like "In State Status equals to True"
- We can perform this transformation at the query level or as postprocessing formatting
    - Can do at query or postprocessing (im leaning towards at query for efficienty)
    - Note: we might require flaggingthat we want it transformed to nice names or something
        Otherwise, we might be locked into always getting the nicename of stuff when
        sometimes we might want it to be in its "raw" form (or some analysis plugins might want it raw)
- QFA: get his thoughts on it


Standardizing date manager stuff
- I was thinking of just putting it all in a file called datemanager.py
    - For each datatype (date, datetime), it lists out the default granularities
    - For each fo these, it also lists out the possible "Transformations"]
        e.g. onlymonth, onlyday, dayofweek, full date, full datetime
-QFA: 
    - where would I put this file? This would be used mostly by the compiler and the statement generator


FOOTNOTE: something andrew made up bout two time frames on a single thing


'''