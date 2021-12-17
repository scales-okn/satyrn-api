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
from .utils import _get_join_field, _name, _outerjoin_name, _entity_from_name

# PREFILTERS = current_app.satConf.preFilters

cache = current_app.cache
CACHE_TIMEOUT=3000


def run_analysis(s_opts, a_opts, targetEntity):
    '''
    Callable function for the views API
    Theoretically no other function should be called from here by views
    Takes:
        s_opts: searching opts
        a_opts: analysis opts
        targetEntity: searchable entity
    Returns the following:
    {
        results: [(), ]
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
    def _get_db_sess(ringId):
        db = current_app.rings[ringId].db
        sess = db.Session()
        return db, sess

    # If across two rings, do a different path
    if len(a_opts["rings"]) > 1:
        analysis_across_two_rings(s_opts, a_opts)
    else:
        db, sess  = _get_db_sess(a_opts["rings"][0])
        return single_ring_analysis(s_opts, a_opts, a_opts["rings"][0], targetEntity, sess, db)


def single_ring_analysis(s_opts, a_opts, ringId, targetEntity, sess, db):
    # Analysis over only one ring

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
        results, new_opts, field_names, col_names, units = complex_operation(s_opts, a_opts, ringId, targetEntity, sess, db, field_types)
        a_opts = new_opts
        # print("new opts")
        # print(a_opts)
        
        results.update({"field_names": field_names, "field_types": col_names, "units": units})

    else:
        if OPS[op]["type"] == "simple":
            new_a_opts = OPS[op]["queryPrep"](s_opts, a_opts, targetEntity)[1]
            query, groupby_args, field_names, col_names = simple_query(s_opts, new_a_opts, ringId, targetEntity, sess, db, field_types)
            query = query.group_by(*groupby_args) if len(groupby_args) else query
        elif OPS[op]["type"] == "recursive":
            query, field_names, col_names = recursive_query(s_opts, a_opts, ringId, targetEntity, sess, db, field_types)
        else:
            print("unclear waht op")
            exit()
        units = get_units(a_opts, ringId, field_types, field_names, col_names)
        results = {"results": query.all(), "field_names": field_names, "field_types": col_names, "units": {"results": units}}
        
    # print("gonna start rocount")
    results.update({
        "entity_counts": row_count_query(s_opts, a_opts, ringId, targetEntity, sess, db, field_types),
    })

    return results


def complex_operation(s_opts, a_opts, ringId, targetEntity, session, db, field_types):
    '''
    Complex operation (distribution, correlationm comparison)

    This would be where there would be space for analysis plugins

    Potentially three main parts that others would define:
    1. What to query
        e.g. with correlation having two targets, querying these
    2. What to do with the queries results
        e.g. with correlation, calculating correlation score
    3. Formatting (if any)
        e.g. with correlation, decimal rounding for the score
             with correaltion, returning score in addition to the raw results
    
    '''
    op_name = a_opts["op"]

    # 1. what to query
    new_a_opts = OPS[op_name]["queryPrep"](s_opts, a_opts, targetEntity)[1]
    for key in field_types["target"]:
        if key in new_a_opts and "extra" not in new_a_opts[key]:
            new_a_opts[key]["extra"] = {}

    # print(new_a_opts)

    query, group_args, field_names, col_names = simple_query(s_opts, new_a_opts, ringId, targetEntity, session, db, field_types)
    query = query.group_by(*group_args)
    results = query.all()

    # 2. and 3. What to do with the queries results and formatting
    results, field_names, col_names = OPS[op_name]["pandasFunc"]["op"](new_a_opts, results, group_args, field_names, col_names)

    # Do units
    init_units = get_units(new_a_opts, ringId, field_types, field_names, col_names)
    units = OPS[op_name]["unitsPrep"](a_opts, field_names, col_names, init_units)


    return results, new_a_opts, field_names, col_names, units


def recursive_query(s_opts, a_opts, ringId, targetEntity, session, db, field_types):
    '''
    averageCount and averageSum operations
    Could expand to other "recursive" operations (e.g. min average)
    requires a "per" field in a_opts
    '''

    copy_opts = deepcopy(a_opts)
    copy_opts["op"] = "count" if a_opts["op"] == "averageCount" else "sum"
    copy_opts = OPS[copy_opts["op"]]["queryPrep"](s_opts, copy_opts, targetEntity)[1]
    query, query_args, field_names, col_names = simple_query(s_opts, copy_opts, ringId, targetEntity, session, db, field_types)

    query = query.group_by(*query_args)

    # Build new query arguments
    s_query = query.subquery()
    query_args = [s_query.c[arg] for arg in query_args]

    # Modifies col_names and field_names to account for new recursive field
    idx = col_names.index("target")
    per_idx = col_names.index("per")
    col_names[idx] = "target/per"
    field_names[idx] = field_names[idx] + "/" + field_names[per_idx]

    # Removes the per field from the query and col/field list
    for lst in [query_args, col_names, field_names]:
        del lst[per_idx]

    # Run query
    query_args.append(func.avg(s_query.c[_name(ringId, a_opts["target"]["entity"], a_opts["target"]["field"], copy_opts["op"])]))
    query = session.query(*query_args)

    # Build new group args by removing the new target field
    query_args.pop() 
    query = query.group_by(*query_args) if query_args else query

    return query, field_names, col_names


def simple_query(s_opts, a_opts, ringId, targetEntity, session, db, field_types):
    '''
    Main querying function use for simple, recursive, and complex analyses
    Queries from db, filters, and joins
    Returns runnable query, grouping arguments, and column/field names
    '''

    # Query fields
    q_args, tables, entity_ids, entity_names, field_names, col_names = _prep_query(a_opts, ringId, db, field_types=field_types)
    query = session.query(*q_args)

    # Do filtering
    query = _do_filters(query, s_opts, ringId, targetEntity, field_names, session, db)

    # Do joins
    query = _do_joins(query, tables, a_opts["relationships"] if "relationships" in a_opts else [], ringId, targetEntity, db)

    # ADD THE UNIQUE CONSTRAINT ON THE TUPLES OF THE IMPORTANT FIELDS IDS
    # PENDING: this gives errors. right now will not add entity ids
    # query = query.distinct(*entity_ids)

    group_args = [_name(ringId, d["entity"], d["field"], transform=d.get("transform")) for d in a_opts["groupBy"]] if "groupBy" in a_opts else []
    for field in field_types["group"]:
        if field in a_opts:
             group_args.append(_name(ringId, a_opts[field]["entity"], a_opts[field]["field"], transform=a_opts[field].get("transform")))

     # Modify field_names so that it also return type of field
    return query, group_args, field_names, col_names



def _remove_duplicates(a_opts):
    repeat_keys = [[k for k in a_opts if a_opts[k] == v] for v in a_opts.values()]
    repeat_keys = [lst for lst in repeat_keys if len(lst) > 1]
    repeat_keys.sort()
    print(repeat_keys)
    del_keys = []
    if repeat_keys:
        last = repeat_keys[-1]
        for i in range(len(repeat_keys)-2, -1, -1):
            if last == repeat_keys[i]:
                del repeat_keys[i]
            else:
                last = repeat_keys[i]
        print(repeat_keys)
        
        for repeat_lst in repeat_keys:
            kept_key = repeat_lst[0]
            for repeat_val in repeat_lst[1:]:
                del_keys.append((repeat_val, a_opts[repeat_val]))
                del a_opts[repeat_val]

    return a_opts, del_keys


def row_count_query(s_opts, a_opts, ringId, targetEntity, session, db, field_types):
    '''
    Queries to count entities
    Basically like a simple query but for the purpose of counting entities
    Currently done in the most inefficient way possible
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
    for key in ["numerator", "numerator2"]:
        a_opts.pop(key, None)

    print(a_opts)
    a_opts, del_keys = _remove_duplicates(a_opts)
    print(a_opts)
    print(del_keys)

    q_args, tables, entity_ids, entity_names, field_names, col_names = _prep_query(a_opts, ringId, db, field_types=field_types, counts=True)
    query = session.query(*q_args)

    # Do filtering
    query = _do_filters(query, s_opts, ringId, targetEntity, field_names, session, db)

    # Do joins
    query = _do_joins(query, tables, a_opts["relationships"] if "relationships" in a_opts else [], ringId, targetEntity, db)

    # Count entities
    entity_counts = {}
    for entity, name in zip(entity_ids, entity_names):
        entity_counts[entity] = query.distinct(entity).count()

    # df = DataFrame(query.all(), columns=field_names).nunique()
    # entity_counts = {}
    # for entity, name in zip(entity_ids, entity_names):
    #     entity_counts[entity] = df[entity]

    return entity_counts

def get_units(a_opts, ringId, field_types, field_names, col_names):
    '''
    Returns units for each field in field_names/col_names
    '''

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


    # Get units for each field type
    units_dict = {}
    for key in field_types["group"]:
        if key in a_opts:
            units_dict[key] = _get_field_units(a_opts[key]["entity"], a_opts[key]["field"], a_opts[key].get("transform", None))

    if "groupBy" in a_opts:
        units_dict["groupBy"] = [_get_field_units(group["entity"], group["field"], group.get("transform, None")) for group in a_opts["groupBy"]]

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
        elif col[0:7] == "groupBy":
            idx = int(col[7:])
            units_lst.append(units_dict["groupBy"][idx])
        else:
            units_lst.append(units_dict[col])

    return units_lst



# PENDING: Add transforms (d: gotta define space of possible transforms to have available transformations)
# PENDING: Add "nice" representation for groupby values (for nice names and stuff) (d: )
def _prep_query(a_opts, ringId, db, field_types, counts=False):
    # Called to query across a (or multiple) computed values

    q_args = []
    tables = []
    col_names = []
    unique_entities = []
    col_fields = []

    # Get groupby fields
    groupby_fields = deepcopy(a_opts["groupBy"]) if "groupBy" in a_opts else []
    col_fields = ["groupBy" + str(idx) for idx, val in enumerate(groupby_fields)]

    for field in field_types["group"]:
        if field in a_opts:
            groupby_fields.append(a_opts[field])
            col_fields.append(field)

    for group in groupby_fields:
        # TODO : add transform here to _get
        the_field, name = _get(ringId, group["entity"], group["field"], db,
                                transform=group.get("transform", None), date_transform=group.get("dateTransform", None))
        q_args.append(the_field)
        col_names.append(name)
        table = _get_table_name(ringId, group["entity"], group["field"])
        if table not in tables:
            tables.append(table)
        if group["entity"] not in unique_entities:
            unique_entities.append(group["entity"])

        
    target_fields = []
    for targ in field_types["target"]:
        if targ in a_opts:
            target = deepcopy(a_opts[targ])             
            target_fields.append(target)
            col_fields.append(targ)

    # print(target_fields)
    for target in target_fields:
        the_field, field_name = _get(ringId, target["entity"], target["field"], db, op=target["op"],
                                        transform=target.get("transform", None))
        q_args.append(OPS[target["op"]]["funcDict"]["op"](the_field, target["extra"]).label(field_name))
        col_names.append(field_name)
        table = _get_table_name(ringId, target["entity"], target["field"])
        if table not in tables:
            tables.append(table)
        if target["entity"] not in unique_entities:
            unique_entities.append(target["entity"])
      
    # Query the IDs of the entities we care about (as well as the nice name stuff for them)
    entity_ids = []
    for entity in unique_entities:
        the_field, name = _get(ringId, entity, "id", db)
        entity_ids.append(name)
        if name not in col_names and counts:
            q_args.append(the_field)
            col_names.append(name)
            col_fields.append(name + "_id")

    return q_args, tables, entity_ids, unique_entities, col_names, col_fields


# PENDING: Finish filling this out for prefilters
def _do_filters(query, s_opts, ringId, targetEntity, col_names, session, db):
    '''
    Adding filters
    '''

    # do prefilters

    query = query.filter(_get(ringId, targetEntity, "id", db) != None)

    # do normal filters
    if s_opts:
        query = rawGetResultSet(s_opts, ringId, targetEntity, targetRange=None, simpleResults=True, just_query=True, sess=session, query=query)

    # do nan filtering
    # Need to get a list of all the attributes that are being used in the query
    # For each of these, get the attr_obj using something like the following

    for name in col_names:
        param_dct = _entity_from_name(name)
        # print(param_dct)
        entity_dict = current_app.ringExtractors[ringId].resolveEntity(param_dct["entity"])[1]
        if param_dct["attribute"] != "id":
            attr_obj = [attr for attr in entity_dict.attributes if attr.name == param_dct["attribute"]][0]
        else:
            attr_obj = None
        if attr_obj:
            if attr_obj.nullHandling and attr_obj.nullHandling == "ignore":
                
                # NOTE: we might need to do something fancy here in case 
                # e.g. if the queried field is avg(amount), i think we have to filter by amount != None
                # meaning we cant directly use the name of the object, so we have to
                # again get the "real" field and filter off of that
                # PENDING: might have issues since there could be multiple fields with same name
                field, name = _get(ringId, param_dct["entity"], param_dct["attribute"], db)
                query = query.filter(field != None)



    '''
    entity_dict = current_app.ringExtractors[ringId].resolveEntity(entity)[1]
    if attribute != "id":
        attr_obj = [attr for attr in entity_dict.attributes if attr.name == attribute][0]

    if attr_obj:
        if attr_obj.nullHandling and attr_obj.nullHandling == "cast":
            field = _nan_cast(field, attr_obj.nullValue)
    '''
    # Then, if nullhandling == drop, do a line like below
    '''
        # Do nan dropping if needed
        for field in fields_list:
            if AMS[field]["nulls"] == "ignore": # and not (AMS[field]["type"] in ["float", "int", "datetime"] and "transform" in AMS[field]):
                query = query.filter(getattr(AMS[field]["model"], AMS[field]["field"]) != None)
                if count_bool:
                    counts.append({"filter": field, "rowCount": query.count(), "type": "nullIgnore"})
    '''

    return query


# PENDING: Should be stress tested, tested more
# PENDING: Should try to share this with seekers as much as possible
# PENDING: have yet to test the case where single entity across multiple tables
# PENDING: have yet to test teh case where multiple entities in one table (tho it should be fine)
def _do_joins(query, tables, relationships, ringId, targetEntity, db):
    '''
    Do joins, if needed
    We have a list of SQL tables that we are using, as well as the needed relationships
    to conduct those joins.
    '''

    joined_tables = []
    p_table = _get_table_name(ringId, targetEntity, "id")
    primary_bool = any(table == p_table for table in tables)


    def do_join(query, path, added_tables=[]):
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

    rel_items = [current_app.ringExtractors[ringId].resolveRelationship(rel)[1] for rel in relationships]

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
            join = current_app.ringExtractors[ringId].resolveJoin(rel_item.join[0])[1]
            if join.from_ != table and join.to != table:
                return False
            return True
        
        # Go over the relationships, find the one(s) that link back to the target table
        rels = [rel for rel in rel_items if rel_contains_entity_table(rel, targetEntity, p_table)]

        # Iterate thru these and join them
        for rel_item in rels:
            join = current_app.ringExtractors[ringId].resolveJoin(rel_item.join[0])[1]
            if join.from_ in tables:
                joined_tables.append(join.from_)
            else:
                joined_tables.append(join.to)
            for path in join.path:
                query, add_table = do_join(query, path, joined_tables)
                joined_tables.append(add_table)

        # update the list of relations to join to to remove the ones we already joined to
        rel_items = [rel for rel in rel_items if not rel_contains_entity_table(rel, targetEntity, p_table)]

    else:
        joined_tables.append(_get_table_name(ringId, targetEntity, "id"))


    for rel_item in rel_items:
        if len(rel_item.join):
            join = current_app.ringExtractors[ringId].resolveJoin(rel_item.join[0])[1]
            for path in join.path:
                query, add_table = do_join(query, path, joined_tables)
                joined_tables.append(add_table)

    return query

# PENDING: Do this
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



# PENDING: handling multiple columns in the columns of source
def _get(ringId, entity, attribute, db, transform=None, date_transform=None, op=None):
    '''
    Returns the field from entity in ring, and the label of the field
    '''
    entity_dict = current_app.ringExtractors[ringId].resolveEntity(entity)[1]
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
    name = _name(ringId, entity, attribute, op, transform)

    # Get null handling
    if attr_obj:
        if attr_obj.nullHandling and attr_obj.nullHandling == "cast":
            field = _nan_cast(field, attr_obj.nullValue)

    if transform:
        field = TRS[transform]["processor"](field)

    return field.label(name), name

def _get_table_name(ringId, entity, attribute):
    '''
    Returns the table name correponding to an entity's attribute in the ring
    '''
    # returns the table corresponding to an entity in ring   
    entity_dict = current_app.ringExtractors[ringId].resolveEntity(entity)[1]
    if attribute != "id":
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

Next steps (11/18/2021):

2. Polish off date manager
DONE- Adding to the config options for
DONE    - Granularity
DONE    - Datetime format
DONE- Modifying compiler to handle granularity and datetime format
DONE- Modifying engine to handle granularity and datetime format (should be minimal)
DONE- test
DONE- cast things to strings (also day of week into actual days? or maybe jsut as string as well for now)

4. Transforms
DONEish- Define a couple of transforms in the transforms.py file to show stuff
DONE- Consider cases where there might be a transform AND a granularity param (fine if this does not exist)
DONE- Define some test cases for transforms
DONE- Modify engine to handle transforms
DONE-test

5. Other engine stuff
DONE- More efficient distinct counting

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
- Handling the case better of whether we have already built the file
- Distinct key checking for when multiple joins
- Transform
    - Add test case where in config we have ae defined inequalities thingy


(D: Just check in with andru to make sure this makes sense)
- Querying the "nice name" of things (e.g. contributor.id -> also query contributor.name)
- Querying multiple columns for a single thing
    e.g. date: year, month, day
    e.g. name: first, middle, last
- Formatting
    - Rounding
    - Transforming stuff (e.g. bools into something else)
- Standardizing things for date manager so that they mostly sit in one place (rn its spread out in engine and compiler)



Querying "Nice Name"
e.g. suppose you want to query "average contribution grouped by contributor"
The database is gonna get it by contributor.id, but in reality we want to have it by contributor.name
- Solution woul
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

Rounding:
- Would add something to the config or to defaults
- QFA: sound good?

NOTE: It might be good to have some sort of "defaults" file, with stuff about
- Rounding defaults
- Null default behavior
- Maybe some of the datemanager stuff there as well? or within the same vicinity


Standardizing date manager stuff
- I was thinking of just putting it all in a file called datemanager.py
    - For each datatype (date, datetime), it lists out the default granularities
    - For each fo these, it also lists out the possible "Transformations"]
        e.g. onlymonth, onlyday, dayofweek, full date, full datetime
-QFA: 
    - where would I put this file? This would be used mostly by the compiler and the statement generator



TRansform
- Walk thru what i've done
- Talk about example for inequality that we might want to use



FOOTNOTE: something andrew made up bout two time frames on a single thing

'''