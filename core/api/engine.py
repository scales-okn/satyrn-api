'''
This file is part of Satyrn.
Satyrn is free software: you can redistribute it and/or modify it under 
the terms of the GNU General Public License as published by the Free Software Foundation, 
either version 3 of the License, or (at your option) any later version.
Satyrn is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with Satyrn. 
If not, see <https://www.gnu.org/licenses/>.
'''

# the main file that handles analytics

import re
from copy import deepcopy

from flask import current_app
from sqlalchemy import func


from pandas import DataFrame
from .operations import OPERATION_SPACE as OPS
from .seekers import rawGetResultSet
from .transforms import TRANSFORMS_SPACE as TRS

from . import utils
from . import sql_func


cache = current_app.cache
CACHE_TIMEOUT=3000

@cache.memoize(timeout=CACHE_TIMEOUT)
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

    db, sess  = _get_db_sess(ring)
    return single_ring_analysis(s_opts, a_opts, ring, extractor, targetEntity, sess, db)


def _add_group_ref(a_opts, field_name, pos=None):
    # if given pos, it means the underlying field was a list
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

    # go thru the field_types[group] and expand as needed (ignore per and timeSeries)
    for field in field_types["group"]:
        if field not in ["per", "timeSeries"]:
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
        "group": [field for field, dct in OPS[op]["required"].items() if dct["fieldType"] == "group"],
        "target": [field for field, dct in OPS[op]["required"].items() if dct["fieldType"] == "target"]
    }
    # NOTE: we assume optionals will only be for grouping
    field_types["group"].extend([field for field, dct in OPS[op]["optional"].items() if field != "groupBy"])


    a_opts, field_types = _expand_grouping(a_opts, field_types)
    # TODO: change the field_types to account for required/optional keys
    if OPS[op]["type"] == "complex":
        if "spawned" in OPS[op]:
            addit_groups = [field for field, dct in OPS[op]["spawned"].items() if dct["fieldType"] == "group" and field not in field_types["group"]]
            addit_target = [field for field, dct in OPS[op]["spawned"].items() if dct["fieldType"] == "target" and field not in field_types["target"]]
            field_types["group"].extend(addit_groups)
            field_types["target"].extend(addit_target)

        results, new_opts, field_names, col_names, units = complex_operation(s_opts, a_opts, ring, extractor, targetEntity, sess, db, field_types)
        a_opts = new_opts
        
        results.update({"field_names": field_names, "field_types": col_names, "units": units})

    else:

        if OPS[op]["type"] == "simple":
            new_a_opts = OPS[op]["queryPrep"](s_opts, a_opts, targetEntity)[1]
            query, groupby_args, field_names, col_names = simple_query(s_opts, new_a_opts, ring, extractor, targetEntity, sess, db, field_types)
            query = query.group_by(*groupby_args) if len(groupby_args) else query
        elif OPS[op]["type"] == "recursive":
            query, field_names, col_names = recursive_query(s_opts, a_opts, ring, extractor, targetEntity, sess, db, field_types)
        else:
            print("Unavailable type of operation")
            return None

        # targetInfo = extractor.resolveEntity(targetEntity)[1]
        # query = query.order_by(targetInfo.id[0])
        units = get_units(a_opts, extractor, field_types, field_names, col_names)
        results = {"results": sorted([list(q) for q in query.all()], key=lambda x:x[0]),#assumes x[0] is x-val & that alphabetical is right order
            "field_names": field_names,
            "field_types": col_names,
            "units": {"results": units}}
        # results = {"results":[1,1], "field_names":field_names, "field_types":col_names, "units":{"results":units}} # dummy results for debugs
        
    results.update({
        "entity_counts": row_count_query(s_opts, a_opts, ring, extractor, targetEntity, sess, db, field_types),
    })
    results["field_names"] = [utils._entity_from_name(col_name) for col_name in results["field_names"]]

    # display day/month names instead of numbers
    day_names = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat']
    month_names = ['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'] # empty string bc month indices are 1-12, lol
    if "timeSeries" in results["field_types"]:
        i = results["field_types"].index("timeSeries") # assumes we only need to make this adjustment once
        date_transform = results["field_names"][i].get("dateTransform")
        if date_transform in ("dayofweek", "onlymonth"):
            names_list = day_names if date_transform=="dayofweek" else month_names
            results["results"] = [x[:i] + [names_list[int(x[i])]] + x[i+1:] for x in results["results"]]

    # reformat when planManager should do multiline or grouped bar (planManager itself can change instead, but that'll require dependency rework)
    if results["results"] and len(results["results"][0])>2:
        new_results, labels_seen = [], set()
        for i,result in enumerate(results["results"]):
            if result[0] not in labels_seen:
                labels_seen.add(result[0])
                new_result = {'label': result[0], 'series': [result[1:]]}
                if i < len(results["results"])-1:
                    for other_result in results["results"][i+1:]:
                        if other_result[0] == result[0]:
                            new_result['series'].append(other_result[1:])
                new_results.append(new_result)
        results["results"] = new_results

    return results


def complex_operation(s_opts, a_opts, ring, extractor, targetEntity, session, db, field_types):
    '''
    Complex operation defined via analysis plugins
    More on these in the analysis_plugins folder
    '''
    op_name = a_opts["op"]

    # PENDING: Path for when queryPrep is a list with len > 1 (and consequently pandasFunc len > 1)
    '''
    Basically, the pipeline would be:
    input: original a_opts and everything
        queryPrep1
            return new_a_opts1
        pandasFunc1
        queryPrep2 (that also takes in as an input results from pandasFunc1)
            returns new_a_opts2
        pandasFunc2
        ...
    output: 

    Other (potential) needed changes:
    - row_counts: does this affect how we count rows? Right now it would only count for
    the last new_a_opts
    - units: same as row_count
    '''

    new_a_opts = OPS[op_name]["queryPrep"](s_opts, a_opts, targetEntity)[1]
    for key in field_types["target"]:
        if key in new_a_opts and "extra" not in new_a_opts[key]:
            new_a_opts[key]["extra"] = {}

    query, group_args, field_names, col_names = simple_query(s_opts, new_a_opts, ring, extractor, targetEntity, session, db, field_types)
    query = query.group_by(*group_args)
    results = [list(q) for q in query.all()]

    # What to do with the queries results and formatting
    results, new_field_names, new_col_names = OPS[op_name]["pandasFunc"](new_a_opts, results, group_args, field_names, col_names)

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
    field_names[idx] = utils._make_recursive_name(field_names[idx], field_names[per_idx], "average")

    # Removes the per field from the query and col/field list
    for lst in [query_args, col_names, field_names]:
        del lst[per_idx]

    # Run query
    avg = func.avg(s_query.c[utils._name(a_opts["target"]["entity"], a_opts["target"]["field"], copy_opts["op"])])
    if a_opts.get("extra",{}).get("rounding", extractor.getRounding()) != "False":
        field = func.round(avg, a_opts.get("extra",{}).get("sigfigs", extractor.getSigFigs()))
        # field = func.round(avg)

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
    q_args, tables, entity_ids, entity_names, field_names, col_names, joins_todo = _prep_query(a_opts, extractor, db, field_types=field_types)
    query = session.query(*q_args)

    # Do filtering
    query , joins_todo2 = _do_filters(query, s_opts, ring, extractor, targetEntity, field_names, session, db)
    for i in joins_todo2:
        if i not in joins_todo:
            joins_todo.append(i)

    # Do joins
    query, joined_tables = utils._do_joins(query, tables, a_opts["relationships"] if "relationships" in a_opts else [], extractor, targetEntity, db, entity_names, joins_todo)
    query, joined_tables = utils.do_multitable_joins(query, joins_todo, extractor, targetEntity, db, joined_tables, tables)

    # ADD THE UNIQUE CONSTRAINT ON THE TUPLES OF THE IMPORTANT FIELDS IDS
    # PENDING: this gives errors. right now will not add entity ids
    # query = query.distinct(*entity_ids)

    group_args = [utils._name(d["entity"], d["field"], transform=d.get("transform")) for d in a_opts["groupBy"]] if "groupBy" in a_opts else []
    for field in field_types["group"]:
        if field in a_opts:
             group_args.append(utils._name(a_opts[field]["entity"], a_opts[field]["field"], transform=a_opts[field].get("transform"), date_transform=a_opts[field].get("dateTransform")))

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

    a_opts, _ = utils._remove_duplicate_vals(a_opts)

    q_args, tables, entity_ids, entity_names, field_names, col_names, joins_todo = _prep_query(a_opts, extractor, db, field_types=field_types, counts=True)
    query = session.query(*q_args)

    # Do filtering
    query, joins_todo = _do_filters(query, s_opts, ring, extractor, targetEntity, field_names, session, db)

    # Do joins
    query, joined_tables = utils._do_joins(query, tables, a_opts["relationships"] if "relationships" in a_opts else [], extractor, targetEntity, db, entity_names, joins_todo)
    query, joined_tables = utils.do_multitable_joins(query, joins_todo, extractor, targetEntity, db, joined_tables, tables)

    # Count entities
    db_type = extractor.getDBType()
    entity_counts = sql_func.count_entities(query, entity_ids, field_names, db_type)

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
            unit = attr_obj.units if attr_obj.units else attr_obj.nicename
        else:
            # breakpoint()
            unit = entity_dict.nicename
        return unit

    def _apply_op_units(target_unit, op):
        op_units = OPS[op]["units"]
        if op_units == "unchanged":
            return target_unit
        elif op_units == "percentage":
            return ["percent" ,"percents"]
        elif op_units == "undefined":
            return ["undefined", "undefined"]
        return ["unknown", "unknown"]


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
            lst = []
            lst.append(units_dict["target"][0] + "/" + units_dict["per"][0])
            lst.append(units_dict["target"][1] + "/" + units_dict["per"][0])
            units_lst.append(lst)
        elif col[0:7] == "groupBy" and col[-3:] != "ref":
            idx = int(col[7:])
            units_lst.append(units_dict["groupBy"][idx])
        else:
            units_lst.append(units_dict[col])

    return units_lst


def _prep_query(a_opts, extractor, db, field_types, counts=False):
    # Called to query across a (or multiple) computed values

    q_args = []
    tables = []
    field_names = []
    unique_entities = []
    col_names = []
    joins_todo = []

    # Get groupby fields
    groupby_fields = deepcopy(a_opts["groupBy"]) if "groupBy" in a_opts else []

    col_names = ["groupBy" + str(idx) for idx, val in enumerate(groupby_fields)]

    # TODO: Modify this so it accounts for lists as well
    for field in field_types["group"]:
        if field in a_opts:
            groupby_fields.append(a_opts[field])
            col_names.append(field)

    for group in groupby_fields:
        the_field, name, joins_todo = utils._get(extractor, group["entity"], group["field"], db,
                                transform=group.get("transform", None), date_transform=group.get("dateTransform", None))
        q_args.append(the_field)
        field_names.append(name)
        table = utils._get_table_name(extractor, group["entity"], group["field"])
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

    for target in target_fields:

        the_field, field_name, joins_todo_group = utils._get(extractor, target["entity"], target["field"], db, op=target.get("op", "None"),
                                        transform=target.get("transform", None), extra=target["extra"])
        q_args.append(the_field.label(field_name))
        field_names.append(field_name)
        table = utils._get_table_name(extractor, target["entity"], target["field"])
        if table not in tables:
            tables.append(table)
        if target["entity"] not in unique_entities:
            unique_entities.append(target["entity"])
        for item in joins_todo_group:
            if item not in joins_todo:
                joins_todo.append(item)
    
    # Query the IDs of the entities we care about (as well as the nice name stuff for them)
    entity_ids = []
    for entity in unique_entities:
        the_field, name, joins_todo_entities = utils._get(extractor, entity, "id", db)
        entity_ids.append(name)
        if name not in field_names and counts:
            q_args.append(the_field)
            field_names.append(name)
            col_names.append(name + "_id")
        for item in joins_todo_entities:
            if item not in joins_todo:
                joins_todo.append(item)

    return q_args, tables, entity_ids, unique_entities, field_names, col_names, joins_todo


# PENDING: Finish filling this out for prefilters
# PENDING: do counts before/after filtering?
def _do_filters(query, s_opts, ring, extractor, targetEntity, col_names, session, db):
    '''
    Adding filters
    '''

    # do prefilters
    pass
    # query = query.filter(utils._get(extractor, targetEntity, "id", db) != None)
    joins_todo = []
    # do normal filters
    if s_opts and "query" in s_opts and s_opts["query"]:
        query, joins_todo = rawGetResultSet(s_opts, ring, extractor, targetEntity, targetRange=None, simpleResults=True, just_query=True, sess=session, query=query, make_joins=False)

    # do nan filtering
    for name in col_names:
        param_dct = utils._entity_from_name(name)
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
                field, name, joins_todo2 = utils._get(extractor, param_dct["entity"], param_dct["field"], db)
                for item in joins_todo2:
                    if item not in joins_todo:
                        joins_todo.append(item)
                query = query.filter(field != None)

    return query, joins_todo



# PENDING: Do this
# PENDING: See if we actually need to do this, not sure if we do
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

