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


from .sql_func import _nan_cast, sql_concat
from .operations import OPERATION_SPACE as OPS
from .transforms import TRANSFORMS_SPACE as TRS
from sqlalchemy import func
from sqlalchemy.sql.expression import cast
import sqlalchemy

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

def _mirrorRel(rel_type):
    dct = {
        "o2o": "o2o",
        "m2m": "m2m",
        "o2m": "m2o",
        "m2o": "o2m"
    }
    return dct[rel_type]

def _rel_math(init_type, new_type):
    if init_type == "o2o":
        return new_type
    if new_type == "o2o":
        return init_type
    if init_type == "m2m" or new_type == "m2m":
        return "m2m"
    if init_type == "o2m":
        if new_type == "m2o":
            return "m2m"
        elif new_type == "o2m":
            return "o2m"
    elif init_type == "m2o":
        if new_type == "m2o":
            return "m2o"
        elif new_type == "o2m":
            return "NA"

def _walk_rel_path(fro, to, rels):
    init_rel = "o2o"
    curr_ent = fro
    for rel in rels:
        if rel.fro == curr_ent:
            curr_rel = rel.relation
            curr_ent = rel.to
        elif rel.to == curr_ent and rel.bidirectional:
            curr_rel = _mirrorRel(rel.relation)
            curr_ent = rel.fro
        else:
            print("Error, not properly formed relationship path")
            return "NA"

        init_rel = _rel_math(init_rel, curr_rel)
    
    if curr_ent != to:
        print("error, not properly formed rleaitonship path")
        return "NA"
    return init_rel

# NOTE: we are assuming here that ALL fields in that reference
# will belong to the entity
def _parse_reference(extractor, entity, db):

    entity_dict = extractor.resolveEntity(entity)[1]
    ref = entity_dict.reference
    val_lst, type_lst = _parse_ref_string(ref)

    concatenable = []
    for val, tpe in zip(val_lst, type_lst):
        if tpe:
            field, _, joins_todo = _get_helper(extractor, entity, val[2:-1], db, None, None, None, None)
            concatenable.append(field)
        else:
            concatenable.append(val)

    name = _name(entity, "reference", None, None)
    return sql_concat(concatenable, extractor.getDBType()).label(name), name, joins_todo


# PENDING: handling multiple columns in the columns of source
# PENDING: Add "Conversion" to pretty name here (e.g. True/False to other stuff)
# PENDING: Adding capabilities for multi-table entities (currently being worked on by developers)
def _get(extractor, entity, attribute, db, transform=None, date_transform=None, op=None, extra=None):
    '''
    Returns the field from entity in ring, and the label of the field
    '''
    if attribute == "reference":
        return _parse_reference(extractor, entity, db)
    else:
        field, name, joins_todo = _get_helper(extractor, entity, attribute, db ,transform, date_transform, op, extra)
        return field.label(name), name, joins_todo


def _get_helper(extractor, entity, attribute, db, transform, date_transform, op, extra):
    '''
    Returns the field from entity in ring, and the label of the field
    '''
    entity_dict = extractor.resolveEntity(entity)[1]
    joins_todo = []
    if attribute != "id":
        attr_obj = [attr for attr in entity_dict.attributes if attr.name == attribute][0]
        ## if attribute is not in the main table:
        if attr_obj.source_table != entity_dict.table:
            ##(multi-table entity) --> we got some joins to do
            for item in attr_obj.source_joins:
                joins_todo.append(item)

        ##go back to business as usual
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
        field = OPS[op]["funcDict"]["op"](field, extractor.getDBType(), extra)

    if attr_obj:
        # Round if object has rounding

        ## NOTE 
            ## the next two lines fail for doubles. For now we're commenting this out
            ## Problem for Future Donna. 
        if attr_obj.rounding == "True": 
            field = func.round(cast(field, sqlalchemy.Numeric), attr_obj.sig_figs)

        if op == "percentage" and extra.get("rounding", extractor.getRounding()) != "False":
            field = func.round(field, extra.get("sigfigs", extractor.getSigFigs()))


    if transform:
        if transform == "threshold":
            extra = extra if extra else {}
            attr_obj = [attr for attr in entity_dict.attributes if attr.name == attribute][0]
            extra["threshold"] = extra.get("threshold", attr_obj.threshold)
        field = TRS[transform]["processor"](field, extractor.getDBType(), extra)
    print(joins_todo)
    return field, name, joins_todo


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


# PENDING: account for case where single entity across multiple tables (e.g. derived data table)
# PENDING: account for case where multiple entities in one table
# PENDING: think about transitioning to checking if we have to have a list of joins we've done in case we might repeat a join
# right we hav a stricter condition than that (we do not do a join if both tables are already joined in the joined query)

def do_join_helper(query, path, db, added_tables=set()):
        # Given a query, a path, and a 
        # For now we are assuming one and only one of the tables in the path is not in added_tables
        if path[0].split(".")[0] not in added_tables :
            the_table = path[0].split(".")[0]
            indices = [0,1]
        elif path[1].split(".")[0] not in added_tables :
            the_table = path[1].split(".")[0]
            indices = [1,0]
        else:
            print(f"path {path} already joined")
            return query, None
        # print(path)
        # print("table to join")
        # print(the_table)
        return query.join(getattr(db, the_table),
                            _get_join_field(path[indices[0]], db) == _get_join_field(path[indices[1]], db),
                            isouter=True
                            ), the_table
# PENDING: Should be stress tested, tested more
# PENDING: Should try to share this with seekers as much as possible
# PENDING: have yet to test the case where single entity across multiple tables
# PENDING: have yet to test teh case where multiple entities in one table (tho it should be fine)
# PENDING: test the case with derived person entity
# PENDING: think if we have to have a list of joins we've done in case we might repeat a join
# RN we hav a strictercondition than that (we do not do a join if both tables are already joined in the query)
def _do_joins(query, tables, relationships, extractor, targetEntity, db, entity_names, joins_todo):
    '''
    Do joins, if needed
    We have a list of SQL tables that we are using, as well as the needed relationships
    to conduct those joins.
    '''
    joined_tables = set()
    p_table = _get_table_name(extractor, targetEntity, "id")
    primary_bool = any(table == p_table for table in tables)
    # seond conditino should be true always for filterin join

    # print(query)
    # print(tables)
    # print(relationships)
    
    rel_items = [extractor.resolveRelationship(rel)[1] for rel in relationships]

    if targetEntity in entity_names and not primary_bool and len(entity_names) == 1:
        ## find joins in join_todo that have a connection to the targetEntity
        the_chosen = []
        for join in joins_todo:
            join_i = extractor.resolveJoin(join)[1]
            if join_i.from_ == p_table or join_i.to == p_table:
                the_chosen.append(join)
                break
        if the_chosen:
            ##do those joins
            query, joined_tables3 = do_multitable_joins(query, the_chosen, extractor, targetEntity, db, joined_tables, tables)
            tables.append(p_table)
            primary_bool = any(table == p_table for table in tables)
        else:
            print("idk something is really wrong.")
            
    if not primary_bool:
        
        # If the primary table is not in the queried fields AND not joined yet to query,
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


        # Iterate thru these and join them
        for rel_item in rels:
            if rel_item.join:

                if rel_item.fro == targetEntity:
                    join_lst = reversed(rel_item.join)
                    join_item = rel_item.join[-1]
                elif rel_item.to == targetEntity:
                    join_lst = rel_item.join
                    join_item = rel_item.join[0]
                else:
                    print("join path does not include target Entity, unclear error")
                    return None

                join = extractor.resolveJoin(join_item)[1]

                if not (join.from_ in joined_tables or join.to in joined_tables):
                    if join.from_ in tables:
                        joined_tables.add(join.from_)
                    elif join.to in tables:
                        joined_tables.add(join.to)
                    else:
                        print("uhhhh hey bruh, something happened")
                        print("neither table in join is in talbes used in query")             


                for join_item in join_lst:
                    join = extractor.resolveJoin(join_item)[1]
                    for path in join.path:
                        query, add_table = do_join_helper( query, path, db, joined_tables)
                        if add_table:
                            joined_tables.add(add_table)

        # update the list of relations to join to to remove the ones we already joined to
        rel_items = [rel for rel in rel_items if not rel_contains_entity_table(rel, targetEntity, p_table)]

    else:
        joined_tables.add(_get_table_name(extractor, targetEntity, "id"))

    # for the pending relationships, join them
    for rel_item in rel_items:
        if rel_item.join: # if relationship requires join
            for join_item in rel_item.join:
                join = extractor.resolveJoin(join_item)[1]
                for path in join.path: #in case we have multiple joins in the path
                    query, add_table = do_join_helper(query, path, db,joined_tables)
                    if add_table:
                        joined_tables.add(add_table)

    return query, joined_tables

def do_multitable_joins(query, joins, extractor, targetEntity, db, joined_tables, tables):
    for join_item in joins:
        if joined_tables == set(): 
            ## add the table in tables in it just cuz it can't be empty
            joined_tables.add(tables[0])
        join = extractor.resolveJoin(join_item)[1]
        for path in join.path: #in case we have multiple joins in the path
            query, add_table = do_join_helper(query, path, db, joined_tables)
            if add_table:
                joined_tables.add(add_table)
    return query, joined_tables




def _get_join_field(path_bit, db):
    model_name, field_name = path_bit.split(".")
    model = getattr(db, model_name)
    field = getattr(model, field_name)
    return field

def _name(entity, attribute, op=None, transform=None, date_transform=None):

    lst = [entity, attribute]
    if transform:
        lst.append(transform + "__transform")
    if op:
        lst.append(op + "__op")
    if date_transform:
        lst.append(date_transform + "__dateTransform")

    return "//".join(lst)



def _entity_from_name(col_name):

    if "./." in col_name:
        return _entity_from_recursive_name(col_name)

    attrs = col_name.split("//")
    dct = {"entity": attrs[0], "field": attrs[1]}
    if len(attrs) > 2:
        for attr in attrs[2:]:
            val, key = attr.split("__")
            dct[key] = val

    return dct


def _make_recursive_name(numer_name, denom_name, op):
    return "./.".join([numer_name, denom_name, op])


def _entity_from_recursive_name(recurse_name):
    numer_name, denom_name, op = recurse_name.split("./.")
    dct_1 = _entity_from_name(numer_name)
    dct_2 = _entity_from_name(denom_name)

    dct_1["op"] = op + dct_1["op"].title()
    dct_1["per"] = dct_2
    return dct_1    


def _remove_duplicate_vals(a_opts):
    # Removes any keys that have duplicate values in them.
    # Used for the row_count_query, since there could be repeated queried values
    # when we no longer do operations/aggregations on the values
    repeat_keys = [[k for k in a_opts if a_opts[k] == v] for v in a_opts.values()]
    repeat_keys = [lst for lst in repeat_keys if len(lst) > 1]
    repeat_keys.sort()
    del_keys = []
    if repeat_keys:
        last = repeat_keys[-1]
        for i in range(len(repeat_keys)-2, -1, -1):
            if last == repeat_keys[i]:
                del repeat_keys[i]
            else:
                last = repeat_keys[i]
        for repeat_lst in repeat_keys:
            kept_key = repeat_lst[0]
            for repeat_val in repeat_lst[1:]:
                del_keys.append((repeat_val, a_opts[repeat_val]))
                del a_opts[repeat_val]

    return a_opts, del_keys

# # for debugging
# def dict_print(d, indent=0):
#     for key, value in d.items():
#         print('\t' * indent + str(key))
#         if isinstance(value, dict):
#             pretty(value, indent+1)
#         else:
#             print('\t' * (indent+1) + str(value))