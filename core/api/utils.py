def _get_join_field(path_bit, db):
    model_name, field_name = path_bit.split(".")
    model = getattr(db, model_name)
    field = getattr(model, field_name)
    return field

def _name(entity, attribute, op=None, transform=None):

    lst = [entity, attribute]
    if transform:
        lst.append(transform)
    if op:
        lst.append(op + "__op")

    return "//".join(lst)

def _outerjoin_name(ringId, join_field):
    return ".".join([ringId, join_field])


def _entity_from_name(col_name):
    attrs = col_name.split("//")
    print(attrs)
    dct = {"entity": attrs[0], "attribute": attrs[1]}
    if len(attrs) > 2:
        if len(attrs[2]) > 4 and attrs[2][-4:] == "__op":
            dct["op"] = attrs[2]
        else:
            dct["transform"] = attrs[2]
            if len(attrs) > 3:
                dct["op"] = attrs[3]
    return dct


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


'''
Needed bifurcation based on sqlite v postgres


- substr: works on sqlite, not on postgres
    - right: works on postgres
- + for concatenating: works on sqlite and postgres
    - concat: only works on postgres
- mode, median: works on postgres, not on sqlite
- distinct.count: works on postgres, not on sqlite
    GOTTA INVESTIGATE THIS MORE

'''