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