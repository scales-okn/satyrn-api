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