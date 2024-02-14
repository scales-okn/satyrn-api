import json
from flask import current_app

def search_mongo_endpoint(dataSourceName, filters, batch_size, page):
    mongo_db = current_app.mongo.db
    ring_data = current_app.ring["mongo"][dataSourceName]
    skip_amount = (page - 1) * batch_size
    where_args = compile_where_args(filters)
    select_fields = compile_select_fields(ring_data)
    results = mongo_db["cases"].find(where_args, select_fields).skip(skip_amount).limit(batch_size)

    return results

def compile_where_args(filters):
    where_args = {}
    for field_name, value in filters.items():
        where_args[field_name] = value
    return where_args

def compile_select_fields(ring_data):
    select_fields = {}
    for field_name, field_details in ring_data["fields"].items():
        if field_details.get("canFilter"):
            select_fields[field_name] = 1
    return select_fields
