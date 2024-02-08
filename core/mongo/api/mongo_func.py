import json
from flask import current_app

def search_mongo_endpoint(dataSource, batch_size, page):
    mongo = current_app.mongo
    filter_values = {"caseType": "CaseCriminal"}

    collection_name = current_app.ring["mongo"][dataSource]

    mongo_query = {}
    for key, value in filter_values.items():
        mongo_query[key] = value

    skip_amount = (page - 1) * batch_size

    # MongoDB aggregation pipeline for filtering and pagination
    pipeline = [
        {"$match": mongo_query},
        {"$skip": skip_amount},
        {"$limit": batch_size}
    ]

    # find the first case
    case_html_results = mongo.db["cases"].find_one()

    # Execute query
    results_cursor = mongo.db
    results = list(results_cursor)

    # Convert MongoDB results to JSON
    results_json = json.loads(json.dumps(results, default=str))  # Ensuring ObjectId is serialized

    return results_json


def convert_results(mongo_results):
    # This function would need to be adjusted based on the structure of your MongoDB documents
    # and what you're aiming to achieve with the conversion.
    # Since MongoDB documents are already JSON-like, you might simplify or directly use the query results.
    return mongo_results
