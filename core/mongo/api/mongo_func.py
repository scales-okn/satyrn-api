import json
import re
from flask import current_app
from datetime import datetime

def search_mongo_endpoint(opts, ring, ringExtractor, targetEntity, page, batchSize):
    mongo_db = current_app.mongo.db
    collection_name = get_collection_name(targetEntity)
    
    where_args = compile_where_args(opts)
    select_fields = compile_select_fields(ring.entities)
    
    cursor = mongo_db[collection_name].find(where_args, {}).limit(batchSize).skip(page * batchSize)
    results = list(cursor)
    total_count = mongo_db[collection_name].estimated_document_count(where_args)

    formatted_results = update_dates_in_documents(results)

    return {
        "totalCount": total_count,
        "page": page,
        "batchSize": 10,
        "activeCacheRange": 12, # what is this?
        "results": formatted_results,
    }

def compile_where_args(filters):
    # Base case: directly return the query if it's already in the correct format
    if 'query' in filters:
        query = filters['query']
        # Recursively process the query to transform it into MongoDB's format
        return process_query(query)
    else:
        return {}

def process_query(query):
    if not query:
        return {}
    if 'AND' in query:
        return {'$and': [process_query(q) for q in query['AND']]}
    elif 'OR' in query:
        return {'$or': [process_query(q) for q in query['OR']]}
    else:
        return {query[0]['field']: {"$eq": query[1]}}

def compile_select_fields(entities):
    select_fields = {}
    for entity in entities:
            select_fields[entity.name] = 1
    return select_fields

def get_collection_name(targetEntity):
    if (targetEntity == "Case"):
        return "cases"
    else:
        return "judges"

def format_date_field(date_field):
    """Formats a date field to 'YYYY-MM-DD' string."""
    if isinstance(date_field, dict) and '$date' in date_field:
        return datetime.strptime(date_field['$date'], "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d")
    elif isinstance(date_field, datetime):
        return date_field.strftime("%Y-%m-%d")

def update_dates_in_documents(documents):
    for doc in documents:
        if 'filing_date' in doc and doc['filing_date'] is not None:
            doc['filing_date'] = format_date_field(doc['filing_date'])

        if 'terminating_date' in doc and doc['terminating_date'] is not None:
            doc['terminating_date'] = format_date_field(doc['terminating_date'])

        nature_suit = doc.pop('nature_suit', None)
        if nature_suit is None:
            doc['case_NOS'] = "None"
        else:
            formatted_nature_suit = re.sub(r'^\d+\s+', '', nature_suit)
            doc['case_NOS'] = formatted_nature_suit
    return documents