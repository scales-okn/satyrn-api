import json
from flask import current_app

cache = current_app.cache
CACHE_TIMEOUT = 3000

mongo = current_app.mongo

def search_mongo_endpoint(graph, batch_size, page):
    filter_values = {"case": "scales:CaseCriminal"}

    query = construct_query(
        current_app.ring["graphs"][graph],
        ["case", "filingDate", "terminatingDate", "natureOfSuit"],
        filter_values,
        page=page,
        limit=batch_size,
    )

    print("query", query)

    # Set and execute the query
    mongo.setQuery(query)
    results = mongo.query().convert()

    return convert_results(results)


def convert_results(mongo_results):
    vars = mongo_results["head"]["vars"]
    bindings = mongo_results["results"]["bindings"]

    formatted_results = []

    for binding in bindings:
        formatted_binding = {}
        for var in vars:
            if var in binding:
                formatted_binding[var] = binding[var]["value"]
            else:
                formatted_binding[var] = "None"
        formatted_results.append(formatted_binding)

    return formatted_results


def get_prefixes(ring):
    return "\n".join(ring["prefixes"])


def get_field_predicates(ring, field_name):
    field = ring["fields"].get(field_name)
    if field:
        return field["predicates"]
    else:
        return None


def build_filters(graph_config, filter_values):
    filters = ""
    for field_name, value in filter_values.items():
        field_config = graph_config["fields"][field_name]
        if field_config.get("canFilter") == True:
            if isinstance(value, list):
                value_list = ", ".join(f'"{v}"' for v in value)
                filters += f'FILTER(?{field_config.get("variable", field_name)} IN ({value_list})) .\n'
            else:
                filters += f'FILTER(?{field_config.get("variable", field_name)} = {value}) .\n'
    return filters


def construct_query(graph_config, select_fields, filter_values=None, page=1, limit=10):
    print("graph_config", graph_config)
    query_prefixes = get_prefixes(graph_config)
    query_select = "SELECT DISTINCT " + " ".join(f"?{field}" for field in select_fields)
    query_where = "WHERE {\n"

    for field_name in select_fields:
        field = graph_config["fields"][field_name]
        print("field", field)
        if field["optional"]:
            query_where += f"OPTIONAL {{ ?{field['parent']} {field['predicate']} ?{field.get('variable', field_name)} . }}\n"
        else:
            query_where += f"?{field['parent']} {field['predicate']} ?{field.get('variable', field_name)} .\n"

    # Add filters if any filter values are provided
    if filter_values:
        query_where += build_filters(graph_config, filter_values)

    query_where += "}"

    # Calculate OFFSET based on the page number and limit
    offset = (page - 1) * limit

    # Append LIMIT and OFFSET clauses to the query
    query_limit_offset = f"LIMIT {limit} OFFSET {offset}"

    return f"{query_prefixes}\n{query_select}\n{query_where}\n{query_limit_offset}"
