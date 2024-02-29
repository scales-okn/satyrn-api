import json
from flask import current_app

cache = current_app.cache
CACHE_TIMEOUT = 3000

sparql = current_app.sparql


def search_sparql_endpoint(graph, batch_size, page, filter_values=None):
    query = construct_query(
        current_app.ring["graphs"][graph],
        ["docketId", "filingDate", "terminatingDate", "natureOfSuit"],
        filter_values,
        page=page,
        limit=batch_size,
    )

    print("query", query)

    # Set and execute the query
    sparql.setQuery(query)
    results = sparql.query().convert()

    return convert_results(results)


def convert_results(sparql_results):
    vars = sparql_results["head"]["vars"]
    bindings = sparql_results["results"]["bindings"]

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


def get_filter_val(field_config, value):
    match field_config.get("type"):
        case "string":
            return f'"{value}"'
        case "date-time":
            # if the value doesn't have a time, add it
            if "T" not in value:
                value += "T00:00:00"
            return f'"{value}"^^xsd:dateTime'
        case "date":
            return f'"{value}"^^xsd:date'
        case _:
            return value


def process_filters(graph_config, request_args):
    filters = {}
    for field_name, value in request_args.items():
        if graph_config["fields"].get(field_name) == None:
            continue

        field_config = graph_config["fields"][field_name]
        if field_config.get("canFilter") == True:
            if graph_config["multiValueDelimiter"] in value:
                value_list = value.split(graph_config["multiValueDelimiter"])
                filters[field_name] = value_list
            else:
                filters[field_name] = value

    return filters


def build_filters(graph_config, filter_values):
    filters = ""
    for field_name, value in filter_values.items():
        field_config = graph_config["fields"][field_name]
        if isinstance(value, list):
            if field_config["type"] == "date" or field_config["type"] == "date-time":
                filters += f"FILTER(?{field_name} >= {get_filter_val(field_config, value[0])} && ?{field_name} <= {get_filter_val(field_config, value[1])}) .\n"
            else:
              value_list = ", ".join(get_filter_val(field_config, v) for v in value)
              filters += f"FILTER(?{field_name} IN ({value_list})) .\n"
        else:
            filters += (
                f"FILTER(?{field_name} = {get_filter_val(field_config, value)}) .\n"
            )

    return filters


def construct_query(graph_config, select_fields, filter_values=None, page=1, limit=10):
    query_prefixes = get_prefixes(graph_config)
    query_select = "SELECT DISTINCT " + " ".join(f"?{field}" for field in select_fields)
    query_where = "WHERE {\n"

    select_fields.extend(filter_values.keys() if filter_values else [])
    select_fields = set(select_fields)
    select_field_parents = select_fields.copy()
    for field_name in select_fields:
        field = graph_config["fields"][field_name]
        if field["parent"] != None and field["parent"] != "s":
          select_field_parents.add(field["parent"])
            
    for field_name in select_field_parents:
        field = graph_config["fields"][field_name]
        if field["optional"] == True:
            query_where += f"OPTIONAL {{ ?{field['parent']} {field['predicate']} ?{field_name} . }}\n"
        else:
            query_where += f"?{field['parent']} {field['predicate']} ?{field_name} .\n"

    if filter_values:
        query_where += build_filters(graph_config, filter_values)

    query_where += "}"
    offset = (page - 1) * limit
    query_limit_offset = f"LIMIT {limit} OFFSET {offset}"

    return f"{query_prefixes}\n{query_select}\n{query_where}\n{query_limit_offset}"
