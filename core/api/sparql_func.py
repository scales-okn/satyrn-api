import json
from flask import current_app

cache = current_app.cache
CACHE_TIMEOUT = 3000

sparql = current_app.sparql


def search_sparql_endpoint(graph, batch_size, page, filter_values=None):
    query = construct_query(
        current_app.ring["graphs"][graph],
        ["docketId", "filingDate", "terminatingDate", "natureOfSuit", "courtName"],
        filter_values,
        page=page,
        limit=batch_size,
    )

    print("Query: ", query)
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
        case "iri":
            prefix = field_config["iriPrefix"]
            return f"<{prefix}/{value}>"
        case "predicate":
            return value
        case _:
            return value


def process_filters(ring, request_args):
    filters = {}
    for field_name, value in request_args.items():
        if ring["fields"].get(field_name) == None:
            continue

        field_config = ring["fields"][field_name]
        if field_config.get("canFilter") == True:
            if ring["multiValueDelimiter"] in value:
                filters[field_name] = value.split(ring["multiValueDelimiter"])
            else:
                filters[field_name] = value

    return filters


def build_filters(ring, filter_values):
    filters = ""
    for field_name, value in filter_values.items():
        field_config = ring["fields"][field_name]
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


def add_parents(ring, select_fields):
    parents_to_add = set()

    for field_name in select_fields:
        field = ring["fields"].get(field_name, {})
        parent = field.get("parent")
        if parent:
            parents_to_add.add(parent)

    if not parents_to_add:
        return select_fields
    else:
        return select_fields.union(add_parents(ring, parents_to_add))


def construct_query(ring, select_fields, filter_values=None, page=1, limit=10):
    query_prefixes = get_prefixes(ring)
    query_select = "SELECT DISTINCT " + " ".join(f"?{field}" for field in select_fields)
    query_where = "WHERE {\n"

    select_fields.extend(filter_values.keys() if filter_values else [])
    select_fields = set(select_fields)
    select_fields = add_parents(ring, select_fields)

    for field_name in select_fields:
        field = ring["fields"][field_name]
        parent = field.get("parent", "root")
        if field.get("optional") == True:
            query_where += (
                f"OPTIONAL {{ ?{parent} {field['predicate']} ?{field_name} . }}\n"
            )
        else:
            query_where += f"?{parent} {field['predicate']} ?{field_name} .\n"

    if filter_values:
        query_where += build_filters(ring, filter_values)

    query_where += "}"
    query_limit_offset = f"LIMIT {limit} OFFSET {(page - 1) * limit}"

    return f"{query_prefixes}\n{query_select}\n{query_where}\n{query_limit_offset}"
