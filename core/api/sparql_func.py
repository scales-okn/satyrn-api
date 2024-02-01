from flask import current_app

cache = current_app.cache
CACHE_TIMEOUT = 3000

sparql = current_app.sparql

def search_sparql_endpoint(opts, page=1, batch_size=10):
    print(opts, page, batch_size)
    # Extract the circuits from opts, defaulting to a list with "Ninth" if not provided
    circuits = opts.get('circuits', ['Ninth'])

    # Prepare the VALUES clause for the SPARQL query
    values_clause = "VALUES ?circuit { " + ' '.join(f'"{circuit}"' for circuit in circuits) + " }"

    # Construct the SPARQL query
    query = f"""
        PREFIX scales: <http://schemas.scales-okn.org/rdf/scales#>
        SELECT ?Name
        WHERE {{
            ?Court scales:isInCircuit ?circuit .
            ?Court scales:hasName ?Name .
        }}
        LIMIT {batch_size}
        OFFSET {(page - 1) * batch_size}
    """

    # Set and execute the query
    sparql.setQuery(query)
    results = sparql.query().convert()

    print("sparql results", results)

    return results

def convert_sparql_results(results):
    # Extract the bindings from the results
    bindings = results['results']['bindings']

    # Extract the names from the bindings
    names = [binding['Name']['value'] for binding in bindings]

    return names

def convert_filter(query):
    # Convert the query to a list of filters
    filters = query.split(',')

    # Convert the filters to a list of tuples
    filters = [tuple(filter.split(':')) for filter in filters]

    return filters