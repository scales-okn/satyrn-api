from flask import current_app

cache = current_app.cache
CACHE_TIMEOUT = 3000

sparql = current_app.sparql

prefix = """
PREFIX scales: <http://schemas.scales-okn.org/rdf/scales#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
"""

case_types = {
    "civil": "CaseCivil",
    "criminal": "CaseCriminal",
}

def search_sparql_endpoint(entity, batch_size, page):
    # print(opts, page, batch_size)
    # # Extract the circuits from opts, defaulting to a list with "Ninth" if not provided
    # circuits = opts.get('circuits', ['Ninth'])

    # # Prepare the VALUES clause for the SPARQL query
    # values_clause = "VALUES ?circuit { " + ' '.join(f'"{circuit}"' for circuit in circuits) + " }"

    # Construct the SPARQL query
    query = f"""
        {prefix}
        SELECT ?entity ?filingDate ?terminatingDate ?natureOfSuit ?courtName
        WHERE {{
          ?entity a ?entityType ;
          scales:hasDocketTable ?docketTable ;
          scales:hasAgent ?agent ;
          scales:hasFilingDate ?filingDate ;
          scales:hasStatus ?status ;
          scales:isInCourt ?court ;
          scales:hasTerminatingDate ?terminatingDate .
          ?docketTable ?p ?docketEntry .
          ?docketEntry scales:hasOntologyLabel ?ontologyLabel .
          ?agent scales:hasAgentType ?agentType ;
                scales:hasName ?agentName ;
                scales:hasRoleInCase ?agentRoleInCase .
          ?court scales:hasName ?courtName ;
                scales:isInCircuit ?courtCircuit .     
          FILTER(?entityType = scales:{case_types[entity]})
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