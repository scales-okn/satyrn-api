# Satyrn API v0.2.1

This is the core codebase for the Satyrn API. Satyrn is developed by the C3 Lab at Northwestern University and is in pre-release alpha. Details about this work are pending publication

### License

This file is part of Satyrn.
Satyrn is free software: you can redistribute it and/or modify it under 
the terms of the GNU General Public License as published by the Free Software Foundation, 
either version 3 of the License, or (at your option) any later version.
Satyrn is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with Satyrn. 
If not, see <https://www.gnu.org/licenses/>.

-----------

The code in this repository stands alone in providing a REST API which can sit behind the Satyrn UX repository.

For an example v0.2.1 config, see the basic_v2-1 directory in the satyrn-templates repo.

## The configuration model: Satyrn and its Rings
See the documentation below for an example ring. Note that rings can be created to leverage tabular datastores (in dev, we have postgres and sqlite versions) with flat file support (e.g. CSVs) in flight

# Satyrn API: Getting Set Up
## Setting up the app environment + Running the App
0. Clone this repo to a place of your choosing. Also, you'll need to have [python3.8](https://www.python.org/downloads/)installed however you see fit (direct download, package manager, etc).

1. cd into the repo directory, create a python virtual env in the base directory of the repo, and load it up. Something like:

```bash
cd satyrn-platform
python -m venv venv   # <-- yup, the .gitignore assumes you have a venv dir named venv
source venv/bin/activate
```

2. Pip install the requirements.txt file into that virtual env

```bash
pip install -r requirements.txt
```

3. Follow the instructions in the env-example.txt and get those in your env however you see fit (~/.profile or something context based -- for Mac/Linux users, this rules: [direnv.net](https://direnv.net)).

4. Now start the server. From the top level directory of satyrn-platform, run:

```bash
flask run
```

5. To test your setup, you'll need [Postman](https://www.postman.com/downloads/) or equivalent to send requests with the "x-api-key" header set to whatever you put for the API_KEY in your environment.

## API Spec

The API currently supports the following primary views:

1. __/api/__

This is just a basic health check to see if the API is running

2. __/api/rings/__

This call will return the rings (and the versions) loaded into memory at the current api instance. It should closely mirror the rings that are listed in the `site.json` file that the site utilizes (determined by the environment variable +++)

3. __/api/rings/{RING_ID}/__

This endpoint returns an object that details the needed information to render the site for `satyrn`. It includes informations on the potential target entities (main searcahble entities for displaying results), the default entity (the searchable entity that will be set by default in case none is explicitly provided), the available filters, the analyzable entities and attributes, and the available operation/analytics space.

Note that similar to this API call, we have `/api/rings/{RING_ID}/{VERSION}/` and `/rings/{RING_ID}/{VERSION}/{TARGET_ENTITY}/`. These allow you to explicitly set a ring version number as well as a searchable entity. If the `VERSION` or the `TARGET_ENTITY` are not provided, then the defaults are utilized.

4. __/autocomplete/{RING_ID}/{VERSION}/{TARGET_ENTITY}/{ATTRIBUTE}/__

An autocomplete endpoint. It takes an `ATTRIBUTE` (a relevant attribute for the `TARGET_ENTITY`) and an optional `query` (a partial string to be shown options for). The query param only works on functions that have implemented support for it. See `autocomplete.py` for the autocomplete functions themselves.

Example:
- `/api/autocomplete/20e114c2-ef05-490c-bdd8-f6f271a6733f/1/Contribution/contributionRecipient/?query=il`
  - The autocomplete endpoint for version # 1 of ring with id 20e114c2-ef05-490c-bdd8-f6f271a6733f with autocompletion on the Contribution entity's contributionRecipient attribute w/ query "il"

5. __/results/{RING_ID}/{VERSION}/{TARGET_ENTITY}/__

This is the primary search endpoint, and it takes a JSON body with a properly formed search query (or an empty JSON body to review all entities). The available search space is defined by the config (as noted above, see satyrn-templates for examples and docs).

In addition to the search params, this also takes optional `page=[int]` and `batchSize=[int]` params that define the size of the slice of results and the "page" of that size to be returned (supporting pagination on the UI). If left off, these default to page=0 and batchSize=10

This will return objects that look like:
```
{"totalCount": {int}, # the total count of all results for the query
"page": {int}, # the page of results returned
"batchSize": {int}, # the max size of the batch returned
"activeCacheRange": [{int}, {int}], # the cache range this result is within on the server
"results": [...} # the list of results where each result is a dictionary and each key is a column for that entry
```
Example:
- `/api/results/20e114c2-ef05-490c-bdd8-f6f271a6733f/1/Contribution/` with the following JSON body:
```
{
    "query": {
        "AND": [
            [
                {"entity": "Contribution",
                "field": "contributionRecipient"},
                "Satyrn Org",
                "contains"
            ]            
        ]
    },
    "relationships": []
}
```
  - The search endpoint for version # 1 of ring with id 20e114c2-ef05-490c-bdd8-f6f271a6733f with a search on the Contribution entity's contributionRecipient attribute with query "Satyrn Org"

Note that to support backwards compatibility with previous version of the endpoint, we also support a version of URL get parameters.

6. __/document/{RING_ID}/{VERSION}/{TARGET_ENTITY}/{ENTITY_ID}/__

An endpoint to view results that have a document view defined in their ring specification. Takes the same set of get parameters as `/results/` (with an added `{ENTITY_ID}` to view a specific entry in the results).

7. __/analysis/{RING_ID}/{VERSION}/{TARGET_ENTITY}/__

The main endpoint utilized to request analyses to the Satyrn API. Similar to `/results/`, it takes in a JSON  body with a properly formed analytic query (which can itself include the search query utilized in `/results/`) and returns the results of the analysis as well as any metadata about the attributes (e.g. the attribute type, units, formatting template).

This will return objects that look like:
```
{
    "counts": {
        "Contribution//id": 3212697 # the count of each entity that was utilized in the analysis
    },
    "fieldNames": [
        {
            "entity": "Contribution",
            "field": "amount",
            "op": "average"
        }
    ], # A list of dictionaries that specify the origin of the results. The order of these mirrors that of each result tuple
    "length": 1, # The number of result entries. This will be equal to 1 unless there was a groupby, timeseries, or similar groupin involved
    "results": [
        [
            "1507.66"
        ]
    ], # a list of results of length "length", each result will be a tuple of the same length as "fieldNames"
    "units": {
        "results": [
            "dollar"
        ]
    } # The units for each of the entries in "fieldNames" (has the same order as "fieldNames")
}
```

Example:
- `/api/analysis/20e114c2-ef05-490c-bdd8-f6f271a6733f/1/Contribution/` with the following JSON body:
```
{
    "target": {
        "entity": "Contribution",
        "field": "amount"
    },
    "op": "average",
    "relationships": []
}
```
  - The analysis endpoint for version # 1 of ring with id 20e114c2-ef05-490c-bdd8-f6f271a6733f, looking at the average Contribution's amount over the entire dataset
- `/api/analysis/20e114c2-ef05-490c-bdd8-f6f271a6733f/1/Contribution/` with the following JSON body:
```
{
    "target": {
        "entity": "Contribution",
        "field": "amount"
    },
    "op": "sum",
    "relationships": [],
    "query": {
            "AND": [
            [
                {"entity": "Contribution",
                "field": "amount"},
                200,
                "exact"
            ]
        ]
    }

}
```
  - The analysis endpoint for version # 1 of ring with id 20e114c2-ef05-490c-bdd8-f6f271a6733f, looking at the sum of Contribution's amount over contributions that have an amount of 200

## Bootstrapping Configs in v2.1

Satyrn is now a multi-service platform, with a Core API (this repo), a frontend UX, and a "backend-for-frontend"/user service for storing users/rings/notebooks (both in the satyrn-ux repo). While doing development on the platform, you have two setup options:

1. Pull both satyrn-api and satyrn-ux repos, set up both codebases, and run three services that point at each other (Core API, frontend, and "backend-for-frontend" or "BfF" service). In this configuration, the BfF will provide rings to the Core API based on the ring id or _rid_ (e.g. 20e114c2-ef05-490c-bdd8-f6f271a6733f) and you can leverage the frontend for testing (or Postman/similar for communication with the API directly). If you follow instructions to set up environments as described above and in the satyrn-ux repo, this configuration should *just work* (but bug reports are always accepted).

2. Pull only the satyrn-api repo, set it up, and run just the one Core API service. In this configuration, you will have to leverage Postman/similar to communicate with the API, and the recommend settings for your environment are:

 - FLASK_ENV = development (should be the case during development anyway, as this means an API key isn't required which would be bad news for prod, but it also means that bootstrapping rings directly is allowed on platform init)
 - SATYRN_SITE_CONFIG = <relevant pointer to a local site.json configured like the one in satyrn-templates/basic_v2-1 which should include references to ring config JSON files to bootstrap at platform initialization>

 If those environment variables are set, the Core API will support loading rings at init, and you can specify the rings to load via the site.json config (again, see satyrn-templates/basic_v2-1 for a working example).

__Important note__: the Core API now expects v2.1 ring configs, so earlier (like those in satyrn-templates/basic_v2) are no longer directly supported by current iterations of the platform. See satyrn-templates/basic_v2-1 for a working schema example.

__Important note #2__: The latter method of loading rings from configs on disk at initialization only works when the platform is running in _development_ mode. In a production environment, the system will require all rings to be loaded from the BfF service.


## Code Structure

### `core`

The files included at the top level of the directory are:

- `compiler.py`: compiles a ring json into a compiled ring that can be used by the api
- `extractors.py`: provides methods for utilizing the compiled ring
- `ring_checker.py`: checks that the given ring configuration is correct
- `satyrnBundler.py`: sets up the flask app for the api
- `defaults.json`: contains the default settings for ring configurations (e.g. null handling behavior)
- `upperOntology.json`: contains an initial stub for a fully-fleshed upper ontology for datatypes

Additionally there is the `sqlite_extensions` folder, which includes some files with sqlite functionality. To ensure that our platform has analytical capabilities for sqlite and for postgres databases, we imported sqlite extensions from https://github.com/nalgeon/sqlean

#### `api`

The bulk of the code is contained in the api folder. The files at the top level directory are:

- `autocomplete.py`: contains autocomplete functionality for searching endpoints
- `engine.py`: contains functions for running analytics
- `operations.py`: contains a library of available analytics. It also compiles the analytics defined in the `analysis_plugins` folder
- `seekers.py`: contains searching functions
- `sql_func.py`: contains SQL functions that work for both sqlite and postgres
- `transforms.py`: defines transformations available for different datatypes (e.g. bucketing numeric values into categories)
- `utils.py`: additional helper functions
- `viewsHelpers.py`: helper functions for the `views.py`
- `views.py`: defines the api endpoints that can be used once the flask app is set up

Additionally, we have an `analysis_plugins` folder that allows the creation of new plugins by defining a new file (more details aon this in the `analysis_plugins/__init__.py` file)

### tests

Defines a set of tests that can be run for one of the template rings
