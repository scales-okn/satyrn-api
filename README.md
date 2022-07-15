# Satyrn API v0.2.1

This is the core codebase for the Satyrn API. Satyrn is developed by the C3 Lab at Northwestern University and is in pre-release alpha. Details about this work are pending publication, and it will be made open-source with clear licensing terms at a later date.

What follows are notes for those actively iterating on Satyrn towards a public release.

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

/api/rings/
 - Rings loaded into memory at the current api instance

/api/rings/20e114c2-ef05-490c-bdd8-f6f271a6733f/
  - The "info" endpoint for the latest available version of ring with id 20e114c2-ef05-490c-bdd8-f6f271a6733f

/api/rings/20e114c2-ef05-490c-bdd8-f6f271a6733f/1
- The "info" endpoint for version # 1 of ring with id 20e114c2-ef05-490c-bdd8-f6f271a6733f

/api/autocomplete/20e114c2-ef05-490c-bdd8-f6f271a6733f/1/Contribution/contributionRecipient/?query=il
 - The autocomplete endpoint for version # 1 of ring with id 20e114c2-ef05-490c-bdd8-f6f271a6733f with autocompletion on the Contribution entity's contributionRecipient attribute w/ query "il"

/api/results/20e114c2-ef05-490c-bdd8-f6f271a6733f/1/Contribution/?contributionRecipient=illinois
  - The search endpoint for version # 1 of ring with id 20e114c2-ef05-490c-bdd8-f6f271a6733f with a search on the Contribution entity's contributionRecipient attribute with query "illinois"

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
