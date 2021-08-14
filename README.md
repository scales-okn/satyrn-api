# Satyrn API

[![Build Status](http://198.211.97.126:8080/job/satyrn-platform/badge/icon)](http://198.211.97.126:8080/job/satyrn-platform/)

This is the core codebase for the Satyrn API, which sites behind the Satyrn UX (and in conjunction with the Satyrn Deployment and Satyrn Prototype repos). It's been ported over from the Satyrn-Platform.

## Known TODOs for V2:
 - Update the documentation to reflect new multi-ring/multi-entity URI endpoints/params on autocomplete and search (and future changes to analysis) -- the below is out of date
 - need to have build_joins in compiler.py properly handle all join conditions (multi hops and many-to-one, many-to-many, etc) -- also, make sure config contains all necessary info (pending final analysis v2 design)
    - this will also effect code in autocomplete + search because assumption is config["model"] is a single model now but will prob be a list in the future
 - leverage the att.nicename (on attributes) list for singular+plural references (currently just selects singular)
 - need to set up support for attribute-level joins (they're being created but not necessarily leveraged)
 - set up entity-as-attribute support in config/search/analysis
 - create a formatResult in extractors.py that takes into account the underlying type (e.g.: if type is currency/USD then render as localized number w/ "$")
 - create a "template" option on entities and attributes in the config and leverage that in formatResults (and in rendering answers) if it exists
 - introduce pre-aggregated attributes (and anchor dimension to peg that pre-aggregation) in config/analysis
 - v2 analysis endpoint dev (pending result of analysis v2 design work + prototype)

## The configuration model: Satyrn and its Rings
The core platform itself is dataset agnostic -- as long as the data itself is tabular, Satyrn can be configured to work with it. New datasets can be brought in by following a few steps: 1) loading data into a SQL-friendly database, 2) defining an ORM in [SQLAlchemy](https://www.sqlalchemy.org) for the relevant tables, 3) defining an accompanying satconf configuration ([examples and documentation can be found in satyrn-templates](https://www.github.com/nu-c3lab/satyrn-templates)), and 4) updating an environment variable so Satyrn knows where to locate the config and ORM. That's it -- the platform does the rest, from providing filter mechanics and rendering the paginating table view over to generating the analysis statements.

Note that there are two repos for these satconfs at present. One is [satyrn-rings](https://www.github.com/nu-c3lab/satyrn-rings), which is the blessed set of configs being worked on within C3 (with corresponding ORMs in [c3-JumboDB](https://github.com/nu-c3lab/c3-JumboDB)). The other is [satyrn-templates](https://www.github.com/nu-c3lab/satyrn-templates), which is a place for simplified satconf+ORM+dataset packages for reference when building a new ring. Config documentation is also available in the satyrn-templates README.

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

The API currently supports five primary views:

1. __/api/__

This is just a basic health check, though we can repurpose as necessary.

 ----

2. __/api/info/__

This is an endpoint for passing metadata about how the UI should be built/behave on init. The contents are automatically generated from the contents of the satconf file. Includes: 1) the available filters and their types, 2) the column names/order/widths as well as which ones are sortable and the default sort, and 3) the analysis space components (for dynamic generation of analysis statements on the frontend). Additional documentation TBD.

An example follows:

```javascript
{
  "filters": [ // <-- a list of tuples (lists), each entry defining an available data filter
    [
      "causeOfAction", // <-- name for building url strings/code-level representation
      {
          autocomplete: false, // <-- whether auto-completable or not (via ac endpoint)
          type: "text", // <-- type is currently text or date, need to introduce int/range
          allowMultiple: false, // <-- does it make sense to allow multiple filters of this type?
          nicename: "Cause of Action", // <-- name for the UI
          desc: "Reason for lawsuit" // <-- annotation for UI description
      }
    ], ...
  ],
  "columns": [ // <-- list of objects, each one defining a column to display on UI
    {
      key: "caseName", // <-- key in the results payload (see results endpoint)
      nicename: "Case Name", // <-- column header name on UI
      width: "34%", // <-- display width of column
      sortable: true // <-- whether column should be sortable on UI (see results endpoint)
    }, ...
  ],
  "defaultSort": { // <-- object that defines which col is sorted by default + direction
    key: "dateFiled",
    direction: "desc"
  },
  "operations": { // <-- the available operations space for generating statements
    average: { // <-- operation entry
      dataTypes: [ // <-- datatypes this operates on
        "float",
        "int"
      ],
      neededFields: [ // <-- requirements for analysis, here "what is the targetField?"
        "targetField"
      ],
        units: "unchanged", // <-- does this analysis change the nature of the targetField's units?
        nicename: "Average" // <-- name for UI
    },
  },
  "analysisSpace": [ // <-- list of objects defining features of data that analysis can run on
    {
      type: "float", // <-- datatype of this entry
      fieldName: [ // <-- nicename of fields for UI in [singular, plural]
        "Case Duration",
        "Case Durations"
      ],
      unit: [ // <-- units of this field in [singular, plural]
        "day",
        "days"
      ],
      targetField: "caseDuration" // <-- the field(s) under the hood associated with this entry
    },
  ],
  "fieldUnits": { // <-- maps fields in the analysisSpace to their units, makes lookup easier
    caseDuration: [
      "day", // <-- singular
      "days" // <-- plural
    ],
  },
  "includesRenderer": true, // <-- tells frontend if items in the results view should be clickable
  "targetModelName" // <-- for UI to reference the type of results
}
```

 ----

3. __/api/results/__

This is the primary search endpoint, and it takes a list of search params (or none to browse all available cases). The available search space is defined by the config (as noted above, see satyrn-templates for examples and docs).

In addition to the search params, this also takes optional `page=[int]` and `batchSize=[int]` params that define the size of the slice of results and the "page" of that size to be returned (supporting pagination on the UI). If left off, these default to page=0 and batchSize=10

This will return objects that look like:

```python
{
  "totalCount": {int}, # <-- the total count of all results for the query
  "page": {int}, # <-- the page of results returned
  "batchSize": {int}, # <-- the max size of the batch returned
  "activeCacheRange": [{int}, {int}], # <-- the cache range this result is within on the server
  "results": [] # <-- the actual list of cases that matched this search
}
```

Examples (which work with the SCALES ring, but are included for reference):
 - `/api/results/?dateFiled=[2013-10-10,2013-12-15]`
 - `/api/results/?judgeName=Gotsch&caseName=Moton&sortBy=caseName&sortDir=asc`
 - `/api/results/?attorneys=Baldwin&attorneys=Dana&caseName=National%20Mutual`

 ----

4. __/api/autocomplete/__

An autocomplete endpoint. It takes a `type=[relevant key from config]` and an optional `query=[partial string to be shown options for]`. The query param only works on functions that have implemented support for it. See `searchSpace.py` for how they're mapped to types, and then `autocompleters.py` for the functions themselves.

Examples (from the SCALES ring):
 - `/api/autocomplete/?type=districts` (note: no query param supported at this endpoint)
 - `/api/autocomplete/?type=judgeName&query=abr`

 ----

5. __/api/result/<item_id>__

An endpoint to view results that have a `get_clean_html` method on their target model. Takes the same set of get parameters as /results/, and highlights elements on page accordingly.

Example (from SCALES ring): `/api/result/1-13-cv-07293%7C%7C%7C1:13-cv-07293?caseName=Nationwide%20Mutual&attorneys=Baldwin&attorneys=Kanellakes`
