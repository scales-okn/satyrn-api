# Satyrn API v2

[![Build Status](http://198.211.97.126:8080/job/satyrn-platform/badge/icon)](http://198.211.97.126:8080/job/satyrn-platform/)

This is the core codebase for the Satyrn API V2, which sits behind the Satyrn UX (and in conjunction with the Satyrn Deployment and Satyrn Prototype repos). It's been ported over from the Satyrn-Platform repo and has been updated to work with Ring V2 configs.

## Known TODOs for V2:
 - Update the documentation to reflect new multi-ring/multi-entity URI endpoints for analysis (the rest are now below)
 - need to have build_joins in compiler.py properly handle all join conditions (multi hops and many-to-one, many-to-many, etc) -- also, make sure config contains all necessary info (pending final analysis v2 design)
    - this will also effect code in autocomplete + search because assumption is config["model"] is a single model now but will prob be a list in the future
 - leverage the att.nicename (on attributes) list for singular+plural references (currently just selects singular)
 - need to set up support for attribute-level joins (they're being created but not necessarily leveraged)
 - set up entity-as-attribute support in config/search/analysis
 - create a formatResult in extractors.py that takes into account the underlying type (e.g.: if type is currency/USD then render as localized number w/ "$")
 - create a "template" option on entities and attributes in the config and leverage that in formatResults (and in rendering answers) if it exists
 - figure out where to store flatfiles associated with rings
 - introduce pre-aggregated attributes (and anchor dimension to peg that pre-aggregation) in config/analysis

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
