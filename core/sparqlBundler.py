'''
This file is part of Satyrn.
Satyrn is free software: you can redistribute it and/or modify it under 
the terms of the GNU General Public License as published by the Free Software Foundation, 
either version 3 of the License, or (at your option) any later version.
Satyrn is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with Satyrn. 
If not, see <https://www.gnu.org/licenses/>.
'''

import glob
import json
import os
import sys

from flask import Flask
from flask_caching import Cache
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_pymongo import PyMongo

from SPARQLWrapper import SPARQLWrapper, JSON

app = Flask(__name__)
CORS(app, supports_credentials=True)

app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 300

# env
app.config["ENV"] = os.environ.get("FLASK_ENV", "development")

# and the api keys
app.config["API_KEY"] = os.environ.get("API_KEY")

# next one can be different if set in env vars but defaults to the same
app.config["UX_SERVICE_API_KEY"] = os.environ.get("UX_SERVICE_API_KEY", app.config["API_KEY"])

# and set up the cache
app.cache = Cache(app)

app.satMetadata = {
    "name": "Satyrn Platform",
    "icon": "",
    "description": "",
    "rings": []
}

sparql = SPARQLWrapper(os.environ.get("SPARQL_ENDPOINT"))
sparql.setReturnFormat(JSON)

app.sparql = sparql

query = """
    PREFIX scales: <http://schemas.scales-okn.org/rdf/scales#>
    SELECT ?Name
    WHERE {
        ?Court scales:isInCircuit "Ninth" .
        ?Court scales:hasName ?Name .
    }
    LIMIT 100
"""

sparql.setQuery(query)

results = sparql.query().convert()
for result in results["results"]["bindings"]:
    print(result["Name"]["value"])
    