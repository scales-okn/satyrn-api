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

import os
import json

from flask import Flask
from flask_caching import Cache
from flask_cors import CORS
from flask_pymongo import PyMongo

app = Flask(__name__)
CORS(app, supports_credentials=True)

app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 300

# MongoDB Configuration
app.config["MONGO_URI"] = os.environ.get("MONGO_URI")
app.mongo = PyMongo(app)

# Initialize cache
app.cache = Cache(app)

# Environment and API keys configuration
app.config["ENV"] = os.environ.get("FLASK_ENV", "development")
app.config["API_KEY"] = os.environ.get("API_KEY")
app.config["UX_SERVICE_API_KEY"] = os.environ.get("UX_SERVICE_API_KEY", app.config["API_KEY"])

# Application metadata
app.satMetadata = {
    "name": "Satyrn Platform",
    "icon": "",
    "description": "",
    "rings": []
}

with open("mongo_ring.json", 'r') as file:
    ring = json.load(file)
    
    app.ring = ring
if __name__ == "__main__":
    app.run(debug=True)
