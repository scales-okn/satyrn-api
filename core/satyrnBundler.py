import json
import os
import sys

from flask import Flask
from flask_caching import Cache
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
CORS(app, supports_credentials=True)

configVersion = os.environ.get("SATYRN_CONFIG_VERSION", "1")

if configVersion == "1":
    raise Exception("This is a V1 Config and won't work with this version of Satyrn.")

try:
    from compiler import compile_rings
    # from extractors import RingConfigExtractor
except:
    from .compiler import compile_rings
    # from .extractors import RingConfigExtractor

# get the root of this repo locally -- used for static dir, user db and downloads dir location downstream
app.config["BASE_ROOT_DIR"] = os.environ.get("SATYRN_ROOT_DIR", os.getcwd())

# track modifications
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# some flask-caching stuff
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 300

# env
app.config["ENV"] = os.environ.get("FLASK_ENV", "development")

# set the location of the downloads folder
app.downloadDir = os.path.join(app.config["BASE_ROOT_DIR"], "bundledDockets")

# set the location of the UX_API for ring requests
app.uxServiceAPI = os.environ.get("UX_SERVICE_API", "http://localhost/api/")

# and the api keys
app.config["API_KEY"] = os.environ.get("API_KEY")
# next one can be different if set in env vars but defaults to the same
app.config["UX_SERVICE_API_KEY"] = os.environ.get("UX_SERVICE_API_KEY", app.config["API_KEY"])

# and set up the cache
app.cache = Cache(app)

# bootstrap the site info and rings from the config json
if os.environ.get("SATYRN_SITE_CONFIG"):
    with open(os.environ.get("SATYRN_SITE_CONFIG")) as f:
        siteConf = json.load(f)
    app.satMetadata = siteConf
else:
    # boilerplate default site config
    app.satMetadata = {
        "name": "Satyrn Platform",
        "icon": "",
        "description": "",
        "rings": []
    }

app.rings = {}
app.ringExtractors = {}

# if we're in local dev, we can bootstrap rings through the site config
# any additional ones will still assume a running version of the FE
if app.config["ENV"].lower() in ["dev", "development"]:
    rings, extractors = compile_rings(app.satMetadata.get("rings", []))
    app.rings = rings
    app.ringExtractors = extractors
