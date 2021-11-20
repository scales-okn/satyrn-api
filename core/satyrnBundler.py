import json
import os
import sys

from flask import Flask
from flask_caching import Cache
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

configVersion = os.environ.get("SATYRN_CONFIG_VERSION", "1")

if configVersion == "1":
    raise Exception("This is a V1 Config and won't work with this version of Satyrn.")

try:
    from compiler import compile_rings
    from extractors import RingConfigExtractor
except:
    from .compiler import compile_rings
    from .extractors import RingConfigExtractor

# get the root of this repo locally -- used for static dir, user db and downloads dir location downstream
app.config["BASE_ROOT_DIR"] = os.environ.get("SATYRN_ROOT_DIR", os.getcwd())

# track modifications
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# some flask-caching stuff
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 300

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
with open(os.environ["SATYRN_SITE_CONFIG"]) as f:
    siteConf = json.load(f)
app.satMetadata = siteConf
# rings = compile_rings(siteConf["rings"])

app.rings = {}
app.ringExtractors = {}
# app.rings = rings
# app.ringExtractors = {rr.id: RingConfigExtractor(rr) for rr in app.rings.values()}
