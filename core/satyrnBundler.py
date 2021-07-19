import json
import os
import sys

from flask import Flask
from flask_caching import Cache
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

configVersion = os.environ.get("SATYRN_CONFIG_VERSION", "1")

if configVersion == "2":
    try:
        from compiler import compile_rings
        from extractors import ConfigExtractor
    except:
        from .compiler import compile_rings
        from .extractors import ConfigExtractor


# get the root of this repo locally -- used for static dir, user db and downloads dir location downstream
app.config["BASE_ROOT_DIR"] = os.environ.get("SATYRN_ROOT_DIR", os.getcwd())

# track modifications
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# some flask-caching stuff
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 300

# set the location of the downloads folder
app.downloadDir = os.path.join(app.config["BASE_ROOT_DIR"], "bundledDockets")

# and some stuff for sessions and password salting
# app.config["SECRET_KEY"] = os.environ.get("SATYRN_SECRET_KEY")
# app.config["SECURITY_PASSWORD_SALT"] = os.environ.get("SATYRN_SECURITY_SALT")

# and the api key
app.config["API_KEY"] = os.environ.get("API_KEY")

# and layer on some sqla
# app.db = SQLAlchemy(app)

# and set up the cache
app.cache = Cache(app)

# import the config via the SATYRN_CONFIG env var
# but now depending on version!
if configVersion == "1":
    raise Exception("This is a V1 Config and won't work with this version of Satyrn.")
else:
    with open(os.environ["SATYRN_SITE_CONFIG"]) as f:
        siteConf = json.load(f)
    app.satMetadata = siteConf
    rings = compile_rings(siteConf["rings"])
    app.rings = rings
    app.spaces = {rr.id: ConfigExtractor(rr) for rr in app.rings.values()}

    # breakpoint()
