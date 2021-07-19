import os
import sys

from flask import Flask
from flask_caching import Cache
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

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
configDir = os.environ.get("SATYRN_CONFIG", None)
if not os.path.exists(configDir):
    raise ValueError("SATYRN_CONFIG env var appears to point to an non-existent folder.")
if not os.path.exists(os.path.join(configDir, "satconf.py")):
    raise ValueError("SATYRN_CONFIG env var appears to point to a folder without a satconf.py file.")

# load and generate the config object
# this path.append is hacky af -- we should figure out a better approach
sys.path.append(configDir)
import satconf
satConf, metadata = satconf.build()
app.satConf = satConf
app.satMetadata = metadata
