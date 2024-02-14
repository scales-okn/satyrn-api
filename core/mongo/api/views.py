import json
from flask import (
    Blueprint,
    current_app,
    jsonify,
    request,
)
from bson.json_util import dumps
from core.mongo.api.mongo_func import search_mongo_endpoint

# # some "local globals"
app = current_app  # this is now the same app instance as defined in appBundler.py
api = Blueprint("api", __name__)
cache = app.cache

# THE ROUTES
# base route as a pseudo health check
# no login necessary to check this
@api.route("/")
def base():
    return json.dumps({"status": "API is up and running"})

@api.route("/results/<dataSourceName>/", methods=["GET", "POST"])
def get_results(dataSourceName):
    batchSize = int(request.args.get("batchSize", 10))
    page = int(request.args.get("page", 1))

    filters = request.args

    results = search_mongo_endpoint(dataSourceName, filters, batchSize, page)

    return dumps(list(results))

# @apiKeyCheck
@api.route("/rings/<ringId>/<version>/", methods=["GET"])
def getRingInfoWithVersion(ringId, version):
    # THIS IS GOING TO ASSUME THE REQUESTING PROXY
    # IS MANAGING WHETHER THE USER HAS THE RIGHT TO DO THIS OR NOT
    # ring, ringExtractor = getOrCreateRing(ringId, version=version)
    # if ringExtractor is None:
    #     # ring will now be an error message
    #     return json.dumps(ring)
    # ringInfo = ringExtractor.generateInfo()
    # ringInfo["operations"] = CLEAN_OPS
    return json.dumps(app.ring)

