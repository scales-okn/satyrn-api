import json
from flask import (
    Blueprint,
    current_app,
    request,
)
from core.api.sparql_func import search_sparql_endpoint

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

@api.route("/results/<graph>/", methods=["GET", "POST"])
def get_results(graph):
    batchSize = int(request.args.get("batchSize", 10))
    page = int(request.args.get("page", 0))
    return json.dumps(search_sparql_endpoint(graph, batchSize, page))
