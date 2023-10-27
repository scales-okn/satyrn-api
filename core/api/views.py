"""
This file is part of Satyrn.
Satyrn is free software: you can redistribute it and/or modify it under 
the terms of the GNU General Public License as published by the Free Software Foundation, 
either version 3 of the License, or (at your option) any later version.
Satyrn is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with Satyrn. 
If not, see <https://www.gnu.org/licenses/>.
"""

import json
import csv
import os
import time
from io import StringIO

from flask import (
    Blueprint,
    Response,
    current_app,
    jsonify,
    request,
    send_from_directory,
)
from flask_cors import cross_origin
from flask_security import login_required

from .engine import run_analysis
from .seekers import getResults
from .autocomplete import runAutocomplete
from .utils import replace_relative_urls

from .viewHelpers import (
    CLEAN_OPS,
    apiKeyCheck,
    errorGen,
    organizeFilters,
    cleanDate,
    getOrCreateRing,
    getRing,
    getRingFromService,
    convertFilters,
    convertFrontendFilters,
    organizeFilters2,
    transform_csv_filters,
    apply_csv_filters,
)

from copy import deepcopy

# # some "local globals"
app = current_app  # this is now the same app instance as defined in appBundler.py
api = Blueprint("api", __name__)
cache = app.cache


# One cache enabled helper function...
# @cache.memoize(timeout=1000)
def cachedAutocomplete(db, theType, searchSpace, opts, extractor, targetEntity):
    # TODO: make this work with the new DB setup!
    # print("chachedAutocomplete opts: ", opts)
    return json.dumps(
        runAutocomplete(db, theType, searchSpace, extractor, targetEntity, opts)
    )


# THE ROUTES
# base route as a pseudo health check
# no login necessary to check this
@api.route("/")
@apiKeyCheck
def base():
    return json.dumps({"status": "API is up and running"})


@api.route("/rings/", methods=["GET"])  # , "POST"])
@apiKeyCheck
def getAPIInfo():
    return json.dumps(
        {
            # "success": success,
            # "message": msg,
            "rings": [
                {
                    "id": rid,
                    "versions": {
                        version: {
                            "name": ringInfo.name,
                            "description": ringInfo.description,
                        }
                        for version, ringInfo in app.rings.get(rid).items()
                    },
                }
                for rid in app.rings.keys()
            ]
        }
    )


@api.route("/rings/<ringId>/", methods=["GET"])
@apiKeyCheck
def getRingInfo(ringId):
    # THIS IS GOING TO ASSUME THE REQUESTING PROXY
    # IS MANAGING WHETHER THE USER HAS THE RIGHT TO DO THIS OR NOT
    # Also, if no version set, "latest" is implied (see next endpoint for explicitly set version)
    ring, ringExtractor = getOrCreateRing(ringId)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    ringInfo = ringExtractor.generateInfo()
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)


@api.route("/rings/<ringId>/<version>/", methods=["GET"])
@apiKeyCheck
def getRingInfoWithVersion(ringId, version):
    # THIS IS GOING TO ASSUME THE REQUESTING PROXY
    # IS MANAGING WHETHER THE USER HAS THE RIGHT TO DO THIS OR NOT
    ring, ringExtractor = getOrCreateRing(ringId, version=version)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    ringInfo = ringExtractor.generateInfo()
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)


@api.route("/rings/<ringId>/<version>/<targetEntity>/")
@apiKeyCheck
def getEntityInfo(ringId, version, targetEntity):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    ringInfo = ringExtractor.generateInfo(targetEntity)
    ringInfo["operations"] = CLEAN_OPS
    return json.dumps(ringInfo)


@api.route("/autocomplete/<ringId>/<version>/<targetEntity>/<theType>/")
@apiKeyCheck
def getAutocompletes(ringId, version, targetEntity, theType):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    limit = request.args.get("limit", 1000)
    opts = {"query": request.args.get("query", None), "limit": limit}
    searchSpace = ringExtractor.getSearchSpace(targetEntity)
    thisEntity = searchSpace.get(None)
    thisAttrs = thisEntity.get("attributes")
    if (
        theType in thisAttrs
        and "autocomplete" in thisAttrs[theType]
        and thisAttrs[theType]["autocomplete"]
    ):
        return cachedAutocomplete(
            ring.db, theType, thisAttrs[theType], opts, ringExtractor, targetEntity
        )
    return json.dumps({"success": False, "message": "Unknown autocomplete type"})


@api.route("/results/<ringId>/<version>/<targetEntity>/", methods=["GET", "POST"])
@cross_origin(supports_credentials=True)
@apiKeyCheck
def searchDB(ringId, version, targetEntity):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    # takes a list of args that match to top-level keys in SEARCH_SPACE
    # or None and it'll return the full set (in batches of limit)
    # set up some args

    # NOTE ON DATES
    # dates always expect a range, either:
    # [2018-01-01, 2018-01-01] for single day
    # [2018-01-01, 2018-06-15] for everything between two dates
    # [null, 2018-01-01] for everything up to a date (and inverse for after)

    batchSize = int(request.args.get("batchSize", 10))
    page = int(request.args.get("page", 0))
    # bundle search terms
    searchSpace = ringExtractor.getSearchSpace(targetEntity)
    sortables = ringExtractor.getSortables(targetEntity)

    opts_first_try = organizeFilters(request, searchSpace, targetEntity)
    opts = opts_first_try or (
        request.json
        if request.content_type == "application/json"
        else {"query": {}, "relationships": []}
    )

    # left in case the filters came in the request body
    # note from scott (only relevant if this block is used at all): this block naively trusts that the sender knows the correct query format
    if not opts_first_try:
        if "page" in opts:
            page = int(opts["page"])
        if "batchSize" in opts:
            batchSize = int(opts["batchSize"])

    # otherwise, we will use the filters in the url/get rather than in the json
    else:
        query = convertFilters(targetEntity, searchSpace, opts)
        # remove case_html_query from query and put it on the opts that gets passed to seekers.py
        if "case_html_query" in query:
            opts = {"query": query, "relationships": [], "case_html_query": query.pop("case_html_query")}
        else:
            opts = {"query": query, "relationships": []}

    opts = organizeFilters2(opts, searchSpace)
    # and manage sorting
    targetInfo = ringExtractor.resolveEntity(targetEntity)[1]
    sortBy = request.args.get("sortBy", targetInfo.id[0])
    # sortBy = request.args.get("sortBy", None)
    sortDir = request.args.get("sortDirection", "desc")
    opts["sortBy"] = sortBy if sortBy in sortables else None
    opts["sortDir"] = sortDir if sortDir in ["asc", "desc"] else "desc"

    # now go hunting
    results = getResults(
        opts, ring, ringExtractor, targetEntity, page=page, batchSize=batchSize
    )
    return json.dumps(results, default=str)


@api.route("/analysis/<ringId>/<version>/<targetEntity>/", methods=["GET", "POST"])
@cross_origin(supports_credentials=True)
@apiKeyCheck
def runAnalysis(ringId, version, targetEntity):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    if ringExtractor is None:
        # ring will now be an error message
        return json.dumps(ring)
    # takes a list of args that match to top-level keys in SEARCH_SPACE (or None)
    # and keys related to analysis with analysisType defining the "frame" (matching a key in analysisSpace.py)

    # The analysis parameters come in via a JSON body
    analysisOpts = request.json

    # first, get the search/filter stuff:
    searchSpace = ringExtractor.getSearchSpace(targetEntity)
    searchOpts = organizeFilters(request, searchSpace, targetEntity)
    if searchOpts:
        query = convertFilters(targetEntity, searchSpace, searchOpts)
        analysisOpts["query"] = query
    if "query" not in analysisOpts:
        analysisOpts["query"] = {}

    searchOpts = analysisOpts
    searchOpts = convertFrontendFilters(
        targetEntity, searchSpace, searchOpts
    )  # kludge added by scott
    searchOpts = organizeFilters2(searchOpts, searchSpace)

    if not analysisOpts:
        print("ill formed analysis opts")
        return jsonify({})

    # searchOpts = organizeFilters(request, searchSpace)
    raw_results = run_analysis(
        s_opts=searchOpts,
        a_opts=analysisOpts,
        targetEntity=targetEntity,
        ring=ring,
        extractor=ringExtractor,
    )

    results = {
        "length": len(raw_results["results"]),
        "results": raw_results["results"],
        "units": raw_results["units"],
        "counts": raw_results["entity_counts"]
        if "entity_counts" in raw_results
        else {},
        "fieldNames": raw_results["field_names"],
    }
    if "score" in raw_results:
        results["score"] = raw_results["score"]
    # this next line is a bit of a hack to deal with un-jsonable things by coercing them
    # to strings without having to write quick managers for every possible type (date, datetime, int64, etc)
    results = json.loads(json.dumps(results, default=str))
    # doing jsonify here manages the mimetype
    return jsonify(results)


@api.route("/document/<ringId>/<version>/<targetEntity>/<entityId>")
@apiKeyCheck
def getResultHTML(ringId, version, targetEntity, entityId):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    searchSpace = ringExtractor.getSearchSpace(targetEntity)
    sess = ring.db.Session()
    resolved_targetEnt = ringExtractor.resolveEntity(targetEntity)
    target_renderAS = resolved_targetEnt[1].renderAs
    # -------------------
    targetEnt_tableName = getattr(resolved_targetEnt[1], "table")
    ## get compiler object
    targetEnt_compilerObj = getattr(ring.db, targetEnt_tableName)
    ## if the renderAs attribute is in another table:
    if searchSpace[None]["attributes"][target_renderAS["attribute"]]["source_joins"]:
        source_join = searchSpace[None]["attributes"][target_renderAS["attribute"]][
            "source_joins"
        ]
        # ------------------- get join info
        source_path = deepcopy(
            searchSpace[None]["attributes"][target_renderAS["attribute"]][
                "source_joins"
            ]
        )
        next_step = source_path.pop()
        joiner = deepcopy(ringExtractor.resolveJoin(source_join[0])[1].path)
        from_ = joiner[0][0]
        to_ = joiner[0][1]
        from_table, from_col = from_.split(".")
        to_table, to_col = to_.split(".")
        from_entity = getattr(ring.db, from_table)
        to_entity = getattr(ring.db, to_table)
        field = searchSpace[None]["attributes"][target_renderAS["attribute"]]["fields"][
            0
        ]
        entityId = entityId.replace("%253B", ";").replace(
            "%3B", ";"
        )  # various ways the ucid might get messed up en route due to encodings
        if hasattr(to_entity, field):
            target_model = (
                sess.query(from_entity, getattr(to_entity, field))
                .join(getattr(from_entity, next_step))
                .where(
                    getattr(
                        targetEnt_compilerObj,
                        ringExtractor.get_primarykey(targetEnt_tableName),
                    )
                    == entityId
                )
            )
            document = target_model.all()[0][1]
        else:
            target_model = (
                sess.query(to_entity, getattr(from_entity, field))
                .join(getattr(from_entity, next_step))
                .where(
                    getattr(
                        targetEnt_compilerObj,
                        ringExtractor.get_primarykey(targetEnt_tableName),
                    )
                    == entityId
                )
            )
            document = target_model.all()[0][1]

    district = entityId.split(";;", 1)[0]
    url_base = f"https://ecf.{district}.uscourts.gov"
    return replace_relative_urls(document, url_base)


@api.route("/download-csv/<ringId>/<version>/<targetEntity>/", methods=["GET"])
@cross_origin(supports_credentials=True)
@apiKeyCheck
def download_filtered_csv(ringId, version, targetEntity):
    ring, ringExtractor = getOrCreateRing(ringId, version)
    # for attrs in ring.entities[0].attributes:
    #     print(attrs.name, attrs.source_table, attrs.source_columns)
    searchSpace = ringExtractor.getSearchSpace(targetEntity)
    filters = organizeFilters(request, searchSpace, targetEntity)
    filters = transform_csv_filters(filters, ring)

    # generator function to stream the csv data
    def stream_csv(batch_size=1000):
        cases_csv_path = os.environ.get("CASES_CSV", "./cases.csv")
        with open(cases_csv_path, "r") as infile:
            reader = csv.DictReader(infile)
            sio = StringIO()
            # using the csv writer to propery quote and escape the data
            # using StringIO because the writerow method expects a file-like object; Response is not
            writer = csv.writer(
                sio,
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"',
                lineterminator="\n",
                escapechar="\\",
                doublequote=True,
                dialect=reader.dialect,
            )

            # Write and yield the header
            writer.writerow(reader.fieldnames)
            yield sio.getvalue()
            sio.truncate(0)  # Clear the StringIO buffer
            sio.seek(0)  # Reset the write position to the beginning of the buffer

            # Write and yield each row
            # start_time = time.time()
            batch = []
            for row in reader:
                filtered_row = apply_csv_filters(row, filters)
                if filtered_row:
                    batch.append(filtered_row.values())

                if len(batch) == batch_size:
                    writer.writerows(batch)
                    yield sio.getvalue()
                    sio.truncate(0)  # Clear the StringIO buffer
                    sio.seek(
                        0
                    )  # Reset the write position to the beginning of the buffer
                    batch.clear()

            if batch: # handle and leftover rows in the batch
                writer.writerows(batch)
                yield sio.getvalue()

    # Stream the response as the data is generated on-the-fly
    return Response(
        stream_csv(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=data.csv"},
    )
