# THIS IS WHERE YOU STOPPED
# WE NOW HAVE app.rings AS A LIST OF COMPILED RINGS
# NEXT NEED TO FIGURE OUT HOW TO WIRE THIS FILE UP TO PROVIDE A FUNCTION TO CREATE THE "INFO" OBJECTS FOR FE
# THEN WORK ON API V2 TO LEVERAGE THE NEW RINGS/EXTRACTORS (to dynamically switch rings over API)
# (SAME GOES FOR QUERY GEN AND ANALYSIS)

# EVERYTHING BELOW IS CURRENTLY A MESS -- COPY PASTED OVER FROM api/views.py TO ILLUSTRATE WHAT SHOULD BE HANDLED HERE

# TODOS FOR V2 OF THIS:
# deal with joins in attributes when the models is a different model
# Set up entity as attribute type
    # Reminder: Everything searchable is an entity
    # (but right now it seems everything has to be an entity to get a model...should we invert to be table up?)
# Add flag "preaggregated"

try:
    from .api.operations import OPERATION_SPACE
except:
    from api.operations import OPERATION_SPACE

# from the satyrn configs...
# V1 stuff
# SEARCH_SPACE = current_app.satConf.searchSpace
# ANALYSIS_SPACE = current_app.satConf.analysisSpace

class ConfigExtractor(object):
    def __init__(self, config):
        self.defaultEntity = config.default_target_entity
        self.config = config
        self.cache = {ent.name: {} for ent in config.entities}

    def resolveEntity(self, target=None):
        targetName = target if target else self.defaultEntity
        if targetName not in self.cache:
            # this isn't an entity so noop
            # TODO: should throw error
            return None, None
        return targetName, [ent for ent in self.config.entities if ent.name == targetName][0]

    def getSearchSpace(self, target=None):
        target, targetEnt = self.resolveEntity(target)
        if "searchSpace" in self.cache[target]:
            return self.cache[target]["searchSpace"]
        searchSpace = {att.name: {
            "autocomplete": att.autocomplete,
            "type": att.baseIsa,
            "model": getattr(self.config.db, targetEnt.name),
            "fields": att.source_columns,
            "allowMultiple": att.allow_multiple,
            "nicename": att.nicename[0], # TODO: leverage the list for singular+plural
            "description": att.description
        } for att in targetEnt.attributes if att.searchable}

        self.cache[target]["searchSpace"] = searchSpace
        return searchSpace

    def getAnalysisSpace(self, target=None):
        target, targetEnt = self.resolveEntity(target)

        if "analysisSpace" in self.cache[target]:
            return self.cache[target]["analysisSpace"]

        analysisSpace = {att.name: {
            "model": getattr(self.config.db, targetEnt.name),
            "field": att.source_columns[0],
            "type": att.baseIsa,
            "fieldName": att.nicename,
            "unit": att.units
        } for att in targetEnt.attributes if att.analyzable}

        # TODO: bring some version of this back for V2 once attr joins are sorted out
        # for att in analysisSpace.values():
        #     if att.join_required:
        #         att["joinTable"] = {
        #             "model": ,
        #             "field":
        #         }

        self.cache[target]["analysisSpace"] = analysisSpace
        return analysisSpace

    def getColumns(self, target=None):
        target, targetEnt = self.resolveEntity(target)

        if "searchSpace" not in self.cache[target]:
            self.getSearchSpace(target)
        if "columns" in self.cache[target]:
            return self.cache[target]["columns"]

        columns = [{
            "key": att.name,
            "nicename": att.nicename[0],
            "width": "{}%".format(100/len(targetEnt.attributes)),
            "sortable": True
        } for att in targetEnt.attributes]

        self.cache[target]["columns"] = columns
        return columns

    # self.defaultSort = {"key": "amount", "direction": "desc"}

    def getDefaultSort(self, target=None):
        target, targetEnt = self.resolveEntity(target)

        if "defaultSort" in self.cache[target]:
            return self.cache[target]["defaultSort"]
        defSort =  {"key": self.getColumns(target)[0]["key"], "direction": "desc"}
        self.cache[target]["defaultSort"] = defSort
        return defSort

    def resultFormatter(self, result, target=None):
        # TODO
        # target = target if target else self.defaultEntity
        # fResult = {att.name: for att in target.attributes}
        pass

    # a few helper functions
    def getCleanAnalysisSpace(self, target=None):
        analysisSpace = self.getAnalysisSpace(target)
        output = []
        for k, v in analysisSpace.items():
            subout = {k1: v1 for k1, v1 in v.items() if k1 in ["type", "fieldName", "unit", "desc"]}
            if "transform" in v:
                subout["transform"] = {"type": v["transform"]["type"]}
            subout["targetField"] = k
            output.append(subout)
        return output

    def getFrontendFilters(self, target=None):
        searchSpace = self.getSearchSpace(target)
        return [(k, {
            "autocomplete": ("autocomplete" in v and v["autocomplete"]),
            "type": v["type"],
            "allowMultiple": v["allowMultiple"],
            "nicename": v["nicename"],
            "desc": v["description"] if "description" in v else None
        }) for k, v in searchSpace.items()]

    def getFieldUnits(self, target=None):
        return {k: v["unit"] for k, v in self.getAnalysisSpace(target).items() if "unit" in v}

    def getRendererCheck(self, target=None):
        target, targetEnt = self.resolveEntity(target)
        return targetEnt.renderable

    def generateInfo(self, target=None):
        target, targetEnt = self.resolveEntity(target)
        if "feInfo" in self.cache[target]:
            return self.cache[target]["feInfo"]
        output = {
            "ringEntities": [ent.name for ent in self.config.entities],
            "filters": self.getFrontendFilters(target),
            "columns": self.getColumns(target),
            "defaultSort": self.getDefaultSort(target),
            "fieldUnits": self.getFieldUnits(target),
            "analysisSpace": self.getCleanAnalysisSpace(target),
            "includesRenderer": self.getRendererCheck(target),
            "targetModelName": "TODO"
        }
        self.cache[target]["feInfo"] = output
        return output

# ALRIGHT THIS IS WHERE YOU STOPPED
# THREE MAJOR THINGS TO DO:
    # 1) FIGURE OUT WHY SERGIO'S CODE IS MAKING THE MODELS WITH THE ATTR NAMES AND NOT THE COLUMN NAMES -- how best to resolve this?
    # 2) FINISH THE RESULTFORMATTER CODE ABOVE ONCE THE MODELS ARE BUILT RIGHT
    # 3) PACKAGE THIS UP TO PROVIDE THE VIEW CODE EVERYTHING IT NEEDS FOR I/O




# static globals for /info/ endpoint
# COLUMNS_INFO = current_app.satConf.columns
# SORT_INFO = current_app.satConf.defaultSort
# SORTABLES = [col["key"] for col in COLUMNS_INFO if col["sortable"] is True]
#

# TARGET_MODEL_NAME = current_app.satConf.targetName
#
# def generateCleanASpace():
#     output = []
#     for k, v in ANALYSIS_SPACE.items():
#         subout = {k1: v1 for k1, v1 in v.items() if k1 in ["type", "fieldName", "unit", "desc"]}
#         if "transform" in v:
#             subout["transform"] = {"type": v["transform"]["type"]}
#         subout["targetField"] = k
#         output.append(subout)
#     return output
# CLEAN_AS = generateCleanASpace()
#
# FIELD_UNITS = {k: v["unit"] for k, v in current_app.satConf.analysisSpace.items() if "unit" in v}
#
#
# SATCONF = current_app.satConf
# db = SATCONF.db
#
# FE_FILTER_INFO = [(k, {
#     "autocomplete": ("autocomplete" in v and v["autocomplete"]),
#     "type": v["type"],
#     "allowMultiple": v["allowMultiple"],
#     "nicename": v["nicename"],
#     "desc": v["desc"] if "desc" in v else None
# }) for k, v in SEARCH_SPACE.items()]
#
#
#
# # a generic filter-prep function
# def organizeFilters(request):
#     opts = {}
#     for k in SEARCH_SPACE.keys():
#         setting = request.args.get(k, None)
#         if setting:
#             if SEARCH_SPACE[k]["type"] == "date":
#                 dateRange = setting.strip('][').split(",")
#                 opts[k] = [cleanDate(dte) for dte in dateRange]
#             elif SEARCH_SPACE[k]["allowMultiple"]:
#                 opts[k] = request.args.getlist(k, None)
#             else:
#                 opts[k] = setting
#     return opts
#
# def cleanDate(dte):
#     return datetime.strptime(dte, '%Y-%m-%d') if dte != "null" else None
#
#
#
# sess = db.Session()
# exampleTarget = sess.query(SATCONF.targetModel).first()
# includesRenderer = hasattr(exampleTarget, "get_clean_html")
# return json.dumps({
#     "filters": FE_FILTER_INFO,
#     "columns": COLUMNS_INFO,
#     "defaultSort": SORT_INFO,
#     "fieldUnits": FIELD_UNITS,
#     "operations": CLEAN_OPS,
#     "analysisSpace": CLEAN_AS,
#     "includesRenderer": includesRenderer,
#     "targetModelName": TARGET_MODEL_NAME
# })
