
# Import try/except to handle running in server or dev CLI context
try:
    from api.operations import OPERATION_SPACE
except:
    from .api.operations import OPERATION_SPACE

class RingConfigExtractor(object):
    ''' A helper class to extract/prepare the compiled rings to JSON for the API/FE '''
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

    def resolveJoin(self, join):
        return join, [ent for ent in self.config.source.joins if ent.name == join][0]

    def resolveRelationship(self, relation):
        return relation, [ent for ent in self.config.relationships if ent.name == relation][0]

    def getSearchSpace(self, target=None):
        target, targetEnt = self.resolveEntity(target)
        if "searchSpace" in self.cache[target]:
            return self.cache[target]["searchSpace"]

        searchSpace = {att.name: {
            "autocomplete": att.autocomplete,
            "type": att.baseIsa,
            "model": getattr(self.config.db, targetEnt.table),
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
            "model": getattr(self.config.db, targetEnt.table),
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

    def getSortables(self, target=None):
        target, targetEnt = self.resolveEntity(target)
        return [col["key"] for col in self.getColumns(target) if col["sortable"] is True]

    def getDefaultSort(self, target=None):
        target, targetEnt = self.resolveEntity(target)

        if "defaultSort" in self.cache[target]:
            return self.cache[target]["defaultSort"]
        defSort =  {"key": self.getColumns(target)[0]["key"], "direction": "desc"}
        self.cache[target]["defaultSort"] = defSort
        return defSort

    def formatResult(self, result, target=None):
        target, targetEnt = self.resolveEntity(target)
        cols = self.getColumns(target)
        searchSpace = self.getSearchSpace(target)
        renderMap = {
            col["key"]: self.getSearchSpace(target)[col["key"]]["fields"] for col in cols
        }
        results = {
            attr: self.coerceValsToString([getattr(result, field) for field in fields])
            for attr, fields in renderMap.items()
        }
        return results

    def coerceValsToString(self, vals):
        # TODO: make this both a) type (leveraging ontology + styling) aware and b) template-compatible
        tmpl = ("{} " * len(vals)).strip()
        return tmpl.format(*vals)

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
            "targetEntities": [ent.name for ent in self.config.entities],
            "defaultEntity": self.defaultEntity,
            "filters": self.getFrontendFilters(target),
            "columns": self.getColumns(target),
            "defaultSort": self.getDefaultSort(target),
            "fieldUnits": self.getFieldUnits(target),
            "analysisSpace": self.getCleanAnalysisSpace(target),
            "includesRenderer": self.getRendererCheck(target),
            "targetModelName": target
        }
        self.cache[target]["feInfo"] = output
        return output
