
# Import try/except to handle running in server or dev CLI context
try:
    from api.operations import OPERATION_SPACE
except:
    from .api.operations import OPERATION_SPACE

try:
    from api.utils import _rel_math, _mirrorRel
except:
    from .api.utils import _rel_math, _mirrorRel


import queue

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

    def getDBType(self):
        return self.config.source.type

    def getRounding(self):
        return self.config.rounding

    def getSigFigs(self):
        return self.config.sig_figs


    def getSearchSpace(self, target=None):
        target, targetEnt = self.resolveEntity(target)
        if "searchSpace" in self.cache[target]:
            return self.cache[target]["searchSpace"]

        ents = self._addAndTraverse_notrecursive(target)

        searchSpace = {} 

        for ent_name, rel, rel_type in ents:
            ent, entObj = self.resolveEntity(ent_name)
            key_name = rel if rel else None
            searchSpace[key_name] = {
                "entity": ent_name,
                "rel_type": rel_type,
                "allowMultiple": rel_type == "m2m",
                "attributes": { att.name: {
                                "autocomplete": att.autocomplete,
                                "type": att.baseIsa,
                                "model": getattr(self.config.db, entObj.table),
                                "fields": att.source_columns,
                                # "allowMultiple": rel_type == "m2m",
                                "nicename": att.nicename[0], # TODO: leverage the list for singular+plural
                                "description": att.description,
                                "resultFormat": att.resultFormat
                            } for att in entObj.attributes if att.searchable
                } 
            }

        self.cache[target]["searchSpace"] = searchSpace
        return searchSpace

    def getAnalysisSpace(self, target=None):
        target, targetEnt = self.resolveEntity(target)

        if "analysisSpace" in self.cache[target]:
            return self.cache[target]["analysisSpace"]

        analysisSpace = {}
        ents = self._addAndTraverse_notrecursive(target)


        for ent_name, rel, rel_type in ents:
            ent, entObj = self.resolveEntity(ent_name)
            key_name = rel if rel else None
            analysisSpace[key_name] = {
                "entity": ent_name,
                "rel_type": rel_type,
                "attributes": { att.name: {
                                "type": att.baseIsa,
                                "model": getattr(self.config.db, entObj.table),
                                "fields": att.source_columns,
                                "nicename": att.nicename[0], # TODO: leverage the list for singular+plural
                                "unit": att.units,
                            } for att in entObj.attributes if att.searchable
                } 
            }

        self.cache[target]["analysisSpace"] = analysisSpace
        return analysisSpace


    def _addAndTraverse_notrecursive(self, init_ent):
        # will only add each entity once

        entities = []
        entities.append((init_ent, None, "o2o"))
        ent_rels = [ent for ent in self._getConnectedEntities(init_ent) if ent[0] not in entities]
        entities.extend(ent_rels)
        return entities


    def _getConnectedEntities(self, target=None):
        if not target:
            target = self.defaultEntity
        entities = []

        for rel in self.config.relationships:
            connected_ent = None
            if rel.fro == target:
                entities.append((rel.to, rel.name, rel.relation))
            elif rel.to == target and rel.bidirectional:
                entities.append((rel.fro, rel.name, _mirrorRel(rel.relation)))

        return entities

    # def _addAndTraverse_simple(self, init_ent):
        # # will only add each entity once
        # # DEPRECATED, not currently used

        # entities = {}
        # q = queue.SimpleQueue()

        # path_so_far = []
        # rel_type = "o2o"
        # q.put((init_ent, path_so_far, rel_type))

        # while not q.empty():
        #     curr_ent, curr_path, curr_tpe = q.get()
        #     if curr_ent not in entities:
        #         entities[curr_ent] = (curr_path, curr_tpe)
        #         ent_rels = [ent for ent in self._getConnectedEntities(curr_ent) if ent[0] not in entities]
        #         for ent, rel, tpe in ent_rels:
        #             new_path = curr_path + [rel]
        #             new_tpe = _rel_math(curr_tpe, tpe)
        #             if new_tpe != "NA":
        #                 q.put((ent, new_path, new_tpe))

        # return entities

    # THIS WAS NEVER COMPLETED AND MIGHT NOT RUN PROPERLY
    # def _addAndTraverse_complex(self, init_ent):
    #     # PENDING DISCUSSION:
    #     # How to prevent loops
    #     # - I THINK WHEN YOU DO A WALKTHRU OF HTE LREATIONSHIPS, KEEP AL IST OF WHICH ENTITIES YOUVE SEEN WITH THIS PATH
    #     # walk thru examples OF DIFFERENT PATHS TO SAME ENTITY, SOME MORE VALID THAN OTHERS

    #     entities = []
    #     q = queue.SimpleQueue()

    #     path_so_far = []
    #     rel_type = "o2o"
    #     ents_so_far = {init_ent}
    #     q.put((init_ent, path_so_far, rel_type, ents_so_far))

    #     while not q.empty():
    #         tple = q.get()
    #         entities.append(tple[0:3])
    #         ent_rels = [ent for ent in self._getConnectedEntities(tple[0])]
    #         paths_list =[entity[1] for entity in entities]
    #         # FOR EACH PATH LIST, MAKE A SET OF ENTITIES VISITED IN THE PATH LIST
    #         for ent, rel, tpe, lst in ent_rels:
    #             new_path = curr_path + [rel]
    #             new_tpe = self._rel_math(curr_tpe, tpe)
    #             if ent not in lst: #AND the new entity is not present in the entity set of path walkthru
    #                 new_set = deepcopy(lst)
    #                 new_set.add(ent)
    #                 if new_rel != "NA": 
    #                     q.put((ent, new_path, new_tpe))

    #     return entities





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
            col["key"]: self.getSearchSpace(target)[None]["attributes"][col["key"]]["fields"] for col in cols
        }
        
        results = {
            attr: self.coerceValsToString([getattr(result, field) for field in fields], searchSpace[None]["attributes"][attr]["resultFormat"])
            for attr, fields in renderMap.items()
        }
        return results

    def coerceValsToString(self, vals, formatting):
        # TODO: make this both a) type (leveraging ontology + styling) aware and b) template-compatible
        if formatting[0] and formatting[1]:
            return  ("{}, {}".format(*vals))
        else: 
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

    # TODO: Needs to be changed to account for multiple entities and whatnot
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
