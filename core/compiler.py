import datetime
from functools import reduce
import json
import os

import sqlalchemy as sa
from sqlalchemy import Boolean, Column, ForeignKey, Integer, Float, String, DateTime, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import create_engine

# This is an abstract class which serves as the superclass for concrete ring classes
class Ring_Object(object):

    # Extract value if list, if not extract and wrap in list
    def safe_extract_list(self, key, dictionary):
        if key in dictionary and dictionary[key]:
            value = dictionary[key]
            if type(value) is list:
                return value
            else:
                return [value]
        return None

    def safe_insert(self, key, value, dictionary):
        if value or type(value) is bool:
            dictionary[key] = value
        else:
            dictionary[key] = None

    # Subclass is required to override this method
    def is_valid(self):
        return False

class Ring_Attribute(Ring_Object):

    def __init__(self, parent_entity=None):

        # Set default values
        self.searchable = False
        self.allow_multiple = False
        self.analyzable = True

        # Initialize other properties
        self.search_style = None
        self.name = None
        self.nicename = None
        self.isa = None
        self.baseIsa = None
        self.units = None
        self.source_table = None
        self.source_columns = None

        # a flag to track whether it requires a join
        self.join_required = False
        self.parent_entity = parent_entity

    def parse(self, name, info):
        self.name = name
        self.nicename = info.get('nicename')
        self.isa = info.get('isa')

        # this next one is to separate conceptual type from data type (currency vs float)
        # doesn't matter know but will be useful later when we leverage upper ontology
        self.baseIsa = info.get('isa')

        self.units = info.get('units')

        # a flag for the analysis engine to avoid aggregating this value if it's a number
        # more often this is False (or not present/pertinent) and defaults accordingly
        self.preaggregated = info.get("preaggregated", False)

        if 'source' in info:
            source = info['source']
            self.source_table = source.get('table')
            self.source_columns = source.get('columns')
            self.join_required = self.source_table != self.parent_entity["table"]

        if 'metadata' in info:
            md = info['metadata']
            self.searchable = md.get('searchable', False)
            self.allow_multiple = md.get('allowMultiple', True)
            self.search_style = md.get('searchStyle', "string")
            self.analyzable = md.get('analyzable', False)
            self.autocomplete = md.get('autocomplete', True if self.searchable else False)
            self.description = md.get('description')

    def construct(self):

        attribute = {}
        self.safe_insert('name', self.name, attribute)
        self.safe_insert('nicename', self.nicename, attribute)
        self.safe_insert('isa', self.isa, attribute)

        source = {}
        self.safe_insert('table', self.source_table, source)
        self.safe_insert('columns', self.source_columns, source)

        md = {}
        self.safe_insert('searchable', self.searchable, md)
        self.safe_insert('allowMultiple', self.allow_multiple, md)
        self.safe_insert('searchStyle', self.search_style, md)
        self.safe_insert('analyzable', self.analyzable, md)
        self.safe_insert('description', self.description, md)

        self.safe_insert('source', source, attribute)
        self.safe_insert('metadata', md, attribute)
        return attribute

    def is_valid(self):
        return bool(self.name and self.nicename and self.isa and self.source_table and self.source_columns)

class Ring_Entity(Ring_Object):

    def __init__(self):

        # Set default values
        self.id = ['id']
        self.id_type = ['integer']

        # Initialize other properties
        self.name = None
        self.table = None
        self.renderable = False
        self.reference = None
        self.attributes = []

    def parse(self, entity):
        self.name = entity.get('name')
        self.reference = entity.get('reference')
        self.table = entity.get('table')
        self.id = self.safe_extract_list('id', entity)
        self.id_type = self.safe_extract_list('idType', entity)
        self.renderable = entity.get('renderable', False)
        self.parse_attributes(entity)

    def parse_attributes(self, entity):
        if 'attributes' in entity:
            attributes = entity['attributes']
            for name, info in attributes.items():
                attribute = Ring_Attribute(entity)
                attribute.parse(name, info)
                self.attributes.append(attribute)

    def construct(self):
        entity = {}
        self.safe_insert('id', self.id, entity)
        self.safe_insert('idType', self.id_type, entity)
        self.safe_insert('name', self.name, entity)
        self.safe_insert('table', self.table, entity)
        self.safe_insert('attributes', list(map((lambda attribute: attribute.construct()), self.attributes)), entity)
        return entity

    def is_valid(self):
        valid = bool(self.name and self.table and self.id and self.id_type and self.attributes)
        valid = valid and reduce((lambda x, y: x and y), map((lambda x: x.is_valid()), self.attributes))
        return valid

class Ring_Relationship(Ring_Object):

    def __init__(self):
        # Set default values
        self.id = None
        self.name = None
        self.fro = None
        self.to = None
        self.join = []
        self.relation = "m2m"
        self.bidirectional = True

    def parse(self, rel):
        self.name = rel.get("name")
        self.fro = rel.get("from")
        self.to = rel.get("to")
        self.join = self.safe_extract_list("join", rel)
        self.relation = rel.get("relation", "m2m")
        self.bidirectional = rel.get("bidirectional", True)

        # construct an id handle from the inputs
        # form is: from + name + to + join = "ContributorMakesContribution"
        self.id = "{}{}{}{}".format(self.fro, self.name, self.to, self.join)

    def construct(self):
        rel = {}
        self.safe_insert('id', self.id, rel)
        self.safe_insert('name', self.name, rel)
        self.safe_insert('from', self.fro, rel)
        self.safe_insert('to', self.to, rel)
        self.safe_insert('join', self.join, rel)
        self.safe_insert('relation', self.relation, rel)
        self.safe_insert('bidirectional', self.bidirectional, rel)
        return rel

    def is_valid(self):
        return bool(self.name and self.fro and self.to and self.join)

class Ring_Source(Ring_Object):

    def __init__(self, base=None):

        # Set default values
        self.type = 'sqlite'

        # Initialize other properties
        self.connection_string = None
        self.tables = None
        self.joins = []

        # # Tie in the base
        self.base = base if base else declarative_base()

    def parse(self, source):
        self.type = source.get('type')
        self.connection_string = source.get('connectionString')
        self.tables = source.get('tables')
        self.parse_joins(source)

    def parse_joins(self, source):
        if 'joins' in source:
            joins = source['joins']
            for join in joins:
                join_object = Ring_Join()
                join_object.parse(join)
                self.joins.append(join_object)

    def construct(self):
        source = {}
        self.safe_insert('type', self.type, source)
        self.safe_insert('connectionString', self.connection_string, source)
        self.safe_insert('tables', self.tables, source)
        self.safe_insert('joins', list(map((lambda join: join.construct()), self.joins)), source)
        return source

    def is_valid(self):
        valid = bool(self.type and self.connection_string and self.tables and self.joins)
        valid = valid and reduce((lambda x, y: x and y), map((lambda x: x.is_valid()), self.joins))
        return valid

    def make_connection(self):
        if self.type == "sqlite":
            self.eng = create_engine("sqlite:///{}".format(self.connection_string))
            self.Session = sessionmaker(bind=self.eng)
        else:
            self.eng = create_engine(self.connection_string)
            self.Session = sessionmaker(bind=self.eng)
        return self.eng, self.Session



class Ring_Join(Ring_Object):

    def __init__(self):

        # Set default values
        self.bidirectional = False

        # Initialize other properties
        self.name = None
        self.from_ = None
        self.to = None
        self.path = None

    def parse(self, join):
        self.name = join.get('name')
        self.from_ = join.get('from')
        self.to = join.get('to')
        self.path = join.get('path')
        self.bidirectional = join.get('bidirectional')

    def construct(self):
        join = {}
        self.safe_insert('name', self.name, join)
        self.safe_insert('from', self.from_, join)
        self.safe_insert('to', self.to, join)
        self.safe_insert('path', self.path, join)
        self.safe_insert('bidirectional', self.bidirectional, join)
        return join

    def is_valid(self):
        return bool(self.name and self.from_ and self.to and self.path)

class Ring_Configuration(Ring_Object):

    def __init__(self):

        # Initialize other properties
        self.name = None
        self.id = None
        self.version = None
        self.source = None
        self.entities = []
        self.relationships = []
        self.default_target_model = None

    def parse(self, configuration):
        self.name = configuration.get('name')
        self.id = configuration.get('id')
        self.version = configuration.get('version')
        self.default_target_entity = configuration.get('defaultTargetEntity')
        self.parse_source(configuration)
        self.parse_entities(configuration)
        self.parse_relationships(configuration)

    def parse_source(self, configuration):
        if 'dataSource' in configuration:
            source = Ring_Source()
            source.parse(configuration['dataSource'])
            self.source = source

    def parse_entities(self, configuration):
        if 'entities' in configuration:
            entities = configuration['entities']
            for entity in entities:
                entity_object = Ring_Entity()
                entity_object.parse(entity)
                self.entities.append(entity_object)

    def parse_relationships(self, configuration):
        if 'relationships' in configuration:
            rels = configuration['relationships']
            for rel in rels:
                relationship_object = Ring_Relationship()
                relationship_object.parse(rel)
                self.relationships.append(relationship_object)

    def parse_file_with_path(self, path):
        with open(path, 'r') as file:
            configuration = json.load(file)
            self.parse(configuration)

    def construct(self):
        configuration = {}
        self.safe_insert('name', self.name, configuration)
        self.safe_insert('version', self.version, configuration)
        self.safe_insert('dataSource', self.source.construct(), configuration)
        self.safe_insert('entities', list(map((lambda entity: entity.construct()), self.entities)), configuration)
        return configuration

    def write_to_file_with_path(self, path):
        with open(path, 'w') as file:
            configuration = self.construct()
            json.dump(configuration, file, indent=4)

    def is_valid(self):
        valid = bool(self.name and self.version and self.source and self.source.is_valid() and self.entities)
        valid = valid and reduce((lambda x, y: x and y), map((lambda x: x.is_valid()), self.entities))
        return valid

class DB_Wrapper(object):
    pass

# # These belong elsewhere but here for dev
# class SatyrnDatetime(DateTime):
#     def __init__(self):
#         DateTime.__init__(self)
#
#     def granularity(self, format):
#         breakpoint()
#
# class SatyrnDate(Date):
#     def __init__(self):
#         Date.__init__(self)

class Ring_Compiler(object):

    def __init__(self, config):
        self.config = config

    def build_ORM(self):
        self.db = DB_Wrapper()
        models = self.build_models()
        for model in models:
            setattr(self.db, model.__name__, model)
        self.db.eng, self.db.Session = self.config.source.make_connection()
        self.config.source.base.metadata.create_all(self.db.eng)
        return self.db

    def build_models(self):
        # Check configuration for validity before constructing models
        if not self.config.is_valid():
            raise ValueError('ERROR: Cannot construct ORM because configuration is invalid')

        # build model stubs
        # different approach than prototype
        # entities/attrs should fill these in
        # as entities are not 1:1 with tables in db
        model_map = {
            table["name"]: {
                "__tablename__": table["name"],
                table["primaryKey"]: self.column_with_type(table["pkType"], primary_key=True)
            }
            for table in self.config.source.tables
        }

        # build the join scaffolding
        # upstream of per-entity stuff
        model_map = self.build_joins(model_map)

        # populate models from entities/attrs
        for entity in self.config.entities:
            model_map =  self.populate_models_from_entity(entity, model_map)

        models = [type(name, (self.config.source.base,), model_info) for name, model_info in model_map.items()]

        return models

    def populate_models_from_entity(self, entity, model_map):

        # Add entity id keys if they don't exist already
        for index, id_key in enumerate(entity.id):
            if id_key not in model_map[entity.table]:
                model_map[entity.table][id_key] = self.column_with_type(entity.id_type[index])

        # Add entity attributes
        for attribute in entity.attributes:
            base_type = self.resolve_base_type(attribute.isa)
            for sc in attribute.source_columns:
                if sc not in model_map[attribute.source_table]:
                    model_map[attribute.source_table][sc] = self.column_with_type(base_type)

        return model_map

    def build_joins(self, model_map):
        # TODO: make this fully handle multi-hops and all one-to-many, many-to-many, etc conditions

        # do the join wiring
        for join in self.config.source.joins:
            for hop in join.path:
                try:
                    from_, to, key_type = hop
                    from_table, from_key = from_.split('.')
                    to_table, to_key = to.split('.')
                except:
                    print(f'ERROR: Failed to parse join path with invalid format: {hop}')

                if from_key not in model_map[join.from_]:
                    model_map[join.from_][from_key] = self.column_with_type(key_type, foreign_key=to)
                    model_map[join.from_][to_table] = relationship(to_table, back_populates=from_table, uselist=True)

                if from_table not in model_map[join.to] and join.bidirectional:
                    model_map[join.to][from_table] = relationship(from_table, back_populates=to_table, uselist=True)

        return model_map


    def column_with_type(self, type_string, primary_key=False, foreign_key=None):
        if type_string not in ["date", "datetime"]:
            sa_type = getattr(sa, type_string.capitalize())
        if primary_key:
            return Column(sa_type, primary_key=True)
        elif foreign_key:
            return Column(sa_type, ForeignKey(foreign_key))
        elif type_string == "datetime":
            return Column(DateTime, default=datetime.datetime.utcnow)
        elif type_string == "date":
            return Column(Date, default=datetime.date.today)
        else:
            return Column(sa_type)

    # TODO: Flesh out details of ontology
    def resolve_base_type(self, type_):
        if (type_ in UPPER_ONTOLOGY):
            return self.resolve_base_type(UPPER_ONTOLOGY[type_]["isa"])
        return type_

def compile_rings(rings_list):
    # for now, rings_list is a list of paths on filesystem
    # in next pass, rings_list will be a list of ids in db OR list of json objects, TBD
    rings = {}
    for ring_path in rings_list:
        config = Ring_Configuration()
        config.parse_file_with_path(ring_path)
        config.compiler = Ring_Compiler(config)
        config.db = config.compiler.build_ORM()
        rings[config.id] = config
    return rings

# NEW VERSION
def currencyConverter(amt, inDen, outDen):
    # someday
    return amt

UPPER_ONTOLOGY = {
    "currency": {
        "isa": "float",
        "subtypes": ["denomination"],
        "conversions": {
            "denominations": currencyConverter
        }
        # TODO: how to cover info about styling (templates?), types of denominations, etc
    }
}
