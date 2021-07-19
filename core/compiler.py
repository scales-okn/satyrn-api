import os
import json
import sqlalchemy as sa
from functools import reduce
from sqlalchemy import Boolean, Column, ForeignKey, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import create_engine

DefaultBase = declarative_base()

# This is an abstract class which serves as the superclass for concrete ring classes
class Ring_Object(object):

    def safe_extract(self, key, dictionary):
        if key in dictionary:
            return dictionary[key]
        return None

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
        self.nicename = self.safe_extract('nicename', info)
        self.isa = self.safe_extract('isa', info)

        # this next one is to separate conceptual type from data type (currency vs float)
        # doesn't matter know but will be useful later when we leverage upper ontology
        self.baseIsa = self.safe_extract('isa', info)

        self.units = self.safe_extract('units', info)
        self.description = self.safe_extract('description', info)

        if 'source' in info:
            source = info['source']
            self.source_table = self.safe_extract('table', source)
            self.source_columns = self.safe_extract('columns', source)
            self.join_required = self.source_table != self.parent_entity["table"]

        if 'ux' in info:
            ux = info['ux']
            self.searchable = self.safe_extract('searchable', ux)
            self.allow_multiple = self.safe_extract('allowMultiple', ux)
            self.search_style = self.safe_extract('searchStyle', ux)
            self.analyzable = self.safe_extract('analyzable', ux)
            self.autocomplete = ux.get('autocomplete', False)

    def construct(self):

        attribute = {}
        self.safe_insert('name', self.name, attribute)
        self.safe_insert('nicename', self.nicename, attribute)
        self.safe_insert('isa', self.isa, attribute)

        source = {}
        self.safe_insert('table', self.source_table, source)
        self.safe_insert('columns', self.source_columns, source)

        ux = {}
        self.safe_insert('searchable', self.searchable, ux)
        self.safe_insert('allowMultiple', self.allow_multiple, ux)
        self.safe_insert('searchStyle', self.search_style, ux)
        self.safe_insert('analyzable', self.analyzable, ux)

        self.safe_insert('source', source, attribute)
        self.safe_insert('ux', ux, attribute)
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
        # TODO: Description
        self.attributes = []

    def parse(self, entity):
        self.name = self.safe_extract('name', entity)
        self.table = self.safe_extract('table', entity)
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

class Ring_Source(Ring_Object):

    def __init__(self):

        # Set default values
        self.type = 'sqlite'

        # Initialize other properties
        self.connection_string = None
        self.tables = None
        self.joins = []

        # Tie in the base
        self.base = DefaultBase

    def parse(self, source):
        self.type = self.safe_extract('type', source)
        self.connection_string = self.safe_extract('connectionString', source)
        self.tables = self.safe_extract('tables', source)
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
        self.name = self.safe_extract('name', join)
        self.from_ = self.safe_extract('from', join)
        self.to = self.safe_extract('to', join)
        self.path = self.safe_extract('path', join)
        self.bidirectional = self.safe_extract('bidirectional', join)

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
        self.default_target_model = None

    def parse(self, configuration):
        self.name = self.safe_extract('name', configuration)
        self.id = self.safe_extract('id', configuration)
        self.version = self.safe_extract('version', configuration)
        self.default_target_entity = self.safe_extract('defaultTargetEntity', configuration)
        self.parse_source(configuration)
        self.parse_entities(configuration)

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
            print('ERROR: Cannot construct ORM because configuration is invalid')
            return None

        models = []
        for entity in self.config.entities:
            model = self.build_model_for_entity(entity)
            models.append(model)

        return models

    def build_model_for_entity(self, entity):

        model_info = {'__tablename__': entity.table}

        # Add primary keys
        for index, primary_key in enumerate(entity.id):
            model_info[primary_key] = self.column_with_type(entity.id_type[index], primary_key=True)

        # Add attributes
        for attribute in entity.attributes:
            base_type = self.resolve_base_type(attribute.isa)
            model_info[attribute.name] = self.column_with_type(base_type)

        for join in self.config.source.joins:
            for hop in join.path:

                try:
                    from_, to, key_type = hop
                    from_table, from_key = from_.split('.')
                    to_table, to_key = to.split('.')
                except:
                    print(f'ERROR: Failed to parse join path with invalid format: {hop}')

                if join.from_ == entity.table:
                    model_info[from_key] = self.column_with_type(key_type, foreign_key=to)
                    model_info[to_table] = relationship(to_table.capitalize(), back_populates=from_table, uselist=True)

                if join.to == entity.table and join.bidirectional:
                    model_info[from_table] = relationship(from_table.capitalize(), back_populates=to_table, uselist=True)

        return type(entity.name, (DefaultBase,), model_info)

    def column_with_type(self, type_string, primary_key=False, foreign_key=None):
        sa_type = getattr(sa, type_string.capitalize())
        if primary_key:
            return Column(sa_type, primary_key=True)
        elif foreign_key:
            return Column(sa_type, ForeignKey(foreign_key))
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

"""
path = '/Users/servantez/Northwestern/Satyrn/satyrn-basic/satyrn-templates/basic/ring.json'
config = Ring_Configuration()
config.parse_file_with_path(path)

for entity in config.entities:
    print('Found entity with name: ' + entity.name)
    for attribute in entity.attributes:
        print('Found attribute with name: ' + attribute.name)

out_path = path = '/Users/servantez/Northwestern/Satyrn/satyrn-basic/satyrn-templates/basic/test_write.json'
config.write_to_file_with_path(path)

compiler = Ring_Compiler(config)
models = compiler.build_ORM()

for model in models:
    print(model)
"""
