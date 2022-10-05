'''
This file is part of Satyrn.
Satyrn is free software: you can redistribute it and/or modify it under 
the terms of the GNU General Public License as published by the Free Software Foundation, 
either version 3 of the License, or (at your option) any later version.
Satyrn is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with Satyrn. 
If not, see <https://www.gnu.org/licenses/>.
'''

import datetime
from functools import reduce
import json
import os
import requests
import sys,getopt

import sqlalchemy as sa
from sqlalchemy import Boolean, Column, ForeignKey, Integer, Float, String, DateTime, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm import column_property
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import case, extract, cast
from sqlalchemy.sql.functions import concat
from sqlalchemy import func

import pandas as pd

from dateutil import parser


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
        ## is valid can return a touple of the following format
            ## return (False, "details of the error message")
                ## return (True, [])
        ## what to do? 
            ## check what exactly is failing and return a message.
                ## how do I check JSON objects or ring objects? 
            ## this would mean that I have to change all other code that uses is_valid.
                
        ## return (False, ["initialization set to false.",...] )
        return (False, {"initialization set to false."})


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

        # Andong started messing stuff up here
        self.nullHandling = None
        self.nullValue = None
        self.dateMinGranularity = None
        self.dateMaxGranularity = None

        ## Donna's trying to do some error handling 
        self.errorSet = set()

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

        # Andong started adding things here
        null_defaults = {
            "string": ("cast", "No value"),
            "float": ("ignore", 0.0),
            "int": ("ignore", 0),
            "integer": ("ignore", 0),
            "boolean": ("ignore", False),
            "date": ("ignore", None),
            "datetime": ("ignore", None),
        }
        self.nullHandling = info.get("nullHandling", null_defaults[self.baseIsa][0])
        self.nullValue = info.get("nullValue", null_defaults[self.baseIsa][1])

        date_defaults = {
            "date": ("day","year"),
            "datetime": ("second", "year"),
            "time": ("second", "hour")
        }
        if self.baseIsa and self.baseIsa in ["date", "datetime", "date"]:
            granularity = info.get("dateGranularity", date_defaults[self.baseIsa])
            self.dateMaxGranularity = granularity[1]
            self.dateMinGranularity = granularity[0]


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

        # Andong started messing with stuff here
        self.safe_insert('nullHandling', self.nullHandling, attribute)
        self.safe_insert('nullValue', self.nullValue, attribute)
        self.safe_insert('dateMinGranularity', self.dateMinGranularity, attribute)
        self.safe_insert('dateMaxGranularity', self.dateMaxGranularity, attribute)

        return attribute

    def is_valid(self):
        ## Donna's trying to do some error handling
        if self.name == None:
            self.errorSet.add("Ring Attribute 'name' is missing.")
        if self.nicename == None:
            self.errorSet.add("Attribute 'nicename' is missing.")
        if self.isa == None:
            self.errorSet.add("Ring Attribute 'isa' value missing.")
        if self.source_table == None:
            self.errorSet.add("Ring Attribute 'table' is missing.")
        if self.source_columns == None:
            self.errorSet.add("Ring Attribute 'column' is missing.")
        ## now we do some imp stuff
        if len(self.errorSet) == 0: 
            ## No errors!
                ## It will return (True, [])
            return (bool(self.name and self.nicename and self.isa and self.source_table and self.source_columns), {})
        else: 
            ## will return a string of errors
            errorString = ' '.join(self.errorSet)
            return (False, errorString)

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

        ## Donna's trying to do some error handling 
        self.errorSet = set()
        self.attribute_name = []

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
                self.attribute_name.append(name)
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
        ## Donna's trying to do some error handling
        if self.name == None:
            self.errorSet.add("Ring Entity 'name' is missing.")
        if self.table == None:
            self.errorSet.add("Ring Entity 'Table' is missing.")
        if self.id == None:
            self.errorSet.add("Ring Attribute 'id' is missing.")
        if self.id_type == None: 
            self.errorSet.add("Ring Attribute 'id type' is missing.")
        if self.attributes == None:
            self.errorSet.add("Ring Attributes are invalid.")
        ## now check to make sure the individual attributes are valid
        if (True and reduce((lambda x, y: x and y), map((lambda x: x.is_valid()[0]), self.attributes))) == False:
            self.errorSet.add("Ring Attribute is invalid.")
        if len(self.errorSet) == 0:
            ## No errors!
                ## It will return (True, [])
            return (True, {})
        else: 
            ## will return a string of errors
            errorString = ' '.join(self.errorSet)
            return (False, self.errorSet)
            #return (False, errorString)

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

        ## Donna's trying to do some error handling 
        self.errorSet = set()

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
        if self.name == None:
            self.errorSet.add("Ring Relatoinhips 'name' is missing.")
        if self.fro == None:
            self.errorSet.add("Ring relationship: 'from' is missing.")
        if self.to == None:
            self.errorSet.add("Ring relationship: 'to' is missing.")
        if self.join == []:
            self.errorSet.add("Ring relationship has no joins.")

        if len(self.errorSet) == 0:
            ## No errors: 
            return (True, {})
        else:
            ## will return a string of errors
            errorString = ' '.join(self.errorSet)
            return (False, errorString)

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

        ## Donna's trying to do some error handling 
        self.errorSet = set()

    def parse(self, source):
        self.type = source.get('type')
        if self.type in ["sqlite", "csv"]:
            ffl = os.environ.get("FLAT_FILE_LOC", "/")
            self.connection_string = os.path.join(ffl, source.get('connectionString'))
        else:
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
        self.safe_insert('connectionString', self.connection_string, source )
        self.safe_insert('tables', self.tables, source)
        self.safe_insert('joins', list(map((lambda join: join.construct()), self.joins)), source)
        return source

    def is_valid(self):
        ## Donna'a addition: let's make joins optional
        if self.type == None:
            self.errorSet.add("Ring Source 'type' is missing.")
        if self.connection_string == None:
            self.errorSet.add("Ring Source 'connection string' is missing.")
        if self.tables == None:
            self.errorSet.add("Ring Source 'tables' is missing.")
        valid =  bool(self.type and self.connection_string and self.tables)
        if self.joins:
            valid = valid and reduce((lambda x, y: x and y), map((lambda x: x.is_valid()[0]), self.joins))
            if valid == False: 
                self.errorSet.add("there is an issue with the validity of the joins.")
        if len(self.errorSet) == 0:
            return (True, {})
        else:
            ## will return a string of errors
            errorString = ' '.join(self.errorSet)
            return (False, errorString)

    def make_connection(self, db):
        if self.type == "sqlite":
            self.eng = create_engine("sqlite:///{}".format(self.connection_string))
            self.Session = sessionmaker(bind=self.eng)
        elif self.type == "csv":
            self.eng, self.Session = self.csv_file_pathway(self.connection_string, db)
        else:
            self.eng = create_engine(self.connection_string)
            self.Session = sessionmaker(bind=self.eng)
        return self.eng, self.Session


    def csv_file_pathway(self, csv_path, db, satyrn_file="satyrn_sql_file.db"):
        # Grab the csv file
        # Grab thedb as a whole

        # NOTE: assumptions we make
        # We assume that  the csv_path is a path to the folder with csvs
        # we assume that the "table names" for each model are the same
        # as the table name for each csv (e..g model "contribution" has "contribution.csv")
        # We assume that all the columns have headers, same headers as column_name
        # We will save the resulting sql file to the same csv_path
        # PENDING: checking if a populated sql file exists

        # if condition to check if all stuff has been created
        path = os.path.join(self.connection_string, satyrn_file)
        if os.path.isfile(path):
            self.eng = create_engine("sqlite:///" + path)
            self.Session = sessionmaker(bind=self.eng)
            # Here add something about checking
            # compare number of rows?
            # compare the unique identifiers or something, or basicaly iterate thru all and see if rows match
            return self.eng, self.Session
        else:
            self.eng = create_engine("sqlite:///" + path)
            self.Session = sessionmaker(bind=self.eng)            

        def cast_value(value, tpe, dateparse=None):
            if tpe == "INTEGER":
                try:
                    return int(value)
                except:
                    return None

            elif tpe == "FLOAT":
                try:
                    return float(value)
                except:
                    return None

            elif tpe == "VARCHAR":
                try:
                    return str(value)
                except:
                    return None

            elif tpe == "DATETIME":
                try:
                    if dateparse:
                        return datetime.datetime.strptime(value, dateparse)
                    else:
                        return parser.parse(value)
                except:
                    return None

            elif tpe == "DATE":
                try:
                    if value:
                        if dateparse:
                            return datetime.datetime.strptime(value, dateparse)
                        else:
                            return parser.parse(value)
                    else:
                        raise ValueError('Value was None, will return None')
                except:
                    return None
            elif tpe == "BOOLEAN":
                return bool(value)
            else:
                print("unrecognized tpe")
                return value

        for model_name in db.__dict__.keys():
            model_class = getattr(db, model_name)
            file_name = "{}{}.csv".format(self.connection_string, model_name)
            df = pd.read_csv(file_name)
            model_list = []
            model_class.metadata.create_all(self.eng)

            for idx, row in df.iterrows():
                new_model = model_class()
                for key in model_class.__dict__.keys():
                    if key[0] != "_" and key in row:
                        # Need to do parsing properly here
                        attr = getattr(model_class, key)
                        tpe = attr.type
                        value = cast_value(row[key], tpe.__str__())
                        setattr(new_model, key, value)


                # PENDING?: Maybe something with primaryKey?

                model_list.append(new_model)

            with self.Session.begin() as session:
                session.add_all(model_list)

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

        ## Donna's trying to do some error handling 
        self.errorSet = set()

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
        if self.name == None:
            self.errorSet.add("Ring Join 'name' is missing.")
        if self.from_ == None:
            self.errorSet.add("Ring Join 'from_' is missing.")
        if self.to == None:
            self.errorSet.add("Ring Join 'to' is missing.")
        if self.path == None:
             self.errorSet.add("Ring Join 'path' is missing.")

        if len(self.errorSet) == 0:
            return (True, {})
        else:
            ## will return a string of errors
            errorString = ' '.join(self.errorSet)
            return (False, errorString)

class Ring_Configuration(Ring_Object):

    def __init__(self):

        # Initialize other properties
        self.name = None
        self.dbid = None
        self.id = None
        self.version = None
        self.schemaVersion = None
        self.source = None
        self.description = None
        self.entities = []
        self.relationships = []
        self.default_target_model = None

        ## Donna's trying to do some error handling 
        self.errorSet = set()
        self.entity_name = []

    def parse(self, configuration):
        self.name = configuration.get('name')
        self.dbid = configuration.get('id')
        self.id = configuration.get('rid')
        self.version = configuration.get('version')
        self.schemaVersion = configuration.get('schemaVersion', 2)
        if self.schemaVersion > 2:
            self.default_target_entity = configuration.get('ontology', {}).get('defaultTargetEntity')
        else:
            self.default_target_entity = configuration.get('defaultTargetEntity')
        self.description = configuration.get('description')
        self.parse_source(configuration)
        self.parse_entities(configuration)
        self.parse_relationships(configuration)

    def parse_source(self, configuration):
        if 'dataSource' in configuration:
            source = Ring_Source()
            source.parse(configuration['dataSource'])
            self.source = source

    def parse_entities(self, configuration):
        if self.schemaVersion > 2:
            entities = configuration.get('ontology', {}).get('entities', [])
        else:
            entities = configuration.get('entities', [])
        for entity in entities:
            self.entity_name.append(entity)
            entity_object = Ring_Entity()
            entity_object.parse(entity)
            self.entities.append(entity_object)

    def parse_relationships(self, configuration):
        if self.schemaVersion > 2:
            rels = configuration.get("ontology", {}).get("relationships", [])
        else:
            rels = configuration.get("relationships", [])
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
        if self.name == None:
            self.errorSet.add("Ring configuration 'name' is missing.")
        if self.version == None:
            self.errorSet.add("Ring configuration 'version' is missing.")
        if self.source == None:
            self.errorSet.add("Ring configuration 'Source' is missing.")
        if self.source.is_valid()[0] == None:
            self.errorSet.add("Ring Configuration source is not valid.")
        if self.entities == None:
            self.errorSet.add("Ring configuration entities value is not valid.")
        if (True and reduce((lambda x, y: x and y), map((lambda x: x.is_valid()[0]), self.entities))) == False:
            issues = []
            sanityCheck = []
            for i in range(len(self.entities)):
                sanityCheck.append(str(self.entity_name[i]))
                if self.entities[i].is_valid()[0] == False:
                    wholeStr = str(self.entity_name[i])
                    nameSplit = wholeStr.split()[1]
                    issues.append(nameSplit)
                    self.errorSet = self.errorSet.union(self.entities[i].is_valid()[1])
            self.errorSet.add("Ring configuration entity is invalid for " + ' '.join([str(elem) for elem in issues]) )
        if len(self.errorSet) == 0:
            return (True, {})
        else:
            ## will return a string of errors
            errorString = ' '.join(self.errorSet)
            return (False, errorString)

class DB_Wrapper(object):
    pass

    def build(self):
        # DEV ONLY DO NOT USE THIS OTHERWISE
        self.Base.metadata.create_all(self.eng)

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
        self.db.eng, self.db.Session = self.config.source.make_connection(self.db) # PENDING: Maybe pass model here or check if corresponding csv
        self.config.source.base.metadata.create_all(self.db.eng)
        return self.db

    def build_models(self):
        # Check configuration for validity before constructing models
        if not self.config.is_valid()[0]:
            raise ValueError( self.config.is_valid()[1])

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

            # Path for datetime stuff
            if base_type == "date" or base_type == "datetime":
                model_map = self.datetime_path(model_map, attribute, base_type)

        return model_map

    def datetime_path(self, model_map, attribute, base_type):
        '''
        PENDING: when defining these, it might be good to have info from the entity attribute
        about the granularity we wanna go to, as well as defaults for granularity
        PENDING: what happens if value is Null? Everything returns null? RN seems to automatically cast it to now()
        Might need to put a path there to also return null if underlying value is also null
        PENDING: add a leading 0 if needed for month
        # NOTE: currently we are assuming only one source_column
        '''
        col_name = attribute.source_columns[0]
        col = model_map[attribute.source_table][col_name]
        table = attribute.source_table

        # todo: need to check if these are the correct names for extracting
        ordered_fields = ["year", "month", "day", "hour", "minute", "second", "microsecond"]
        minField = attribute.dateMinGranularity
        maxField = attribute.dateMaxGranularity
        minID = ordered_fields.index(minField)
        maxID = ordered_fields.index(maxField)

        relevant_fields = ordered_fields[maxID:minID+1]

        extr_dct = {}
        for idx, field in enumerate(relevant_fields):

            gran_name ="_only" + field if field != "year" else "_" + field
            # todo: need to cast after extract
            extr = extract(field, col)
            if field != "year" and field != "microsecond":
                # extr = func.right("00" + cast(extr, String), 2)
                pass
            else:
                extr = cast(extr, String)
            model_map[table][col_name + gran_name] = column_property(extr)
            extr_dct[field] = extr

        # check if year month day valid
        if minID > 1 and maxID == 0:
            # do year month day
            # model_map[table][col_name + "_date"] = column_property(concat(extr_dct["year"], "/", extr_dct["month"], "/", extr_dct["day"]))
            # do day of week
            model_map[table][col_name + "_dayofweek"] = column_property(cast(extract("dow", model_map[attribute.source_table][col_name]), String))
        
        # check if year month valid
        # if minID > 0 and maxID == 0:
        #     model_map[table][col_name + "_month"] = column_property(concat(extr_dct["year"], "/", extr_dct["month"]))

        # # check if month day valid
        # if minID > 1 and maxID < 2:
        #     model_map[table][col_name + "_monthday"] = column_property(concat(extr_dct["month"], "/", extr_dct["day"]))

        # # check if time valid
        # if minID == 5 and maxID < 4:
        #     model_map[table][col_name + "_time"] = column_property(concat(extr_dct["hour"], ":", extr_dct["minute"], ":", extr_dct["second"]))

        # # check if datetime valid
        # if minID == 5 and maxID == 0:
        #     model_map[table][col_name + "_datetime"] = column_property(concat(extr_dct["year"], "/", extr_dct["month"], "/", extr_dct["day"], "|", extr_dct["hour"], ":", extr_dct["minute"], ":", extr_dct["second"]))


        # For the fields smaller than "field", concatenate and add new column property
        # Edge cases to remove/rename:
        # - datetime: year month day hour minute second
        # - date: year month day
        # - time: hour minute second



        # # year
        # model_map[table][col_name + "_year"] = column_property(extract('year', col)) 

        # # month
        # model_map[table][col_name + "_onlymonth"] = column_property(extract('month', col)) 

        # # day
        # model_map[table][col_name + "_onlyday"] = column_property(extract('day', col)) 

        # # month + year
        # model_map[table][col_name + "_month"] = column_property(concat(extract('year', col), "/", extract('month', col))) 

        # month + year + day (i.e. strip time)

        # if datetime: include more

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
    extractors = {}
    for ring_path in rings_list:
        config = compile_ring(ring_path, in_type="path")
        if config.id not in rings:
            rings[config.id] = {}
            extractors[config.id] = {}
        rings[config.id][config.version] = config
        ##extractors[config.id][config.version] = RingConfigExtractor(config)
    return rings, extractors

def compile_ring(ring, in_type="json"):
    config = Ring_Configuration()
    if in_type == "path": # ring is a path to a json file
        config.parse_file_with_path(ring)
    else: # ring should be the json of a ring
        config.parse(ring)
    config.compiler = Ring_Compiler(config)
    config.db = config.compiler.build_ORM()
    return config

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

def main():
    inputfile = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "r:")
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
        #raise ValueError("ring path is not valid, check the input and try again.")
    ring_path = None
    count = 1
    for opt, arg in opts:
        if opt in ['-r']:
            ring_path = arg
        else:
            raise ValueError ("Incorrect usage: only vailable option is: -r .")
    compile_ring(ring_path, in_type="path")


if __name__ == "__main__":
    '''
    Usage: To check the validity of a ring run the following command:
        python ring_checker.py -r <local path to ring>
    '''
    main()