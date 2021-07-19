# the placeholder for the analytics engine
# might delegate to seekers.py for search/return of results? analysis optimizations TBD
from copy import deepcopy

from flask import current_app
import numpy as np
import pandas as pd
from sqlalchemy import func
from sqlalchemy.sql.expression import case

from .operations import OPERATION_SPACE as OPS
from .seekers import rawGetResultSet
from .transforms import TRANSFORMS_SPACE as TRS

# satyrn configs...
# TARGET_MODEL = current_app.satConf.targetModel
# AMS = current_app.satConf.analysisSpace
# db = current_app.satConf.db
# PREFILTERS = current_app.satConf.preFilters

cache = current_app.cache
CACHE_TIMEOUT=3000


# Method to change the AMS so that it has the defaults in it

def add_defaults(ams):
    # PENDING: id should never be cast i thinks
    nulldefault = {
            "string": ("cast", "No value"),
            "id": ("ignore", "0"),
            "float": ("ignore", 0.0),
            "int": ("ignore", 0),
            "bool": ("ignore", False),
            "date": ("ignore", None),
            "datetime": ("ignore", None),
            "stringplaceholder": ("ignore", "No value")
        }
    for key in ams.keys():
        if "nulls" not in ams[key]:
            ams[key]["nulls"] = nulldefault[ams[key]["type"]][0]
        if ams[key]["nulls"] == "cast" and "nullCast" not in ams[key]:
            ams[key]["nullCast"] = nulldefault[ams[key]["type"]][1]

# add_defaults(AMS)

class AnalyticsEngine(object):
    def __init__(self, analysisOpts={}, searchOpts={}, start_year=2007, end_year=2016):
        '''
        Main class for analysis.
        a_opts is a dict indicating what kind of analysis to do
            valid examples are:
            {
                "operation": "count",
                "groupBy": ["judge"]
                "targetField": "case"
                "timeSeries": "year"
            }
            {
                "operation": "count",
                "targetField": "case"
            }
            {
                "operation": "averageCount",
                "groupBy": ["district"],
                "targetField": "attorney
                "perField": "case"
                "timeSeries": "year"
        }
        s_opts is dict indicating what kind of search (can be identical to that of seekers.py)
            example:
            {'districts': 'Illinois', 'sortBy': 'dateFiled', 'sortDir': 'desc'}
        '''
        self.set_opts(searchOpts, analysisOpts)


    def set_opts(self, searchOpts, analysisOpts):
        # AREA OF cHANGE WITH DATES: make groupby a list of dicts with field and transform
        # will enable future of users defining their own transforms
        # move some of the stuff about transforms from prepquery to here
        if searchOpts != None:
            self.s_opts = deepcopy(searchOpts)
        # Ditch sortby and sortdir
        if "sortDir" in self.s_opts:
            del self.s_opts["sortDir"]
        if "sortBy" in self.s_opts:
            del self.s_opts["sortBy"]

        if analysisOpts != None:
            self.a_opts = deepcopy(analysisOpts)

        # Deal with groupby, semi hacky solution
        if "groupBy" in self.a_opts:
            if type(self.a_opts["groupBy"]) != list:
                self.a_opts["groupBy"] = [self.a_opts["groupBy"]]


        # Deal with timeseries
        if "timeSeries" in analysisOpts:
            if "groupBy" in analysisOpts:
                self.a_opts["groupBy"].insert(0, analysisOpts["timeSeries"])
            else:
                self.a_opts["groupBy"] = [analysisOpts["timeSeries"]]


    def update(self, analysisOpts=None, searchOpts=None):
        # TODO: method for updating search/analysis scope
        pass



    def do_correlation(self, sess):
        query_args = []
        query_args.append(getattr(AMS[self.a_opts["target1"]]["model"],AMS[self.a_opts["target1"]]["field"]).label(self.a_opts["target1"]))
        query_args.append(getattr(AMS[self.a_opts["target2"]]["model"],AMS[self.a_opts["target2"]]["field"]).label(self.a_opts["target2"]))
        fields_list = [self.a_opts["target1"], self.a_opts["target2"]]
        query = sess.query(*query_args)
        query = self._do_filters(sess, query, fields_list, False)
        query = self._do_joins(query, self.a_opts)
        lst = query.all()

        print(len(lst))
        # turn lst into
        df = pd.DataFrame(lst)
        corr_matrix = df.corr("pearson")
        print(corr_matrix)

        # Slap on units
        units = None
        # print({"results": results, "units": units})
        # print(results)

        # Do row count
        row_query = self._row_counts_query(sess, fields_list)
        row_query = self._do_joins(row_query, self.a_opts)
        # row_query = sess.query(func.count("*"))
        _, counts = self._do_filters(sess, row_query, fields_list, True)
        # print(counts)


        return {"results": [(corr_matrix[self.a_opts["target1"]][self.a_opts["target2"]], )], "units": units, "counts": counts} # TODO: add no. rows before and after drop

        exit()

    # @cache.memoize(timeout=CACHE_TIMEOUT)
    def run(self, searchOpts=None, analysisOpts=None):
        '''
        Returns list of the wanted analysis
            e.g.
            s_opts = {'districts': 'Illinois'}
            a_opts = {
                    "operation": "count",
                    "targetField": "case",
                    "timeSeries": "year",
            }
            is asking: "In district of illinois, show a year-by-year timeseries of number of cases"
            [(2007, 7760), (2008, 8223), (2009, 8880), (2010, 9241),
            (2011, 9910), (2012, 11063), (2013, 10009), (2014, 10850),
            (2015, 12305), (2016, 13138), (2017, 282), (2018, 4)]
        '''
        if searchOpts or analysisOpts:
            self.set_opts(searchOpts, analysisOpts)


        sess = db.Session()

        # HACKY CORRELATION
        if self.a_opts["operation"] == "correlation":
            return self.do_correlation(sess)

        # Prep query arguments (and do transformations if needed)
        query_args, fields_list = self._prep_query(opts=self.a_opts, q_args=[], fields_list=[])

        # Do filtering
        query = sess.query(*query_args)
        query = self._do_filters(sess, query, fields_list, False)

        # Do final analysis
        results = self._execute_analysis(sess=sess, query=query, opts=self.a_opts)

        # Make human legible
        results_df = self._make_legible(sess=sess, a_opts=self.a_opts, results=results)

        # Sorting
        if "groupBy" in self.a_opts and "judgeTenure" in self.a_opts["groupBy"]:
            # TODO PATCH: This sorting right now is only a patch for the judge tenure example, will need to be
            # generalized later. It should be doable, the main case is that it needs to have a list of the ordered
            # categories, which right now we do not have access to in this level of function. This requires a bit of
            # restructuring
            ord_list = ["Unknown", "< 5 years", "5 to 10 years", "10 to 15 years",
                        "15 to 20 years", "> 20 years"]
            ord_list = [item for item in ord_list if any(results_df["judgeTenure"] == item)]
            results_df["judgeTenure"] = results_df["judgeTenure"].astype("category")
            results_df["judgeTenure"].cat.set_categories(ord_list, inplace=True)
            results_df.sort_values(self.a_opts["groupBy"], inplace=True)

            # print(results_df)

            results = [tuple(r) for r in results_df.to_numpy()]
        else:

            results = [tuple(r) for r in results_df.to_numpy()]
            # Make into list of tuples again?
            # if len(results):
            default_val = next((item[0] for item in results if item[0] is not None), None)
            # else:
            #     default_val = None

            results = sorted(results, key=lambda tup: tup[0] if tup[0] else default_val)
            # sorted_by_second = sorted(data, key=lambda tup: tup[1])


        # Slap on units
        units = self._return_units(self.a_opts)
        # print({"results": results, "units": units})
        # print(results)

        # Do row count
        row_query = self._row_counts_query(sess, fields_list)
        row_query = self._do_joins(row_query, self.a_opts)
        # row_query = sess.query(func.count("*"))
        _, counts = self._do_filters(sess, row_query, fields_list, True)
        # print(counts)


        return {"results": results, "units": units, "counts": counts} # TODO: add no. rows before and after drop



    def _prep_query(self, opts, q_args=[], fields_list=[]):
        # Preps the query argument that will be passed to sess.query
        # q_args order: Order: (GROUPBY ARG(S), PER_FIELD (OPT), TARGETFIELD )

        # AREAOF CHANGE WITH DATES: move some transform stuff to setopts, maketransform will change to apply transform specified by opts

        if "groupBy" in opts:
            # Need to modify this a bit
            for group in opts["groupBy"]:
                # TO CHECK: check what happens with transform to change nans and setting to drop nans
                group_field = getattr(AMS[group]["model"], AMS[group]["field"])
                if (AMS[group]["type"] in ["float", "int", "stringplaceholder"] and "transform" in AMS[group]) or (AMS[group]["type"] in ["datetime"] and "transform" in AMS[group]):
                    group_field = self._make_transform(group_field, **AMS[group]["transform"])
                else:
                    if AMS[group]["nulls"] == "cast":
                        # print("in null cast")
                        group_field = self._nan_cast(group_field, AMS[group]["nullCast"])
                    else:
                        group_field = group_field
                q_args.append(group_field.label(group))
                fields_list.append(group)

        # NOTE: SHOULD BE DEPRECATED IN THE FUTURE
        if "perField" in opts:
            q_args.append(getattr(AMS[opts["perField"]]["model"],AMS[opts["perField"]]["field"]).label(opts["perField"]))
            fields_list.append(opts["perField"])


        if type(opts["targetField"]) == dict:
            return self._prep_query(opts["targetField"], q_args, fields_list)

        model_field = getattr(AMS[opts["targetField"]]["model"], AMS[opts["targetField"]]["field"])


        # TO CHECK: check what happens with transform to change nans and setting to drop nans
        if AMS[opts["targetField"]]["type"] in OPS[opts["operation"]]["dataTypes"]:
            if AMS[opts["targetField"]]["nulls"] == "cast":
                model_field = self._nan_cast(model_field, AMS[opts["targetField"]]["nullCast"])
            else:
                model_field = model_field
        else:
            if "transform" in AMS[opts["targetField"]] and AMS[opts["targetField"]]["transform"]["type"] in OPS[opts["operation"]]["dataTypes"]:
                model_field = self._make_transform(model_field, **AMS[opts["targetField"]]["transform"])
            else:
                print("ERROR transform not available")
                exit()


        if "processing" in OPS[opts["operation"]]:
            processing_args = [model_field]
            if "numeratorField" in opts:
                processing_args.append(opts["numeratorField"])
            model_field = OPS[opts["operation"]]["processing"](*processing_args)

        q_args.append(OPS[opts["operation"]]["operation"](model_field).label(opts["targetField"]))
        fields_list.append(opts["targetField"])

        return q_args, fields_list



    def _row_counts_query(self, sess, fields_list):

        q_args = []
        for field in fields_list:
            q_args.append(getattr(AMS[field]["model"],AMS[field]["field"]))

        return sess.query(*q_args)

    def _make_transform(self, field, transformBy, transformCases, type, default=None):
        return TRS[transformBy]["processor"](field, transformCases, default)


    def _nan_cast(self, field, cast_val):
        return case([(field == None, cast_val)], else_=field)


    def _do_filters(self, sess, query, fields_list, count_bool=False):

        counts = []

        query = rawGetResultSet(self.s_opts, just_query=True, sess=sess, query=query)

        # Do prefilters
        for field in PREFILTERS:
            for filt in PREFILTERS[field]:
                query = query.filter(filt)
        if count_bool:
            counts.append({"rowCount": query.count(), "type": "prefilter"})


        # Do nan dropping if needed
        for field in fields_list:
            if AMS[field]["nulls"] == "ignore": # PENDING: add here condition for if you're using the transform
                query = query.filter(getattr(AMS[field]["model"], AMS[field]["field"]) != None)
                if count_bool:
                    counts.append({"filter": field, "rowCount": query.count(), "type": "nullIgnore"})

        if count_bool:
            return query, counts

        return query




    def _execute_analysis(self, sess, query, opts):
        # Executes analysis after filtering

        query = self._do_joins(query, opts)

        # Do groupby's
        group_args = deepcopy(opts["groupBy"]) if "groupBy" in opts else []

        if opts["operation"] == "averageCount":

            group_args.append(opts["perField"])
            query = query.group_by(*group_args).subquery()

            # The build query will function as a subquery actually
            s_query = query

            # Build new query arguments
            query_args = [s_query.c[arg] for arg in group_args if arg != opts["perField"]]
            query_args.append(func.avg(s_query.c[opts["targetField"]]))
            query = sess.query(*query_args)

            # Build new group args
            new_group_args = query_args
            query_args.pop()
            if new_group_args:
                query = query.group_by(*new_group_args)

        else: # average/count case
            query = query.group_by(*group_args)
            # query = self._chain_subquery(query, opts, sess)


        return query.all()

    def _do_joins(self, query, opts):
        target_model_bool = False
        table_list = []
        alt_list = []

        if opts["operation"] == "correlation":
            key_list = [opts["target1"], opts["target2"]]
        else:
            key_list = self._get_join_fields(opts, key_list=[])
        # TO BE DEPRECATED
        if "perField" in opts:
            key_list.append(opts["perField"])

        def _update_lists(the_dict):
            if not any([the_dict == table for table in table_list]):
                # Table not added to list, will likely need to be added
                if the_dict["model"] == TARGET_MODEL:
                    # jointable model same as target model, can easily join
                    table_list.append(the_dict)
                    return True
                else:
                    # Add to potential list of tables to add if target model is false
                    if not any([the_dict == table for table in alt_list]):
                        alt_list.append(the_dict)
                    return False
            else:
                pass # table already in list of args
                return False


        for key in key_list:
            if "joinTable" in AMS[key]:
                join_dict = AMS[key]["joinTable"]
                if AMS[key]["model"] == TARGET_MODEL:
                    # Model is the same as the target model, no need to join
                    target_model_bool = True
                elif type(AMS[key]["joinTable"]["model"]) == list:
                    # Multi hop join
                    for model, field in zip(AMS[key]["joinTable"]["model"], AMS[key]["joinTable"]["field"]):
                        target_model_bool = _update_lists(the_dict={"model": model, "field": field}) or target_model_bool
                else:
                    target_model_bool = _update_lists(the_dict=join_dict) or target_model_bool

        if not target_model_bool and alt_list:
            table_list.append({"model": TARGET_MODEL, "field": None})
        else:
            table_list.extend(alt_list)

        table_list = [getattr(table["model"], table["field"]) if table["field"] else table["model"] for table in table_list ]
        query = query.join(*table_list)
        return query

    def _get_join_fields(self, opts, key_list=[]):
        key_list.extend(deepcopy(opts["groupBy"]) if "groupBy" in opts else [])
        if type(opts["targetField"]) == dict:
            return self._get_join_fields(opts["targetField"], key_list)
        else:
            key_list.append(opts["targetField"])
            key_list.reverse()
            return key_list


    def _make_legible(self, sess, a_opts, results):
        col_list = deepcopy(a_opts["groupBy"]) if "groupBy" in a_opts else []
        target_col = f"{a_opts['operation']} of {a_opts['targetField']}"
        if "perField" in a_opts:
            target_col += f" per {a_opts['perField']}"
        col_list.append(target_col)

        # Make human legible
        results_df = pd.DataFrame(results, columns=col_list)
        if "groupBy" in a_opts:
            for group in a_opts["groupBy"]:
                # TODO: add changin default (might not be needed if already handled in query before)
                if "rowNameConversion" in AMS[group]:
                    name_changer = AMS[group]["rowNameConversion"]
                    if type(AMS[group]["rowNameConversion"]) == dict:
                        name_map = name_changer
                    elif type(AMS[group]["rowNameConversion"]) == list:
                        name_map = self._get_name_map(sess, group, results_df[group])
                    else:
                        print("Unsure how to change data with this rowNameConversion data type")
                        name_map = {}
                    if name_map:
                        results_df[group] = results_df[group].apply(lambda x: name_map[x])
                elif "transform" in AMS[group] and AMS[group]["transform"]["transformBy"] == "month_transform":
                    # TODO: This is quite gnarly and patchy, need more sustainable solution for this
                    name_func = lambda x: "/".join(y.zfill(2) for y in x.split("/"))
                    results_df[group] = results_df[group].apply(lambda x: name_func(x))



        if self.a_opts["operation"] == "percentage":
            # TODO PATCH: Should have more sustainable solution in the future if needed
            results_df[target_col] = results_df[target_col].apply(lambda x: 100*x)

        return results_df


    def _get_name_map(self, sess, group, group_col):
        # eventual todo: generalize this better?
        model_list = AMS[group]["rowNameConversion"]
        unique_ids = set(group_col)
        def format_name(tup):
            return " ".join([str(x) for x in tup if x])
        name_map = {ind: format_name(sess.query(*model_list).filter(getattr(AMS[group]["model"], AMS[group]["field"]) == ind).first()) for ind in unique_ids}
        return name_map



    def _return_units(self, opts={}):
        # For returning a tuple of units. Tuple should follow same order as the tuples that will be returned, i.e. (YEAR, GROUPBY, TARGET)
        # BIG TODO: Handling chained operations, does not handle it rn
        if opts == {}:
            opts = self.a_opts

        if type(opts["targetField"]) == dict:
            print("chained, will not return stuff")
            return None

        units = []

        if "groupBy" in opts:
            for group in opts["groupBy"]:
                units.append(AMS[group]["unit"])


        # Get effect of operation on units
        effect = OPS[opts["operation"]]["units"]


        # Make target units depending on effect of operation
        if effect == "unchanged":
            units.append(AMS[opts["targetField"]]["unit"])
        elif effect == "target/per":
            units.append([x + "/" + AMS[opts["perField"]]["unit"][0] for x in AMS[opts["targetField"]]["unit"] ])
        elif effect == "percentage":
            numer_string = "/".join([ str(x) for x in opts["numeratorField"] ] )
            units.append([ f"% of {x} that are equal to {numer_string}" for x in AMS[opts["targetField"]]["unit"]])


        return units


    # CUrrently not in use
    # def _chain_subquery(self, query, opts, sess):
    #     # group_args includes the grouping at this level
    #     # group_args_sofar does not include grouping so far
    #     # THIS ONLY APPLIES FOR TWO LEVELS, FUTURE WORK IS TO MAKE IT INDEFINITE LENGTH
    #     group_args = deepcopy(opts["groupBy"]) if "groupBy" in opts else []
    #     if type(opts["targetField"]) == dict:

    #         group_args.extend(opts["targetField"]["groupBy"])
    #         query = query.group_by(*group_args).subquery()

    #         # The build query will function as a subquery actually
    #         s_query = query

    #         # Build new query arguments
    #         query_args = [s_query.c[arg] for arg in group_args if arg not in opts["targetField"]["groupBy"]]
    #         model_field = s_query.c[opts["targetField"]["targetField"]]
    #         if "processing" in OPS[opts["operation"]]:
    #             model_field = OPS[opts["operation"]]["processing"](model_field)

    #         query_args.append(OPS[opts["operation"]]["operation"](model_field).label(opts["targetField"]["targetField"]))
    #         query = sess.query(*query_args)

    #         # Build new group args
    #         new_group_args = query_args
    #         new_group_args.pop()
    #         if new_group_args:
    #             query = query.group_by(*new_group_args)

    #     else: # average/count case
    #         query = query.group_by(*group_args)
    #     return query
