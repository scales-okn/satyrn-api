'''
To define an operation, we need to
- establish what are the fields, their types/requirements, etc
	- The fields would be the ones that users would set via the plan manager
	- Here we also define "spawned fields", more details on that later on the stuff about the query prep
- Units method
- Query prep
	- From here we take
- Analysis
	- This is the methods that receives all the stuff from the query (i.e. the things from the d atabase) and
	does stuff to analyze it (e.g. correlation matrix, doing divisions/alterations, etc.)

def queryTranslationFunction(s_opts, orig_a_opts, targetEntity):

	# From the analysis statement/query, create an analysis opts that queries stuff from satyrn in its “primitive” forms 
	# (aggregations, groupbys, transformations)
	# Input: original search opts, original analysis opts, original target entity
	# Output: new search opts, new analysis opts, new target entity
	# The original analysis opts are in the form that is defined by the dictionary below
	# More often than not, search opts and target entity remain unchanged

    return new_s_opts, new_a_opts, new_targetEntity


def postQueryProcessingFunction(a_opts, results, group_args, field_names, col_names):

	# Given the queried results (resulting from the translated query), do the necessary analysis to
	# return the "real"/final analysis
	# Inputs:
		# A_opts: original opts (before the query translation)
		# Results: list of results
		# Group_args: the names of the groupby fields (e.g. “Contributor//id”)
		# Field_names: the name of each field in the result (e.g. “Contribution//amount//avg”)
		# Col_names: the type of field in the result based on the query given (e.g. “target”, “per field”)

	# Outputs:
		# Results: dictionary of results with their respective labels 
			# (in case there are multiple results, such as with correlation having the data points and the score)
			# e.g. {"results": [(),(),...], "score": 0.5},
		# Field_names: the name of each field in the result (e.g. “Contribution//amount//avg”). 
			# We return this in case this got changed in the analysis operation
		# Col_names: the type of field in the result based on the query given (e.g. “target”, “per field”). 
			# We return this in case this got changed in the analysis operation


    return results_dict field_names, col_names


def unitsFunction(a_opts, field_names, col_names, init_units):

	# Specifies the units for the results of the postQuery results
	# Input

		# A_opts: the analysis opts before the 
		# Field_names:  the name of each field in the result (e.g. “Contribution//amount//avg”). 
		# Col_names: the type of field in the result based on the query given (e.g. “target”, “per field”). 
		# Init_units: list of units in the order of field_names

	# Output
		# Units: dictionary with units for each of the results
		# e.g. {"results": init_units, "score": "no units"}

    return new_units

dct = {    
    "correlation": {
        "fields": { # dictionary of fields that will be used
			"fieldName": {
				"types": list of available types to be used
				"fieldType": "target" or "group". Determines if Satyrn Engine will groupby it or "operate"/"aggregate" on it
				"extra": { # optional, in case you want to define a numerator
					...
				},
				"spawned": True/False # optional. If True, it means that the user will not fill out this, but the Satyrn engine will expect this.
										# So, it should be added/created in the queryTranslationFunction
			},
			...
        }
        "unitsPrep": unitsFunction, 
        "nicename": string that has a nicename for the analysis opration (for aesthetic purposes only),
        "queryPrep": queryTranslationFunction,
        "pandasFunc": {
            "op": postQueryProcessingFunction,
        },
        "type": "complex",
        "groupingAllowed": {
            "groupType": [],
            "numberGroups": 0
        }
    },

}
'''