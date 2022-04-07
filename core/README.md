# Log of known issues/bugs



## Core Capabilities

#### Addresses

- Currently not considered in the implementation of Satyrn

### Filtering

- Filtering on different date granularities (e.g. Contributions only from 2020, rather than having to specify [01-01-2020,12-31-2020])

- Prefilters: currently not implemented

### Analysis

- Mode not currently implemented


#### Bucketing based on Percentiles

The types of questinos that we would answer with this are:
- Count of contributions grouped by contributor age percentiles
- Count of cases grouped by judge tenure percentiles
- Average contributor age grouped by contributor annual income percentile
- Average count of cases per judge grouped by judge tenure percentile
- Average judge income grouped by judge tenure percentiles
- Count of cases grouped by judge tenure percentiles year over year

It is worth noting that none of these examples are doable with the current data (except the ones with the SCALES data, presumably)

There are a couple of approaches that we can take to implement this feature:

1. Identify a way to do it all in SQL. It seems relatively feasible with proper use of functions such as `percent_rank`, `rank`, `partition`, `over`. However, it would take some time to work through it. In preliminary explorations that Andong has done with this, it does not work when you simply try to do a `case` statement on a `percent_rank` "transformation" of the numeric field
2. Do a multi-query approach, meaning first do a query to obtain the percentile thresholds needed (e.g. the 0th 25th, 50th, 75th, 100th percentiles). Then, do the actual query we want to do, with a `case` statement do the bucketing of the field into its respective percentiles. There are a couple of problems with this approach
	-  We currently do not distinguish between the different uses of relationships. All relationships utilized are lumped together into one list. This might be problematic when doing that initial query because we need to maintain all the relationship related to filtering and to the field which we are trying to obtain its percdentile thresholds (e.g. suppose that our searchable entity is Contributions and the field we are trying to bucket is in Contributor). One fix for this would be to have relationships associated with each individual field (and maybe also have them in each filter?). It is certainly doable, but would require some engine and plan changes
	- The more difficult issue would be when there is grouping/timeseries in addition to the bucketing. Consider the test case "Count of cases grouped by judge tenure percentiles year over year". Here, we would actually want the percentiels per year (meaning that we want to bucket into the 0,25,50,75,100th percentiles for each year). To do this, we would have to get the percentiles for each of the other groups (e.g. the percentiles for years 2010, 2011, 2012, etc.), and then create a `case` statement that buckets into its respective percentiles given the value of the other groups. It is unclear how complex (and feasible) this case statement would be. (NOTE: It is also unclear if this problem would be solved if we did everything through SQL. We solved a relatively-similar problem in the `distribution` function by implementing it as a plugin through pandas)

#### Formatting results

Currently we do not do formatting of results for booleans (i.e. it'll only return True/False)


### Analysis Plugins 

#### Restructure

We had planned on implementing the analysis plugins as a separate library/pip service that gets imported.

#### Tests

Though we have tests to see if the analysis plugins return the correct results, we do not currently hae a checker that checks if the operation is properly implemented (or if the operation dict is fine).

#### Multi-query Analysis

Currently we do not have this implemented nor do we have examples of this. However, we have a general idea of how it work

## Plan Verification

### Search Space Verification

- Relationship checking: Check if the given relationships actually make sense or conflict with each other in some way
- Sanitizing parameters for SQL (to prevent people trying to drop or alter the database)
- Data typoe and format validity for filters: Currently it exists but checking if it is all good

### Analysis Space Verification

- Relationship checking: Check if the given relationships actually make sense or conflict with each other in some way
- Sanitizing parameters for SQL (to prevent people trying to drop or alter the database)
- Statement parameter checking: checking if the parameters for that operation (e.g. "numerator" field) were properly filled

## Analysis Space Creation

- Analysis space augmentation/creation based on entities that are not the search entity

## Statement Manager

- Multiple Groupbys
- Handling granularities in date timeseries

## Ring Configuration/Compiler

- Id types that are not based on database-style ids
- Partially-defined date fields
	- E.g. only “year” available for a date field
- Relationship checking for ring consistency


## Tests

- Static, more complex dataset for
	- Derived relationships
	- Multihop relationships
- Single entity across multiple tables
- Multiple entities in single table
- Redefine test assert equal to account for changes in column order, rounding issues


## Visualization