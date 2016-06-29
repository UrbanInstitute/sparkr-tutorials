###############################################
## Performing SparkR operations on DataFrame ##
###############################################
## Objective: provide examples of essential SparkR operations, discuss how lazy computing works & differences between cache/persist, provide examples of persist/cache (with
## timing tracked to show differences in compute time for cache placement)
## Operations discussed: groupBy, aggregate, collect, persist, cache, unpersist

library(SparkR)

## Initiate SparkContext:

sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", 
                                  spark.driver.memory="1g",
                                  spark.driver.maxResultSize="1g")
                  ,sparkPackages="com.databricks:spark-csv_2.11:1.4.0") # Load CSV Spark Package

## AWS EMR is using Spark 2.11 so we need the associated version of spark-csv: http://spark-packages.org/package/databricks/spark-csv
## Define Spark executor memory, as well as driver memory and maxResultSize according to cluster configuration

## Initiate SparkRSQL:

sqlContext <- sparkRSQL.init(sc)

## Read in loan performance example data:

df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header='false', inferSchema='true')
cache(df)

## The SparkR DataFrame (DF) API supports a number of operations to do structured data processing. These operations range from the simple tasks that we used in the SparkR
## Basics I tutorial (e.g. subsetting a DF by column(s) using `select` and counting the number of rows in a DF using `nrow`) to more complex tasks like aggregating statistics
## by DF column, which we will discuss below.

## Warning: Most of the operations discussed below create new DFs, so be sure to specify a new name if saving the result of the transformation so as to not override original
## DF


## Grouping & Aggregating: want to aggregate statistics across all elements in a DataFrame that share a common identifier - agg & summarize compute aggregations of DF entries
## based on a specified list of columns (Note: for consistency, we'll use only `agg` for the remainder of the tutorial)

# Average loan age across the entire DF:
df1 <- agg(df, loan_age_avg = avg(df$loan_age))
head(df1)

# There are number of aggregation functions that can be computed when included in `agg` - these are the statistics that can be aggregated in SparkR (though the list below is
# not exhaustive):
# count & n returns the number of items/rows in a group. The resulting DataFrame will also contain the grouping columns
# avg, mean: returns the avg for the group
# sd, stddev, stddev_samp: returns the unbiased sample standard deviation of the expression in a group
# stddev_pop: returns the population standard deviation of the expression in a group
# var, variance, var_samp: returns the unbiased variance of the values in a group
# var_pop: returns the population variance of the values in a group
# countDistinct, n_distinct: returns the number of distinct items in a group
# first, last: returns the first, last item in a group
# max, min: returns the maximum, minimum value of the expression in a group
# sum: returns the sum of all values in the expression

## groupBy: by embedding `groupBy` in `agg`, `agg` returns aggregated statistics across distinct elements of the DF column specified in `groupBy'

# For each distinct `"servicer_name"` entry, the following `agg` operation returns the average loan_age and the number of observations in the DF for a distinct
# `"servicer_name"` entry:
df2 <- agg(groupBy(df, df$servicer_name), loan_age_avg = avg(df$loan_age), count = n(df$loan_age))
cache(df2)
head(df2)
# Note that we can specify the `agg` operation to return several statistics



## Arrange/orderBy - Sort a DataFrame by the specified column(s)
## Using the DF `df2` that we created above, we can order the DF rows by either `loan_age_avg` or `count`:

head(arrange(df2, df2$loan_age_avg))	# Default is "asc"
head(arrange(df2, desc(df2$count), asc(df2$loan_age_avg)))

## You can also specify ordering as logical statements:
arrange(df2, "loan_age_avg", decreasing = FALSE)
arrange(df2, "count", "loan_age_avg", decreasing = c(TRUE, FALSE))

unpersist(df2)


## Create new DF with new col

## The values of `loan_age` are the number of calendar months since the first full month the mortgage loan accrues interest. If we want to work with this measurement in
## terms of calendar years, we can create a new DF using the `withColumn` operation - this DF contains every column originally included in `df`, as well as with an
## additional column `loan_age_yrs` that has the described year values as entries
head(df3 <- withColumn(df, "loan_age_yrs", df$loan_age * (1/12)))

## We can rename a column using the `withColumnRenamed` operation - returns a DF that is equivalent to `df`, but we have replaced `loan_age` with `loan_age_yrs`
df4 <- withColumnRenamed(df, "servicer_name", "servicer")


## [Insert: section on UDFs (available in SparkR 2.0)]


## Differentiating between operation types & why :

## Throughout this tutorial, as well as in the SparkR Basics I tutorial, you may have noticed that some operations result in a new DF (e.g. `agg`) and some return an output
## (e.g. `head`). SparkR operations can be classified as
## * transformations - those operations that return a new SparkR DataFrame; or,
## * actions - those operations that return an output (these outputs range from a single, aggregated statistic to the entire DF being printed)

## A fundamental characteristic of Apache Spark that allows us SparkR-users to perform efficient analysis on massive data is that transformations are lazily
## evaluated, meaning that SparkR delays evaluating these operations until we direct it to return some ouput (as communicated by an action operation). We can intuitively
## think of transformations as instructions that SparkR acts on only once its directed to return a result.
## This lazy evaluation strategy (1) reduces the number of processes SparkR is required to complete and (2) allows SparkR to interpret the entire set of instructions
## (transformations) before acting, and make processing decisions that are obscured from SparkR-users in order to further optimize the evaluation of the expressions that we communicate
## to SparkR.

## DataFrame Persistence (& what is a DataFrame actually?)

## Note that, in this tutorial, we have been saving the output of transformation operations (e.g. `withColumn`) in the format `dfi`. SparkR saves the output of a transformation
## as a SparkR DataFrame, which is distinct from an R data.frame. We store the instructions communicated by a transformation as a SparkR DataFrame. An R data.frame,
## conversely, is an actual data structure defined by a list of vectors.

## We saved the output of the first transformation included in this tutorial, `read.df`, as `df`. This operation does not load data into SparkR - the DataFrame `df` details
## instructions that the data should be read in and how SparkR should interpret the data as it is read in. Every time we directed SparkR to evaluate the expressions
head(df, 5)
head(df, 10)
## SparkR would need to:
## (1) Read in the data as a DataFrame
## (2) Look for the first five (5) rows of the DataFrame
## (3) Return the first five (5) rows of the DataFrame
## (4) Read in the data as a DataFrame
## (5) Look for the first ten (10) rows of the DataFrame
## (6) Return the first ten (10) rows of the DataFrame
## Note that nothing is stored since the DataFrame is not data! This would be incredibly inefficient if not for the `cache` operation, which directs each node in our
## cluster to store in memory any partitions of a DataFrame that it computes (in the course of evaluating an action) and then to reuse them in subsequent actions evaluated
## on that DataFrame (or DataFrames derived from it). By caching a given DataFrame, we can ensure that future actions on that DataFrame (or those derived from it) are
## evaluated much more efficiently. Both `cache` and `persist` can be used to cache a DataFrame. The `cache` operation stores a DataFrame in memory, while `persist` allows
## SparkR-users to persist a DataFrame using different storage levels (i.e. store to disk, memory or both). The default storage level for `persist` is memory only and, at
## this storage level, `persist` and `cache` are equivalent operations. More often than not, we can simply use `cache` - if our DataFrames can fit in memory only, then
## we should exclusively store DataFrames in memory only since this is the most CPU-efficient storage option.

## Now that we have some understanding of how DataFrame persistence works in SparkR, let's see this powerful operation in action. In the following expressions, we are
## giving SparkR directions to:
## (1) Read in the data as a DataFrame
## (2) Cache the DataFrame
## (3) Look for the first five (5) rows of the DataFrame
## (4) Return the first five (5) rows of the DataFrame
## (5) Look for the first ten (10) rows of the DataFrame
## (6) Return the first ten (10) rows of the DataFrame
df_ <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header='false', inferSchema='true')
cache(df_)
head(df_, 5)
head(df_, 10)
## While the number of steps required remains six (6), the time required to `cache` a DataFrame is significantly less than that required to read in data as a DataFrame.
## If we continuited to perform actions on `df_`, clearly directing SparkR to load and then cache the DataFrame would reduce our overal evaluation time. We can direct
## SparkR to stop persisting a DataFrame with the `unpersist` operation:
unpersist(df_)

## Let's compare computation time for several sequences of operations with, and without, caching:

.df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header='false', inferSchema='true')
(t1 <- system.time(ncol(.df)))
(t2 <- system.time(nrow(.df)))
(t3 <- system.time(dim(.df)))
rm(.df)

.df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header='false', inferSchema='true')
cache(.df)
(t1_ <- system.time(ncol(.df)))
(t2_ <- system.time(nrow(.df)))
(t3_ <- system.time(dim(.df)))
unpersist(.df)
rm(.df)