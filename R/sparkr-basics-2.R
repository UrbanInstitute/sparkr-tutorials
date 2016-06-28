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

dat <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header='false', inferSchema='true')
cache(dat)

## The SparkR DataFrame (DF) API supports a number of operations to do structured data processing. These operations range from the simple tasks that we used in the SparkR
## Basics I tutorial (e.g. subsetting a DF by column(s) using `select` and counting the number of rows in a DF using `nrow`) to more complex tasks like aggregating statistics
## by DF column, which we will discuss below.

## Warning: Most of the operations discussed below create new DFs, so be sure to specify a new name if saving the result of the transformation so as to not override original
## DF


## Grouping & Aggregating: want to aggregate statistics across all elements in a DataFrame that share a common identifier - agg & summarize compute aggregations of DF entries
## based on a specified list of columns (Note: for consistency, we'll use only `agg` for the remainder of the tutorial)

# Average loan age across the entire DF:
df1 <- agg(dat, loan_age_avg = avg(dat$loan_age))
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
df2 <- agg(groupBy(dat, dat$servicer_name), loan_age_avg = avg(dat$loan_age), count = n(dat$loan_age))
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
## terms of calendar years, we can create a new DF using the `withColumn` operation - this DF contains every column originally included in `dat`, as well as with an
## additional column `loan_age_yrs` that has the described year values as entries
head(df3 <- withColumn(dat, "loan_age_yrs", dat$loan_age * (1/12)))

## We can rename a column using the `withColumnRenamed` operation - returns a DF that is equivalent to `dat`, but we have replaced `loan_age` with `loan_age_yrs`
df4 <- withColumnRenamed(dat, "servicer_name", "servicer")


## [Insert: section on UDFs (available in SparkR 2.0)]




## Lazy computing - local v. distributed in SparkR




## Cache v. Persist & unpersist


## Examples of cache/persist