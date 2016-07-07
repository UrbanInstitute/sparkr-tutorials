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

### Specify null values when loading data:

df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header="false", inferSchema="true", nullValue="")
cache(df)

# Join two DataFrames based on the given join expression.
# join(x, y, joinExpr = NULL, joinType = NULL)
# The type of join to perform. The following join types are available: 'inner', 'outer', 'full', 'fullouter', leftouter', 'left_outer', 'left', 'right_outer', 'rightouter', 'right', and 'leftsemi'. The default joinType is "inner".
join(df1, df2) # Performs a Cartesian
join(df1, df2, df1$col1 == df2$col2) # Performs an inner join based on expression
join(df1, df2, df1$col1 == df2$col2, "right_outer")


# Return a new DataFrame containing the union of rows in this DataFrame and another DataFrame. Note that this does not remove duplicate rows across the two DataFrames.
# This is equivalent to 'UNION ALL' in SQL.
unionAll(x, y)
# Combines two (2) or more SparkR DataFrames by rows. Does not remove duplicate rows.
rbind(x, y, z, w)


# Check for duplicates prior to merging with `intersect`:
intersect(x, y)

# Return a new DF that includes only distinct rows, i.e. filters out duplicate rows that may result from merging:
distinctDF <- distinct(df)