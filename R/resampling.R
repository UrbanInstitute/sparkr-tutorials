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

## Note that we now include the `nullValue` option in the `read.df` transformation below. By setting `nullValue` equal to an empty string in `read.df`, we direct `read.df` and the `sqlContext` to interpret empty entries in the dataset as being equal to a null value in the DataFrame. Therefore, any DF entries matching this string, below set to equal an empty entry, will be set as nulls in `df`.

df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header="false", inferSchema="true", nullValue="")
cache(df)
n <- nrow(df)