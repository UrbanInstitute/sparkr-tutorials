#########################################################
### Social Science Module 1: Basic Summary Statistics ###
#########################################################

library(SparkR)

## Initiate SparkContext:
sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", 
                                  spark.driver.memory="1g",
                                  spark.driver.maxResultSize="1g")
                  ,sparkPackages="com.databricks:spark-csv_2.11:1.4.0") ## Load CSV Spark Package
## AWS EMR is using Spark 2.11 so we need the associated version of spark-csv: http://spark-packages.org/package/databricks/spark-csv
## Define Spark executor memory, as well as driver memory and maxResultSize according to cluster configuration

## Initiate SparkRSQL:
sqlContext <- sparkRSQL.init(sc)

## Read in example HFPC data (quarterly performance data from XXXX) from AWS S3:
data <- read.df(sqlContext, "s3://ui-hfpc/hfpc_ex_par", header='false', inferSchema='true')

## Count the number of observations by 'servicer_name' (string variable)
collect(arrange(count(groupBy(data, "servicer_name")), "servicer_name"))