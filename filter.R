################################################################
### DataFrame Operations (Structured Data Processing) Module ###
################################################################
### Objective: 
### Operations discussed: filter, select, arrange

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

## Read in example HFPC data (quarterly performance data from XXXX) from AWS S3 as a DataFrame (DF):
data <- read.df(sqlContext, "s3://ui-hfpc/hfpc_ex_par", header='false', inferSchema='true')
cache(data)

# SUBSET BY ROW/COLUMN
# ********************

n <- nrow(data)
columns(data)
printSchema(data)

## Subset DF by row, i.e. filter the rows of a DF according to a given condition, using the Spark operation 'filter()':

## Subset DF into a new DF, 'f1', that includes only those loans for which JPMorgan Chase is the servicer, as denoted by the entry for 'servicer_name' (string); note that
## we execute 'filter()' with an 'is equal to' logical condition (==) and an 'or' logical operation (|):
f1 <- filter(data, data$servicer_name == "JP MORGAN CHASE BANK, NA" | data$servicer_name == "JPMORGAN CHASE BANK, NA" | data$servicer_name == "JPMORGAN CHASE BANK, NATIONAL ASSOCIATION")
n_serv_JPM <- nrow(f1)

## Subset DF as 'f2', which includes only those loans for which the servicer name is known; execute 'filter()' with an 'is not equal to' logical condition (!=) and an 'or'
## logical operation (|):
f2 <- filter(data, data$servicer_name != "OTHER" | data$servicer_name != "NA")
n_serv_known <- nrow(f2)

## Subset DF as 'f3', which includes only loans for which the 'loan_age', i.e. the number of calendar months since the first full month the mortgage loan accrues interest, is
## greater than 5 years (60 calendar months); execute 'filter()' with an 'is greater than' logical condition (>):
f3 <- filter(data, data$loan_age > 60
n_gt5 <- nrow(f3)

## Subset DF as 'f4', which includes only loans for which the 'loan_age' is greater than, or equal to, 10 years (120 calendar months); execute 'filter()' with an 'is greater
## than or equal to' logical condition (>=):
f4 <- filter(data, data$loan_age >= 120)
n_get10 <- nrow(f4)

## Subset DF as 'f5', which includes only loans 
Filter by both of two column conditions:
f5 <- filter(data, data$new_int_rt == "NA" & data$loan_age >= 7)
n5 <- nrow(f5)

## Filter by one of  two column conditions:
f6 <- filter(data, data$new_int_rt == "NA" | data$loan_age >= 7)
n6 <- nrow(f6)


## Subset DF by column, i.e. select the columns of a DF according to a given condition, using the Spark operation 'select()':



## Note that 'select()' or 'filter()' can be written with SQL statement strings; for example:
f7 <- filter(data, "loan_age = 7")


## Sort DF on a specified column(s), using the 'arrange()' function (results are returned in ascending order by default):

## Arrange data by ascending 'loan_age' (and print head rows of data)
head(arrange(data, data$loan_age) ## Or, can be written in SQL statement string format as head(arrange(data, "loan_age"))

## Arrange data by descending 'loan_age'
arrange(data, desc(data$loan_age))