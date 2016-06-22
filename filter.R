##############################################################
## DataFrame Operations (Structured Data Processing) Module ##
##############################################################
## Objective: 
## Operations discussed: filter, select, arrange

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

################################################
## (1) Subset DataFrame by row and/or column: ##
################################################

## Save the dimensions our (n x m)-sized DF and the DF column names:
n <- nrow(data)
m <- ncol(data)
col <- columns(data)
## Print the schema of the DF, which describes the data types of each column in the data:
printSchema(data)

## ======================================================================================================================== ##
## (i) Subset DF by row, i.e. filter the rows of a DF according to a given condition, using the Spark operation 'filter()': ##
## ======================================================================================================================== ##


## Subset the DF into a new DF, 'f1', that includes only those loans for which JPMorgan Chase is the servicer, as denoted by the entry for 'servicer_name' (string); note that
## we execute 'filter()' with an 'is equal to' logical condition (==) and an 'or' logical operation (|):
f1 <- filter(data, data$servicer_name == "JP MORGAN CHASE BANK, NA" | data$servicer_name == "JPMORGAN CHASE BANK, NA" | data$servicer_name == "JPMORGAN CHASE BANK, NATIONAL ASSOCIATION")
cache(f1) # Note: Ignore the 'cache()' and 'unpersist()' functions for now--these will be discussed in detail in subsequent modules
n1 <- nrow(f1)

## Note that 'select()' or 'filter()' can be written with SQL statement strings; for example, here is the previous example in SQL statement format:
f2 <- filter(data, "servicer_name = 'JP MORGAN CHASE BANK, NA' or servicer_name = 'JPMORGAN CHASE BANK, NA' or servicer_name = 'JPMORGAN CHASE BANK, NATIONAL ASSOCIATION'")
cache(f2)
n2 <- nrow(f2)

## Run a quick check that the two 'filter()' operations are equivalent by checking that the number of rows for each subsetted DF are equal:
if (n1!=n2) {
  "Error: No. of rows not equal"
} else {
  "No. of rows are equal!"
}

## Check that f1 (and f2) are subsets of 'data':
if (n>n1) {
  "DF 'f1' is a subset of DF 'data'"
} else {
  "Error: 'data' did not filter correctly"
}

unpersist(f1)
unpersist(f2)


## Subset the DF as 'f3', which includes only those loans for which the servicer name is known; execute 'filter()' with an 'is not equal to' logical condition (!=) and an
## 'or' logical operation (|), then confirm that 'f3' is a subset of 'data':
f3 <- filter(data, data$servicer_name != "OTHER" | data$servicer_name != "NA")
cache(f3)
n3 <- nrow(f3)
if (n>n3) {
  "DF 'f3' is a subset of DF 'data'"
} else {
  "Error: 'data' did not filter correctly"
}
unpersist(f3)


## Subset DF as 'f4', which includes only loans for which the 'loan_age', i.e. the number of calendar months since the first full month the mortgage loan accrues interest, is
## greater than 5 years (60 calendar months); execute 'filter()' with an 'is greater than' logical condition (>), then confirm that 'f4' is a subset of 'data'::
f4 <- filter(data, data$loan_age > 60)
cache(f4)
n4 <- nrow(f4)
if (n>n4) {
  "DF 'f4' is a subset of DF 'data'"
} else {
  "Error: 'data' did not filter correctly"
}
unpersist(f4)


## Subset DF as 'f5', which includes only loans for which the 'loan_age' is greater than, or equal to, 10 years (120 calendar months); execute 'filter()' with an 'is greater
## than or equal to' logical condition (>=), then confirm that 'f5' is a subset of 'data'::
f5 <- filter(data, data$loan_age >= 120)
cache(f5)
n5 <- nrow(f5)
if (n>n5) {
  "DF 'f5' is a subset of DF 'data'"
} else {
  "Error: 'data' did not filter correctly"
}
unpersist(f5)


## Subset DF as 'f6', which includes only loans 
Filter by both of two column conditions:
f6 <- filter(data, data$aj_mths_remng != "NA" & data$loan_age >= 60)
cache(f6)
n6 <- nrow(f6)
if (n>n6) {
  "DF 'f6' is a subset of DF 'data'"
} else {
  "Error: 'data' did not filter correctly"
}
unpersist(f6)


## Filter by one of two column conditions:
f7 <- filter(data, data$new_int_rt == "NA" | data$loan_age >= 7)
n7 <- nrow(f7)


## =============================================================================================================================== ##
## (ii) Subset DF by column, i.e. select the columns of a DF according to a given condition, using the Spark operation 'select()': ##
## =============================================================================================================================== ##


## Note that 'select()' or 'filter()' can be written with SQL statement strings; for example:
f8 <- filter(data, "loan_age = 7")


## Sort DF on a specified column(s), using the 'arrange()' function (results are returned in ascending order by default):

## Arrange data by ascending 'loan_age' (and print head rows of data)
head(arrange(data, data$loan_age) ## Or, can be written in SQL statement string format as head(arrange(data, "loan_age"))

## Arrange data by descending 'loan_age'
arrange(data, desc(data$loan_age))


## http://stackoverflow.com/questions/31598611/how-to-handle-null-entries-in-sparkr

## Not run: 
##D   # Columns can be selected using `[[` and `[`
##D   df[[2]] == df[["age"]]
##D   df[,2] == df[,"age"]
##D   df[,c("name", "age")]
##D   # Or to filter rows
##D   df[df$age > 20,]
##D   # DataFrame can be subset on both rows and Columns
##D   df[df$name == "Smith", c(1,2)]
##D   df[df$age %in% c(19, 30), 1:2]
##D   subset(df, df$age %in% c(19, 30), 1:2)
##D   subset(df, df$age %in% c(19), select = c(1,2))
##D   subset(df, select = c(1,2))
## End(Not run)