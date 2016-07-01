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

## Read in example HFPC data from AWS S3 as a DataFrame (DF):
df <- read.df(sqlContext, "s3://ui-hfpc/hfpc_ex_par", header='false', inferSchema='true')
cache(df)


## Save the dimensions our (n x m)-sized DF and the DF column names so that we can compare the dimension sizes of `df` and the subsets that we define throughout this
## tutorial:
(n <- nrow(df))
(m <- ncol(df))
(col <- columns(df))

#############################################################################################################################
## (1) Subset DataFrame by row, i.e. filter the rows of a DF according to a given condition, using the operation `filter`: ##
#############################################################################################################################

## Print the schema of `df` since the dtype of each column will determine how we will specify subsetting conditions.
printSchema(df)

## Subset the DF into a new DF, 'f1', that includes only those loans for which JPMorgan Chase is the servicer, as denoted by the entry for 'servicer_name' (string); note that
## we execute 'filter()' with an 'is equal to' logical condition (==) and an 'or' logical operation (|):
f1 <- filter(df, df$servicer_name == "JP MORGAN CHASE BANK, NA" | df$servicer_name == "JPMORGAN CHASE BANK, NA" | df$servicer_name == "JPMORGAN CHASE BANK, NATIONAL ASSOCIATION")
(n1 <- nrow(f1))

## Note that 'select()' or 'filter()' can be written with SQL statement strings; for example, here is the previous example in SQL statement format:
f2 <- filter(df, "servicer_name = 'JP MORGAN CHASE BANK, NA' or servicer_name = 'JPMORGAN CHASE BANK, NA' or servicer_name = 'JPMORGAN CHASE BANK, NATIONAL ASSOCIATION'")
(n2 <- nrow(f2))

## Subset the DF as 'f3', which includes only those loans for which the servicer name is known, i.e. servicer name is not equal to an empty entry or listed as "other".
## Execute 'filter()' with an 'is not equal to' logical condition (!=) and an 'and' logical operation (&):
f3 <- filter(df, df$servicer_name != "OTHER" & df$servicer_name != "")
(n3 <- nrow(f3))

## Subset DF as 'f4', which includes only loans for which the 'loan_age', i.e. the number of calendar months since the first full month the mortgage loan accrues interest, is
## greater than 5 years (60 calendar months); execute 'filter()' with an 'is greater than' logical condition (>):
f4 <- filter(df, df$loan_age > 60)
(n4 <- nrow(f4))

## Subset DF as 'f5', which includes only loans for which the 'loan_age' is greater than, or equal to, 10 years (120 calendar months); execute 'filter()' with an 'is greater
## than or equal to' logical condition (>=):
f5 <- filter(df, df$loan_age >= 120)
(n5 <- nrow(f5))

## An alias for `filter` is `where`, which presents diction that is often much more intuitive, particularly when `where` is embedded in a complex statement. For example, the following expression can be read as "aggregate and return the mean loan age and count values for observations in `df` where loan age is less than 60 months"

f6 <- agg(groupBy(where(df, df$loan_age < 60), where(df, df$loan_age < 60)$servicer_name), loan_age_avg = avg(where(df, df$loan_age < 60)$loan_age), count = n(where(df, df$loan_age < 60)$loan_age))
head(f6)

#############################################################################################################################
## (2) Subset DF by column, i.e. select the columns of a DF according to a given condition, using the operation `select`: ###
#############################################################################################################################

## 


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