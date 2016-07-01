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


## Check the dimensions our (n x m)-sized DF and the DF column names so that we can compare the dimension sizes of `df` and the subsets that we define throughout this
## tutorial:
nrow(df)
ncol(df)
columns(df)

#############################################################################################################################
## (1) Subset DataFrame by row, i.e. filter the rows of a DF according to a given condition, using the operation `filter`: ##
#############################################################################################################################

## Print the schema of `df` since the dtype of each column will determine how we will specify subsetting conditions.
printSchema(df)

## Subset the DF into a new DF, 'f1', that includes only those loans for which JPMorgan Chase is the servicer, as denoted by the entry for 'servicer_name' (string); note that
## we execute 'filter()' with an 'is equal to' logical condition (==) and an 'or' logical operation (|):
f1 <- filter(df, df$servicer_name == "JP MORGAN CHASE BANK, NA" | df$servicer_name == "JPMORGAN CHASE BANK, NA" | df$servicer_name == "JPMORGAN CHASE BANK, NATIONAL ASSOCIATION")
nrow(f1)

## Note that 'select()' or 'filter()' can be written with SQL statement strings; for example, here is the previous example in SQL statement format:
f2 <- filter(df, "servicer_name = 'JP MORGAN CHASE BANK, NA' or servicer_name = 'JPMORGAN CHASE BANK, NA' or servicer_name = 'JPMORGAN CHASE BANK, NATIONAL ASSOCIATION'")
nrow(f2)

## Or, alternatively, in syntax similar to how we can subset data.frames by row in base R:

f3 <- df[df$servicer_name == "JP MORGAN CHASE BANK, NA" | df$servicer_name == "JPMORGAN CHASE BANK, NA" | df$servicer_name == "JPMORGAN CHASE BANK, NATIONAL ASSOCIATION",]
nrow(f3)

## Subset the DF as 'f3', which includes only those loans for which the servicer name is known, i.e. servicer name is not equal to an empty entry or listed as "other".
## Execute 'filter()' with an 'is not equal to' logical condition (!=) and an 'and' logical operation (&):
f4 <- filter(df, df$servicer_name != "OTHER" & df$servicer_name != "")
nrow(f4)

## Subset DF as 'f4', which includes only loans for which the 'loan_age', i.e. the number of calendar months since the first full month the mortgage loan accrues interest, is
## greater than 5 years (60 calendar months); execute 'filter()' with an 'is greater than' logical condition (>):
f5 <- filter(df, df$loan_age > 60)
nrow(f5)

## Subset DF as 'f5', which includes only loans for which the 'loan_age' is greater than, or equal to, 10 years (120 calendar months); execute 'filter()' with an 'is greater
## than or equal to' logical condition (>=):
f6 <- filter(df, df$loan_age >= 120)
nrow(f6)

## An alias for `filter` is `where`, which presents diction that is often much more intuitive, particularly when `where` is embedded in a complex statement. For example, the following expression can be read as "aggregate and return the mean loan age and count values for observations in `df` where loan age is less than 60 months"

f7 <- agg(groupBy(where(df, df$loan_age < 60), where(df, df$loan_age < 60)$servicer_name), loan_age_avg = avg(where(df, df$loan_age < 60)$loan_age), count = n(where(df, df$loan_age < 60)$loan_age))
head(f7)

#############################################################################################################################
## (2) Subset DF by column, i.e. select the columns of a DF according to a given condition, using the operation `select`: ###
#############################################################################################################################

## Subset `df` by a specified list of columns. In the expression below, we create a subsetted DF that includes only the number of calendar months remaining until the borrower is expected to pay the mortgage loan in full (remaining maturity) and adjusted remaining maturity:
s1 <- select(df, "mths_remng", "aj_mths_remng")
ncol(s1)
# We can also reference the column names through the DF name, i.e. `select(df, df$mths_remng, df$aj_mths_remng)`. Or, we save a list of columns as a combination of strings. Here, if we wanted to make a list of all columns that relate to remaining maturity, we could evaluate the expression `remng_mat <- c("mths_remng", "aj_mths_remng")` and then easily subset by our list of columns later on with `select(df, remng_mat)`.

## Besides subsetting by a list of columns, we can also subset a DF by column expressions, or by both as we do in the example below. The DF `s2` includes the columns `"mths_remng"` and `"aj_mths_remng"` as in `s1`, but now with a column that details the absolute value of the difference between the unadjusted and adjusted remaining maturity:
s2 <- select(df, df$mths_remng, df$aj_mths_remng, abs(df$aj_mths_remng - df$mths_remng))
ncol(s2)
head(s2)

## Just as we can subset by row with syntax similar to base R, we can similarly acheive subsetting by column. The following expressions are equivalent:
select(df, df$period)
df[,"period"]
df[,2]

## To simultaneously subset by both column and row specifications, you can simply embed a `where` expression in a `select` operation (or vice versa). The following expression creates a DF that lists loan age values only for observations in which servicer name is unknown:
s4 <- select(where(df, df$servicer_name == "" | df$servicer_name == "OTHER"), "loan_age")
head(s4)
## Note that we could have also written the above expression as `df[df$servicer_name == "" | df$servicer_name == "OTHER", "loan_age"]`

###################################
## (2i) Drop a column from a DF: ##
###################################

## We can drop a column from a DF really simply by assigning `NULL` to a DF column. Below, we drop `"aj_mths_remng"` from `s1`:
head(s1)
s1$aj_mths_remng <- NULL
head(s1)

#################################################
## (3) Subset a DF by taking a random sample: ###
#################################################

## Perhaps the most useful subsetting operation is `sample`, which returns a randomly sampled subset of a DF. With `subset`, we can specify whether we want to sample with or without replace, the approximate size of the sample that we want the new DF to call and whether or not we want to define a random seed. If our initial DF is so massive that performing analysis on the entire dataset requires a more expensive cluster, we can: sample the massive dataset, interactively develop our analysis in SparkR using our sample and then evaluate the resulting program using our initial DF, which calls the entire massive dataset, only as is required. This strategy will help us to minimize wasting resources.

## Below, we take a random sample of `df` without replacement that is, in size, approximately equal to 1% of `df`. Notice that we must define a random seed in order to be able to reproduce our random sample.

df_samp1 <- sample(df, withReplacement = FALSE, fraction = 0.01)
df_samp2 <- sample(df, withReplacement = FALSE, fraction = 0.01)
count(df_samp1)
count(df_samp2)


df_samp3 <- sample(df, withReplacement = FALSE, fraction = 0.01, seed = 0)
df_samp4 <- sample(df, withReplacement = FALSE, fraction = 0.01, seed = 0)
count(df_samp3)
count(df_samp4)

##########################################################
## (3i) Collect a random sample as a local data.frame: ###
##########################################################

## An additional use of `sample` is to collect a random sample of a massive dataset as a local data.frame in R. This would allow us to work with a sample dataset in a traditional analysis environment that is likely more representative of the population since we are sampling from a larger set of observations than we are normally doing so. This can be achieved by simply using `collect` to create a local data.frame:

dat <- collect(df_samp4)

## If we want to export this data.frame from RStudio as a single .csv file that we can work with in any environment, we can export the data as we normally do in R:

#write.csv(dat, file = "hfpc_samp.csv") ### NOTE: Currently, cannot export .csv file to S3. Need to fix.
#write.table(dat, file = "hfpc_samp.csv",row.names=FALSE, na="",col.names=FALSE, sep=",")

## Warning: we cannot collect a DF as a data.frame unless it is sufficiently small in size since it must fit onto a single node!

