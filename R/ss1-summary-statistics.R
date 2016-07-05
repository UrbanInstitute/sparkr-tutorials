#########################################################
### Social Science Module 1: Basic Summary Statistics ###
#########################################################

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



## NUMERICAL
# The operation `describe` creates a new DF that consists of several key aggregations (count, mean, max, mean, standard deviation) for a specified DF or list of DF columns. We can either (1) use the action `showDF` to print the aggregation DF or (2) save the aggregation DF as a local data.frame with `collect`. Below we perform both of these actions on the aggregation DF `sumstats_mthsremng`:
sumstats_mthsremng <- describe(df, "mths_remng")
showDF(sumstats_mthsremng) 
sumstats_mthsremng.l <- collect(sumstats_mthsremng)
sumstats_mthsremng.l

## Measures of Location
# Only mean is currently available in SparkR, which we compute using `agg` just as we did in the SparkR Basics II tutorial. Remember that `agg` returns another DF. Therefore, we can either print the DF with `showDF` or we can save the aggregation as a local data.frame. Collecting the DF may be preferred if we want to work with the mean `"mths_remng"` value as a single value in RStudio.
showDF(agg(df, mean(df$mths_remng)))
typeof(agg(df, mean(df$mths_remng)))

mths_remng.avg <- collect(agg(df, mean(df$mths_remng)))
(mths_remng.avg <- mths_remng.avg[,1])
typeof(mths_remng.avg)

## Measures of dispersion
# Range width & limits
mr_range <- agg(df, min(df$mths_remng), max(df$mths_remng), range_width = abs(max(df$mths_remng) - min(df$mths_remng)))
showDF(mr_range)
# Variance: Here we compute sample variance (which we could also compute with `variance` or `var_samp`) or we could compute population variance with `var_pop`:
mr_var <- agg(df, mr_var = var(df$mths_remng))
showDF(mr_var)
# Standard Deviation Here we compute sample standard deviation (which we could also compute with `stddev` or `stddev_samp`) or we could compute population variance with `stddev_pop`:
mr_sd <- agg(df, mr_sd = sd(df$mths_remng))
showDF(mr_sd)
# Quantiles
# [Insert: section on `approxQuantile` transformation that is included in Spark 2.0.0 release.]

## Measures of shape of the distribution
# Direction of variance (Skewness), or 3rd moment
mr_sk <- agg(df, mr_sk = skewness(df$mths_remng))
showDF(mr_sk)
# Tailedness of Probability Distribution (Kurtosis), or 4th moment
mr_kr <- agg(df, mr_kr = kurtosis(df$mths_remng))
showDF(mr_kr)

## Measures of Dependence
# The actions `cov` and `corr` return the sample covariance and correlation measures of dependency between two DF columns, respectively. Currently, Pearson is the only supported method for calculating correlation. Here we compute the covariance and correlation of `"loan_age"` and `"mths_remng"`. We can also save these values for later use. Note that, in doing so, we are not required to first collect since `cov` and `corr` return values, rather than DFs:
cov(df, "loan_age", "mths_remng")
corr(df, "loan_age", "mths_remng", method = "pearson")


## String/categorical: We can compute descriptive statistics for categorical data using the `groupBy` operation that we used in the Basics II tutorial to compute aggregations of numerical data over groups, as well as several operations built into SparkR.

# Recast cd_zero_bal as a categorical variable: Define what cd_zero_bal is here
df$cd_zero_bal <- cast(df$cd_zero_bal, 'string')

# Frequency table: Return a frequency table, listing the number of observations for each distinct value of `"cd_zero_bal"`:
zb_f <- count(groupBy(df, "cd_zero_bal"))
showDF(zb_f)
# We could also embed a grouping into an `agg` operation as we saw in the Basics II tutorial, i.e. `agg(groupBy(df, df$cd_zero_bal), count = n(df$cd_zero_bal))`.
# Relative frequency table:
zb_rf <- agg(groupBy(df, df$cd_zero_bal), count = n(df$cd_zero_bal), perc = 100 * (n(df$cd_zero_bal)/nrow(df))) ## 100 * won't work - need to trouble shoot with toy data
showDF(zb_rf)
# Contingency table:
crosstab(df, "servicer_name", "cd_zero_bal")

