############################
## Time Series I Tutorial ##
############################

### In this tutorial, we discuss how to perform several essential time series operations with SparkR. In particular, we discuss:

# Identify and parse date dtype DF columns
# Extract and modify components of a date dtype column
# Resample a time series DF to a particular unit of time frequency
# Compute relative dates based on a specified increment of time

library(SparkR)
# Initiate SparkContext:
sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", 
                                  spark.driver.memory="1g",
                                  spark.driver.maxResultSize="1g")
                  ,sparkPackages="com.databricks:spark-csv_2.11:1.4.0") # Load CSV Spark Package

# AWS EMR is using Spark 2.11 so we need the associated version of spark-csv: http://spark-packages.org/package/databricks/spark-csv
# Define Spark executor memory, as well as driver memory and maxResultSize according to cluster configuration
# Initiate SparkRSQL:
sqlContext <- sparkRSQL.init(sc)


### Load data:

df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header="false", inferSchema="true", nullValue="")
cache(df)


### Identify and parse date dtype DF columns:

# View dtype of each column - Note that columns with date values are currently strings
str(df)

# Create "period", "matr_dt" and "dt_zero_bal" date dtype variable and create separate date level columns:

# `period`
period_uts <- unix_timestamp(df$period, 'MM/dd/yyyy')	# Gets current Unix timestamp in seconds
period_ts <- cast(period_uts, 'timestamp')	# Casts Unix timestamp `period_uts` as timestamp
period_dt <- cast(period_ts, 'date')	# Casts timestamp `period_ts` as date dtype
df <- withColumn(df, 'period_dt', period_dt)	# Add date dtype column `period_dt` to `df`
# `dt_matr`
matr_uts <- unix_timestamp(df$dt_matr, 'MM/yyyy')
matr_ts <- cast(matr_uts, 'timestamp')
matr_dt <- cast(matr_ts, 'date')
df <- withColumn(df, 'matr_dt', matr_dt)
# `dt_zero_bal`
zero_bal_uts <- unix_timestamp(df$dt_zero_bal, 'MM/yyyy')
zero_bal_ts <- cast(zero_bal_uts, 'timestamp')
zero_bal_dt <- cast(zero_bal_ts, 'date')
df <- withColumn(df, 'zero_bal_dt', zero_bal_dt)

str(df)
# Note that the `"zero_bal_dt"` entries corresponding to the missing date entries in `"dt_zero_bal"`, which were empty strings, are now nulls.


### Extract components of a date dtype column:

## Extract components of date dtype column:
# Year and month values for `"period_dt"`
df <- withColumn(df, 'period_yr', year(period_dt))
df <- withColumn(df, "period_m", month(period_dt))
# Year value for `"matr_dt"`
df <- withColumn(df, 'matr_yr', year(matr_dt))
df <- withColumn(df, "matr_m", month(matr_dt))
# Year value for `"zero_bal_dt"`
df <- withColumn(df, 'zero_bal_yr', year(zero_bal_dt))
df <- withColumn(df, "zero_bal_m", month(zero_bal_dt))
# Extract date components: `year`, `month`, `hour`, `minute`, `second` (as info is needed)
# See new date dtype columns in `df`:
str(df)



### Compute relative dates and measures based on a specified increment of time

cols_dt <- c("period_dt", "matr_dt")
df_dt <- select(df, cols_dt)

## Relative dates: `last_day`, `next_day`, `add_months`,  `date_add`, `date_sub`

# Given a date column, returns the last day of the month which the given date belongs to. For example, input "2015-07-27" returns "2015-07-31" since July 31 is the last day of the month in July 2015.
df_dt <- withColumn(df_dt, 'p_ld', last_day(df_dt$period_dt))
# Given a date column, returns the first date which is later than the value of the date column that is on the specified day of the week.
df_dt <- withColumn(df_dt, 'p_nd', next_day(df_dt$period_dt, "sunday"))
# Returns the date that is numMonths after startDate.
df_dt <- withColumn(df_dt, 'p_addm', add_months(df_dt$period_dt, 1))
# Returns the date that is 'days' days after 'start'
df_dt <- withColumn(df_dt, 'p_dtadd', date_add(df_dt$period_dt, 1))
# Returns the date that is 'days' days before 'start'
df_dt <- withColumn(df_dt, 'p_dtsub', date_sub(df_dt$period_dt, 1))


## Relative measures of time: `weekofyear`, `dayofyear`, `dayofmonth`, `datediff`, `months_between`
# Extracts the week number as an integer from a given date/timestamp/string.
df_dt <- withColumn(df_dt, 'p_woy', weekofyear(df_dt$period_dt))
# Extracts the day of the year as an integer from a given date/timestamp/string.
df_dt <- withColumn(df_dt, 'p_doy', dayofyear(df_dt$period_dt))
# Extracts the day of the month as an integer from a given date/timestamp/string.
df_dt <- withColumn(df_dt, 'p_dom', dayofmonth(df_dt$period_dt))
# Returns number of months between dates 'date1' and 'date2'.
df_dt <- withColumn(df_dt, 'p_mbtw', months_between(df_dt$matr_dt, df_dt$period_dt))
# Returns the number of days from 'start' to 'end'.
df_dt <- withColumn(df_dt, 'p_dbtw', datediff(df_dt$matr_dt, df_dt$period_dt))


str(df_dt)




### Resample a time series DF to a particular unit of time frequency

# Create new DF with only columns of numerical and date dtype
cols <- c("period_yr", "period_m", "matr_yr", "matr_m", "zero_bal_yr", "zero_bal_m", "new_int_rt", "act_endg_upb", "loan_age", "mths_remng", "aj_mths_remng")
dat <- select(df, cols)
unpersist(df)
cache(dat)
head(dat)

## Note that, in our loan-level data, each row represents a unique loan (each made distinct by the `"loan_id"` column in `df`) and its corresponding characteristics such as `"loan_age"` and `"mths_remng"`. Note that `dat` is simply a subsetted DF of `df` and, therefore, also refers to loan-level data. We can resample the data over the distinct values of any of the columns in `dat`, but an intuitive approach is to resample the loan-level data as aggregates of the DF columns for a unit of time. Below, we aggregate the columns of `dat` (taking the mean of the column entries) by `"period_yr"`, and then by `"period_yr"` and `"period_m"`:
dat1 <- agg(groupBy(dat, dat$period_yr), m.period_m = mean(dat$period_m), m.matr_yr = mean(dat$matr_yr), m.zero_bal_yr = mean(dat$zero_bal_yr), m.new_int_rt = mean(dat$new_int_rt), m.act_endg_upb = mean(dat$act_endg_upb), m.loan_age = mean(dat$loan_age), m.mths_remng = mean(dat$mths_remng), m.aj_mths_remng = mean(dat$aj_mths_remng))
head(dat1)

dat2 <- agg(groupBy(dat, dat$period_yr, dat$period_m), m.matr_yr = mean(dat$matr_yr), m.zero_bal_yr = mean(dat$zero_bal_yr), m.new_int_rt = mean(dat$new_int_rt), 
            m.act_endg_upb = mean(dat$act_endg_upb), m.loan_age = mean(dat$loan_age), m.mths_remng = mean(dat$mths_remng), m.aj_mths_remng = mean(dat$aj_mths_remng))
head(arrange(dat2, dat2$period_yr, dat2$period_m), 15)	# Arrange the first 15 rows of `dat2` by ascending `period_yr` and `period_m` values

## We are using the `agg` and `groupBy` operations that we discussed in the SparkR Basics II tutorial to resample the data: in practice, we resample a DF in SparkR, by computing aggregations over grouped data. Note that we specify the list of DF columns that we want to resample on by including it in `groupBy`. In the preceding example, we aggregated by taking the mean of each column. However, we could use any of the aggregation functions that `agg` is able to interpret (discussed in SparkR Basics II tutorial) that is inline with the resampling that we are trying to achieve.




