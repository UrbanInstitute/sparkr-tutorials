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

### Load data:

df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header="false", inferSchema="true", nullValue="")
cache(df)

# See structure; want to resample by year of "period" and "servicer_name" - but is string
str(df)

# Create "period", "matr_dt" and "dt_zero_bal" date dtype variable and create separate date level columns
period_dt <- cast(cast(unix_timestamp(df$period, 'MM/dd/yyyy'), 'timestamp'), 'date')
df <- withColumn(df, 'period_dt', period_dt)
df <- withColumn(df, 'period_yr', year(period_dt))
df <- withColumn(df, "period_m", month(period_dt))

matr_dt <- cast(cast(unix_timestamp(df$dt_matr, 'MM/yyyy'), 'timestamp'), 'date')
df <- withColumn(df, 'matr_dt', matr_dt)
df <- withColumn(df, 'matr_yr', year(matr_dt))

zero_bal_dt <- cast(cast(unix_timestamp(df$dt_zero_bal, 'MM/yyyy'), 'timestamp'), 'date')
df <- withColumn(df, 'zero_bal_dt', zero_bal_dt)
df <- withColumn(df, 'zero_bal_yr', year(zero_bal_dt))

str(df)

## Resample DataFrame by unit of time:

# Create new DF with only columns of numerical and date dtype
cols <- c("period_yr", "period_m", "matr_yr", "zero_bal_yr", "new_int_rt", "act_endg_upb", "loan_age", "mths_remng", "aj_mths_remng")
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

## Resample DataFrame by category:

## We can very easily apply the method for resampling by unit of time described in the preceding section to resample data over distinct measures of a categorical variable. In order to demonstrate this, we create a new DF which includes the `"servicer_name"` category of `df`:
cols_ <- c("servicer_name", "period_yr", "period_m", "matr_yr", "zero_bal_yr", "new_int_rt", "act_endg_upb", "loan_age", "mths_remng", "aj_mths_remng")
dat_ <- select(df, cols_)

## Then, we again aggregate over grouped data (this time over the distinct values of `"servicer_name"`):
dat3 <- agg(groupBy(dat_, dat_$servicer_name), m.period_yr = mean(dat_$period_yr), m.period_m = mean(dat_$period_m), m.matr_yr = mean(dat_$matr_yr), m.zero_bal_yr = mean(dat_$zero_bal_yr), m.new_int_rt = mean(dat_$new_int_rt), m.act_endg_upb = mean(dat_$act_endg_upb), m.loan_age = mean(dat_$loan_age), m.mths_remng = mean(dat_$mths_remng), m.aj_mths_remng = mean(dat_$aj_mths_remng), count = n(dat_$servicer_name))	# Include count measure so that we can see how many observations there are per category
head(dat3, 20)



