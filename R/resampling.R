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

# See structure; want to resample by year of "period"
head(df)

# See what dtype "period" is - it's currently in string
printSchema(df)

# Create "period", "matr_dt" and "dt_zero_bal" date dtype variable and create separate "period_yr" column
period_dt <- cast(cast(unix_timestamp(df$period, 'MM/dd/yyyy'), 'timestamp'), 'date')
df <- withColumn(df, 'period_dt', period_dt)
df <- withColumn(df, 'period_yr', year(period_dt))

matr_dt <- cast(cast(unix_timestamp(df$dt_matr, 'MM/yyyy'), 'timestamp'), 'date')
df <- withColumn(df, 'matr_dt', matr_dt)
df <- withColumn(df, 'matr_yr', year(matr_dt))

zero_bal_dt <- cast(cast(unix_timestamp(df$dt_zero_bal, 'MM/yyyy'), 'timestamp'), 'date')
df <- withColumn(df, 'zero_bal_dt', zero_bal_dt)
df <- withColumn(df, 'zero_bal_yr', year(zero_bal_dt))

str(df)

# Create new DF with just "period_yr" column and all columns of numerical dtype
cols <- c("period_yr", "new_int_rt", "act_endg_upb", "loan_age", "mths_remng", "aj_mths_remng", "period_yr", "matr_yr", "zero_bal_yr")
dat <- select(df, cols)
head(dat)