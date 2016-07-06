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

## Note that we now include the `nullValue` option in the `read.df` transformation below. By setting `nullValue` equal to an empty string in `read.df`, we direct `read.df` and the `sqlContext` to interpret empty string entries as being equal to a null value. Therefore, any DF entries matching this string will be set as nulls in `df`:

df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header="false", inferSchema="true", nullValue="")
cache(df)
n <- nrow(df)

## We can replace this empty string with any string that we know indicates a null entry in the dataset, i.e. with `nullValue="<string>"`.


### Condition on null entries:

## We saw in the subsetting tutorial how to subset a DF based on a conditional statement. We can extend this reasoning in order to identify missing data in a DF and to explore the distribution of missing data within a DF. Conditional operations for missing data in SparkR are `isNull`, `isNaN` and `isNotNull`, which can be used to condition on missing or NaN entries of a DF column. Below, we count the number of missing entries in `"loan_age"` and in `"mths_remng"`. We can see below that there are no missing or NaN entries in `"loan_age"`. Note that the `isNull` and `isNaN` count results differ for `"mths_remng"` - while there are missing values in `"mths_remng"`, there are no NaN entries (entires that are "not a number").
count(where(df, isNull(df$loan_age)))
count(where(df, isNull(df$mths_remng)))

count(where(df, isNaN(df$loan_age)))
count(where(df, isNaN(df$mths_remng)))

## We can also condition on missing data when aggregating over grouped data in order to see how missing data is distributed over a categorical variable within our data. In order to view the distribution of missing `"mths_remng"` observations over `"servicer_name"`, we (1) create the DF `df_MRnull` which includes only those rows for which `"mths_remng"` is empty, (2) create the DF `MRnulls.ByServ` which consists of the number of observations in `df_MRnull` grouped by `"servicer_name"` and (3) collect `MRnulls.ByServ` into a nicely formatted table as a local data.frame:
df_MRnull <- where(df, isNull(df$mths_remng))
MRnulls.ByServ <- agg(groupBy(df_MRnull, df_MRnull$servicer_name), Nulls = n(df_MRnull$servicer_name))
MRnulls.ByServ.tab <- collect(MRnulls.ByServ)
MRnulls.ByServ.tab

# dropna
# Across entire DF: Can decide whether we drop a row if it contains "any" nulls or if we drop a row if "all" of its values are null with the `how =` specification in `dropna`. We can see below that there are no rows in `df` for which all of its values are null.
df_any <- dropna(df, how = "any")
(n_any <- nrow(df_any))
df_all <- dropna(df, how = "all")
(n_all <- nrow(df_all))
# Within `dropna`, we can also determine a minimum number of non-null entries required for a row to remain in the DF by specifying a `minNonNulls` value. If included in `dropna`, SparkR is directed to drop rows that have less than `minNonNulls = <value>` non-null entries. Note that including `minNonNulls` overwrites the `how` specification. Below, we [STOP]
df_5 <- dropna(df, minNonNulls = 5)
(n_5 <- nrow(df_5))
df_12 <- dropna(df, minNonNulls = 12)
(n_12 <- nrow(df_12))
n - n_12
# Drop rows with nulls within a DF column:
count(where(df, isNull(df$mths_remng)))
df_ <- filter(df, isNotNull(df$mths_remng))
count(where(df_, isNull(df_$mths_remng)))

# na.omit

# fillna
# ifelse



