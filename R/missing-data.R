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
n <- nrow(df)

## We can replace this empty string with any string that we know indicates a null entry in the dataset, i.e. with `nullValue="<string>"`. Note that SparkR only reads empty entries in numeric datatype (dtype) DF columns as null values, meaning that empty entries in DF columns of string dtype will simply equal an empty string. We consider how to work with this type of observation alongside null values throughout this tutorial. Using the `printSchema` operation, we can see the dtype of each column in `df` and use this to determine how we should examine missing data in each DF column.

### Condition on null entries:

## We saw in the subsetting tutorial how to subset a DF based on a conditional statement. We can extend this reasoning in order to identify missing data in a DF and to explore the distribution of missing data within a DF. Conditional operations for missing numerical data in SparkR are `isNull`, `isNaN` and `isNotNull`, which can be used to condition on entries of null or NaN value in a DF column. Below, we count the number of missing entries in `"loan_age"` and in `"mths_remng"`, which are both of integer dtype. We can see below that there are no missing or NaN entries in `"loan_age"`. Note that the `isNull` and `isNaN` count results differ for `"mths_remng"` - while there are missing values in `"mths_remng"`, there are no NaN entries (entires that are "not a number").
count(where(df, isNull(df$loan_age)))
count(where(df, isNull(df$mths_remng)))

count(where(df, isNaN(df$loan_age)))
count(where(df, isNaN(df$mths_remng)))

## If we want to count the number of rows with missing entries for `"servicer_name"`, a column of string dtype, we can simply use the equality logical condition (==) to direct SparkR to `count` the number of rows `where` the entries in the `"servicer_name"` column are equal to an empty string:
count(where(df, df$servicer_name == ""))

## We can also condition on missing data when aggregating over grouped data in order to see how missing data is distributed over a categorical variable within our data. In order to view the distribution of `"mths_remng"` observations with null values over distinct entries of `"servicer_name"`, we (1) create the DF `df_MRnull` which includes only those rows for which `"mths_remng"` is a null value, (2) create the DF `MRnulls.ByServ` which consists of the number of observations in `df_MRnull` grouped by `"servicer_name"` and (3) collect `MRnulls.ByServ` into a nicely formatted table as a local data.frame:
df_MRnull <- where(df, isNull(df$mths_remng))
MRnulls.ByServ <- agg(groupBy(df_MRnull, df_MRnull$servicer_name), Nulls = n(df_MRnull$servicer_name))
MRnulls.ByServ.tab <- collect(MRnulls.ByServ)
MRnulls.ByServ.tab

## Drop rows with missing data:

## The SparkR operation `dropna` (or its alias `na.omit`) creates a new DF that omits rows with null value entries. We can configure `dropna` in a number of ways, including omitting rows with null values in a specified list of columns in a DF or across all columns within a DF. If we want to drop rows with null value entries for a list of columns in `df`, we can specify a list of column names and then include this in `dropna` or we could embed this list directly in the operation. Below we specify a list of columns on which we condition `dropna`:
mrlist <- list("mths_remng", "aj_mths_remng")
df_mrNoNulls <- dropna(df, cols = mrlist)
(n_mrNoNulls <- nrow(df_mrNoNulls))

## Alternatively, we could `filter` the DF using the `isNotNull` condition as follows:
df_mrNoNulls_ <- filter(df, isNotNull(df$mths_remng) & isNotNull(df$aj_mths_remng))
(n_mrNoNulls_ <- nrow(df_mrNoNulls_))

## If we want to create a new DF that does not include any row with missing entries for a column of string dtype, we could also use `filter` to accomplish this. In order to remove observations with a missing `"servicer_name"` value, we simply filter `df` on the condition that `"servicer_name"` does not equal an empty string entry. 
df_snNoEmpty <- filter(df, df$servicer_name != "")
(n_snNoEmpty <- nrow (df_snNoEmpty))

## If we want to consider all columns in a DF when omitting rows with null values, we can use either the `how` or `minNonNulls` paramters of `dropna`. The parameter `how` allows us to decide whether we want to drop a row if it contains `"any"` nulls or if we want to drop a row if `"all"` of its values are null. We can see below that there are no rows in `df` in which all of its values are null, but only a small percentage of the rows in `df` have no null value entries:
df_all <- dropna(df, how = "all")
(n_all <- nrow(df_all))		# Equal in value to n
df_any <- dropna(df, how = "any")
(n_any <- nrow(df_any))
(n_any/n)*100

## We can also set a minimum number of non-null entries required for a row to remain in the DF by specifying a `minNonNulls` value. If included in `dropna`, this specification directs SparkR to drop rows that have less than `minNonNulls = <value>` non-null entries. Note that including `minNonNulls` overwrites the `how` specification. Below, we omit rows with that have less than 5 and 12 entries that are _not_ nulls. Note that there are no rows in `df` that have less than 5 non-null entries, and there are only approximately 8,000 rows with less than 12 non-null entries.
df_5 <- dropna(df, minNonNulls = 5)
(n_5 <- nrow(df_5))
df_12 <- dropna(df, minNonNulls = 12)
(n_12 <- nrow(df_12))
n - n_12

## Replace null and empty entries:

## Related to the `dropna` operation, `fillna` allows us to replace null entries with in syntax similar to that of `dropna`. In order to replace null entries in every numeric column in `df` with a value, we simply evaluate the expression `fillna(df, <value>)`. We replace every null entry in `df` with the value 12345 below:
str(df)
df_ <- fillna(df, value = 12345)
str(df_)
rm(df_)

## If we want to replace null values within a list of DF columns, we can specify a list just as we did in `dropna`. Here, we replace only the null values in `"act_endg_upb"` with 12345:
str(df)
df_ <- fillna(df, list("act_endg_upb" = 12345))
str(df_)
rm(df_)

## Finally, we can replace the empty entries in DF columns of string dtype with the `ifelse` operation. Here, we replace the empty entries in `"servicer_name"` with the string `"Unknown"`:
str(df)
df$servicer_name <- ifelse(df$servicer_name == "", "Unknown", df$servicer_name)
str(df)