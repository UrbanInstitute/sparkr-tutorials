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

### Specify null values when loading data:

df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header="false", inferSchema="true", nullValue="")
cache(df)


### Join two DataFrames based on the given join expression:

# Create separate DFs to join:

columns(df)
cols_a <- c("loan_id", "period", "servicer_name", "new_int_rt", "act_endg_upb", "loan_age", "mths_remng")
cols_b <- c("loan_id", "aj_mths_remng", "dt_matr", "cd_msa", "delq_sts", "flag_mod", "cd_zero_bal", "dt_zero_bal")
a <- select(df, cols_a)
b <- select(df, cols_b)
str(a)
str(b)

# The SparkR operation `join` allows us to perform most SQL join types on SparkR DFs. Join types that we are able to specify include:

# "inner" (default): Returns rows where there is a match in both DFs
# "outer": Returns rows where there is a match in both DFs, as well as rows in both the right and left DF where there was no match
# "full", "fullouter": Returns rows where there is a match in one of the DFs
# "left", "leftouter", "left_outer": Returns all rows from the left DF, even if there are no matches in the right DF
# "right", "rightouter", "right_outer": Returns all rows from the right DF, even if there are no matches in the left DF
# Cartesian: Returns the Cartesian product of the sets of records from the two or more joined DFs - `join` will return this DF when we _do not_ specify a join type

# We communicate to SparkR what condition we want to join DFs on with the `joinExpr` specification in `join`. Below, we join the DFs `a` and `b` on the condition that their `"loan_id"` values be equal:

ab1 <- join(a, b, a$loan_id == b$loan_id, "fullouter")
str(ab1)

# Note that we have an extra `"loan_id"` column. Drop column
ab1$loan_id <- NULL
# but drops both `"loan_id"` columns

# merge operation lets you specify which merging column you want to drop

# We could also use the `merge` operation to join to DFs. Rather than defining a `joinExpr`, we explictly specify the column(s) that SparkR should `merge` the DFs on with the operation parameters `by` and `by.x`/`by.y` (if we do not specify `by`, SparkR will merge the DFs on the list of common column names shared by the DFs). Rather than specifying a type of join, `merge` determines how SparkR should merge DFs based on boolean values: `all.x` and `all.y` indicate whether all the rows in `x` and `y` should be including in the join, respectively. We can specify `merge` type with the following specifications:

# `all.x = FALSE`, `all.y = FALSE`: Returns an inner join (this is the default and can be achieved by not specifying values for all.x and all.y)
# `all.x = TRUE`, `all.y = FALSE`: Returns a left outer join
# `all.x = FALSE`, `all.y = TRUE`: Returns a right outer join
# `all.x = TRUE`, `all.y = TRUE`: Returns a full outer join

# The following `merge` expression is equivalent to the `join` expression in the preceding example:

ab2 <- merge(a, b, by = "loan_id")
str(ab2)

ab2$loan_id_y <- NULL
ab2 <- withColumnRenamed(ab2, "loan_id_x", "loan_id")
str(ab2)


# Return a new DataFrame containing the union of rows in this DataFrame and another DataFrame. Note that this does not remove duplicate rows across the two DataFrames.
# This is equivalent to 'UNION ALL' in SQL.
unionAll(x, y)
# Combines two (2) or more SparkR DataFrames by rows. Does not remove duplicate rows.
rbind(x, y, z, w)


# Check for duplicates prior to merging with `intersect`:
intersect(x, y)

# Return a new DF that includes only distinct rows, i.e. filters out duplicate rows that may result from merging:
distinctDF <- distinct(df)