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

# Note that the resulting DF includes two (2) `"loan_id"` columns. Unfortunately, we cannot specify only one of these columns in SparkR, and the following command drops both `"loan_id"` columns:

ab1$loan_id <- NULL

# The `merge` operation, alternatively, allows us to join DFs and also produces two (2) distinct merge columns. We can use this feature to retain the column on which we joined the DFs. Therefore, `join` is a convenient operation, but we should use `merge` if we want the resulting DF to include the merging column. We discuss `merge` in further detail below.

# Rather than defining a `joinExpr`, we explictly specify the column(s) that SparkR should `merge` the DFs on with the operation parameters `by` and `by.x`/`by.y` (if we do not specify `by`, SparkR will merge the DFs on the list of common column names shared by the DFs). Rather than specifying a type of join, `merge` determines how SparkR should merge DFs based on boolean values: `all.x` and `all.y` indicate whether all the rows in `x` and `y` should be including in the join, respectively. We can specify `merge` type with the following specifications:

# `all.x = FALSE`, `all.y = FALSE`: Returns an inner join (this is the default and can be achieved by not specifying values for all.x and all.y)
# `all.x = TRUE`, `all.y = FALSE`: Returns a left outer join
# `all.x = FALSE`, `all.y = TRUE`: Returns a right outer join
# `all.x = TRUE`, `all.y = TRUE`: Returns a full outer join

# The following `merge` expression is equivalent to the `join` expression in the preceding example:

ab2 <- merge(a, b, by = "loan_id")
str(ab2)

# Note that the two merging columns are distinct, indicated by the `_x` and `_y` assignments. We utilize this distinction in the expressions below to retain a single merging column:

ab2$loan_id_y <- NULL
ab2 <- withColumnRenamed(ab2, "loan_id_x", "loan_id")
str(ab2)

### Append rows of data to a DataFrame:

# Subset `df` into two distinct DFs, `A` and `B`, which we will use to discuss how to append the rows of one DF to those of another:

A <- sample(df, withReplacement = FALSE, fraction = 0.5)
B <- except(df, A)

## Check for duplicates

nrow(A)
nrow(B)
nrow(intersect(A, B))

## Append rows - DFs with same columns

# Combines two (2) or more SparkR DataFrames by rows. Does not remove duplicate rows.
df1 <- rbind(A, B)
nrow(df1)
# Return a new DataFrame containing the union of rows in this DataFrame and another DataFrame. Note that this does not remove duplicate rows across the two DataFrames.
# This is equivalent to 'UNION ALL' in SQL.
df2 <- unionAll(A, B)
nrow(df2)


## Different column lists

columns(B)
# Remove `"period"` and `"servicer_name"`
cols_ <- c("loan_id","new_int_rt","act_endg_upb","loan_age","mths_remng","aj_mths_remng","dt_matr","cd_msa","delq_sts","flag_mod","cd_zero_bal","dt_zero_bal")
B_ <- select(B, cols_)
df2 <- rbind(A, B_)
nrow(df2)
