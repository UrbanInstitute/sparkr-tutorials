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

# In order to discuss how to append the rows of one DF to those of another, we must first subset `df` into two (2) distinct DataFrames, `A` and `B`:

A <- sample(df, withReplacement = FALSE, fraction = 0.5)
B <- except(df, A)

# Let's also first examine the row count for each subsetted row and confirm that `A` and `B` are disjoint DataFrames (we can achieve this with the SparkR operation `intersect`, which performs the interaction set operation on two DFs):

nA <- nrow(A)
nB <- nrow(B)
nA + nB # Equal to nrow(df)
nrow(intersect(A, B))

## Append rows when column names are equal across DFs:

# If we are certain that the two DFs have equivalent column name lists, then appending the rows of one DF to another is straightforward. Here, we append the rows of `B` to `A` with the `rbind` operation:

df1 <- rbind(A, B)
nrow(df1)
nrow(df)

# We can see in the results above that `df1` is equivalent to `df`. We could, alternatively, accomplish this with the `unionALL` operation (e.g. `df1 <- unionAll(A, B)`. Note that `unionAll` is not an alias for `rbind` - we can combine any number of DFs with `rbind` while `unionAll` can only consider two (2) DataFrames at a time.

## Append rows when DF column name lists are not equal:

# 

# We saw that in missing data tutorial that there are no null values in `"loan_age"`


columns(B)
# Remove "loan_age"
cols_ <- c("loan_id", "period", "servicer_name", "new_int_rt", "act_endg_upb", "mths_remng", "aj_mths_remng", "dt_matr", "cd_msa", "delq_sts", "flag_mod", "cd_zero_bal", "dt_zero_bal")
B_ <- select(B, cols_)
