##################################################################################
## Data as a Spark DataFrame: How to Load Data into SparkR and Explore the Data ##
##################################################################################


library(SparkR)

## Initiate SparkContext:

sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", 
                                  spark.driver.memory="1g",
                                  spark.driver.maxResultSize="1g")
                  ,sparkPackages="com.databricks:spark-csv_2.10:1.4.0") # Load CSV Spark Package

## AWS EMR is using Spark 2.11 so we need the associated version of spark-csv: http://spark-packages.org/package/databricks/spark-csv
## Define Spark executor memory, as well as driver memory and maxResultSize according to cluster configuration

## Initiate SparkRSQL:

sqlContext <- sparkRSQL.init(sc)


######################################
## (1) Load a csv file into SparkR: ##
######################################

## Use the operation `read.df` to load in quarterly Fannie Mae single-family loan performance data from the AWS S3 folder `"s3://ui-hfpc/"` as a Spark DataFrame (DF). Note
## that, when initiating our SparkR context, we specified that the `spark-csv` package should be included by by specifying
## `sparkPackages="com.databricks:spark-csv_2.11:1.4.0"` in our `sparkR.init` operation. Below, we load a single quarter (2000, Q1) into SparkR, and save it as the DF
## `perf_`:

perf_ <- read.df(sqlContext, "s3://ui-hfpc/Performance_2000Q1.txt", header='false', delimiter="|", source="csv", inferSchema='true', nullValue="")
cache(perf_)

## In the 'read.df' operation, we included typical specifications included when reading data into Stata and SAS, such as what character represents the delimiter in the .csv
## file. However, we also included 'sqlContext' (which we previously defined with the 'sparkRSQL.init' operation). This is essential for SparkR to understand how the read-in
## data should be partitioned and distributed across the computers (nodes) that make up the AWS cluster currently being used. Another input that is SparkR-specific is
## 'inferSchema', which SparkRSQL requires in order to interpet data types for each column in the DF. We discuss this in more detail later on in this module. An additional
## detail is that 'read.df' includes the 'nullValue=""' specification because we want 'read.df' to read entries of empty strings in our .csv dataset as NA in the SparkR DF,
## i.e. we are telling read.df to read entries equal to '""' as 'NA' in the DF. We will discuss how SparkR handles empty and null entries in further detail in a subsequent
## module. Similarly, the 'cache' operation will be discussed in subsequent modules although it will be used throughout this module along with the related operations
## 'persist' and 'unpersist'.
## Note: documentation for the quarterly loan performance data can be found at http://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html.

## We can compute headline measurements of the 'perf_' DF through the following operations. Note that wrapping the computation with () forces SparkR/R to print the computed
## value:

(n1 <- nrow(perf_))	# Save the number of rows in 'perf_'
## > (n1 <- nrow(perf_))
## [1] 6887100
(m1 <- ncol(perf_))	# Save the number of columns in 'perf_'
## > (m1 <- ncol(perf_))
## [1] 28


############################################
## (2) Update a DF with new rows of data: ##
############################################

## In order to take advantage of the newfound computing power that SparkR provides, and since we'll want to analyze loan performance data beyond 2000 Q1, we append the 'perf_'
## DF below with the data from subsequent quarters of the same single-family loan performance dataset. Here, we're only appending one subsequent quarter (2000 Q2) to the DF
## so that our analysis in these modules runs quickly, but the following code can be easily adapted by specifying the 'a' and 'b' values to reflect the quarters that we want
## to append to our DF. Note that the for-loop below again includes the 'read.df' operation, here specified just as we did when initial loading the .csv file as a DF:

a <- 2
b <- 2
for(q in a:b){
  
  filename <- paste0("Performance_2000Q", q)
  filepath <- paste0("s3://ui-hfpc/", filename, ".txt")
  .perf <- read.df(sqlContext, filepath, header='false', delimiter="|", source="csv", inferSchema='true', nullValue="")
  
  perf <- rbind(perf_, .perf)
}
cache(perf)
unpersist(perf_)

## The result of the for-loop is a new DF, 'perf', that consists of the same columns as the initial 'perf_' DF, but now with many additional rows, appended to 'perf_'. We
## can confirm this by taking the dimensions of the new DF:

(n2 <- nrow(perf))
## > (n2 <- nrow(perf))
## [1] 13216516
(m2 <- ncol(perf))
## > (m2 <- ncol(perf))
## [1] 28

## If the reader needs to adapt the above for-loop (or create their own) to update DFs in their analysis, guides for the SparkR and R operations used in the above for-loop
## can be found at the links below:
## 'for': http://www.r-bloggers.com/how-to-write-the-first-for-loop-in-r/
## 'past0': http://www.r-bloggers.com/paste-paste0-and-sprintf/
## 'rbind': https://stat.ethz.ch/R-manual/R-devel/library/base/html/cbind.html
## `read.df`: https://docs.cloud.databricks.com/docs/latest/databricks_guide/10%20SparkR/1%20Functions/read.df.html


########################################
## (3) Subset a DF by column name(s): ##
########################################

## The 'select' operation selects columns specified as strings in the operation line and then returns a new DF including only those specified columns. Here, we create a new
## DF called 'perf_lim' that includes only the first 14 columns in the 'perf' DF, i.e. the DF 'perf_lim' is a subset of 'perf':

perf_lim <- select(perf, c("C0","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","C11","C12","C13"))
cache(perf_lim)
unpersist(perf)

## If we want to confirm that 'perf_lim' includes 14 columns, rather than the 28 columns included in 'perf_' and 'perf', but the same number of rows as 'perf', we would run
## following operations:

(n3 <- nrow(perf_lim))
## > (n3 <- nrow(perf_lim))
## [1] 13216516
(m3 <- ncol(perf_lim))
## > (m3 <- ncol(perf_lim))
## [1] 14


#################################
## (4) Rename column(s) in DF: ##
#################################

## Using a for-loop (this time looping through the 14 columns of the 'perf_lim' DF) and the SparkR operation 'withColumnRenamed', we rename the columns of 'perf_lim'. The
## operation 'withColumnRenamed' renames an existing column, or columns, in a DF and returns a new DF. By specifying the "new" DF name as 'perf_lim', we are simply renaming
## the columns of 'perf_lim', but we could create an entirely separate DF with new column names by specifying a different DF name for 'withColumnRenamed':

old_colnames <- c("C0","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","C11","C12","C13")
new_colnames <- c("loan_id","period","servicer_name","new_int_rt","act_endg_upb","loan_age","mths_remng","aj_mths_remng","dt_matr","cd_msa","delq_sts","flag_mod","cd_zero_bal","dt_zero_bal")

for(i in 1:14){
  perf_lim <- withColumnRenamed(perf_lim, old_colnames[i], new_colnames[i] )
}

## We can check the column names of 'perf_lim' with the 'columns' operation:

columns(perf_lim)
## > columns(perf_lim)
##  [1] "loan_id"       "period"        "servicer_name" "new_int_rt"    "act_endg_upb"  "loan_age"      "mths_remng"    "aj_mths_remng" "dt_matr"       "cd_msa"        "delq_sts"      "flag_mod"     
## [13] "cd_zero_bal"   "dt_zero_bal"  


###########################################################
## (5) View DF & explore with basic variable statistics: ##
###########################################################

## If we want to explore the structure of the DF beyond computing its dimensions and viewing column names, we can use the 'head' operation to display the first n-many rows
## of 'perf_lim' (here, we'll take the first five (5) rows of the DF):

head(perf_lim, 5)
## > head(perf_lim, 5)
##        loan_id     period servicer_name new_int_rt act_endg_upb loan_age mths_remng aj_mths_remng dt_matr cd_msa delq_sts flag_mod cd_zero_bal dt_zero_bal
## 1 100007365142 01/01/2000                        8           NA        0        360           359 01/2030      0        0        N          NA            
## 2 100007365142 02/01/2000                        8           NA        1        359           358 01/2030      0        0        N          NA            
## 3 100007365142 03/01/2000                        8           NA        2        358           357 01/2030      0        0        N          NA            
## 4 100007365142 04/01/2000                        8           NA        3        357           356 01/2030      0        0        N          NA            
## 5 100007365142 05/01/2000                        8           NA        4        356           355 01/2030      0        0        N          NA            


# The R 'str' operation provides a compact visualization
str(perf_lim)					
## > str(perflim_sub)
## 'data.frame':	5 obs. of  14 variables:
##  $ loan_id      : num  1e+11 1e+11 1e+11 1e+11 1e+11
##  $ period       : chr  "01/01/2000" "02/01/2000" "03/01/2000" "04/01/2000" ...
##  $ servicer_name: chr  "" "" "" "" ...
##  $ new_int_rt   : num  8 8 8 8 8
##  $ act_endg_upb : logi  NA NA NA NA NA
##  $ loan_age     : int  0 1 2 3 4
##  $ mths_remng   : int  360 359 358 357 356
##  $ aj_mths_remng: int  359 358 357 356 355
##  $ dt_matr      : chr  "01/2030" "01/2030" "01/2030" "01/2030" ...
##  $ cd_msa       : int  0 0 0 0 0
##  $ delq_sts     : chr  "0" "0" "0" "0" ...
##  $ flag_mod     : chr  "N" "N" "N" "N" ...
##  $ cd_zero_bal  : logi  NA NA NA NA NA
##  $ dt_zero_bal  : chr  "" "" "" "" ...





############################################
## (6) Understanding data-types & schema: ##
############################################

## We can see in the output for the command head(perf_lim, 5) that we have what appears to be several different data types (dtypes) in our DF, but we obviously cannot infer
## what dtype is currently specified for each column in our DF by simply looking at that output. Luckily, there are three (3) different ways to view dtype in SparkR - the
## operations 'dtypes', 'schema' and 'printSchema'. As stated above, SparkRSQL relies on a "schema" to determine what data type to assign to each column in the DF (which is
## easy to remember since the English schema comes from the Greek word for shape or plan!). We can print a visual representation of the schema for a DF with the operations
## 'schema' and 'printSchema':

dtypes(perf_lim)	# Prints pairs of strings that correspond to a column name and its data type
## > dtypes(perf_lim)
## [[1]]
## [1] "loan_id" "bigint" 
## 
## [[2]]
## [1] "period" "string"
## 
## ... 
## 
## [[13]]
## [1] "cd_zero_bal" "int"        
## 
## [[14]]
## [1] "dt_zero_bal" "string"  

schema(perf_lim)	# Prints the schema of the DF
## > schema(perf_lim)
## StructType
## |-name = "loan_id", type = "LongType", nullable = TRUE
## |-name = "period", type = "StringType", nullable = TRUE
## 
## ...
## 
## |-name = "cd_zero_bal", type = "IntegerType", nullable = TRUE
## |-name = "dt_zero_bal", type = "StringType", nullable = TRUE

printSchema(perf_lim) # Prints the schema of the DF in a concise tree format
## > printSchema(perf_lim)
## root
##  |-- loan_id: long (nullable = true)
##  |-- period: string (nullable = true)
## 
## ...
## 
##  |-- cd_zero_bal: integer (nullable = true)
##  |-- dt_zero_bal: string (nullable = true)

## Remember that, when we read in our DF from the S3 .csv file, we included the condition 'inferSchema='true''. This is just one of three (3) ways to communicate to SparkRSQL
## how the dtypes of the DF columns should be assigned. By specifying 'inferSchema='true'' in 'read.df', we allow SparkRSQL to infer the dtype of each column in the DF.
## Conversely, we could specify our own schema and pass this into the load call, forcing SparkRSQL to adopt our dtype specifications for each column. Each of these approaches
## have their pros and cons, which determine when it is appropriate to prefer one over the other:
##
## <*> 'inferSchema='true'': This approach minimizes programmer-driven error since we aren't required to make assertions about the dtypes of each column; however, it is
##		comparatively computationally expensive
## <*> 'customSchema': While computationally more efficient, manually specifying a schema will lead to errors if incorrect dtypes are assigned to columns - if SparkRSQL is
##		not able to interpret a column as the specified dtype, 'read.df' will fill that column in the DF with NA
##
## Clearly, the situations in which these approaches would be helpful are starkly different. In the context of this module, an efficient use of both approaches would be to
## use 'inferSchema='true'' when reading in our first DF, 'perf_'. At this point, we could print the schema with 'schema' or 'printSchema', note the dtype for each column
## (all 28 of them), and then write a customSchema with the corresponding specifications. We could then use this customSchema when appending the subsequent quarters to
## 'perf_'. While writing the customSchema may be tedious, including it in the appending for-loop would help that process to be much more efficient - this would be especially
## useful if we were appending, for example, 20 years worth of quarterly data together. The third way to communicate to SparkRSQL how to define dtypes is to not
## specify any schema, i.e. to not include 'inferSchema' in 'read.df'. Under this condition, every column in the DF is read in as a string dtype. Below is the an example of
## how we could specify a customSchema (here, however, we just use the same dtypes as interpreted for 'inferSchema='true''):

## customSchema <- structType(
##   structField("loan_id", "long"),
##   structField("period", "string"),
##   structField("servicer_name", "string"),
##   structField("new_int_rt", "double"),
##   structField("act_endg_upb", "double"),
##   structField("loan_age", "integer"),
##   structField("mths_remng", "integer"),
##   structField("aj_mths_remng", "integer")
##   structField("dt_matr", "string")
##   structField("cd_msa", "integer")
##   structField("delq_sts", "string")
##   structField("flag_mod", "string")
##   structField("cd_zero_bal", "integer")
##   structField("dt_zero_bal", "string")
## )

## Finally, dtypes can be changed after the DF has been created, using the 'cast' operation. However, it is clearly more efficient to properly specify dtypes when creating
## the DF. A quick example of using the 'cast' operation is given below:

perf_lim$loan_id <- cast(perf_lim$loan_id, 'string')		# We can see in the results from the previous printSchema output that 'loan_id' is a 'long' dtype, here we 'cast'
## > perf_lim$loan_id <- cast(perf_lim$loan_id, 'string')	# it as a 'string' and then call 'printSchema' on this new DF
printSchema(perf_lim)
## > printSchema(perf_lim)
## root
##  |-- loan_id: string (nullable = true)
##  |-- period: string (nullable = true)
## 
## ...
## 
##  |-- cd_zero_bal: integer (nullable = true)
##  |-- dt_zero_bal: string (nullable = true)
perf_lim$loan_id <- cast(perf_lim$loan_id, 'long')			# If we want our original 'perf_lim' DF, we can simply recast 'loan_id' as a 'long' dtype
## > perf_lim$loan_id <- cast(perf_lim$loan_id, 'long')
printSchema(perf_lim)
## > printSchema(perf_lim)
## root
##  |-- loan_id: long (nullable = true)
##  |-- period: string (nullable = true)
## 
## ...
## 
##  |-- cd_zero_bal: integer (nullable = true)
##  |-- dt_zero_bal: string (nullable = true)


#######################################
## (6) Export DF as data file to S3: ##
#######################################

## Throughout this module, we've built the Spark DataFrame 'perf_lim' of quarterly loan performance data, which we'll use in many subsequent moduls. In order to use this DF
## later on, we must first export it to a location that can handle large data sizes and in a data structure that works with our cluster computing approach. We'll save this
## example data to an AWS S3 folder from which we'll access other example datasets and, in the script below, we'll save it as a parquet file type using the 'write.df'
## operation:

write.df(perf_lim, "s3://sparkr-tutorials/hfpc_ex", "parquet", "overwrite")

## The DF is saved as a folder called "hfpc_ex" within the "s3://ui-hfpc/sparkr-tutorials/" folder in S3, and the "hfpc_ex" folder is filled with several individual parquet
## type files. Note that, in calling the DF 'perf_lim' during our previous analysis, we were really accessing data that was partitioned across our cluster. When saving the
## DF for later use, we similarly save the partitions across the cluster. Consider the process of saving this DF, or even a DF much larger in size, to a single .csv file. In
## order to save a massive DF to a single .csv file, it would need to be able to fit onto a single node of our cluster, i.e. it would need to be able to fit onto a single
## computer. Any dataset that would prompt us to consider employing SparkR for analysis, will likely not fit onto a single computer.

## The partitioned nature of 'hfpc_ex' does not affect our ability to load it back into SparkR and pursue further analysis. Below, we use the 'read.df' to read in the
## partitioned parquet file from S3 as the DF 'dat':

dat <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header='false', inferSchema='true')

## Below, we confirm that the dimensions and column names of 'hfpc_ex' and 'perf_lim' are equal:

dim(perf_lim)
## > dim(perf_lim)
## [1] 13216516       14
dim(dat)
## > dim(dat)
## [1] 13216516       14
columns(perf_lim)
## > columns(perf_lim)
##  [1] "loan_id"       "period"        "servicer_name" "new_int_rt"    "act_endg_upb"  "loan_age"     
##  [7] "mths_remng"    "aj_mths_remng" "dt_matr"       "cd_msa"        "delq_sts"      "flag_mod"     
## [13] "cd_zero_bal"   "dt_zero_bal"  
columns(dat)
## > columns(dat)
##  [1] "loan_id"       "period"        "servicer_name" "new_int_rt"    "act_endg_upb"  "loan_age"     
##  [7] "mths_remng"    "aj_mths_remng" "dt_matr"       "cd_msa"        "delq_sts"      "flag_mod"     
## [13] "cd_zero_bal"   "dt_zero_bal"  

## We can also save the DF as a folder of partitioned .csv files with syntax similar to that which we used to export the DF as partitioned parquet files. Note, however, that this does not retain the column names like saving as partitioned parquet files does. Additionally, exporting a DataFrame as partitioned .csv files requires that we specify the 2.10 version of the Databricks .csv package as the value of the `sparkPackages` parameter when initiating our SparkR context, i.e. we must set `sparkPackages = "com.databricks:spark-csv_2.10:1.4.0"` in the `sparkR.init` operation discussed in the README file for these tutorials. The `write.df` expression for exporting the DF as a folder of partitioned .csv files is given below:

write.df(perf_lim, "s3://sparkr-tutorials/hfpc_ex.csv", "com.databricks.spark.csv", "overwrite")

## Unpersist DFs:

unpersist(perf_lim)
unpersist(dat)

## We can read in the files as a DF with the following expression:

dat2 <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex.csv", source = "com.databricks.spark.csv", inferSchema = "true", header='false')

cache(dat2)

## Note that the DF columns are now given generic names, but we can use the same for-loop from a previous section in this tutorial to rename the columns in our new DF:

str(dat2)
for(i in 1:14){
  dat2 <- withColumnRenamed(dat2, old_colnames[i], new_colnames[i] )
}
str(dat2)

## Unpersist DFs:

unpersist(dat2)

## Stop SparkContext:

## sparkR.stop()