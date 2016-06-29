# SparkR Basics I: From CSV to SparkR DataFrame
Sarah Armstrong, Urban Institute  
June 23, 2016  




**Objective**: Become comfortable working with the SparkR DataFrame (DF) API; particularly, understand how to:

* Read a .csv file into SparkR as a DF
* Measure dimensions of a DF
* Append a DF with additional rows
* Rename columns of a DF
* Print column names of a DF
* Print a specified number of rows from a DF
* Print the SparkR schema
* Specify schema in `read.df` file
* Manually specify a schema
* Change the data type of a column in a DF
* Export a DF to AWS S3 as a folder of partitioned parquet files
* Read partitioned file from S3 to SparkR

**SparkR/R Operations Discussed**: `read.df`, `nrow`, `ncol`, `dim`, `for`, `past0`, `rbind`, `withColumnRenamed`, `columns`, `head`, `take`, `str`, `describe`, `dtypes`, `schema`, `printSchema`, `cast`, `write.df`

***

<span style="color:red">**Warning**</span>: Before beginning this tutorial, please visit the SparkR Tutorials README file (found [here](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/README.md)) in order to load the SparkR library and subsequently initiate your SparkR and SparkR SQL contexts.



You can confirm that you successfully initiated these contexts by looking at the global environment of RStudio. Can you see `sc` and `sqlContext` listed as values? If the answer is yes, then you are ready to learn how to work with tabular data in SparkR!

***

### Load a csv file into SparkR:

Use the operation `read.df` to load in quarterly Fannie Mae single-family loan performance data from the AWS S3 folder `"s3://ui-hfpc/"` as a Spark DataFrame (DF). Note that, when initiating our SparkR context, we specified that the `spark-csv` package should be included by by specifying `sparkPackages="com.databricks:spark-csv_2.11:1.4.0"` in our `sparkR.init` operation. Below, we load a single quarter (2000, Q1) into SparkR, and save it as the DF `perf`:


```r
perf <- read.df(sqlContext, "s3://ui-hfpc/Performance_2000Q1.txt", header='false', delimiter="|", source="csv", inferSchema='true', nullValue="")
```



In the `read.df` operation, we included typical specifications included when reading data into Stata and SAS, such as what character represents the delimiter in the .csv file. However, we also included `sqlContext` (which we previously defined with the `sparkRSQL.init` operation in the README). This is essential for SparkR to understand how the read-in data should be partitioned and distributed across the computers (nodes) that make up the AWS cluster currently being used. Another input that is SparkR-specific is `inferSchema`, which SparkRSQL requires in order to interpet data types for each column in the DF. We discuss this in more detail later on in this tutorial. An additional detail is that 'read.df' includes the `nullValue=""` specification because we want `read.df` to read entries of empty strings in our .csv dataset as NA in the SparkR DF, i.e. we are telling read.df to read entries equal to `""` as `NA` in the DF. We will discuss how SparkR handles empty and null entries in further detail in a subsequent tutorial.


_Note_: documentation for the quarterly loan performance data can be found at http://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html.


We can save the dimensions of the 'perf' DF through the following operations. Note that wrapping the computation with () forces SparkR/R to print the computed value:


```r
(n1 <- nrow(perf))	# Save the number of rows in 'perf'
## [1] 6887100
(m1 <- ncol(perf))	# Save the number of columns in 'perf'
## [1] 28
```

***
### Update a DataFrame with new rows of data:

In order to take advantage of the newfound computing power that SparkR provides, and since we'll want to analyze loan performance data beyond 2000 Q1, we append the `perf` DF below with the data from subsequent quarters of the same single-family loan performance dataset. Here, we're only appending one subsequent quarter (2000 Q2) to the DF so that our analysis in these tutorials runs quickly, but the following code can be easily adapted by specifying the `a` and `b` values to reflect the quarters that we want to append to our DF. Note that the for-loop below again includes the `read.df` operation, here specified just as we did when initial loading the .csv file as a DF:


```r
a <- 2
b <- 2
for(q in a:b){
  
  filename <- paste0("Performance_2000Q", q)
  filepath <- paste0("s3://ui-hfpc/", filename, ".txt")
  .perf <- read.df(sqlContext, filepath, header='false', delimiter="|", source="csv", inferSchema='true', nullValue="")
  
  perf <- rbind(perf, .perf)
}
```

The result of the for-loop is an appended `perf` DF that consists of the same columns as the initial `perf` DF that we read in from S3, but now with many appended rows. We can confirm this by taking the dimensions of the new DF:


```r
(n2 <- nrow(perf))
## [1] 13216516
(m2 <- ncol(perf))
## [1] 28
```

***
<a id="rename_columns"></a>
### Rename DataFrame column(s):

The `select` operation selects columns specified as strings in the operation line and then returns a new DF including only those specified columns. Here, we create a new DF called `perf_lim` that includes only the first 14 columns in the `perf` DF, i.e. the DF `perf_lim` is a subset of `perf`:


```r
perf_lim <- select(perf, c("C0","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","C11","C12","C13"))
```



We will discuss subsetting DataFrames in further detail in the "Basics II" tutorial. For now, we will use this subsetted DF to learn how to change column names of DataFrames.


Using a for-loop and the SparkR operation `withColumnRenamed`, we rename the columns of `perf_lim`. The operation `withColumnRenamed` renames an existing column, or columns, in a DF and returns a new DF. By specifying the "new" DF name as `perf_lim`, we are simply renaming the columns of `perf_lim`, but we could create an entirely separate DF with new column names by specifying a different DF name for `withColumnRenamed`:


```r
old_colnames <- c("C0","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","C11","C12","C13")
new_colnames <- c("loan_id","period","servicer_name","new_int_rt","act_endg_upb","loan_age","mths_remng","aj_mths_remng","dt_matr","cd_msa","delq_sts","flag_mod","cd_zero_bal","dt_zero_bal")
for(i in 1:14){
  perf_lim <- withColumnRenamed(perf_lim, old_colnames[i], new_colnames[i] )
}
```

We can check the column names of `perf_lim` with the `columns` operation:


```r
columns(perf_lim)
##  [1] "loan_id"       "period"        "servicer_name" "new_int_rt"   
##  [5] "act_endg_upb"  "loan_age"      "mths_remng"    "aj_mths_remng"
##  [9] "dt_matr"       "cd_msa"        "delq_sts"      "flag_mod"     
## [13] "cd_zero_bal"   "dt_zero_bal"
```

Additionally, we can use the `head` operation to display the first n-many rows of `perf_lim` (here, we'll take the first five (5) rows of the DF):


```r
head(perf_lim, 5)
##        loan_id     period servicer_name new_int_rt act_endg_upb loan_age
## 1 100007365142 01/01/2000                        8           NA        0
## 2 100007365142 02/01/2000                        8           NA        1
## 3 100007365142 03/01/2000                        8           NA        2
## 4 100007365142 04/01/2000                        8           NA        3
## 5 100007365142 05/01/2000                        8           NA        4
##   mths_remng aj_mths_remng dt_matr cd_msa delq_sts flag_mod cd_zero_bal
## 1        360           359 01/2030      0        0        N          NA
## 2        359           358 01/2030      0        0        N          NA
## 3        358           357 01/2030      0        0        N          NA
## 4        357           356 01/2030      0        0        N          NA
## 5        356           355 01/2030      0        0        N          NA
##   dt_zero_bal
## 1            
## 2            
## 3            
## 4            
## 5
```

***

### Understanding data-types & schema:

We can see in the output for the command head(perf_lim, 5) that we have what appears to be several different data types (dtypes) in our DF, but we obviously cannot infer what dtype is currently specified for each column in our DF by simply looking at that output. Luckily, there are three (3) different ways to view dtype in SparkR - the operations `dtypes`, `schema` and `printSchema`. As stated above, SparkRSQL relies on a "schema" to determine what data type to assign to each column in the DF (which is easy to remember since the English schema comes from the Greek word for shape or plan!). We can print a visual representation of the schema for a DF with the operations `schema` and `printSchema`:


```r
dtypes(perf_lim)	# Prints pairs of strings that correspond to a column name and its data type
## [[1]]
## [1] "loan_id" "bigint" 
## 
## [[2]]
## [1] "period" "string"
## 
## [[3]]
## [1] "servicer_name" "string"       
## 
## [[4]]
## [1] "new_int_rt" "double"    
## 
## [[5]]
## [1] "act_endg_upb" "double"      
## 
## [[6]]
## [1] "loan_age" "int"     
## 
## [[7]]
## [1] "mths_remng" "int"       
## 
## [[8]]
## [1] "aj_mths_remng" "int"          
## 
## [[9]]
## [1] "dt_matr" "string" 
## 
## [[10]]
## [1] "cd_msa" "int"   
## 
## [[11]]
## [1] "delq_sts" "string"  
## 
## [[12]]
## [1] "flag_mod" "string"  
## 
## [[13]]
## [1] "cd_zero_bal" "int"        
## 
## [[14]]
## [1] "dt_zero_bal" "string"
schema(perf_lim)	# Prints the schema of the DF
## StructType
## |-name = "loan_id", type = "LongType", nullable = TRUE
## |-name = "period", type = "StringType", nullable = TRUE
## |-name = "servicer_name", type = "StringType", nullable = TRUE
## |-name = "new_int_rt", type = "DoubleType", nullable = TRUE
## |-name = "act_endg_upb", type = "DoubleType", nullable = TRUE
## |-name = "loan_age", type = "IntegerType", nullable = TRUE
## |-name = "mths_remng", type = "IntegerType", nullable = TRUE
## |-name = "aj_mths_remng", type = "IntegerType", nullable = TRUE
## |-name = "dt_matr", type = "StringType", nullable = TRUE
## |-name = "cd_msa", type = "IntegerType", nullable = TRUE
## |-name = "delq_sts", type = "StringType", nullable = TRUE
## |-name = "flag_mod", type = "StringType", nullable = TRUE
## |-name = "cd_zero_bal", type = "IntegerType", nullable = TRUE
## |-name = "dt_zero_bal", type = "StringType", nullable = TRUE
printSchema(perf_lim) # Prints the schema of the DF in a concise tree format
## root
##  |-- loan_id: long (nullable = true)
##  |-- period: string (nullable = true)
##  |-- servicer_name: string (nullable = true)
##  |-- new_int_rt: double (nullable = true)
##  |-- act_endg_upb: double (nullable = true)
##  |-- loan_age: integer (nullable = true)
##  |-- mths_remng: integer (nullable = true)
##  |-- aj_mths_remng: integer (nullable = true)
##  |-- dt_matr: string (nullable = true)
##  |-- cd_msa: integer (nullable = true)
##  |-- delq_sts: string (nullable = true)
##  |-- flag_mod: string (nullable = true)
##  |-- cd_zero_bal: integer (nullable = true)
##  |-- dt_zero_bal: string (nullable = true)
```

#### Specifying schema in `read.df` operation & defining a custom schema:

Remember that, when we read in our DF from the S3 .csv file, we included the condition `inferSchema='true'`. This is just one of three (3) ways to communicate to SparkRSQL how the dtypes of the DF columns should be assigned. By specifying `inferSchema='true'` in `read.df`, we allow SparkRSQL to infer the dtype of each column in the DF. Conversely, we could specify our own schema and pass this into the load call, forcing SparkRSQL to adopt our dtype specifications for each column. Each of these approaches have their pros and cons, which determine when it is appropriate to prefer one over the other:

* `inferSchema='true'`: This approach minimizes programmer-driven error since we aren't required to make assertions about the dtypes of each column; however, it is comparatively computationally expensive

* `customSchema`: While computationally more efficient, manually specifying a schema will lead to errors if incorrect dtypes are assigned to columns - if SparkRSQL is not able to interpret a column as the specified dtype, `read.df` will fill that column in the DF with NA

Clearly, the situations in which these approaches would be helpful are starkly different. In the context of this tutorial, an efficient use of both approaches would be to use `inferSchema='true'` when reading in `perf`. At this point, we could print the schema with `schema` or `printSchema`, note the dtype for each column (all 28 of them), and then write a customSchema with the corresponding specifications. We could then use this customSchema when appending the subsequent quarters to `perf`. While writing the customSchema may be tedious, including it in the appending for-loop would help that process to be much more efficient - this would be especially useful if we were appending, for example, 20 years worth of quarterly data together. The third way to communicate to SparkRSQL how to define dtypes is to not specify any schema, i.e. to not include `inferSchema` in `read.df`. Under this condition, every column in the DF is read in as a string dtype. Below is the an example of how we could specify a customSchema (here, however, we just use the same dtypes as interpreted for `inferSchema='true'`):


```r
customSchema <- structType(
 structField("loan_id", "long"),
 structField("period", "string"),
 structField("servicer_name", "string"),
 structField("new_int_rt", "double"),
 structField("act_endg_upb", "double"),
 structField("loan_age", "integer"),
 structField("mths_remng", "integer"),
 structField("aj_mths_remng", "integer")
 structField("dt_matr", "string")
 structField("cd_msa", "integer")
 structField("delq_sts", "string")
 structField("flag_mod", "string")
 structField("cd_zero_bal", "integer")
 structField("dt_zero_bal", "string")
)
```

Finally, dtypes can be changed after the DF has been created, using the `cast` operation. However, it is clearly more efficient to properly specify dtypes when creating the DF. A quick example of using the `cast` operation is given below:


```r
perf_lim$loan_id <- cast(perf_lim$loan_id, 'string')		# We can see in the results from the previous printSchema output that `loan_id` is a `long` dtype, here we `cast` it as a `string` and then call `printSchema` on this new DF
printSchema(perf_lim)
## root
##  |-- loan_id: string (nullable = true)
##  |-- period: string (nullable = true)
##  |-- servicer_name: string (nullable = true)
##  |-- new_int_rt: double (nullable = true)
##  |-- act_endg_upb: double (nullable = true)
##  |-- loan_age: integer (nullable = true)
##  |-- mths_remng: integer (nullable = true)
##  |-- aj_mths_remng: integer (nullable = true)
##  |-- dt_matr: string (nullable = true)
##  |-- cd_msa: integer (nullable = true)
##  |-- delq_sts: string (nullable = true)
##  |-- flag_mod: string (nullable = true)
##  |-- cd_zero_bal: integer (nullable = true)
##  |-- dt_zero_bal: string (nullable = true)
perf_lim$loan_id <- cast(perf_lim$loan_id, 'long')			# If we want our original `perf_lim` DF, we can simply recast `loan_id` as a `long` dtype
printSchema(perf_lim)
## root
##  |-- loan_id: long (nullable = true)
##  |-- period: string (nullable = true)
##  |-- servicer_name: string (nullable = true)
##  |-- new_int_rt: double (nullable = true)
##  |-- act_endg_upb: double (nullable = true)
##  |-- loan_age: integer (nullable = true)
##  |-- mths_remng: integer (nullable = true)
##  |-- aj_mths_remng: integer (nullable = true)
##  |-- dt_matr: string (nullable = true)
##  |-- cd_msa: integer (nullable = true)
##  |-- delq_sts: string (nullable = true)
##  |-- flag_mod: string (nullable = true)
##  |-- cd_zero_bal: integer (nullable = true)
##  |-- dt_zero_bal: string (nullable = true)
```

***

### Export DF as data file to S3:

Throughout this tutorial, we've built the Spark DataFrame `perf_lim` of quarterly loan performance data, which we'll use in several subsequent tutorials. In order to use this DF later on, we must first export it to a location that can handle large data sizes and in a data structure that works with the SparkR environment. We'll save this example data to an AWS S3 folder (`"sparkr-tutorials"`) from which we'll access other example datasets. Below, we save `perf_lim` as a collection of parquet type files into the folder `"hfpc_ex"` using the `write.df` operation:


```r
write.df(perf_lim, "s3://sparkr-tutorials/hfpc_ex", source = "parquet", mode = "overwrite")
```

When working with the DF `perf_lim` in the analysis above, we were really accessing data that was partitioned across our cluster. In order to export this partitioned data, we export each partition from its node (computer) and then collect them into the folder `"hfpc_ex"`. This "file" of indiviudal, partitioned files should be treated like an indiviudal file when organizing an S3 folder, i.e. __do not__ attempt to save other DataFrames or files to this file. Also, note that we have specified `mode = "overwrite"`, indicating that existing data in this folder is expected to be overwritten by the contents of this DF (additional mode specifications include `"error"`, `"ignore"` and `"append"`).


SparkR saves the DF in this partitioned structure to accomodate massive data. Consider the conditions required for us to be able to save a DataFrame as a single .csv file: the given DF would need to be able to fit onto a single node of our cluster, i.e. it would need to be able to fit onto a single computer. Any data that would necessitate using SparkR in analysis will likely not fit onto a single computer.


The partitioned nature of `"hfpc_ex"` does not affect our ability to load it back into SparkR and perform further analysis. Below, we use the `read.df` to read in the partitioned parquet file from S3 as the DF `dat`:


```r
dat <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header='false', inferSchema='true')
```

Below, we confirm that the dimensions and column names of `dat` and `perf_lim` are equal:


```r
dim1 <- dim(perf_lim)
dim2 <- dim(dat)
if (dim1[1]!=dim2[1] | dim1[2]!=dim2[2]) {
  "Error: dimension values not equal; DataFrame did not export correctly"
} else {
  "Dimension values are equal"
}
## [1] "Dimension values are equal"
```

__End of tutorial__ - Next up is SparkR Basics II


