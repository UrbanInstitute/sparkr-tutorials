# SparkR Basics I: From CSV to SparkR DataFrame
Sarah Armstrong, Urban Institute  
June 23, 2016  




**Objective**: Become comfortable working with the SparkR DataFrame (DF) API; particularly, understand how to:

* Read a .csv file into SparkR as a DF
* Append a DF with additional rows
* Measure dimensions of a DF, print column names of a DF
* Subset a DF by columns
* Rename columns of a DF
* Print a specified number of rows from a DF
* Print basic summary statistics for the numerical columns of a DF
* Print the data types of each column of a DF
* Print the SparkR schema, specify schema in `read.df` file
* Manually specify a schema
* Change the data type of a column in a DF
* Export a DF to AWS S3 as a folder of partitioned parquet files

**SparkR/R Operations Discussed**: `read.df`, `nrow`, `ncol`, `dim`, `for`, `past0`, `rbind`, `select`, `withColumnRenamed`, `columns`, `head`, `take`, `str`, `describe`, `dtypes`, `schema`, `printSchema`, `cast`, `write.df`


 


*Initiate SparkR Context*:

Initiate your SparkR context for this module. Note that we direct 'sparkR.init' to include the Spark package `"com.databricks:spark-csv_2.11:1.4.0"` when initiating our SparkR context - we will rely on this package to load .csv files into SparkR:

`sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", spark.driver.memory="1g", spark.driver.maxResultSize="1g"), sparkPackages="com.databricks:spark-csv_2.11:1.4.0")`

Make sure that you have initiated the SparkR context and the SparkR SQL context, loaded the `SparkR` library, using the SparkR code given in the README file for this repository (found in the "Getting Started" section).


### Load a csv file into SparkR:

Use the operation `read.df` to load in quarterly Fannie Mae single-family loan performance data from the AWS S3 folder `"s3://ui-hfpc/"` as a Spark DataFrame (DF). Note that, when initiating our SparkContext, we specified that SparkR should include the `spark-csv` package in our SparkContext by including `sparkPackages="com.databricks:spark-csv_2.11:1.4.0"` in our `sparkR.init` operation. Below, we load a single quarter (2000, Q1)

_2.11:1.4.0 in our sparkR.init operation. Below, we load a single quarter (2000, Q1) into SparkR, and save it as the DF perf_:






```r
summary(cars)
```

```
##      speed           dist       
##  Min.   : 4.0   Min.   :  2.00  
##  1st Qu.:12.0   1st Qu.: 26.00  
##  Median :15.0   Median : 36.00  
##  Mean   :15.4   Mean   : 42.98  
##  3rd Qu.:19.0   3rd Qu.: 56.00  
##  Max.   :25.0   Max.   :120.00
```

## Including Plots

You can also embed plots, for example:

![](load-csv-into-sparkr-as-df-md_files/figure-html/pressure-1.png)<!-- -->

Note that the `echo = FALSE` parameter was added to the code chunk to prevent printing of the R code that generated the plot.
