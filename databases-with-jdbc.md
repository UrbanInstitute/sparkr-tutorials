# Database Connectivity - JDBC
Alex C. Engler, Urban Institute  

**Last Updated**: March 31, 2017


**Objective**: Understand the basic process of reading from a databse with SparkR.

* Read a .csv file into SparkR as a DF
* Measure dimensions of a DF
* Append a DF with additional rows

**SparkR Operations Discussed**: `read.jdbc`

***

:heavy_exclamation_mark: **Warning**: Before beginning this tutorial, please visit the SparkR Tutorials README file (found [here](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/README.md)) in order to load the SparkR library and subsequently initiate a SparkR session.


The following error indicates that you have not initiated a SparkR session:


```r
Error in getSparkSession() : SparkSession not initialized
```

If you receive this message, return to the SparkR tutorials [README](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/README.md) for guidance.

***

### Interacting with databases using SparkR

Communicating with databases in Spark can be confusing, and may require further alterations to the bootstrap scripts in order to do. Functionality for MySQL is already loaded in the bootstrap scripts assocaited with these tutorials, and if that is what you need, feel free to skip the next step on installing new database connectivity JAR files.

#### Adding new database connectivity JAR files 

Currently, the bootstrap scripts for SparkR in the [Spark Social Science Repository](https://github.com/UrbanInstitute/spark-social-science/blob/master/sparkr/rstudio_sparkr_emr5lyr-proc.sh) install an add-on for working with mysql. This file is the MySQL Connector for JDBC, or [MySQL Coonector/J](https://dev.mysql.com/downloads/connector/j/5.1.html). In order for Spark to query MySQL databases (regardless of whether you are using SparkR, PySpark, or Scala), you need to install this connector, which is a .far file. To do this consistently, I first placed the zipped jar file in AWS S3. Then, [in the bootstrap scripts](https://github.com/UrbanInstitute/spark-social-science/blob/master/sparkr/rstudio_sparkr_emr5lyr-proc.sh#L256), I copy and unzip the zipped jar file to the /usr/lib/spark/jars folder, which Spark automatically loads.

```
aws s3 cp s3://ui-spark-social-science/emr-util/mysql-connector-java-5.1.41.tar.gz .
tar -xvzf mysql-connector-java-5.1.41.tar.gz
sudo mv mysql-connector-java-5.1.41/mysql-connector-java-5.1.41-bin.jar /usr/lib/spark/jars
rm -r mysql-connector-java-5.1.41
```

#### Querying databases with `read.jdbc()`

Once you have the appropriate jar file for your database of choice, you can use R's `read.jdbc()` function to query your database. Below, I am querying an AWS Aurora database (which is [AWS's MySQL-compatible database](https://aws.amazon.com/rds/aurora/)). 

First, you want to very carefully specify the URL for your database. You can see a full guide to the [JDBC URL Format](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-configuration-properties.html) here.

```r
your_url <- "jdbc:mysql:database_location/database_name"

```

Then you an pass this url, as well as the table you would like to fetch, your username, and your password to `read.jdbc()`.

```r
dat <- read.jdbc(url= your_url
                 , source="jdbc"
                 , driver="com.mysql.jdbc.Driver"
                 , tableName="your_table"
                 , user="your_username"
                 , password="your_password")
showDF(dat)
```

This is a fairly intuitive way to query an entire table, assuming that you have done some form of ODBC or JDBC in the past. However, for more complex queries, the syntax get less obvious.

#### Selecting columns within the tablename argument 

In order to perform a query that uses a MySQL `SELECT` statement 


```r
dat2 <- read.jdbc(url= your_url
                 , source="jdbc"
                 , driver="com.mysql.jdbc.Driver"
                 , tableName="(SELECT year, income FROM your_table) tmp"
                 , user="your_username"
                 , password="your_password")
showDF(dat2)
```



In the `read.df` operation, we give specifications typically included when reading data into Stata and SAS, such as the delimiter character for .csv files. However, we also include SparkR-specific input including `inferSchema`, which Spark uses to interpet data types for each column in the DF. We discuss this in more detail later on in this tutorial. An additional detail is that `read.df` includes the `na.strings = ""` specification because we want `read.df` to read entries of empty strings in our .csv dataset as NA in the SparkR DF, i.e. we are telling read.df to read entries equal to `""` as `NA` in the DF. We will discuss how SparkR handles empty and null entries in further detail in a subsequent tutorial.


_Note_: documentation for the quarterly loan performance data can be found at http://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html.


We can save the dimensions of the 'perf' DF through the following operations. Note that wrapping the computation with () forces SparkR/R to print the computed value:


```r
(n1 <- nrow(perf))  # Save the number of rows in 'perf'
## [1] 6887100
(m1 <- ncol(perf))  # Save the number of columns in 'perf'
## [1] 28
```

***


### Update a DataFrame with new rows of data

Since we'll want to analyze loan performance data beyond 2000 Q1, we append the `perf` DF below with the data from subsequent quarters of the same single-family loan performance dataset. Here, we're only appending one subsequent quarter (2000 Q2) to the DF so that our analysis in these tutorials runs quickly, but the following code can be easily adapted by specifying the `a` and `b` values to reflect the quarters that we want to append to our DF. Note that the for-loop below also uses the `read.df` operation, specified here just as when we loaded the initial .csv file as a DF:


```r
a <- 2
b <- 2

for(q in a:b){
  
  filename <- paste0("Performance_2000Q", q)
  filepath <- paste0("s3://sparkr-tutorials/", filename, ".txt")
  .perf <- read.df(filepath, header = "false", delimiter = "|", 
                   source = "csv", inferSchema = "true", na.strings = "")
  
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



__End of tutorial__ - Next up is []()


