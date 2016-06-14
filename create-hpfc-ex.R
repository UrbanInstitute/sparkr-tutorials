library(SparkR)

## Initiate SparkContext:
sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", 
                                  spark.driver.memory="1g",
                                  spark.driver.maxResultSize="1g")
                  ,sparkPackages="com.databricks:spark-csv_2.11:1.4.0") ## Load CSV Spark Package
## AWS EMR is using Spark 2.11 so we need the associated version of spark-csv: http://spark-packages.org/package/databricks/spark-csv
## Define Spark executor memory, as well as driver memory and maxResultSize according to cluster configuration

## Define SparkSQL:
sqlContext <- sparkRSQL.init(sc)

## Load initial data frame, here 2000, Q1 HFPC data:
perf_ <- read.df(sqlContext, "s3://ui-hfpc/Performance_2000Q1.txt", header='false', delimiter="|", source="csv", inferSchema='true')

## Load additional quarterly HFPC data (here 2000 Q2, Q3) and append these two quarters to our previously loaded data:
for(q in 2:3){
  
  filename <- paste0("Performance_2000Q",q)
  filepath <- paste0("s3://ui-hfpc/", filename, ".txt")
  .perf <- read.df(sqlContext, filepath, header='false', delimiter="|", source="csv", inferSchema='true')
  
  perf <- rbind(perf_, .perf)
}

## Subset the data by columns s.t. we consider only 14 columns of interest:
perf_lim <- select(perf, c("C0","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","C11","C12","C13"))

## Rename the columns:
perf_old_colnames <- c("C0","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","C11","C12","C13")
perf_new_colnames <- c("loan_id","period","servicer_name","new_int_rt","act_endg_upb","loan_age","mths_remng","aj_mths_remng","dt_matr","cd_msa","delq_sts","flag_mod","cd_zero_bal","dt_zero_bal")
for(i in 1:14){
  perf_lim <- withColumnRenamed(perf_lim, perf_old_colnames[i], perf_new_colnames[i] )
}
cache(perf_lim)

## Check column names of DataFrame perf_lim:
columns(perf_lim)
dim1 <- dim(perf_lim)
#head(perf_lim)
#printSchema(perf_lim)

## Export the data frame to S3:
write.df(perf_lim, "s3://ui-hfpc/hfpc_ex", "parquet", "overwrite")

## Read in example data:
data <- read.df(sqlContext, "s3://ui-hfpc/hfpc_ex", header='false', inferSchema='true')
## Check dimensions of example data--should match dimensions of perf_lim DF
dim2 <- dim(data)

## Check that dimensions & column names of perf_lim DataFrame and example data are equal:
if (dim1[1]!=dim2[1] | dim1[2]!=dim2[2]) {
  "Error: dimension values not equal; DataFrame did not export correctly"
} else {
  "Dimension values are equal"
}
if (columns(perf_lim)!=columns(data)) {
  "Error: data & DataFrame columns are not equal; DataFrame did not export correctly"
} else {
  "Column names are equal"
}

## Uncache perf_lim DataFrame
unpersist(perf_lim)

## Stop SparkContext:
#sparkR.stop()