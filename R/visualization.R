library(SparkR)
devtools::install_github("SKKU-SKT/ggplot2.SparkR")
library(ggplot2.SparkR)
sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", spark.driver.memory="1g", spark.driver.maxResultSize="1g"), sparkPackages="com.databricks:spark-csv_2.11:1.4.0")
sqlContext <- sparkRSQL.init(sc)

df <- read.df(sqlContext, "s3://ui-spark-data/diamonds.csv", header='true', delimiter=",", source="com.databricks.spark.csv", inferSchema='true', nullValue="")
cache(df)
head(df)