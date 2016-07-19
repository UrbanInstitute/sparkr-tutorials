install.packages('devtools')
library(devtools)
devtools::install_github("SKKU-SKT/ggplot2.SparkR")
library(SparkR)
library(ggplot2.SparkR)

sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", spark.driver.memory="1g", spark.driver.maxResultSize="1g"), sparkPackages="com.databricks:spark-csv_2.11:1.4.0")
sqlContext <- sparkRSQL.init(sc)

# Load diamonds dataset as `df` DF into SparkR:

df <- read.df(sqlContext, "s3://ui-spark-data/diamonds.csv", header='true', delimiter=",", source="com.databricks.spark.csv", inferSchema='true', nullValue="")
cache(df)
head(df)



### Bar graph:

# Basic bar graph

ggplot(df, aes(x = clarity)) + geom_bar()

# Bar graph over grouped data (bar graph with `fill` parameter specified)
# Note on performing `geom_bar` on grouped data: The following expression successfully returns a bar graph that describes frequency of observations by clarity, grouped over diamond color. Note, however, that the varied color blocks are not ordered similarly across different levels of clarity.

ggplot(df, aes(x = clarity, fill = cut)) + geom_bar()

# While creating a stacked bar graphs may yield heterogeneous fill-level ordering, the `"dodge"` position specification ensures that count bars for each group are in constant order across clarity levels.

ggplot(df, aes(clarity, fill = cut)) + geom_bar(position = "dodge")

# Alternatively, we can change the string-valued entries of `df$cut` to be integers so that SparkR will use this ordering to....

df <- withColumn(df, cut_int, cast(lit(NULL), "double"))

df_f <- filter(df, df$cut == "Fair")
df <- fillna(df_f, list("cut_int" = 1))

df_g <- filter(df, df$cut == "Good")
df <- fillna(df_g, list("cut_int" = 2))

df_vg <- filter(df, df$cut == "Very Good")
df <- fillna(df_vg, list("cut_int" = 3))

df_p <- filter(df, df$cut == "Premium")
df <- fillna(df_p, list("cut_int" = 4))

df_i <- filter(df, df$cut == "Ideal")
df <- fillna(df_i, list("cut_int" = 5))

showDF(count(groupBy(df, "cut")))
showDF(count(groupBy(df, "cut_int")))

ggplot(df, aes(x = clarity, fill = cut_int)) + geom_bar()


### Histogram

# Default bin number/bin width

ggplot(df, aes(carat)) + geom_histogram()

# Can specify binwidth or number of bins

ggplot(df, aes(carat)) + geom_histogram(binwidth = 0.01)
ggplot(df, aes(carat)) + geom_histogram(bins = 200)

# Stacked histograms:

ggplot(df, aes(price, fill = cut)) + geom_histogram(binwidth = 500)