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


##################
### Bar graph: ###
##################

geom_bar(mapping = NULL, data = NULL, stat = "count", position = "stack", ..., width = NULL, binwidth = NULL, na.rm = FALSE, show.legend = NA, inherit.aes = TRUE)

# Default bar graph

ggplot(df, aes(x = cut)) + geom_bar()
ggplot(df, aes(x = cut)) + geom_bar(binwidth = 0.5) # Specify binwidth

# Weigthed bar graph (column value sums over grouped data) - NOTE cannot calculate mean across grouped data

ggplot(df, aes(cut)) + geom_bar(aes(weight = carat)) + ylab("carats")

# Bar graph over grouped data (bar graph with `fill` parameter specified)
# Note on performing `geom_bar` on grouped data: The following expression successfully returns a bar graph that describes frequency of observations by clarity, grouped over diamond color. Note, however, that the varied color blocks are not necessarily ordered similarly across different levels of `"cut"`. Similarly, the `"fill"` position will not necessariyl return constant factor=level ordering across different levels of `"cut"`.

ggplot(df, aes(x = cut, fill = clarity)) + geom_bar() # `position = "stack"` is default

ggplot(df, aes(x = cut, fill = clarity)) + geom_bar(position = "fill")

# While creating a stacked and filled bar graph may yield heterogeneous factor-level ordering, the `"dodge"` position specification ensures that count bars for each group are in constant order across clarity levels.

ggplot(df, aes(x = cut, fill = clarity)) + geom_bar(position = "dodge")


##################
### Histogram: ###
##################

geom_histogram(mapping = NULL, data = NULL, stat = "bin", position = "stack", ..., binwidth = NULL, bins = NULL, na.rm = FALSE, show.legend = NA, inherit.aes = TRUE)

# Default histogram (can specify `binwidth` _or_ number of `bins`)

ggplot(df, aes(price)) + geom_histogram()
ggplot(df, aes(price)) + geom_histogram(binwidth = 250)
ggplot(df, aes(price)) + geom_histogram(bins = 50)

# Weighted histogram:

ggplot(df, aes(cut)) + geom_histogram(aes(weight = price)) + ylab("total value")

# Stacked histograms:


ggplot(df, aes(price, fill = cut)) + geom_histogram(aes(fill = cut))


ggplot(df, aes(price, fill = cut)) + geom_histogram() # `position = "stack"` is default
ggplot(df, aes(price, fill = cut)) + geom_histogram(position = "fill")
ggplot(df, aes(price, fill = cut)) + geom_histogram(position = "dodge")



###########################
### Frequency Polygons: ###
###########################

geom_freqpoly(mapping = NULL, data = NULL, stat = "bin", position = "identity", ..., na.rm = FALSE, show.legend = NA, inherit.aes = TRUE)

# Default frequency polygon

ggplot(df, aes(price)) + geom_freqpoly()
ggplot(df, aes(price)) + geom_freqpoly(binwidth = 250)
ggplot(df, aes(price)) + geom_freqpoly(bins = 50)

ggplot(df, aes(price, ..density..)) + geom_freqpoly()

# Frequency polygons over grouped data are perhaps more easily interpreted than stacked histograms; the following is equivalent to the preceding stacked histogram. Note that we specify `"cut"` as `colour`, rather than `fill` as we did when using `geom_histogram`:

ggplot(df, aes(price, colour = cut)) + geom_freqpoly()

# To make it easier to compare distributions with very different counts, put density on the y axis instead of the default count # taken from ggplot2 site - edit
ggplot(df, aes(price, ..density.., colour = cut)) + geom_freqpoly()


#################################################################
### Dealing with overplotting in scatterplot using `stat_sum` ###
#################################################################

stat_sum(mapping = NULL, data = NULL, geom = "point", position = "identity", ...)

# Default `stat_sum`

ggplot(df, aes(x = carat, y = price)) + stat_sum()