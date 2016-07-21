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

#ggplot(df, aes(price, fill = cut)) + geom_histogram() # `position = "stack"` is default
#ggplot(df, aes(price, fill = cut)) + geom_histogram(position = "fill")


###########################
### Frequency Polygons: ###
###########################

geom_freqpoly(mapping = NULL, data = NULL, stat = "bin", position = "identity", ..., na.rm = FALSE, show.legend = NA, inherit.aes = TRUE)

# Default frequency polygon

ggplot(df, aes(price)) + geom_freqpoly()
ggplot(df, aes(price)) + geom_freqpoly(binwidth = 250)
ggplot(df, aes(price)) + geom_freqpoly(bins = 50)

# Frequency polygons over grouped data are perhaps more easily interpreted than stacked histograms; the following is equivalent to the preceding stacked histogram. Note that we specify `"cut"` as `colour`, rather than `fill` as we did when using `geom_histogram`:

ggplot(df, aes(price, colour = cut)) + geom_freqpoly()

#################################################################
### Dealing with overplotting in scatterplot using `stat_sum` ###
#################################################################

stat_sum(mapping = NULL, data = NULL, geom = "point", position = "identity", ...)

# Default `stat_sum`

# Numerical, numerical
ggplot(df, aes(x = carat, y = price)) + stat_sum()

ggplot(df, aes(x, y)) + stat_sum()
ggplot(df, aes(x, z)) + stat_sum()
ggplot(df, aes(y, z)) + stat_sum()

# Categorical, numerical
ggplot(df, aes(cut, price)) + stat_sum()

# Categorical, categorical
ggplot(df, aes(cut, clarity)) + stat_sum()

################
### Heatmap: ###
################

# Numerical, numerical

x_min <- SparkR::collect(SparkR::agg(df, min(df$x)))
x_max <- SparkR::collect(SparkR::agg(df, max(df$x)))
x.bin <- seq(floor(x_min[[1]]), ceiling(x_max[[1]]), length=nbins)

y_min <- SparkR::collect(SparkR::agg(df, min(df$y)))
y_max <- SparkR::collect(SparkR::agg(df, max(df$y)))
y.bin <- seq(floor(y_min[[1]]), ceiling(y_max[[1]]), length=nbins)
  
x_num <- as.numeric(unlist(collect(df[,"x"])))
y_num <- as.numeric(unlist(collect(df[,"y"])))
freq <-  as.data.frame(base::table(findInterval(x_num, x.bin),findInterval(y_num, y.bin)))
freq[,1] <- as.numeric(freq[,1])
freq[,2] <- as.numeric(freq[,2])
  
freq2D <- diag(nbins)*0
freq2D[cbind(freq[,1], freq[,2])] <- freq[,3]

# 2-D
image(x.bin, y.bin, log(freq2D), col=brewer.pal(max(freq2D), "RdYlBu"))
contour(x.bin, y.bin, log(freq2D), add=TRUE, col=rgb(1,1,1,.7))

# 3-D
palette(11, "RdYlBu")
cols <- (freq2D[-1,-1] + freq2D[-1,-(nbins-1)] + freq2D[-(nbins-1),-(nbins-1)] + freq2D[-(nbins-1),-1])/4
persp(freq2D, col=cols, xlab = "x", ylab = "y", zlab = "Count", theta = 20, phi = 20, r = sqrt(3))

#################
### Boxplots: ###
#################