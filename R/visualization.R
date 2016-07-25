install.packages('devtools')
library(devtools)
library(SparkR)
devtools::install_github("SKKU-SKT/ggplot2.SparkR")
library(ggplot2.SparkR)
library(ggplot2)

sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", spark.driver.memory="1g", spark.driver.maxResultSize="1g"), sparkPackages="com.databricks:spark-csv_2.11:1.4.0")
sqlContext <- sparkRSQL.init(sc)

# Throughout this tutorial, we will use the diamonds data that is included in the `ggplot2` package and is frequently used `ggplot2` examples. The data consists of prices and quality information about 54,000 diamonds. The data contains the four C’s of diamond quality, carat, cut, colour and clarity; and five physical measurements, depth, table, x, y and z.

df <- read.df(sqlContext, "s3://ui-spark-data/diamonds.csv", header='true', delimiter=",", source="com.databricks.spark.csv", inferSchema='true', nullValue="")
cache(df)

# We can see what the data set looks like using the `str` operation:

str(df)

# Introduced in the spring of 2016, the SparkR extension of Hadley Wickham's `ggplot2` package, `ggplot2.SparkR`, allows SparkR users to build ggplot-type visualizations by specifying a SparkR DataFrame and DF columns in ggplot expressions identical to how we would specify R data.frame components when using the `ggplot2` package, i.e. the extension package allows SparkR users to implement ggplot without having to modify the SparkR DataFrame API.


# As of the publication date of this tutorial (first version), the `ggplot2.SparkR` package is still nascent and has identifiable bugs. However, we provide `ggplot2.SparkR` in this example for its ease of use, particularly for SparkR users wanting to build basic plots. We alternatively discuss how a SparkR user may develop their own plotting function and provide an example in which we plot a two-dimensional histogram.

# The description of the `diamonds` data given above was taken from http://ggplot2.org/book/qplot.pdf.

##################
### Bar graph: ###
##################

# geom_bar(mapping = NULL, data = NULL, stat = "count", position = "stack", ..., width = NULL, binwidth = NULL, na.rm = FALSE, show.legend = NA, inherit.aes = TRUE)

# Just as we would when using `ggplot2`, the following expression plots a basic bar graph that gives frequency counts across the different levels of `"cut"` quality in the data:

p1 <- ggplot(df, aes(x = cut))
p1 + geom_bar()

##### Stacked & proportional bar graphs

# One recognized bug within `ggplot2.SparkR` is that, when specifying a `fill` value, using the `"stack"` and `"fill"` specifications for `position` do not necessarily return plots with constant factor-level ordering across groups. For example, the following expression successfully returns a bar graph that gives frequency counts of `"clarity"` levels (string dtype), grouped over diamond `"cut"` types (also string dtype). Note, however, that the varied color blocks representing `"clarity"` levels are not ordered similarly across different levels of `"cut"`. The same issue results when we specify the `"fill"` position:

p2 <- ggplot(df, aes(x = cut, fill = clarity))
p2 + geom_bar() # `position = "stack"` is default
p2 + geom_bar(position = "fill")

# While creating a stacked or filled bar graph may yield heterogeneous factor-level ordering, the `"dodge"` position specification ensures constant across `"cut"` levels.

p2 + geom_bar(position = "dodge")


##################
### Histogram: ###
##################

# geom_histogram(mapping = NULL, data = NULL, stat = "bin", position = "stack", ..., binwidth = NULL, bins = NULL, na.rm = FALSE, show.legend = NA, inherit.aes = TRUE)

# Just as we would when using `ggplot2`, the following expression plots a histogram that gives frequency counts across binned `"price"` values in the data:

p3 <- ggplot(df, aes(price))
p3 + geom_histogram()

# The preceding histogram plot assumes the `ggplot2` default, `bins = 30`, but we can change this value or override the `bins` specification by setting a `binwidth` value as we do in the following examples:

p3 + geom_histogram(binwidth = 250)
p3 + geom_histogram(bins = 50)

# Weighted histogram:

# ggplot(df, aes(cut)) + geom_histogram(aes(weight = price)) + ylab("total value") NOT available in `ggplot2.SparkR`

# Stacked histograms:

# ggplot(df, aes(price, fill = cut)) + geom_histogram() # NOT available in `ggplot2.SparkR`
# ggplot(df, aes(price, fill = cut)) + geom_histogram(position = "fill")


###########################
### Frequency Polygons: ###
###########################

# geom_freqpoly(mapping = NULL, data = NULL, stat = "bin", position = "identity", ..., na.rm = FALSE, show.legend = NA, inherit.aes = TRUE)

# Frequency polygons provide a visual alternative to histogram plots (note that they describe equivalent aggregations). We can also fit frequency polygons with `ggplot2` syntax - the following expression returns a frequency polygon that is equivalent to the first histogram plotted in the preceding section:

p3 + geom_freqpoly()

# Again, we can change the class intervals by specifying `binwidth` or the number of `bins` for the frequency polygon:

p3 + geom_freqpoly(binwidth = 250)
p3 + geom_freqpoly(bins = 50)

# Frequency polygons over grouped data are perhaps more easily interpreted than stacked histograms; the following is equivalent to the preceding stacked histogram. Note that we specify `"cut"` as `colour`, rather than `fill` as we did when using `geom_histogram`:

# ggplot(df, aes(price, colour = cut)) + geom_freqpoly() NOT currently supported by `ggplot2.SparkR`

#################################################################
### Dealing with overplotting in scatterplot using `stat_sum` ###
#################################################################

# stat_sum(mapping = NULL, data = NULL, geom = "point", position = "identity", ...)

# NOT supported by `ggplot2.SparkR`

################
### Boxplot: ###
################

# Finally, we can create boxplots just as we would in `ggplot2`. The following expression gives a boxplot of `"price"` values across levels of `"clarity"`:

p4 <- ggplot(df, aes(x = clarity, y = price))
p4 + geom_boxplot()

##################################################
### Additional `ggplot2.SparkR` functionality: ###
##################################################

# We can adapt the plot types discussed in the previous sections with the specifications given below: 

#+ Facets: `facet_grid`, `facet_wrap` and `facet_null` (default)
#+ Coordinate systems: `coord_cartesian` and `coord_flip`
#+ Position adjustments: `position_dodge`, `position_fill`, `position_stack` (as seen in previous example)
#+ Scales: `scale_x_log10`, `scale_y_log10`, `labs`, `xlab`, `ylab`, `xlim` and `ylim`

# For example, the following expression facets our previous histogram example across the different levels of `"cut"` quality:

p3 + geom_histogram() + facet_wrap(~cut)

##################################################################
### Functionality gaps between `ggplot2` and SparkR extension: ###
##################################################################

# Below, we list several operations supported by `ggplot2` that are not currently supported by its SparkR extension package. The list is not exhaustive and is subject to change as the package continues to be developed:

#+ Weighted bar graph (i.e. specify `weight` in aesthetic)
#+ Weighted histogram
#+ Strictly ordered layers for filled and stacked bar graphs (as we saw in an earlier example)
#+ Stacked or filled histograms
#+ Layer frequency polygon (i.e specify `colour` in aesthetic)
#+ Density plot using `geom_freqpoly` by specifying `y = ..density..` in aesthetic (note that extension package does not support `geom_density`)

############################################
### Two-Dimensional Histogram (Heatmap): ###
############################################

# Dealing with overplotting in scatterplot using `stat_sum`/2-D histogram

geom_tile.SparkR <- function(df, x, y, nbins){
  
  library(ggplot2)
  
  x_min <- collect(agg(df, min(df[[x]])))
  x_max <- collect(agg(df, max(df[[x]])))
  x.bin <- seq(floor(x_min[[1]]), ceiling(x_max[[1]]), length = nbins)
  
  y_min <- collect(agg(df, min(df[[y]])))
  y_max <- collect(agg(df, max(df[[y]])))
  y.bin <- seq(floor(y_min[[1]]), ceiling(y_max[[1]]), length = nbins)
  
  x.bin.w <- x.bin[[2]]-x.bin[[1]]
  y.bin.w <- y.bin[[2]]-y.bin[[1]]
  
  df_ <- withColumn(df, "x_bin_", ceiling((df[[x]] - x_min[[1]]) / x.bin.w))
  df_ <- withColumn(df_, "y_bin_", ceiling((df[[y]] - y_min[[1]]) / y.bin.w))
  
  df_ <- mutate(df_, x_bin = ifelse(df_$x_bin_ == 0, 1, df_$x_bin_))
  df_ <- mutate(df_, y_bin = ifelse(df_$y_bin_ == 0, 1, df_$y_bin_))
  
  dat <- collect(agg(groupBy(df_, "x_bin", "y_bin"), count = n(df_$x_bin)))
  
  p <- ggplot(dat, aes(x = x_bin, y = y_bin, fill = count)) + geom_tile()
  
  return(p)
}

p1 <- geom_tile.SparkR(df = df, x = "carat", y = "price", nbins = 250)
p1 + scale_colour_brewer(palette = "9-class Blues", type = "seq") + ggtitle("This is a title") + xlab("Carat") + ylab("Price")
