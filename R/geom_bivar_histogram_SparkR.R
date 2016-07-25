##########################################
## geom_bivar_histogram.SparkR Function ##
##########################################
# Sarah Armstrong & Alex Engler, Urban Institute
# July 21, 2016

# Summary: Plots a two-dimensional (2-D) histogram of frequency counts for two numerical DataFrame columns over a `nbin`-by-`nbin` grid of bins.

# Inputs:
# (*) df: SparkR DataFrame
# (*) x, y (string): The names of two numerical-valued columns in the SparkR DataFrame df
# (*) nbins (integer): The square root of the total number of bins that the frequency counts for x and y are aggregated over
# (*) title (string): A string specifying the input for `ggtitle` input in `ggplot`
# (*) xlab, ylab (string): A string specifying the input for `xlab` and `ylab` input in `ggplot`, respectively

# Returns: 2-D histogram of frequency counts (using `geom_tile` from ggplot2 package)

# Example:
# p1 <- geom_bivar_histogram.SparkR(df = df, x = "carat", y = "price", nbins = 250)
# p1 + scale_colour_brewer() + ggtitle("This is a title") + xlab("Carat") + ylab("Price")

geom_bivar_histogram.SparkR <- function(df, x, y, nbins){
  
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