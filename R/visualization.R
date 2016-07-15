## Supported by ggplot2.SparkR package:

# Note: Brewer colour scale does not work with package
# Use Brewer colour scale: We will add `+ scale_colour_brewer()` to each plot in tutorial


### Graph types:
#######################################################
### * `geom_bar`: Bars, rectangles with bases on x-axis

ggplot(df, aes(clarity)) + geom_bar()
ggplot(df, aes(clarity)) + geom_bar(width = 0.2)
ggplot(df, aes(clarity)) + geom_bar() + coord_flip()

## Note on performing `geom_bar` on grouped data: The following expression successfully returns a bar graph that describes frequency of observations by clarity, grouped over diamond color. Note, however, that the varied color blocks are not ordered similarly across different levels of clarity.
ggplot(df, aes(clarity, fill = color)) + geom_bar()
# The following does _not_ fix:
# In order to produce a filled bar graph with constant fill-level ordering across categories, we must first arrange `df` by the string entries of `"color"`, then 
#df_ <- arrange(df, df$color)
#ggplot(df_, aes(clarity, fill = color)) + geom_bar()
#rm(df_)
# While creating a stacked bar graphs may yield heterogeneous fill-level ordering, the `"dodge"` position specification ensures that count bars for each group are in constant order across clarity levels. 
ggplot(df, aes(clarity, fill = color)) + geom_bar(position = "dodge")
#######################################################
# * `geom_histogram`: Histogram

ggplot(df, aes(carat)) + geom_histogram()

# set binwidth = bw <- diff(range(x)) / (2 * IQR(x) / length(x)^(1/3)), but need to be able to calc IQR (or approx - coming out in SparkR 2.0.0?)
# Pick binwidth & no. of bins: # VERY slow - need to trouble shoot
ggplot(df, aes(carat)) + geom_histogram(binwidth = 0.01)
ggplot(diamonds, aes(carat)) + geom_histogram(bins = 200)
# Stack histograms
ggplot(df, aes(price, fill = color)) + geom_histogram(binwidth = 500)
#######################################################
# * `geom_freqpoly`: Frequency polygon
# Instead of stacking bar graphs and histograms, 

ggplot(df, aes(price, colour = color)) + geom_freqpoly()

#######################################################
# * `stat_sum`: Sum unique values

ggplot(df, aes(x = cut, y = clarity)) + stat_sum(aes(size = ..prop.., group = 1)) + scale_size_area(max_size = 10)

ggplot(df, aes(x = cut, y = clarity)) + stat_sum(aes(size = ..prop.., group = cut)) + scale_size_area(max_size = 10)

ggplot(df, aes(x = cut, y = clarity)) + geom_count(aes(size = ..prop.., group = clarity)) + scale_size_area(max_size = 10)

# * `geom_boxplot`: Box and whiskers plot
# * `geom_bin2d`: Heatmap of 2-D bin counts


# Positions:
# * `position_stack`: Stack overlapping objects on top of one another
# * `position_fill`: Same as `position_stack`, but with a standardized range
# * `position_dodge`: Adjust position by dodging overlaps to the side

# Facets:
# * `facet_null`: A single panel facet specification
# * `facet_grid`: Lay out panels in a grid
# * `facet_wrap`: Wrap a 1-D ribbon of panels into 2-D
ggplot(df, aes(clarity)) + geom_bar() + facet_wrap(~ color)

# Scales:
# * `scale_x_log10`: Force the x-axis onto a log-scale
# * `scale_y_log10`: Force the y-axis onto a log-scale

# Coordinates:
# * `coord_cartesian`: Cartesian coordinates
# * `coord_flip`: Flip the Cartesian coordinates

# Ranges
# * `xlim`: Set the range limits of the x-axis
# * `ylim`: Set the range limits of the y-axis

# Texts
# * `xlab`: Change the label of the x-axis
# * `ylab`: Change the label of the y-axis
# * `ggtitle`: Change the graph title
