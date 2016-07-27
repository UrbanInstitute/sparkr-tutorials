##############################
## rbind.intersect Function ##
##############################
# Sarah Armstrong, Urban Institute
# July 14, 2016

# Summary: Function that allows us to append rows of one SparkR DataFrame (DF) to another, regardless of the column names for each DF. Takes simple intersection of lists of column names and performs `rbind` SparkR operation on two (2) DFs, considering only the column names included in the intersected list of names.

# Inputs: x (a DF) and y (another DF)
# Returns: DataFrame

rbind.intersect <- function(x, y) {
  cols <- base::intersect(colnames(x), colnames(y))
  return(SparkR::rbind(x[, sort(cols)], y[, sort(cols)]))
}