#########################
## rbind.fill Function ##
#########################
# Sarah Armstrong, Urban Institute
# July 14, 2016

# Summary: Function that allows us to append rows of one SparkR DataFrame (DF) to another, regardless of the column names for each DF. If one DF contains columns not included in the other, that column is appended onto the first DF and the entries are set equal to null values.

# Inputs: x (a DF) and y (another DF)
# Returns: DataFrame

rbind.fill <- function(x, y) {

  m1 <- ncol(x)
  m2 <- ncol(y)
  col_x <- colnames(x)
  col_y <- colnames(y)
  
  if (m2 < m1) {
    col_ <- list(setdiff(col_x, col_y), setdiff(col_y, col_x))
    len <- length(col_)
    for (j in 1:len){
      y <- withColumn(y, col_[[j]], lit(NA))
    }
  } else { 
    if (m2 == m1) {
      col_ <- list(setdiff(col_x, col_y), setdiff(col_y, col_x))
      len <- length(col_)
      for (j in 1:len){
        x <- withColumn(x, col_[[j]], lit(NA))
        y <- withColumn(y, col_[[j]], lit(NA))
      }
    } else {
      col_ <- list(setdiff(col_x, col_y), setdiff(col_y, col_x))
      len <- length(col_)
      for (j in 1:len){
          x <- withColumn(x, col_[[j]], lit(NA))
      }
    }         
  }
  return(SparkR::rbind(x, y))
}