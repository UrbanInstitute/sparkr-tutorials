#########################
## rbind.fill Function ##
#########################
# Sarah Armstrong, Urban Institute
# July 14, 2016

# Updated: July 28, 2016

# Summary: Function that allows us to append rows of one SparkR DataFrame (DF) to another, regardless of the column names for each DF. The function dentifies the outersection of the list of column names for two (2) DataFrames and adds them onto one (1) or both of the DataFrames as needed using `withColumn`. The function appends these columns as string dtype, and we can later recast columns as needed.

# Inputs: x (a DF) and y (another DF)
# Returns: DataFrame

# Example:
# df3 <- rbind.fill(df1, df2)
# df3$col <- cast(df3$col, dataType = "integer")


rbind.fill <- function(x, y) {
  
  m1 <- ncol(x)
  m2 <- ncol(y)
  col_x <- colnames(x)
  col_y <- colnames(y)
  outersect <- function(x, y) {setdiff(union(x, y), intersect(x, y))}
  col_outer <- outersect(col_x, col_y)
  len <- length(col_outer)
  
  if (m2 < m1) {
    for (j in 1:len){
      y <- withColumn(y, col_outer[j], cast(lit(""), "string"))
    }
  } else { 
    if (m2 > m1) {
        for (j in 1:len){
          x <- withColumn(x, col_outer[j], cast(lit(""), "string"))
        }
      }
    if (m2 == m1 & col_x != col_y) {
      for (j in 1:len){
        x <- withColumn(x, col_outer[j], cast(lit(""), "string"))
        y <- withColumn(y, col_outer[j], cast(lit(""), "string"))
      }
    } else { }         
  }
  x_sort <- x[,sort(colnames(x))]
  y_sort <- y[,sort(colnames(y))]
  return(SparkR::rbind(x_sort, y_sort))
}