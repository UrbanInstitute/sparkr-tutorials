## Tutorial for Filtering (aka Subsetting) an RDD


## Read in example data
data <- read.df(sqlContext, "s3://ui-hfpc/Performance_2015Q1.txt", header='false', delimiter="|", source="csv", inferSchema='true')
##


## Filter with 'is equal to'
data_lim <- filter(data, data$col1 == "Yes")


## Filter with 'is not equal to'
data_lim <- filter(data, data$col1 != "Yes")


## Filter with 'is greater than'
data_lim <- filter(data, data$col2 > 2)


## Filter with 'is greater than or equal to'
data_lim <- filter(data, data$col2 >= 2)


## Filter by both of two column conditions:
data_lim <- filter(data, data$col1 == "Yes" & data$col2 >= 2)


## Filter by one of  two column conditions:
data_lim <- filter(data, data$col1 == "Yes" | data$col2 >= 2)

