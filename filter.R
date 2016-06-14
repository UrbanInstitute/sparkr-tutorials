## Tutorial for Filtering (a.k.a. Subsetting) an RDD


## Read in example HFPC data (quarterly performance data from 2000) from AWS S3:
data <- read.df(sqlContext, "s3://ui-hfpc/hfpc_ex.csv", header='false', delimiter="|", source="csv", inferSchema='true')

## Filter with 'is equal to' => returns observations 
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

