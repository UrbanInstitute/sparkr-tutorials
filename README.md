# sparkr-tutorials
Code snippets and tutorials for working with SparkR.


## Getting Started


You must always load the SparkR library and initiliaze the spark context before working with SparkR and tabular data.

```r
library(SparkR)
sc <- sparkR.init(sparkPackages="com.databricks:spark-csv_2.11:1.4.0")

sqlContext <- sparkRSQL.init(sc)
```

Example data loading will be included in each tutorial.