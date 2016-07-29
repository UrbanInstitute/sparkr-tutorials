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

# Table of Contents:

  * [Merging SparkR DataFrames](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/merging.md#merging-sparkr-dataframes)
      * [Join (merge) two DataFrames by column condition(s):](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/merging.md#join-merge-two-dataframes-by-column-conditions)
      * [Append rows of data to a DataFrame:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/merging.md#append-rows-of-data-to-a-dataframe)
        * [Append rows when column name lists are equal across DFs:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/merging.md#append-rows-when-column-name-lists-are-equal-               across-dfs)
        * [Append rows when DF column name lists are not equal:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/merging.md#append-rows-when-df-column-name-lists-are-not-equ               al)

  * [Subsetting SparkR DataFrames](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subsetting-sparkr-dataframes)
      * [Subset DataFrame by row:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subset-dataframe-by-row)
      * [Subset DataFrame by column:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subset-dataframe-by-column)
        * [Drop a column from a DF:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#drop-a-column-from-a-df)
      * [Subset a DF by taking a random sample:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subset-a-df-by-taking-a-random-sample)
        * [Collect a random sample as a local data\.frame:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#collect-a-random-sample-as-a-local-dataframe)
        * [Export DF sample as a single \.csv file to S3:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#export-df-sample-as-a-single-csv-file-to-s3)

  * [Computing Summary Statistics with SparkR](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr)
    * [Numerical Data](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#numerical-data)
      * [Measures of Location:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#measures-of-location)
        * [Mean](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#mean)
      * [Measures of dispersion:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#measures-of-dispersion)
        * [Range width &amp; limits:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#range-width--limits)
        * [Variance &amp; standard deviation:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#variance--standard-deviation)
        * [Quantiles: [Insert section on measuring (approx\. quantiles) with release of SparkR 2\.0\.0]](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.m               d#quantiles-insert-section-on-measuring-approx-quantiles-with-release-of-sparkr-200)
      * [Measures of distribution shape:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#measures-of-distribution-shape)
        * [Skewness:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#skewness)
        * [Kurtosis:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#kurtosis)
      * [Measures of dependence:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#measures-of-dependence)
        * [Covariance &amp; correlation:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#covariance--correlation)
    * [Categorical Data](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#categorical-data)
        * [Frequency table:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#frequency-table)
        * [Relative frequency table:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#relative-frequency-table)
        * [Contingency table:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#contingency-table)

  * [Data Visualizations in SparkR](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#data-visualizations-in-sparkr)
      * [Bar graph:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#bar-graph)
        * [Stacked or proportional bar graph:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#stacked-or-proportional-bar-graph)
      * [Histogram:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#histogram)
      * [Frequency polygon:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#frequency-polygon)
      * [Boxplot:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#boxplot)
      * [Additional ggplot2\.SparkR functionality:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#additional-ggplot2sparkr-functionality)
      * [Functionality gaps between ggplot2 and SparkR extension:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#functionality-gaps-between-ggplot2-and-s               parkr-extension)
      * [Bivariate histogram:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#bivariate-histogram)

  * [SparkR Basics II: Essential DataFrame Operations](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#sparkr-basics-ii-essential-dataframe-operations)
      * [Aggregating:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#aggregating)
      * [Grouping:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#grouping)
      * [Arranging (Ordering) rows in a DataFrame:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#arranging-ordering-rows-in-a-dataframe)
      * [Append a column to a DataFrame:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#append-a-column-to-a-dataframe)
      * [User\-defined Functions (UDFs): [Note insert upon SparkR 2\.0\.0 release]](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#user-defined-functions               -udfs-note-insert-upon-sparkr-200-release)
      * [Types of SparkR operations:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#types-of-sparkr-operations)
      * [DataFrame Persistence:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#dataframe-persistence)
      * [Converting a SparkR DataFrame to a local R data\.frame:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#converting-a-sparkr-dataframe-to-a-local               -r-dataframe)

  * [Time Series I: Working with the Date Datatype &amp; Resampling a DataFrame](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#time-series-i-working-with-t               he-date-datatype--resampling-a-dataframe)
      * [Converting a DataFrame column to 'date' dtype:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#converting-a-dataframe-column-to-date-dtype)
      * [Compute relative dates and measures based on a specified unit of time:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#compute-relative-dates-and-m               easures-based-on-a-specified-unit-of-time)
        * [Relative dates:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#relative-dates)
        * [Relative measures of time:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#relative-measures-of-time)
      * [Extract components of a date dtype column:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#extract-components-of-a-date-dtype-column)
      * [Resample a time series DF to a particular unit of time frequency](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#resample-a-time-series-df-to-a-par               ticular-unit-of-time-frequency)

  * [SparkR Basics I: From CSV to SparkR DataFrame](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#sparkr-basics-i-from-csv-to-sparkr-dataframe)
      * [Load a csv file into SparkR:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#load-a-csv-file-into-sparkr)
      * [Update a DataFrame with new rows of data:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#update-a-dataframe-with-new-rows-of-data)
      * [Rename DataFrame column(s):](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#rename-dataframe-columns)
      * [Understanding data\-types &amp; schema:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#understanding-data-types--schema)
        * [Specifying schema in read\.df operation &amp; defining a custom schema:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#specifying-schema-in-r               eaddf-operation--defining-a-custom-schema)
      * [Export DF as data file to S3:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#export-df-as-data-file-to-s3)

  * [Dealing with Missing Data in SparkR](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr)
      * [Specify null values when loading data in as a SparkR DataFrame (DF):](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#specify-null-values-when-loadi               ng-data-in-as-a-sparkr-dataframe-df)
      * [Conditional expressions on empty DF entries:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#conditional-expressions-on-empty-df-entries)
        * [Null and NaN indicator operations:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#null-and-nan-indicator-operations)
        * [Empty string entries:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#empty-string-entries)
        * [Distribution of missing data across grouped data:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#distribution-of-missing-data-across-grouped-dat               a)
      * [Drop rows with missing data:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#drop-rows-with-missing-data)
        * [Null value entries:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#null-value-entries)
        * [Empty string entries:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#empty-string-entries-1)
      * [Fill missing data entries:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#fill-missing-data-entries)
        * [Null value entries:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#null-value-entries-1)
        * [Empty string entries:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#empty-string-entries-2)

TOC created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)
