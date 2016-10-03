# sparkr-tutorials

Description: Code snippets and tutorials for working with SparkR.

## Spark Social Science Manual

The tutorials included in this repository are geared towards social scientists and policy researchers that want to undertake research using "big data" sets. A manual to accompany these tutorials is linked below. The objective of the manual is to provide social scientists with a brief overview of the distributed computing solution developed by The Urban Institute's Research Programming Team, and of the changes in how researchers manage and analyze data required by this computing environment.

[Spark Social Science Manual](https://bookdown.org/SarahArmstrong/spark-social-science-manual/intro.html)

## Getting Started with SparkR Tutorials

**Last Updated**: October 3, 2016 (Updated to include Spark Social Science Manual)

In order to begin working with SparkR, users must first:

1. Make sure that `SPARK_HOME` is set in environment (using `Sys.getenv`)
2. Load the `SparkR` library
3. Initiate a `sparkR.session`

```r
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/home/spark")
}

# Load the SparkR library
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

# Initiate a SparkR session
sparkR.session()
```

:heavy_exclamation_mark: The expressions given above _must_ be evaluated in SparkR before beginning any of the tutorials hosted here. Example data loading is included in each tutorial.

Users can end a SparkR session with the following expression:

```r
sparkR.session.stop()
```

**Note**: the data visualization tutorial, linked to below, is currently not updated for SparkR 2.0, but does still function with SparkR 1.6.

## Table of Contents:

* [SparkR Basics I: From CSV to SparkR DataFrame](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#sparkr-basics-i-from-csv-to-sparkr-dataframe)
    * [Load a csv file into SparkR](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#load-a-csv-file-into-sparkr)
    * [Update a DataFrame with new rows of data](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#update-a-dataframe-with-new-rows-of-data)
    * [Rename DataFrame column(s)](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#rename-dataframe-columns)
    * [Understanding data\-types &amp; schema](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#understanding-data-types--schema)
      * [Specifying schema in read\.df operation &amp; defining a custom schema](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#specifying-schema-in-readdf-operation--defining-a-custom-schema)
    * [Export DF as data file to S3](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md#export-df-as-data-file-to-s3)

* [SparkR Basics II: Essential DataFrame Operations](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#sparkr-basics-ii-essential-dataframe-operations)
    * [Aggregating](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#aggregating)
    * [Grouping:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#grouping)
    * [Arranging (Ordering) rows in a DataFrame](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#arranging-ordering-rows-in-a-dataframe)
    * [Append a column to a DataFrame](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#append-a-column-to-a-dataframe)
    * [Types of SparkR operations](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#types-of-sparkr-operations)
    * [DataFrame Persistence:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#dataframe-persistence)
    * [Converting a SparkR DataFrame to a local R data\.frame](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md#converting-a-sparkr-dataframe-to-a-local-r-dataframe)

* [Subsetting SparkR DataFrames](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subsetting-sparkr-dataframes#subsetting-sparkr-dataframes)
    * [Subset DataFrame by row](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subsetting-sparkr-dataframes#subset-dataframe-by-row)
    * [Subset DataFrame by column](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subsetting-sparkr-dataframes#subset-dataframe-by-column)
      * [Drop a column from a DF](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subsetting-sparkr-dataframes#drop-a-column-from-a-df)
    * [Subset a DF by taking a random sample](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subsetting-sparkr-dataframes#subset-a-df-by-taking-a-random-sample)
      * [Collect a random sample as a local data\.frame](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subsetting-sparkr-dataframes#collect-a-random-sample-as-a-local-dataframe)
      * [Export DF sample as a single \.csv file to S3](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/subsetting.md#subsetting-sparkr-dataframes#export-df-sample-as-a-single-csv-file-to-s3)

* [Dealing with Missing Data in SparkR](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#dealing-with-missing-data-in-sparkr)
    * [Specify null values when loading data in as a SparkR DataFrame (DF)](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#specify-null-values-when-loading-data-in-as-a-sparkr-dataframe-df)
    * [Conditional expressions on empty DF entries](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#conditional-expressions-on-empty-df-entries)
      * [Null and NaN indicator operations](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#null-and-nan-indicator-operations)
      * [Empty string entries:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#empty-string-entries)
      * [Distribution of missing data across grouped data](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#distribution-of-missing-data-across-grouped-data)
    * [Drop rows with missing data](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#drop-rows-with-missing-data)
      * [Null value entries](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#null-value-entries)
      * [Empty string entries](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#empty-string-entries-1)
    * [Fill missing data entries](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#fill-missing-data-entries)
      * [Null value entries](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#null-value-entries-1)
      * [Empty string entries](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/missing-data.md#dealing-with-missing-data-in-sparkr#empty-string-entries-2)

* [Computing Summary Statistics with SparkR](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#computing-summary-statistics-with-sparkr)
  * [Numerical Data](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#numerical-data)
    * [Measures of Location](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#measures-of-location)
      * [Mean](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#mean)
    * [Measures of dispersion](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#measures-of-dispersion)
      * [Range width &amp; limits](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#range-width--limits)
      * [Variance &amp; standard deviation](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#variance--standard-deviation)
      * [Approximate Quantiles](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#approximate-quantiles)
    * [Measures of distribution shape](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#measures-of-distribution-shape)
      * [Skewness](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#skewness)
      * [Kurtosis](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#kurtosis)
    * [Measures of dependence](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#measures-of-dependence)
      * [Covariance &amp; correlation](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#covariance--correlation)
  * [Categorical Data](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#categorical-data)
      * [Frequency table](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#frequency-table)
      * [Relative frequency table](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#relative-frequency-table)
      * [Contingency table](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/summary-statistics.md#computing-summary-statistics-with-sparkr#contingency-table)

* [Merging SparkR DataFrames](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/merging.md#merging-sparkr-dataframes#merging-sparkr-dataframes)
    * [Join (merge) two DataFrames by column condition(s)](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/merging.md#merging-sparkr-dataframes#join-merge-two-dataframes-by-column-conditions)
    * [Append rows of data to a DataFrame:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/merging.md#merging-sparkr-dataframes#append-rows-of-data-to-a-dataframe)
      * [Append rows when column name lists are equal across DFs](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/merging.md#merging-sparkr-dataframes#append-rows-when-column-name-lists-are-equal-across-dfs)
      * [Append rows when DF column name lists are not equal](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/merging.md#merging-sparkr-dataframes#append-rows-when-df-column-name-lists-are-not-equal)

* [Data Visualizations in SparkR](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#data-visualizations-in-sparkr#data-visualizations-in-sparkr)
    * [Bar graph](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#data-visualizations-in-sparkr#bar-graph)
      * [Stacked or proportional bar graph](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#data-visualizations-in-sparkr#stacked-or-proportional-bar-graph)
    * [Histogram:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#data-visualizations-in-sparkr#histogram)
    * [Frequency polygon](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#data-visualizations-in-sparkr#frequency-polygon)
    * [Boxplot:](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#data-visualizations-in-sparkr#boxplot)
    * [Additional ggplot2\.SparkR functionality](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#data-visualizations-in-sparkr#additional-ggplot2sparkr-functionality)
    * [Functionality gaps between ggplot2 and SparkR extension](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#data-visualizations-in-sparkr#functionality-gaps-between-ggplot2-and-sparkr-extension)
    * [Bivariate histogram](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/visualizations.md#data-visualizations-in-sparkr#bivariate-histogram)

* [Time Series I: Working with the Date Datatype &amp; Resampling a DataFrame](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#time-series-i-working-with-t%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20he-date-datatype--resampling-a-dataframe#time-series-i-working-with-the-date-datatype--resampling-a-dataframe)
    * [Converting a DataFrame column to 'date' dtype](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#time-series-i-working-with-t%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20he-date-datatype--resampling-a-dataframe#converting-a-dataframe-column-to-date-dtype)
    * [Compute relative dates and measures based on a specified unit of time](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#time-series-i-working-with-t%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20he-date-datatype--resampling-a-dataframe#compute-relative-dates-and-measures-based-on-a-specified-unit-of-time)
      * [Relative dates](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#time-series-i-working-with-t%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20he-date-datatype--resampling-a-dataframe#relative-dates)
      * [Relative measures of time](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#time-series-i-working-with-t%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20he-date-datatype--resampling-a-dataframe#relative-measures-of-time)
    * [Extract components of a date dtype column as integer values](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#time-series-i-working-with-t%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20he-date-datatype--resampling-a-dataframe#extract-components-of-a-date-dtype-column-as-integer-values)
    * [Resample a time series DF to a particular unit of time frequency](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/timeseries-1.md#time-series-i-working-with-t%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20he-date-datatype--resampling-a-dataframe#resample-a-time-series-df-to-a-particular-unit-of-time-frequency)

* [Fitting Generalized Linear Models in SparkR](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#fitting-generalized-linear-models-in-sparkr)
    * [Examine data prior to model fitting &amp; perform data transformations](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#examine-data-prior-to-model-fitting--perform-data-transformations)
    * [Fit Generalized Linear Model](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#fit-generalized-linear-model)
      * [Print model summary](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#print-model-summary)
      * [General linear model measurements from Gaussian/identity GLM](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#general-linear-model-measurements-from-gaussianidentity-glm)
      * [Calculate fitted values from model](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#calculate-fitted-values-from-model)
      * [Compute sum of squared totals and residuals](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#compute-sum-of-squared-totals-and-residuals)
      * [Compute R\-squared and adjusted R\-squared](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#compute-r-squared-and-adjusted-r-squared)
      * [Fit Gaussian/identity GLM and general linear model in R for comparison](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#fit-gaussianidentity-glm-and-general-linear-model-in-r-for-comparison)
    * [Fit other GLM distribution families supported by SparkR](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#fit-other-glm-distribution-families-supported-by-sparkr)
      * [Create binary response variable](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#create-binary-response-variable)
      * [Fit binomial, Gamma and Poisson GLMs in SparkR](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#fit-binomial-gamma-and-poisson-glms-in-sparkr)
    * [Linear model diagnostics](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#linear-model-diagnostics)
      * [Fitted v\. residual values plot](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#fitted-v-residual-values-plot)
      * [Q\-Q normality plot of the standardized residuals](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/glm.md#q-q-normality-plot-of-the-standardized-residuals)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)