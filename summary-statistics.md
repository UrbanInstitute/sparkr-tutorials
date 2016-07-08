# Summary Statistics
Sarah Armstrong, Urban Institute  
July 8, 2016  




**Objective**: Summary statistics and aggregations are essential means of summarizing a set of observations. In this tutorial, we discuss how to compute location, statistical dispersion, distribution and dependence measures of numerical variables in SparkR, as well as methods for examining categorical variables. In particular, we consider how to compute the following measurements in SparkR:

_Numerical Data_

* Measures of location:
    + Mean
    + Extract summary statistics as local value
* Measures of dispersion:
    + Range width & limits
    + Variance
    + Standard deviation
    + Quantiles
* Measures of distribution shape:
    + Skewness
    + Kurtosis
* Measures of Dependence:
    + Covariance
    + Correlation

_Categorical Data_

* Frequency table
* Relative frequency table
* Contingency table

**SparkR/R Operations Discussed**: `describe`, `collect`, `showDF`, `agg`, `mean`, `typeof`, `min`, `max`, `abs`, `var`, `sd`, `skewness`, `kurtosis`, `cov`, `corr`, `count`, `n`, `groupBy`, `nrow`, `crosstab`

***

<span style="color:red">**Warning**</span>: Before beginning this tutorial, please visit the SparkR Tutorials README file (found [here](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/README.md)) in order to load the SparkR library and subsequently initiate your SparkR and SparkR SQL contexts.



You can confirm that you successfully initiated these contexts by looking at the global environment of RStudio. Only proceed if you can see `sc` and `sqlContext` listed as values in the global environment or RStudio.

***

**Read in initial data as DF**: Throughout this tutorial, we will use the loan performance example dataset that we exported at the conclusion of the SparkR Basics I tutorial.


```r
df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header='false', inferSchema='true')
cache(df)
```

_Note_: documentation for the quarterly loan performance data can be found at http://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html.

***


## Numerical Data

The operation `describe` (or its alias `summary`) creates a new DF that consists of several key aggregations (count, mean, max, mean, standard deviation) for a specified DF or list of DF columns (note that columns must be of a numerical datatype). We can either (1) use the action operation `showDF` to print this aggregation DF or (2) save it as a local data.frame with `collect`. Here, we perform both of these actions on the aggregation DF `sumstats_mthsremng`, which returns the aggregations listed above for the column `"mths_remng"` in `df`:


```r
sumstats_mthsremng <- describe(df, "mths_remng")  # Specified list of columns here consists only of "mths_remng"

showDF(sumstats_mthsremng)  # Print the aggregation DF
## +-------+------------------+
## |summary|        mths_remng|
## +-------+------------------+
## |  count|          13208202|
## |   mean| 330.9827155126792|
## | stddev|35.497058452032455|
## |    min|               -19|
## |    max|               482|
## +-------+------------------+

sumstats_mthsremng.l <- collect(sumstats_mthsremng) # Collect aggregation DF as a local data.frame
sumstats_mthsremng.l
##   summary         mths_remng
## 1   count           13208202
## 2    mean  330.9827155126792
## 3  stddev 35.497058452032455
## 4     min                -19
## 5     max                482
```

Note that measuring all five (5) of these aggregations at once is computationally expensive, particularly if we are interested in only a subset of these measurements. Below, we outline ways to measure these aggregations individually, as well as several other key summary statistics for numerical data.

***


### Measures of Location:


#### Mean

While there are several measures of central tendency, SparkR currently only supports computing averages of numerical DF columns. The operations `mean` and `avg` can be used with the `agg` operation that we discussed in the SparkR Basics II tutorial to measure the average of a numerical DF column. Remember that `agg` returns another DF. Therefore, we can either print the DF with `showDF` or we can save the aggregation as a local data.frame. Collecting the DF may be preferred if we want to work with the mean `"mths_remng"` value as a single value in RStudio.


```r
mths_remng.avg <- agg(df, mean = mean(df$mths_remng)) # Create an aggregation DF
showDF(mths_remng.avg) # Print this DF
## +-----------------+
## |             mean|
## +-----------------+
## |330.9827155126792|
## +-----------------+
typeof(mths_remng.avg) # Aggregation DF is of class S4
## [1] "S4"

mths_remng.avg.l <- collect(mths_remng.avg) # Collect the DF as a local data.frame
(mths_remng.avg.l <- mths_remng.avg.l[,1])  # Overwrite data.frame with numerical mean value (was entry in d.f)
## [1] 330.9827
typeof(mths_remng.avg.l)  # Object is now of a numerical dtype
## [1] "double"
```

***


### Measures of dispersion:


#### Range width & limits:

We can also use `agg` to create a DF that lists the minimum and maximum values within a numerical DF column (i.e. the limits of the range of values in the column) and the width of the range of these values. Here, we create compute these values for `"mths_remng"` and print the resulting DF with `showDF`:


```r
mr_range <- agg(df, minimum = min(df$mths_remng), maximum = max(df$mths_remng), range_width = abs(max(df$mths_remng) - min(df$mths_remng)))
showDF(mr_range)
## +-------+-------+-----------+
## |minimum|maximum|range_width|
## +-------+-------+-----------+
## |    -19|    482|        501|
## +-------+-------+-----------+
```


#### Variance & standard deviation:

Again using `agg`, we compute the variance and standard deviation of `"mths_remng"` with the expressions below. Note that, here, we are computing sample variance and standard deviation (which we could also measure with their respective aliases, `variance` and `stddev`). To measure population variance and standard deviation, we would use `var_pop` and `stddev_pop`, respectively.


```r
mr_var <- agg(df, variance = var(df$mths_remng))  # Sample variance
showDF(mr_var)
## +------------------+
## |          variance|
## +------------------+
## |1260.0411587470085|
## +------------------+

mr_sd <- agg(df, std_dev = sd(df$mths_remng)) # Sample standard deviation
showDF(mr_sd)
## +-----------------+
## |          std_dev|
## +-----------------+
## |35.49705845203245|
## +-----------------+
```


#### Quantiles:

[Insert section on measuring (approx. quantiles) with release of SparkR 2.0.0]


***


### Measures of distribution shape:


#### Skewness:

We can measure the magnitude and direction of skew in the distribution of a numerical column (relative to horizontal symmetry) in a DF by using the operation `skewness` with `agg`, just as we did to measure the `mean`, `variance` and `stddev` of a numerical variable. Below, we measure the `skewness` of `"mths_remng"`:


```r
mr_sk <- agg(df, skewness = skewness(df$mths_remng))
showDF(mr_sk)
## +-------------------+
## |           skewness|
## +-------------------+
## |-2.1817587718789557|
## +-------------------+
```


#### Kurtosis:

Similarly, we can meaure the magnitude of, and how sharp is, the central peak of the distribution of a numerical variable, i.e. the "peakedness" of the distribution, (relative to a standard bell curve) with the `kurtosis` operation. Here, we measure the `kurtosis` of `"mths_remng"`:


```r
mr_kr <- agg(df, kurtosis = kurtosis(df$mths_remng))
showDF(mr_kr)
## +------------------+
## |          kurtosis|
## +------------------+
## |5.2248521022372465|
## +------------------+
```

***


### Measures of dependence:

#### Covariance & correlation:

The actions `cov` and `corr` return the sample covariance and correlation measures of dependency between two DF columns, respectively. Currently, Pearson is the only supported method for calculating correlation. Here we compute the covariance and correlation of `"loan_age"` and `"mths_remng"`. Note that, in saving the covariance and correlation measures, we are not required to first `collect` locally since `cov` and `corr` return values, rather than DFs:


```r
cov_la.mr <- cov(df, "loan_age", "mths_remng")
corr_la.mr <- corr(df, "loan_age", "mths_remng", method = "pearson")
cov_la.mr
## [1] -1233.101
corr_la.mr
## [1] -0.9454057

typeof(cov_la.mr)
## [1] "double"
typeof(corr_la.mr)
## [1] "double"
```

***


## Categorical Data


We can compute descriptive statistics for categorical data using the `groupBy` operation that we used in the Basics II tutorial to compute aggregations of numerical data over groups, as well as operations native to SparkR for this purpose.



***


#### Frequency table:

To create a frequency table for a categorical variable in SparkR, i.e. list the number of observations for each distinct value in a column of strings, we can simply use the `count` transformation with grouped data. Group the data by the categorical variable for which we want to return a frequency table. Here, we create a frequency table for using this approach `"cd_zero_bal"`:


```r
zb_f <- count(groupBy(df, "cd_zero_bal"))
showDF(zb_f)
## +-----------+--------+
## |cd_zero_bal|   count|
## +-----------+--------+
## |    Unknown|12797138|
## |          1|  412432|
## |          3|     969|
## |          6|    1280|
## |          9|    4697|
## +-----------+--------+
```

We could also embed a grouping into an `agg` operation as we saw in the Basics II tutorial to achieve the same frequency table DF, i.e. we could evaluate the expression `agg(groupBy(df, df$cd_zero_bal), count = n(df$cd_zero_bal))`.

#### Relative frequency table:

We could similarly create a DF that consists of a relative frequency table. Here, we reproduce the frequency table from the preceding section, but now including the relative frequency for each distinct string value, as measured by "Percentage":


```r
n <- nrow(df)
zb_rf <- agg(groupBy(df, df$cd_zero_bal), Count = n(df$cd_zero_bal), Percentage = n(df$cd_zero_bal) * (100/n))
showDF(zb_rf)
## +-----------+--------+--------------------+
## |cd_zero_bal|   Count|          Percentage|
## +-----------+--------+--------------------+
## |    Unknown|12797138|   96.82686420536244|
## |          1|  412432|  3.1205803405375514|
## |          3|     969|0.007331735534538754|
## |          6|    1280| 0.00968485189288917|
## |          9|    4697| 0.03553886667257846|
## +-----------+--------+--------------------+
```

#### Contingency table:

Finally, we can create a contingency table with the operation `crosstab`, which returns a data.frame that consists of a contingency table between two categorical columns of a DF. Here, we create and print a contingency table for `"servicer_name"` and `"cd_zero_bal"`:


```r
conting_sn.zb <- crosstab(df, "servicer_name", "cd_zero_bal")
conting_sn.zb
```

Here, is the contingency table, the output of `crosstab`, in a formatted table:


servicer_name_cd_zero_bal                      Unknown        1     3      6      9
-------------------------------------------  ---------  -------  ----  -----  -----
FLAGSTAR BANK, FSB                                 106        4     0      0      0
GMAC MORTGAGE, LLC                               15467     1107     0      0      0
FLAGSTAR CAPITAL MARKETS CORPORATION                55        0     0      0      0
OTHER                                            62772     3518     6      4     10
EVERBANK                                           179        1     0      0      0
JPMORGAN CHASE BANK, NA                          51278     4090     1      3      0
AMTRUST BANK                                       420       18     0      0      0
SUNTRUST MORTGAGE INC.                            1602      159     0      0      0
MATRIX FINANCIAL SERVICES CORPORATION               22        0     0      0      0
WELLS FARGO BANK, N.A.                            1286       37     0      0      0
JPMORGAN CHASE BANK, NATIONAL ASSOCIATION        43417     3484     0     16      0
GREEN TREE SERVICING, LLC                         1404        5     0      0      0
DITECH FINANCIAL LLC                              1099        7     0      0      0
IRWIN MORTGAGE, CORPORATION                         10        3     0      0      0
CITIMORTGAGE, INC.                               24333     1844     0      5      0
QUICKEN LOANS INC.                                   4        0     0      0      0
PHH MORTGAGE CORPORATION                          8333      635     0      0      0
JP MORGAN CHASE BANK, NA                           439        5     0      0      0
OCWEN LOAN SERVICING, LLC                            2        0     0      0      0
FANNIE MAE/SETERUS, INC. AS SUBSERVICER            504        0     0      0      0
BANK OF AMERICA, N.A.                            32394     2309     0      1      0
METLIFE BANK, NA                                   240        4     0      0      0
USAA FEDERAL SAVINGS BANK                         2972      301     0      0      0
NATIONSTAR MORTGAGE, LLC                           261        0     0      0      0
FIRST TENNESSEE BANK, NATIONAL ASSOCIATION       11921      853     0      0      0
FREEDOM MORTGAGE CORP.                               2        0     0      0      0
Unknown                                       12523765   393278   962   1250   4687
U.S. BANK N.A.                                    1608       24     0      0      0
REGIONS BANK                                      3151      239     0      0      0
CITIMORTGAGE ASSET MANAGEMENT, INC.               7897      507     0      1      0
PNC BANK, N.A.                                     195        0     0      0      0

__End of tutorial__ - Next up is [Insert next tutorial]
