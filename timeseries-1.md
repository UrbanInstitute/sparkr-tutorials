# Time Series I: Working with the Date Datatype & Resampling a DataFrame
Sarah Armstrong, Urban Institute  
July 12, 2016  




**Objective**: In this tutorial, we discuss how to perform several essential time series operations with SparkR. In particular, we discuss how to:

* Identify and parse date datatype (dtype) DF columns,
* Compute relative dates based on a specified increment of time,
* Extract and modify components of a date dtype column and
* Resample a time series DF to a particular unit of time frequency

**SparkR/R Operations Discussed**: `unix_timestamp`, `cast`, `withColumn`, `to_date`, `last_day`, `next_day`, `add_months`, `date_add`, `date_sub`, `weekofyear`, `dayofyear`, `dayofmonth`, `datediff`, `months_between`, `year`, `month`, `hour`, `minute`, `second`, `agg`, `groupBy`, `mean`

***

:heavy_exclamation_mark: **Warning**: Before beginning this tutorial, please visit the SparkR Tutorials README file (found [here](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/README.md)) in order to load the SparkR library and subsequently initiate your SparkR and SparkR SQL contexts.



You can confirm that you successfully initiated these contexts by looking at the global environment of RStudio. Only proceed if you can see `sc` and `sqlContext` listed as values in the global environment or RStudio.

***

**Read in initial data as DF**: Throughout this tutorial, we will use the loan performance example dataset that we exported at the conclusion of the [SparkR Basics I](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-1.md) tutorial.


```r
df <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header='false', inferSchema='true')
cache(df)
```

_Note_: documentation for the quarterly loan performance data can be found at http://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html.

***


### Converting a DataFrame column to 'date' dtype:


As we saw in previous tutorials, there are several columns in our dataset that list dates which are helpful in determining loan performance. We will specifically consider the following columns throughout this tutorial:

* `"period"` (Monthly Reporting Period): The month and year that pertain to the servicer’s cut-off period for mortgage loan information
* `"dt_matr"`(Maturity Date): The month and year in which a mortgage loan is scheduled to be paid in full as defined in the mortgage loan documents 
* `"dt_zero_bal"`(Zero Balance Effective Date): Date on which the mortgage loan balance was reduced to zero

Let's begin by reviewing the format and dtype that `read.df` infers our date columns as. Note that each of our three (3) date columns were read in as strings:


```r
str(df)
## 'DataFrame': 14 variables:
##  $ loan_id      : num 100007365142 100007365142 100007365142 100007365142 100007365142 100007365142
##  $ period       : chr "01/01/2000" "02/01/2000" "03/01/2000" "04/01/2000" "05/01/2000" "06/01/2000"
##  $ servicer_name: chr "" "" "" "" "" ""
##  $ new_int_rt   : num 8 8 8 8 8 8
##  $ act_endg_upb : num NA NA NA NA NA NA
##  $ loan_age     : int 0 1 2 3 4 5
##  $ mths_remng   : int 360 359 358 357 356 355
##  $ aj_mths_remng: int 359 358 357 356 355 355
##  $ dt_matr      : chr "01/2030" "01/2030" "01/2030" "01/2030" "01/2030" "01/2030"
##  $ cd_msa       : int 0 0 0 0 0 0
##  $ delq_sts     : chr "0" "0" "0" "0" "0" "0"
##  $ flag_mod     : chr "N" "N" "N" "N" "N" "N"
##  $ cd_zero_bal  : int NA NA NA NA NA NA
##  $ dt_zero_bal  : chr "" "" "" "" "" ""
```

While we could parse the date strings into separate year, month and day integer dtype columns, converting the columns to date dtype allows us to utilize the datetime functions available in SparkR, as well as employ the time window API in SparkR SQL, which we will discuss in a subsequent time series tutorial.


We can convert `"period"`, `"matr_dt"` and `"dt_zero_bal"` to date dtype with the following expressions:


```r
# `period`
period_uts <- unix_timestamp(df$period, 'MM/dd/yyyy')	# 1. Gets current Unix timestamp in seconds
period_ts <- cast(period_uts, 'timestamp')	# 2. Casts Unix timestamp `period_uts` as timestamp
period_dt <- cast(period_ts, 'date')	# 3. Casts timestamp `period_ts` as date dtype
df <- withColumn(df, 'p_dt', period_dt)	# 4. Add date dtype column `period_dt` to `df`

# `dt_matr`
matr_uts <- unix_timestamp(df$dt_matr, 'MM/yyyy')
matr_ts <- cast(matr_uts, 'timestamp')
matr_dt <- cast(matr_ts, 'date')
df <- withColumn(df, 'mtr_dt', matr_dt)

# `dt_zero_bal`
zero_bal_uts <- unix_timestamp(df$dt_zero_bal, 'MM/yyyy')
zero_bal_ts <- cast(zero_bal_uts, 'timestamp')
zero_bal_dt <- cast(zero_bal_ts, 'date')
df <- withColumn(df, 'zb_dt', zero_bal_dt)
```

Note that the string entries of these date DF columns are written in the formats `'MM/dd/yyyy'` and `'MM/yyyy'`. While SparkR is able to easily read a date string when it is in the default format, `'yyyy-mm-dd'`, additional steps are required for string to date conversions when the DF column entries are in a format other than the default. In order to create `"p_dt"` from `"period"`, for example, we must:

1. Define the Unix timestamp for the date string, specifying the date format that the string assumes (here, we specify `'MM/dd/yyyy'`),
2. Use the `cast` operation to convert the Unix timestamp of the string to `'timestamp'` dtype,
3. Similarly recast the `'timestamp'` form to `'date'` dtype and
4. Append the new date dtype `"p_dt"` column to `df` using the `withColumn` operation.

We similarly create date dtype columns using `"dt_matr"` and `"dt_zero_bal"`. If the date string entries of these columns were in the default format, converting to date dtype would straightforward. If `"period"` was in the format `'yyyy-mm-dd'`, for example, we would be able to append `df` with a date dtype column using a simple `withColumn`/`cast` expression: `df <- withColumn(df, 'p_dt', cast(df$period, 'date'))`. We could also directly convert `"period"` to date dtype using the `to_date` operation: `df$period <- to_date(df$period)`.


If we are lucky enough that our date entires are in the default format, then dtype conversion is simple and we should use either the `withColumn`/`cast` or `to_date` expressions given above. Otherwise, the longer conversion process is required. Note that, if we are maintaining our own dataset that we will use SparkR to analyze, adopting the default date format at the start will make working with date values during analysis much easier. 


Now that we've appended our date dtype columns to `df`, let's again look at the DF and compare the date dtype values with their associated date string values:


```r
str(df)
## 'DataFrame': 17 variables:
##  $ loan_id      : num 100007365142 100007365142 100007365142 100007365142 100007365142 100007365142
##  $ period       : chr "01/01/2000" "02/01/2000" "03/01/2000" "04/01/2000" "05/01/2000" "06/01/2000"
##  $ servicer_name: chr "" "" "" "" "" ""
##  $ new_int_rt   : num 8 8 8 8 8 8
##  $ act_endg_upb : num NA NA NA NA NA NA
##  $ loan_age     : int 0 1 2 3 4 5
##  $ mths_remng   : int 360 359 358 357 356 355
##  $ aj_mths_remng: int 359 358 357 356 355 355
##  $ dt_matr      : chr "01/2030" "01/2030" "01/2030" "01/2030" "01/2030" "01/2030"
##  $ cd_msa       : int 0 0 0 0 0 0
##  $ delq_sts     : chr "0" "0" "0" "0" "0" "0"
##  $ flag_mod     : chr "N" "N" "N" "N" "N" "N"
##  $ cd_zero_bal  : int NA NA NA NA NA NA
##  $ dt_zero_bal  : chr "" "" "" "" "" ""
##  $ p_dt         : Date 2000-01-01 2000-02-01 2000-03-01 2000-04-01 2000-05-01 2000-06-01
##  $ mtr_dt       : Date 2030-01-01 2030-01-01 2030-01-01 2030-01-01 2030-01-01 2030-01-01
##  $ zb_dt        : Date NA NA NA NA NA NA
```

Note that the `"zb_dt"` entries corresponding to the missing date entries in `"dt_zero_bal"`, which were empty strings, are now nulls.

***


### Compute relative dates and measures based on a specified unit of time:

As we mentioned earlier, converting date strings to date dtype allows us to utilize SparkR datetime operations. In this section, we'll discuss several SparkR operations that return

* Date dtype columns that list dates relative to a specified date DF column
* Integer or numerical dtype columns that list measures of time relative to a specified date DF column

For convenience, we will review these operations using the `df_dt` DF, which includes only the date columns `"p_dt"` and `"mtr_dt"`, which we created in the preceding section:


```r
cols_dt <- c("p_dt", "mtr_dt")
df_dt <- select(df, cols_dt)
```


#### Relative dates:

SparkR operations that return relative date DF columns include:

* `last_day`: Returns the last day of the month which the given date belongs to (e.g. inputting "2013-07-27" returns "2013-07-31")
* `next_day`: Returns the first date which is later than the value of the date column that is on the specified day of the week
* `add_months`: Returns the date that is `'numMonths'` after `'startDate'`
* `date_add`: Returns the date that is `'days'` days after `'start'`
* `date_sub`: Returns the date that is `'days'` days before `'start'`

Below, we create relative date columns (defining `"p_dt"` as the input date) using each of these operations and `withColumn`:


```r
df_dt1 <- withColumn(df_dt, 'p_ld', last_day(df_dt$p_dt))
df_dt1 <- withColumn(df_dt1, 'p_nd', next_day(df_dt$p_dt, "Sunday"))
df_dt1 <- withColumn(df_dt1, 'p_addm', add_months(df_dt$p_dt, 1)) # 'startDate'="pdt", 'numMonths'=1
df_dt1 <- withColumn(df_dt1, 'p_dtadd', date_add(df_dt$p_dt, 1)) # 'start'="pdt", 'days'=1
df_dt1 <- withColumn(df_dt1, 'p_dtsub', date_sub(df_dt$p_dt, 1)) # 'start'="pdt", 'days'=1
str(df_dt1)
## 'DataFrame': 7 variables:
##  $ p_dt   : Date 2000-01-01 2000-02-01 2000-03-01 2000-04-01 2000-05-01 2000-06-01
##  $ mtr_dt : Date 2030-01-01 2030-01-01 2030-01-01 2030-01-01 2030-01-01 2030-01-01
##  $ p_ld   : Date 2000-01-31 2000-02-29 2000-03-31 2000-04-30 2000-05-31 2000-06-30
##  $ p_nd   : Date 2000-01-02 2000-02-06 2000-03-05 2000-04-02 2000-05-07 2000-06-04
##  $ p_addm : Date 2000-02-01 2000-03-01 2000-04-01 2000-05-01 2000-06-01 2000-07-01
##  $ p_dtadd: Date 2000-01-02 2000-02-02 2000-03-02 2000-04-02 2000-05-02 2000-06-02
##  $ p_dtsub: Date 1999-12-31 2000-01-31 2000-02-29 2000-03-31 2000-04-30 2000-05-31
```

#### Relative measures of time:

SparkR operations that return DF columns which list relative measures of time include:

* `weekofyear`: Extracts the week number as an integer from a given date
* `dayofyear`: Extracts the day of the year as an integer from a given date
* `dayofmonth`: Extracts the day of the month as an integer from a given date
* `datediff`: Returns number of months between dates 'date1' and 'date2'
* `months_between`: Returns the number of days from 'start' to 'end'

Here, we use `"p_dt"` and `"mtr_dt"` as inputs in the above operations. We again use `withColumn` do append the new columns to a DF:


```r
df_dt2 <- withColumn(df_dt, 'p_woy', weekofyear(df_dt$p_dt))
df_dt2 <- withColumn(df_dt2, 'p_doy', dayofyear(df_dt$p_dt))
df_dt2 <- withColumn(df_dt2, 'p_dom', dayofmonth(df_dt$p_dt))
df_dt2 <- withColumn(df_dt2, 'mbtw_p.mtr', months_between(df_dt$mtr_dt, df_dt$p_dt)) # 'date1'=p_dt, 'date2'=mtr_dt
df_dt2 <- withColumn(df_dt2, 'dbtw_p.mtr', datediff(df_dt$mtr_dt, df_dt$p_dt)) # 'start'=p_dt, 'end'=mtr_dt
str(df_dt2)
## 'DataFrame': 7 variables:
##  $ p_dt      : Date 2000-01-01 2000-02-01 2000-03-01 2000-04-01 2000-05-01 2000-06-01
##  $ mtr_dt    : Date 2030-01-01 2030-01-01 2030-01-01 2030-01-01 2030-01-01 2030-01-01
##  $ p_woy     : int 52 5 9 13 18 22
##  $ p_doy     : int 1 32 61 92 122 153
##  $ p_dom     : int 1 1 1 1 1 1
##  $ mbtw_p.mtr: num 360 359 358 357 356 355
##  $ dbtw_p.mtr: int 10958 10927 10898 10867 10837 10806
```

Note that operations that consider two different dates are sensitive to how we specify column ordering in the operation expression. For example, if we incorrectly define `"p_dt"` as `date2` and `"mtr_dt"` as `date1`, `"mbtw_p.mtr"` will consist of negative values. Similarly, `datediff` will return negative values if `start` and `end` are misspecified.

***


### Extract components of a date dtype column:

There are also datetime functions supported by SparkR that allow us to extract individual components of a date dtype column and return these as integers. Below, we use the `year` and `month` operations to create integer dtype columns for each of our date columns. Similar functions include `hour`, `minute` and `second`.


```r
# Year and month values for `"period_dt"`
df <- withColumn(df, 'p_yr', year(df$p_dt))
df <- withColumn(df, "p_m", month(df$p_dt))

# Year value for `"matr_dt"`
df <- withColumn(df, 'mtr_yr', year(df$mtr_dt))
df <- withColumn(df, "mtr_m", month(df$mtr_dt))

# Year value for `"zero_bal_dt"`
df <- withColumn(df, 'zb_yr', year(df$zb_dt))
df <- withColumn(df, "zb_m", month(df$zb_dt))
```

We can see that each of the above expressions returns a column of integer values representing the requested date value:


```r
str(df)
## 'DataFrame': 23 variables:
##  $ loan_id      : num 100007365142 100007365142 100007365142 100007365142 100007365142 100007365142
##  $ period       : chr "01/01/2000" "02/01/2000" "03/01/2000" "04/01/2000" "05/01/2000" "06/01/2000"
##  $ servicer_name: chr "" "" "" "" "" ""
##  $ new_int_rt   : num 8 8 8 8 8 8
##  $ act_endg_upb : num NA NA NA NA NA NA
##  $ loan_age     : int 0 1 2 3 4 5
##  $ mths_remng   : int 360 359 358 357 356 355
##  $ aj_mths_remng: int 359 358 357 356 355 355
##  $ dt_matr      : chr "01/2030" "01/2030" "01/2030" "01/2030" "01/2030" "01/2030"
##  $ cd_msa       : int 0 0 0 0 0 0
##  $ delq_sts     : chr "0" "0" "0" "0" "0" "0"
##  $ flag_mod     : chr "N" "N" "N" "N" "N" "N"
##  $ cd_zero_bal  : int NA NA NA NA NA NA
##  $ dt_zero_bal  : chr "" "" "" "" "" ""
##  $ p_dt         : Date 2000-01-01 2000-02-01 2000-03-01 2000-04-01 2000-05-01 2000-06-01
##  $ mtr_dt       : Date 2030-01-01 2030-01-01 2030-01-01 2030-01-01 2030-01-01 2030-01-01
##  $ zb_dt        : Date NA NA NA NA NA NA
##  $ p_yr         : int 2000 2000 2000 2000 2000 2000
##  $ p_m          : int 1 2 3 4 5 6
##  $ mtr_yr       : int 2030 2030 2030 2030 2030 2030
##  $ mtr_m        : int 1 1 1 1 1 1
##  $ zb_yr        : int NA NA NA NA NA NA
##  $ zb_m         : int NA NA NA NA NA NA
```

Note that the `NA` entries of `"zb_dt"` result in `NA` values for `"zb_yr"` and `"zb_m"`.

***


### Resample a time series DF to a particular unit of time frequency

When working with time series data, we are frequently required to resample data to a different time frequency. Combing the `agg` and `groupBy` operations, as we saw in the [SparkR Basics II](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md) tutorial, is a convenient strategy for accomplishing this in SparkR. We create a new DF, `dat`, that only includes columns of numerical, integer and date dtype to use in our resampling examples:




```r
cols <- c("p_yr", "p_m", "mtr_yr", "mtr_m", "zb_yr", "zb_m", "new_int_rt", "act_endg_upb", "loan_age", "mths_remng", "aj_mths_remng")
dat <- select(df, cols)

unpersist(df)
## DataFrame[loan_id:bigint, period:string, servicer_name:string, new_int_rt:double, act_endg_upb:double, loan_age:int, mths_remng:int, aj_mths_remng:int, dt_matr:string, cd_msa:int, delq_sts:string, flag_mod:string, cd_zero_bal:int, dt_zero_bal:string, p_dt:date, mtr_dt:date, zb_dt:date, p_yr:int, p_m:int, mtr_yr:int, mtr_m:int, zb_yr:int, zb_m:int]
cache(dat)
## DataFrame[p_yr:int, p_m:int, mtr_yr:int, mtr_m:int, zb_yr:int, zb_m:int, new_int_rt:double, act_endg_upb:double, loan_age:int, mths_remng:int, aj_mths_remng:int]

head(dat)
##   p_yr p_m mtr_yr mtr_m zb_yr zb_m new_int_rt act_endg_upb loan_age
## 1 2000   1   2030     1    NA   NA          8           NA        0
## 2 2000   2   2030     1    NA   NA          8           NA        1
## 3 2000   3   2030     1    NA   NA          8           NA        2
## 4 2000   4   2030     1    NA   NA          8           NA        3
## 5 2000   5   2030     1    NA   NA          8           NA        4
## 6 2000   6   2030     1    NA   NA          8           NA        5
##   mths_remng aj_mths_remng
## 1        360           359
## 2        359           358
## 3        358           357
## 4        357           356
## 5        356           355
## 6        355           355
```

Note that, in our loan-level data, each row represents a unique loan (each made distinct by the `"loan_id"` column in `df`) and its corresponding characteristics such as `"loan_age"` and `"mths_remng"`. Note that `dat` is simply a subsetted DF of `df` and, therefore, also refers to loan-level data. While we can resample the data over distinct values of any of the columns in `dat`, we will resample the loan-level data as aggregations of the DF columns by units of time since we are working with time series data. Below, we aggregate the columns of `dat` (taking the mean of the column entries) by `"p_yr"`, and then by `"p_yr"` and `"p_m"`:


```r
# Resample by "period_yr"
dat1 <- agg(groupBy(dat, dat$p_yr), p_m = mean(dat$p_m), mtr_yr = mean(dat$mtr_yr), zb_yr = mean(dat$zb_yr), 
            new_int_rt = mean(dat$new_int_rt), act_endg_upb = mean(dat$act_endg_upb), loan_age = mean(dat$loan_age), 
            mths_remng = mean(dat$mths_remng), aj_mths_remng = mean(dat$aj_mths_remng))
head(dat1)
##   p_yr      p_m   mtr_yr zb_yr new_int_rt act_endg_upb  loan_age
## 1 2000 7.589687 2029.872  2000   8.184620    124417.36  4.852425
## 2 2001 5.959435 2029.886  2001   8.178165    119181.03 14.978654
## 3 2002 5.892121 2029.866  2002   8.125896    106678.68 27.143671
## 4 2003 5.657919 2029.868  2003   8.125598     94280.04 38.963491
## 5 2004 6.024530 2029.878  2004   8.094487     84831.20 51.341908
## 6 2005 6.161613 2029.889  2005   8.061325     79880.03 63.497287
##   mths_remng aj_mths_remng
## 1   355.0996      350.8954
## 2   344.9771      323.5532
## 3   332.8287      305.9929
## 4   321.0887      288.5209
## 5   308.8654      283.2608
## 6   296.8613      272.9676

# Resample by "period_yr" and "period_m"
dat2 <- agg(groupBy(dat, dat$p_yr, dat$p_m), mtr_yr = mean(dat$mtr_yr), zb_yr = mean(dat$zb_yr), 
            new_int_rt = mean(dat$new_int_rt), act_endg_upb = mean(dat$act_endg_upb), loan_age = mean(dat$loan_age), 
            mths_remng = mean(dat$mths_remng), aj_mths_remng = mean(dat$aj_mths_remng))
head(arrange(dat2, dat2$p_yr, dat2$p_m), 15)	# Arrange the first 15 rows of `dat2` by ascending `period_yr` and `period_m` values
##    p_yr p_m   mtr_yr    zb_yr new_int_rt act_endg_upb   loan_age
## 1  2000   1 2029.598       NA   7.920325           NA  0.5187644
## 2  2000   2 2029.725 2000.000   7.972492     132853.3  1.1355798
## 3  2000   3 2029.793 2000.000   8.064929     132700.5  1.6506320
## 4  2000   4 2029.839 2000.000   8.141130     135745.8  2.0978267
## 5  2000   5 2029.872 2000.000   8.186453     135012.6  2.5041818
## 6  2000   6 2029.895 2000.000   8.217745     130102.5  2.9146392
## 7  2000   7 2029.895 2000.000   8.216798     125682.1  3.9119169
## 8  2000   8 2029.895 2000.000   8.215426     125482.9  4.9118220
## 9  2000   9 2029.895 2000.000   8.213992     124377.1  5.9116439
## 10 2000  10 2029.895 2000.000   8.212640     123973.4  6.9117313
## 11 2000  11 2029.895 2000.000   8.211105     124095.1  7.9122057
## 12 2000  12 2029.895 2000.000   8.209340     124294.5  8.9136587
## 13 2001   1 2029.894 2000.999   8.207565     124010.5  9.9154489
## 14 2001   2 2029.894 2001.000   8.204004     123407.7 10.9250416
## 15 2001   3 2029.892 2001.000   8.198123     122390.5 11.9460216
##    mths_remng aj_mths_remng
## 1    359.4432      358.5354
## 2    358.8161      357.1092
## 3    358.2956      356.0617
## 4    357.8518      355.3690
## 5    357.4464      354.5026
## 6    357.0377      353.8146
## 7    356.0404      352.3075
## 8    355.0404      350.6185
## 9    354.0408      349.3763
## 10   353.0406      347.6863
## 11   352.0405      346.0224
## 12   351.0395      344.4075
## 13   350.0376      338.9795
## 14   349.0281      331.7495
## 15   348.0077      326.2878
```

Note that we specify the list of DF columns that we want to resample on by including it in `groupBy`. Here, we aggregated by taking the mean of each column. However, we could use any of the aggregation functions that `agg` is able to interpret (listed in [SparkR Basics II](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/sparkr-basics-2.md) tutorial) and that is inline with the resampling that we are trying to achieve.


We could resample to any unit of time that we can extract from a date column, e.g. `year`, `month`, `day`, `hour`, `minute`, `second`. Furthermore, could have skipped the step of creating separate year- and month-level date columns - instead, we could have embedded the datetime functions directly in the `agg` expression. The following expression creates a DF that is equivalent to `dat1` in the preceding example:


```r
df2 <- agg(groupBy(df, year(df$p_dt)), p_m = mean(month(df$p_dt)), mtr_yr = mean(year(df$mtr_dt)), 
           zb_yr = mean(month(df$mtr_dt)), new_int_rt = mean(df$new_int_rt), act_endg_upb = mean(df$act_endg_upb), 
           loan_age = mean(df$loan_age), mths_remng = mean(df$mths_remng), aj_mths_remng = mean(df$aj_mths_remng))
```


__End of tutorial__ - Next up is [Insert next tutorial]
