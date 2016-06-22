###################################################################################
## Social Science Methodologies: Difference-in-differences (Diff-in-diff) Module ##
###################################################################################
## Objective: 
## Operations discussed: 

## Notes: ann overview of the Differences-in-differences method can be found at http://www.nber.org/WNE/lect_10_diffindiffs.pdf and at
## http://eml.berkeley.edu/~webfac/saez/e131_s04/diff.pdf.
## References: the SparkR code outlined below is adapted from the introductory Diff-in-Diff R code posted by Dr. Torres-Reyna at Princeton Unviersity, posted at
## http://www.princeton.edu/~otorres/DID101R.pdf. The data used in this module may be found at http://dss.princeton.edu/training/Panel101.dta.

library(foreign)
library(magrittr)
library(SparkR)

## Initiate SparkContext:
sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", 
                                  spark.driver.memory="1g",
                                  spark.driver.maxResultSize="1g")
                  ,sparkPackages="com.databricks:spark-csv_2.11:1.4.0") ## Load CSV Spark Package
## AWS EMR is using Spark 2.11 so we need the associated version of spark-csv: http://spark-packages.org/package/databricks/spark-csv
## Define Spark executor memory, as well as driver memory and maxResultSize according to cluster configuration

## Initiate SparkRSQL:
sqlContext <- sparkRSQL.init(sc)

## Read in example panel data from AWS S3 as a DataFrame (DF):
data <- read.df(sqlContext, "s3://sparkr-tutorials/DinD_R_ex.csv", header='true', delimiter=",", source="csv", inferSchema='true')
cache(data)
head(data)

###########################################################################################
## (1) Create indicators for countries receiving treatment & time periods for treatment: ##
###########################################################################################

## Create an indicator variable, 'time', identifying the unit of time at which treatment began (here, the unit of time is years and the year in which treatment began is
## 1994). Therefore, 'time' at year 1994, and at subsequent years, is assigned a value of 1 and, for years preceding 1994, is given a value of 0. This indicator variable
## represents the 
data. <- withColumn(data, "trt_time", ifelse(data$year >= 1994, 1, 0)) # Create a new DF, 'data_', with the variable 'time' appended; note that function format given by ifelse(test, yes, no)
## Stata: gen trt_time = (year >= 1994) & !missing(year)


## Create another indicator variable, 'treatment', indicating the within sample group exposed to the treatment. Here, countries E, F and G were received the treatment, so the
## 'treatment' variable value for observations in these countries is set equal to 1, while the 'treatment' value for observations within countries A, B, C and D is set equal
## to 0.
data_ <- withColumn(data., "trt_region", ifelse(data$country == "E" | data$country == "F" | data$country == "G", 1, 0))
cache(data_)
unpersist(data)
## Stata: gen trt_region = (country > 4) & !missing(country)


## Rename updated DFs to 'data':
head(data_) # Check the columns of updated DF to confirm DF updated properly
data <- data_ # Rename 'data.' to 'data'
rm(data.)
rm(data_)
head(data)
## Stata: rename data_ data
## Stata: drop data_ data.
## Stata: list _all in 1/5


##############################################################################################
## (2) Manually compute the treatment effect (i.e. take the difference of the differences): ##
##############################################################################################

## To mimic an experimental design with observational data, exploiting an observed natural experiment, the diff-in-diff method measures the effect of a treatment on an
## outcome by comparing the average change over time in the response variable for the treatment group and compares this to the average change over time for the control group.
## This diff-in-diff estimator can be computed manually, as we do immediately below, or it can be computed as the parameter estimate of the treatment indicator in a linear
## model, which we outline futher below.

## Compute the four (4) measurements required to calculate the diff-in-diff estimator:
a <- collect(select(data[data$trt_time == 0 & data$trt_region == 0], mean(data$y)))
b <- collect(select(data[data$trt_time == 0 & data$trt_region == 1], mean(data$y)))
c <- collect(select(data[data$trt_time == 1 & data$trt_region == 0], mean(data$y)))
d <- collect(select(data[data$trt_time == 1 & data$trt_region == 1], mean(data$y)))

## Now, manually calculate the diff-in-diff estimator; as you can see, we are literally calculating the difference between the differences:
did_est <- (d-c)-(b-a)
did_est


############################################################
## (3) Run a simple difference-in-differences regression: ##
############################################################

## As previously stated, the parameter estimation of the interaction term 'trt_time:trt_region' included in the below linear model is the diff-in-diff estimator. This can be
## verified by comparing the 'did_est' value, which we calculated in Section (2), with the parameter estimation for 'trt_time:trt_region'. Note that the values are equal,
## and that the interaction term 'trt_time:trt_region' is a binary variable that indicates treatment status.
m1 <- glm(y ~ trt_region + trt_time + trt_region:trt_time, data = data, family = "gaussian")
summary(m1)


#########################################
## (4) Check diff-in-diff assumptions: ##
#########################################

## Is a line graph possible in SparkR? Would be nice to be able to provide visualization of parallel trend assumption - traditionally necessary for causality justification!

## Include leads in regression to measure whether or not there is any evidence of an anticipatory effect (if there is no effect, then leads should be approx 0 - this supports parallel trend assumption)
## Include lags to measure direction and maginitude of effect following initatial treatment exposure
## >>> Create lead and lag and then re-run glm

## Could perhaps run an F-test on the difference in mean(y) across the treatment and control groups (here, countries) for the pre-treatment years - if parallel trend
## asumption is valid, this F-test should yield stat. insignificant result; Note: this is a necessary condition, but not a sufficient condition for validation of parallel
## trend assumption since statistical insignificance of F-test results could be due to low test power