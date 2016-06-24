############################################################################
## Social Science Methodologies: Generalized Linear Models (GLM) Module 2 ##
############################################################################
## Objective: 
## Operations discussed: glm

library(SparkR)

## Initiate SparkContext:

sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", 
                                  spark.driver.memory="1g",
                                  spark.driver.maxResultSize="1g")
                  ,sparkPackages="com.databricks:spark-csv_2.11:1.4.0") # Load CSV Spark Package

## AWS EMR is using Spark 2.11 so we need the associated version of spark-csv: http://spark-packages.org/package/databricks/spark-csv
## Define Spark executor memory, as well as driver memory and maxResultSize according to cluster configuration

## Initiate SparkRSQL:

sqlContext <- sparkRSQL.init(sc)

## Read in loan performance example data as DataFrame (DF) 'dat':

dat <- read.df(sqlContext, "s3://sparkr-tutorials/hfpc_ex", header='false', inferSchema='true')
cache(dat)
columns(dat)
## > columns(dat)
##  [1] "loan_id"       "period"        "servicer_name" "new_int_rt"    "act_endg_upb"  "loan_age"     
##  [7] "mths_remng"    "aj_mths_remng" "dt_matr"       "cd_msa"        "delq_sts"      "flag_mod"     
## [13] "cd_zero_bal"   "dt_zero_bal" 

## 'loan_id' (Loan Identifier): A unique identifier for the mortgage loan
## 'period' (Monthly Reporting Period): The month and year that pertain to the servicer’s cut-off period for mortgage loan information
## 'servicer_name' (Servicer Name): the name of the entity that serves as the primary servicer of the mortgage loan
## 'new_int_rt' (Current Interest Rate): The interest rate on a mortgage loan in effect for the periodic installment due
## 'act_endg_upb' (Current Actual Unpaid Principal Balance (UPB)): The actual outstanding unpaid principal balance of the mortgage loan (for liquidated loans, the unpaid
## principal balance of the mortgage loan at the time of liquidation)
## 'loan_age' (Loan Age): The number of calendar months since the first full month the mortgage loan accrues interest
## 'mths_remng' (Remaining Months to Maturity): The number of calendar months remaining until the borrower is expected to pay the mortgage loan in full 
## 'aj_mths_remng' (Adjusted Remaining Months To Maturity): the number of calendar months remaining until the borrower is expected to pay the mortgage loan in full
## 'dt_matr' (Maturity Date): The month and year in which a mortgage loan is scheduled to be paid in full as defined in the mortgage loan documents
## 'cd_msa' (Metropolitan Statistical Area (MSA)): The numeric Metropolitan Statistical Area Code for the property securing the mortgage loan
## 'delq_sts' (Current Loan Delinquent Status): The number of days, represented in months, the obligor is delinquent as determined by the governing mortgage documents
## 'flag_mod' (Modification Flag): An indicator that denotes if the mortgage loan has been modified
## 'cd_zero_bal' (Zero Balance Code): A code indicating the reason the mortgage loan's balance was reduced to zero
## 'dt_zero_bal' (Zero Balance Effective Date): Date on which the mortgage loan balance was reduced to zero

## Print the schema for the DF to see if the data types specifications in the schema make sense, given the variable descriptions above:

printSchema(dat)
## > printSchema(dat)
## root
##  |-- loan_id: long (nullable = true)
##  |-- period: string (nullable = true)		# Should be recast as a 'date'
##  |-- servicer_name: string (nullable = true)
##  |-- new_int_rt: double (nullable = true)
##  |-- act_endg_upb: double (nullable = true)
##  |-- loan_age: integer (nullable = true)
##  |-- mths_remng: integer (nullable = true)
##  |-- aj_mths_remng: integer (nullable = true)
##  |-- dt_matr: string (nullable = true)		# Should be recast as a 'date'
##  |-- cd_msa: integer (nullable = true)		# Should be recast as a 'string'
##  |-- delq_sts: string (nullable = true)
##  |-- flag_mod: string (nullable = true)
##  |-- cd_zero_bal: integer (nullable = true)		# Should be recast as a 'string'
##  |-- dt_zero_bal: string (nullable = true)		# Should be recast as a 'date'

# Cast each of the columns noted above into the correct dtype before proceeding with specifying glms

period_dt <- cast(cast(unix_timestamp(dat$period, 'dd/MM/yyyy'), 'timestamp'), 'date')
dat <- withColumn(dat, 'period_dt', period_dt) # Note that we collapse this into a single step for subsequent casts to date dtype
dat$period <- NULL # Drop string form of period; below, we continue to drop string forms of date dtype columns

dat <- withColumn(dat, 'matr_dt', cast(cast(unix_timestamp(dat$dt_matr, 'MM/yyyy'), 'timestamp'), 'date'))
dat$dt_matr <- NULL

dat$cd_msa <- cast(dat$cd_msa, 'string') # We do not need to drop `cd_msa` since we can directly recast this column as a string

dat$cd_zero_bal <- cast(dat$cd_zero_bal, 'string')

dat <- withColumn(dat, 'zero_bal_dt', cast(cast(unix_timestamp(dat$dt_zero_bal, 'MM/yyyy'), 'timestamp'), 'date'))
dat$dt_zero_bal <- NULL

head(dat)
printSchema(dat) # We now have each DF column in the appropriate dtype

###################################
## (1) Fit a Gaussian GLM model: ##
###################################

# Fit a gaussian GLM model over the 
m1 <- glm(act_endg_upb ~ servicer_name + new_int_rt + mths_remng + aj_mths_remng + cd_msa + delq_sts + flag_mod, data = dat, family = "gaussian")

summary(m1)
# print summary here          

# Make predictions based on the model:
pred1 <- predict(m1, newData = dat)
head(select(predictions, "act_endg_upb", "pred1"))
# print predictions and actual act_endg_upb values here

###################################
## (1) Fit a Binomial GLM model: ##
###################################

# Create indicator variable from DF


# Fit a binomial GLM model over the dataset.
m2 <- glm(y ~ x, data = dat, family = "binomial")

summary(m2)

pred2 <- predict(m2, newData = dat)
head(select(predictions, "XXXX", "pred2"))
