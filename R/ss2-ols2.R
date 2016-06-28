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

## Preprocessing data:

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

dat$matr_yr <- year(dat$matr_dt) # Extract year of maturity date of loan as an integer in dat DF
dat$zero_bal_yr <- year(dat$zero_bal_dt) # Extract year loan set to 0 as an integer in dat DF



head(dat)
printSchema(dat) # We now have each DF column in the appropriate dtype

# Drop rows with NAs:

nrow(dat)
dat_ <- dropna(dat)
nrow(dat_)
dat <- dat_
rm(dat_)
cache(dat)

###################################
## (1) Fit a Gaussian GLM model: ##
###################################

# Fit ordinary linear regression 
m1 <- SparkR::glm(act_endg_upb ~ new_int_rt + loan_age + mths_remng + matr_yr + zero_bal_yr, data = dat, family = "gaussian")

     

output <- summary(m1)
coeffs <- output$coefficients[,1]

# Calculate average y value:
act_endg_upb_avg <- collect(agg(dat, act_endg_upb_avg = mean(dat$act_endg_upb)))$act_endg_upb_avg
# Predict fitted values using the DF OLS model -> yields new DF
act_endg_upb_hat <- predict(m1, dat)
cache(act_endg_upb_hat)
head(act_endg_upb_hat) # so you can see what the prediction DF looks like
# Transform the SparkR fitted values DF (yhat2_df) so that it is easier to read and includes squared residuals and squared totals & extract yhat vector (as new DF)
act_endg_upb_hat <- transform(act_endg_upb_hat, sq_res = (act_endg_upb_hat$act_endg_upb - act_endg_upb_hat$prediction)^2, sq_tot = (act_endg_upb_hat$act_endg_upb - act_endg_upb_avg)^2)
act_endg_upb_hat <- transform(act_endg_upb_hat, act_endg_upb_hat = act_endg_upb_hat$prediction)
head(select(act_endg_upb_hat, "act_endg_upb", "act_endg_upb_hat", "sq_res", "sq_tot"))
head(act_endg_upb_hat <- select(act_endg_upb_hat, "act_endg_upb_hat"))

# Compute sum of squared residuals and totals, then use these values to calculate R-squared:
SSR2 <- collect(agg(yhat2_df, SSR2=sum(yhat2_df$sq_res2)))  ##### Note: produces data.frame - get values out of d.f's in order to calculate aRsq and Rsq
SST2 <- collect(agg(yhat2_df, SST2=sum(yhat2_df$sq_res2)))
Rsq2 <- 1-(SSR2/SST2)
p <- 3
N <- nrow(df)
aRsq2 <- 1-(((1-Rsq2)*(N-1))/(N-p-1))


n <- 10
b0 <- rep(0,n)
b1 <- rep(0,n)
b2 <- rep(0,n)
b3 <- rep(0,n)
b4 <- rep(0,n)
b5 <- rep(0,n)
for(i in 1:n){
  model <- SparkR::glm(act_endg_upb ~ new_int_rt + loan_age + mths_remng + matr_yr + zero_bal_yr, data = dat, family = "gaussian")
  b0[i] <- unname(summary(model)$coefficients[,1]["(Intercept)"])
  b1[i] <- unname(summary(model)$coefficients[,1]["new_int_rt"])
  b2[i] <- unname(summary(model)$coefficients[,1]["loan_age"])
  b3[i] <- unname(summary(model)$coefficients[,1]["mths_remng"])
  b4[i] <- unname(summary(model)$coefficients[,1]["matr_yr"])
  b5[i] <- unname(summary(model)$coefficients[,1]["zero_bal_yr"])
}


# Prepare parameter estimate lists above as data.frames to pass into ggplot:
b_ests_ <- data.frame(cbind(b0 = unlist(b0), b1 = unlist(b1), b2 = unlist(b2), b3 = unlist(b3), Iteration = seq(1, n, by = 1)))
b_ests <- melt(b_ests_, id.vars ="Iteration", measure.vars = c("b0", "b1", "b2", "b3"))
names(b_ests) <- cbind("Iteration", "Variable", "Value")


p <- ggplot(data = b_ests, aes(x = Iteration, y = Value, col = Variable), size = 5) + geom_point() + geom_hline(yintercept = unname(coeffs1["(Intercept)"]), linetype = 2) + geom_hline(yintercept = unname(coeffs1["x1"]), linetype = 2) + geom_hline(yintercept = unname(coeffs1["x2"]), linetype = 2) + geom_hline(yintercept = unname(coeffs1["x3"]), linetype = 2) + labs(title = "L.R. Parameters Estimated via L-BFGS")