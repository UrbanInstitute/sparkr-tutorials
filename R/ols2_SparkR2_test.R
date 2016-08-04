# Confirm that SPARK_HOME is set in environment: set SPARK_HOME to be equal to "/home/spark"
# if the size of the elements of SPARK_HOME are less than 1:
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/home/spark")
}

# Load the SparkR package
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

# Call the SparkR session
sparkR.session(sparkPackages="com.databricks:spark-csv_2.10:1.4.0")

# Load data as DataFrame
dat <- read.df("s3://sparkr-tutorials/hfpc_ex", header = "false", inferSchema = "true")
cache(dat)

# Recast variables as needed
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
dat$zero_bal_yr <- year(dat$zero_bal_dt)

# Drop nulls
list <- list("act_endg_upb", "new_int_rt", "loan_age", "mths_remng", "matr_yr", "zero_bal_yr")
dat <- dropna(dat, cols = list)
nrow(dat)


# Fit Gaussian family GLM with identity link
glm.gauss <- spark.glm(dat, act_endg_upb ~ new_int_rt + loan_age + mths_remng + matr_yr + zero_bal_yr, family = "gaussian")

# Save model summary and outputs
output <- summary(glm.gauss)
coeffs <- output$coefficients[,1]

# Calculate average y value:
act_endg_upb_avg <- collect(agg(dat, act_endg_upb_avg = mean(dat$act_endg_upb)))$act_endg_upb_avg

# Predict fitted values using the DF OLS model -> yields new DF
dat_pred <- predict(glm.gauss, dat)
head(dat_pred) # so you can see what the prediction DF looks like

# Transform the SparkR fitted values DF (dat_pred) so that it is easier to read and includes squared residuals and squared totals & extract yhat vector (as new DF)
dat_pred <- transform(dat_pred, sq_res = (dat_pred$act_endg_upb - dat_pred$prediction)^2, sq_tot = (dat_pred$act_endg_upb - act_endg_upb_avg)^2)
dat_pred <- transform(dat_pred, act_endg_upb_hat = dat_pred$prediction)
head(dat_pred2 <- select(dat_pred, "act_endg_upb", "act_endg_upb_hat", "sq_res", "sq_tot"))
head(act_endg_upb_hat <- select(dat_pred2, "act_endg_upb_hat"))

# Compute sum of squared residuals and totals, then use these values to calculate R-squared:
SSR <- collect(agg(dat_pred2, SSR = sum(dat_pred2$sq_res)))  ##### Note: produces data.frame - get values out of d.f's in order to calculate aRsq and Rsq
SST <- collect(agg(dat_pred2, SST = sum(dat_pred2$sq_tot)))
Rsq2 <- 1-(SSR[[1]]/SST[[1]])
p <- 5
N <- nrow(dat)
aRsq2 <- Rsq2 - (1 - Rsq2)*((p - 1)/(N - p))

# Compare iterations of spark.glm outputs

n <- 10
b0 <- rep(0,n)
b1 <- rep(0,n)
b2 <- rep(0,n)
b3 <- rep(0,n)
b4 <- rep(0,n)
b5 <- rep(0,n)
for(i in 1:n){
  model <- spark.glm(dat, act_endg_upb ~ new_int_rt + loan_age + mths_remng + matr_yr + zero_bal_yr, family = "gaussian")
  b0[i] <- unname(summary(model)$coefficients[,1]["(Intercept)"])
  b1[i] <- unname(summary(model)$coefficients[,1]["new_int_rt"])
  b2[i] <- unname(summary(model)$coefficients[,1]["loan_age"])
  b3[i] <- unname(summary(model)$coefficients[,1]["mths_remng"])
  b4[i] <- unname(summary(model)$coefficients[,1]["matr_yr"])
  b5[i] <- unname(summary(model)$coefficients[,1]["zero_bal_yr"])
}

# Prepare parameter estimate lists above as data.frames to pass into ggplot:
library(reshape2)
library(ggplot2)
b_ests_ <- data.frame(cbind(b0 = unlist(b0), b1 = unlist(b1), b2 = unlist(b2), b3 = unlist(b3), b4 = unlist(b4), b5 = unlist(b5), 
                            Iteration = seq(1, n, by = 1)))
b_ests <- melt(b_ests_, id.vars ="Iteration", measure.vars = c("b0", "b1", "b2", "b3", "b4", "b5"))
names(b_ests) <- cbind("Iteration", "Parameter", "Value")

p <- ggplot(data = b_ests, aes(x = Iteration, y = Value, col = Parameter), size = 5)
p + geom_point() + labs(title = "L.R. Parameters Estimated via OWLQN")

# Check functionality of other GLM families

# Create binary response variable:
dat <- mutate(dat, act_endg_upb_large = ifelse(dat$act_endg_upb > 122640, lit(1), lit(0)))
# Create non-negative loan_age column to use as count data for Poisson
dat <- mutate(dat, loan_age_pos = abs(dat$loan_age))

# binomial(link = "logit")
glm.logit <- spark.glm(dat, act_endg_upb_large ~ new_int_rt + loan_age + mths_remng + matr_yr + zero_bal_yr, family = "binomial")
# Gamma(link = "inverse")
glm.gamma <- spark.glm(dat, act_endg_upb ~ new_int_rt + loan_age + mths_remng + matr_yr + zero_bal_yr, family = "Gamma")
# inverse.gaussian(link = "1/mu^2")
glm.invgauss <- spark.glm(dat, act_endg_upb ~ new_int_rt + loan_age + mths_remng + matr_yr + zero_bal_yr, family = "inverse.gaussian")
# poisson(link = "log")
glm.poisson <- spark.glm(dat, loan_age_pos ~ act_endg_upb + new_int_rt + mths_remng + matr_yr + zero_bal_yr, family = "poisson")
# quasi(link = "identity", variance = "constant")
glm.quasi <- spark.glm(dat, loan_age_pos ~ act_endg_upb + new_int_rt + mths_remng + matr_yr + zero_bal_yr, family = "quasi")
# quasibinomial(link = "logit")
glm.quasibin <- spark.glm(dat, act_endg_upb_large ~ new_int_rt + loan_age + mths_remng + matr_yr + zero_bal_yr, family = "quasibinomial")
# quasipoisson(link = "log")
glm.quasipoiss <- spark.glm(dat, loan_age_pos ~ act_endg_upb + new_int_rt + mths_remng + matr_yr + zero_bal_yr, family = "quasipoisson")

summary(glm.logit)
summary(glm.invgamma)
summary(glm.invgauss)
summary(glm.poisson)
summary(glm.quasi)
summary(glm.quasibin)
summary(glm.quasipoiss)