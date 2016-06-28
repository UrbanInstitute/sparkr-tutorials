############################################################################
## Social Science Methodologies: Generalized Linear Models (GLM) Module 1 ##
############################################################################
## Objective: 
## Operations discussed: glm

library(SparkR)
library(ggplot2)
library(reshape2)

## Initiate SparkContext:

sc <- sparkR.init(sparkEnvir=list(spark.executor.memory="2g", 
                                  spark.driver.memory="1g",
                                  spark.driver.maxResultSize="1g")
                  ,sparkPackages="com.databricks:spark-csv_2.11:1.4.0") # Load CSV Spark Package

## AWS EMR is using Spark 2.11 so we need the associated version of spark-csv: http://spark-packages.org/package/databricks/spark-csv
## Define Spark executor memory, as well as driver memory and maxResultSize according to cluster configuration

## Initiate SparkRSQL:

sqlContext <- sparkRSQL.init(sc)

## Create a local R data.frame:

x1 <- rnorm(n=200, mean=10, sd=2)
x2 <- rnorm(n=200, mean=17, sd=3)
x3 <- rnorm(n=200, mean=8, sd=1)
y <- 1 + .2 * x1 + .4 * x2 + .5 * x3 + rnorm(n=200, mean=0, sd=.1) # Can see what the true values of the model parameters are
dat <- cbind.data.frame(y, x1, x2, x3)

## Ordinary linear regression (OLR) model with local data.frame and print model summary:

m1 <- stats::lm(y ~ x1 + x2 + x3, data = dat) # Include `stats::` to require SparkR to estimate `m1` with base R `lm` operation
summary(m1)

## Compute OLR model statistics:

output1 <- summary(m1)
yavg1 <- mean(dat$y)
yhat1 <- m1$fitted.values
coeffs1 <- m1$coefficients
r1 <- m1$resid
SSR1 <- deviance(m1)
Rsq1 <- output1$r.squared
aRsq1 <- output1$adj.r.squared
s1 <- output1$sigma
covmatr1 <- s1^2*output1$cov


## Note: use `lm` function from `stats` R package to estimate ordinary linear regression model for local data.frame to easily compute Rsq and aRsq
## The `glm` operation of neither `stats` nor `SparkR` yield Rsq/aRsq, which makes sense since Rsq/aRsq are widely-accepted measures of goodness-of-fit (GOF) for ordinary
## linear regression, but not for generalized linear models. Other GOF measures are typically used when assessing GLMs since the meaning of the Rsq/aRsq values for a GLM
## become convoluted when fitting a GLM of a family and with a link function different than Gaussian and identiy, respectively (in fact, there are several types of
## residuals that can be computed for GLMs!). The `glm` function in R usually prints AIC, deviance residuals and null deviance in its model summary function. Below, we
## fit an OLR model using the SparkR `glm` operation since g(Y) = Y = XB + e for the identity link function, g(Y) = Y.



## Create SparkR DataFrame (DF) from local data.frame:

df <- as.DataFrame(sqlContext, dat)

## Perform OLS estimation on DF with the same specifcations for our data.frame OLS estimation:

m2 <- SparkR::glm(y ~ x1 + x2 + x3, data = df, solver = "l-bfgs")
summary(m2)

## Comput OLR model statistics:

output2 <- summary(m2)
coeffs2 <- output2$coefficients[,1]

# Calculate average y value:
yavg2 <- collect(agg(df, yavg_df = mean(df$y)))$yavg_df
# Predict fitted values using the DF OLS model -> yields new DF
yhat2_df <- predict(m2, df)
head(yhat2_df) # so you can see what the prediction DF looks like
# Transform the SparkR fitted values DF (yhat2_df) so that it is easier to read and includes squared residuals and squared totals & extract yhat vector (as new DF)
yhat2_df <- transform(yhat2_df, sq_res2 = (yhat2_df$y - yhat2_df$prediction)^2, sq_tot2 = (yhat2_df$y - yavg2)^2)
yhat2_df <- transform(yhat2_df, yhat = yhat2_df$prediction)
head(select(yhat2_df, "y", "yhat", "sq_res2", "sq_tot2"))
head(yhat2 <- select(yhat2_df, "yhat"))
# Compute sum of squared residuals and totals, then use these values to calculate R-squared:
SSR2 <- collect(agg(yhat2_df, SSR2=sum(yhat2_df$sq_res2)))  ##### Note: produces data.frame - get values out of d.f's in order to calculate aRsq and Rsq
SST2 <- collect(agg(yhat2_df, SST2=sum(yhat2_df$sq_res2)))
Rsq2 <- 1-(SSR2/SST2)
p <- 3
N <- nrow(df)
aRsq2 <- 1-(((1-Rsq2)*(N-1))/(N-p-1))

## Iteratively fit linear regression models using SparkR `glm`, using l-bfgs for optimization, and plot resulting coefficient estimations with `lm` estimate values

n <- 10
b0 <- rep(0,n)
b1 <- rep(0,n)
b2 <- rep(0,n)
b3 <- rep(0,n)
for(i in 1:n){
  model <- SparkR::glm(y ~ x1 + x2 + x3, data = df)
  b0[i] <- unname(summary(model)$coefficients[,1]["(Intercept)"])
  b1[i] <- unname(summary(model)$coefficients[,1]["x1"])
  b2[i] <- unname(summary(model)$coefficients[,1]["x2"])
  b3[i] <- unname(summary(model)$coefficients[,1]["x3"])
}

# Prepare parameter estimate lists above as data.frames to pass into ggplot:
b_ests_ <- data.frame(cbind(b0 = unlist(b0), b1 = unlist(b1), b2 = unlist(b2), b3 = unlist(b3), Iteration = seq(1, n, by = 1)))
b_ests <- melt(b_ests_, id.vars ="Iteration", measure.vars = c("b0", "b1", "b2", "b3"))
names(b_ests) <- cbind("Iteration", "Variable", "Value")


p <- ggplot(data = b_ests, aes(x = Iteration, y = Value, col = Variable), size = 5) + geom_point() + geom_hline(yintercept = unname(coeffs1["(Intercept)"]), linetype = 2) + geom_hline(yintercept = unname(coeffs1["x1"]), linetype = 2) + geom_hline(yintercept = unname(coeffs1["x2"]), linetype = 2) + geom_hline(yintercept = unname(coeffs1["x3"]), linetype = 2) + labs(title = "L.R. Parameters Estimated via L-BFGS")