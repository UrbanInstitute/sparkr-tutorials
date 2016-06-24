############################################################################
## Social Science Methodologies: Generalized Linear Models (GLM) Module 1 ##
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
yavg <- mean(dat$y)
yhat1 <- m1$fitted.values

SSR1 <- deviance(m1)


coeffs1 <- m1$coefficients
r1 <- m1$resid
s1 <- output1$sigma
Rsq1 <- output1$r.squared
aRsq1 <- output1$adj.r.squared
covmatr1 <- s^2*output1$cov
aic1 <- AIC(m1)

## Note: use `lm` function from `stats` R package to estimate ordinary linear regression model for local data.frame to easily compute Rsq and aRsq
## The `glm` operation of neither `stats` nor `SparkR` yield Rsq/aRsq, which makes sense since Rsq/aRsq are widely-accepted measures of goodness-of-fit (GOF) for ordinary
## linear regression, but not for generalized linear models. Other GOF measures are typically used when assessing GLMs since the meaning of the Rsq/aRsq values for a GLM
## become convoluted when fitting a GLM of a family and with a link function different than Gaussian and identiy, respectively (in fact, there are several types of
## residuals that can be computed for GLMs!). The `glm` function in R usually prints AIC, deviance residuals and null deviance in its model summary function. Below, we
## fit an OLR model using the SparkR `glm` operation since g(Y) = Y = XB + e for the identity link function, g(Y) = Y.



## Create SparkR DataFrame (DF) from local data.frame:

df <- as.DataFrame(sqlContext, dat)

## Perform OLS estimation on DF with the same specifcations for our data.frame OLS estimation:

m2 <- SparkR::glm(y ~ x1 + x2 + x3, data = df)
summary(m2)

## Comput OLR model statistics:

output2 <- summary(m2)

# Calculate average y value:
(yavg_df <- collect(agg(df, yavg_df = mean(df$y)))$yavg_df)
# Predict fitted values using the DF OLS model
yhat2 <- predict(m2, df)
# Transform the SparkR fitted values matrix (yhat2) so that it is easier to read and includes squared residuals and squared totals
yhat2 <- transform(yhat2, sq_res2=(yhat2$y - yhat2$prediction)^2, sq_tot2=(yhat2$y - yavg_df)^2)
head(select(yhat2, "y", "prediction", "sq_res2", "sq_tot2"))
# Compute sum of squared residuals and totals, then use these values to calculate R-squared:
SSR2 <- collect(agg(yhat2, SSR2=sum(yhat2$sq_res2)))
SST2 <- collect(agg(yhat2, SST2=sum(yhat2$sq_res2)))
Rsq2 <- 1-(SSR2/SST2)
p <- 3
N <- nrow(df)
aRsq2 <- 1-(((1-Rsq2)*(N-1))/(N-p-1))





features
             coefficients <- callJMethod(jobj, "rCoefficients")
-            coefficients <- as.matrix(unlist(coefficients))
-            colnames(coefficients) <- c("Estimate")
+            deviance.resid <- callJMethod(jobj, "rDevianceResiduals")
+            dispersion <- callJMethod(jobj, "rDispersion")
+            null.deviance <- callJMethod(jobj, "rNullDeviance")
+            deviance <- callJMethod(jobj, "rDeviance")
+            df.null <- callJMethod(jobj, "rResidualDegreeOfFreedomNull")
+            df.residual <- callJMethod(jobj, "rResidualDegreeOfFreedom")
+            aic <- callJMethod(jobj, "rAic")
+            iter <- callJMethod(jobj, "rNumIterations")
+            family