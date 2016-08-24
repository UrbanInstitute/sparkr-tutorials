# Confirm that SPARK_HOME is set in environment: set SPARK_HOME to be equal to "/home/spark"
# if the size of the elements of SPARK_HOME are less than 1:
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/home/spark")
}

# Load the SparkR package
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

# Call the SparkR session
sparkR.session()



library(ggplot2)

df <- read.df("s3://sparkr-tutorials/diamonds.csv", header = "true", delimiter = ",", source = "csv", inferSchema = "true", na.strings = "")
cache(df)
str(df)

## Data checks

# Check distribution of price
hstats1 <- histogram(df, "price", nbins = 250)
x <- "price"
title <- "Freq. Polygon: price"
p1 <- ggplot(hstats1, aes(x = centroids, y = counts)) + geom_path() + xlab(x) + ylab("Frequency") + ggtitle(title)
p1

# Insert column with log(price) values
df <- withColumn(df, "lprice", log(df$price))

# Check distribution of log(price)
hstats2 <- histogram(df, "lprice", nbins = 250)
x <- "lprice"
title <- "Freq. Polygon: log(price)"
p2 <- ggplot(hstats2, aes(x = centroids, y = counts)) + geom_path() + xlab(x) + ylab("Frequency") + ggtitle(title)
p2

# Plot relationships between predictors and response variable (log(price))

p3 <- geom_bivar_histogram.SparkR(df = df, x = "carat", y = "lprice", nbins = 250)
p3 + scale_colour_brewer(palette = "Blues", type = "seq") + xlab("carat") + ylab("log(price") + 
  ggtitle("carat v. log(price)")

df <- withColumn(df, "cbrt_carat", cbrt(df$carat))

p4 <- geom_bivar_histogram.SparkR(df = df, x = "cbrt_carat", y = "lprice", nbins = 250)
p4 + scale_colour_brewer(palette = "Blues", type = "seq") + xlab("cbrt(carat)") + ylab("log(price)") + 
  ggtitle("cbrt(carat) v. log(price)")


p5 <- geom_bivar_histogram.SparkR(df = df, x = "x", y = "lprice", nbins = 250)
p5 + scale_colour_brewer(palette = "Blues", type = "seq") + xlab("x") + ylab("log(price)") + 
  ggtitle("x v. log(price)")


# Collect `df` as local data.frame to perform base R linear regression
dat <- collect(df)
head(dat)

## Fit model

# Fit a simple multiple linear regression model with two (2) numerical predictor variables and one (1) categorical (SparkR converts to )
lm1 <- spark.glm(df, lprice ~ cbrt_carat + x + clarity, family = "gaussian")
output1 <- summary(lm1)
output1

# Save list of parameter estimates
coeffs1 <- output1$coefficients[,1]
coeffs1

## General linear regression measurements

y <- df$lprice

# Calculate average y value:
y_avg <- collect(agg(df, y_avg = mean(y)))$y_avg

# Predict fitted values using the DF OLS model -> yields new DF
df <- predict(lm1, df)
head(df) # so you can see what the prediction DF looks like

# Transform the SparkR fitted values DF (df_pred) so that it is easier to read and includes squared residuals and squared totals & extract yhat vector (as new DF)
df <- transform(df, y_hat = df$prediction, sq_res = (y - df$prediction)^2, sq_tot = (y - y_avg)^2, res = y - df$prediction)
df$prediction <- NULL
head(select(df, "y", "y_hat", "sq_res", "sq_tot"))

# Compute sum of squared residuals and totals, then use these values to calculate R-squared:
SSR <- collect(agg(df, SSR = sum(df$sq_res)))  ##### Note: produces dfa.frame - get values out of d.f's in order to calculate aRsq and Rsq
SST <- collect(agg(df, SST = sum(df$sq_tot)))
Rsq2 <- 1-(SSR[[1]]/SST[[1]])
p <- 3
N <- nrow(df)
aRsq2 <- Rsq2 - (1 - Rsq2)*((p - 1)/(N - p))

Rsq2
aRsq2


### Fit (Gaussian/identity) glm with base R & compare with spark.glm output

lm2 <- glm(lprice ~ cbrt_carat + x + clarity, data = dat, family = gaussian)
output2 <- summary(lm2)
coeffs2 <- output2$coefficients

# lm3 <- lm(lprice ~ cbrt_carat + x + clarity, data = dat)

# Compare outputs
output1
output2


### Distribution families and link functions available in SparkR

# "gaussian" -> "identity", "log", "inverse"
# "binomial" -> "logit", "probit", "cloglog"
# "poisson" -> "log", "identity", "sqrt"
# "gamma" -> "inverse", "identity", "log"

# Create binary response variable:
lprice_avg <- collect(agg(df, avg = avg(df$lprice)))[[1]]
df <- mutate(df, lprice_high = ifelse(df$lprice > lprice_avg, lit(1), lit(0)))


# binomial(link = "logit")
glm.logit <- spark.glm(df, lprice_high ~ cbrt_carat + x + clarity, family = "binomial")

# Gamma(link = "inverse")
glm.gamma <- spark.glm(df, lprice ~ cbrt_carat + x + clarity, family = "Gamma")

# poisson(link = "log")
glm.poisson <- spark.glm(df, price ~ cbrt_carat + x + clarity, family = "poisson")


summary(glm.logit)
summary(glm.gamma)
summary(glm.poisson)


### Linear Regression Diagnostics

# Fitted v. Residual Values plot

p6 <- geom_bivar_histogram.SparkR(df = df, x = "y_hat", y = "res", nbins = 250)
p6 + scale_colour_brewer(palette = "Blues", type = "seq") + xlab("Fitted Value") + ylab("Residual") + 
  ggtitle("Fitted v. Residual Values")

# Q-Q plot of the residuals

qqres_plot.SparkR <- function(df, residuals, qn = 100, error){
  
  resdf <- select(df, residuals)
  
  n <- nrow(resdf)
  mean.res <- collect(agg(resdf, avg(resdf[[residuals]])))[[1]]
  var.res <- collect(agg(resdf, var(resdf[[residuals]])))[[1]]
  sd.res <- collect(agg(resdf, stddev(resdf[[residuals]])))[[1]]
  min.res <- collect(agg(resdf, min(resdf[[residuals]])))[[1]]
  max.res <- collect(agg(resdf, max(resdf[[residuals]])))[[1]]
  
  probs <- seq(0, 1, length = qn)
  
  norm_quantiles <- qnorm(probs, mean = mean.res, sd = sd.res)
  res_quantiles <- unlist(approxQuantile(resdf, col = residuals, probabilities = probs, relativeError = error))
  
  dat <- data.frame(sort(norm_quantiles), sort(res_quantiles))
  
  p <- ggplot(dat, aes(norm_quantiles, res_quantiles))
  
  p + geom_point(color = "#FF3333") + geom_abline(intercept = 0, slope = 1) + xlab("Theoretical Quantiles") + ylab("Sample Quantiles") + geom_hline(aes(yintercept = min(dat$sort.res_quantiles.), linetype = "Residual Extremum Values"), show.legend = TRUE) + geom_hline(yintercept = max(dat$sort.res_quantiles.), linetype = "dotted") + scale_linetype_manual(values = c(name = "none", "Residual Extremum Values" = "dotted")) + guides(linetype = guide_legend("")) + theme(legend.position = "bottom")
  
}

p7 <- qqres_plot.SparkR(df = df, residuals = "res", qn = 100, error = 0.0001)
p7 + ggtitle("This is a title")

