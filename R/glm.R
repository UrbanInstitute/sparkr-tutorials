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

# Fitted v. Residual Values plot

p5 <- geom_bivar_histogram.SparkR(df = df, x = "y_hat", y = "res", nbins = 250)
p5 + scale_colour_brewer(palette = "Blues", type = "seq") + xlab("Fitted Value") + ylab("Residual") + 
  ggtitle("Fitted v. Residual Values")


### Fit lm with base R

lm2 <- glm(lprice ~ cbrt_carat + x + clarity, data = dat, family = gaussian)
output2 <- summary(lm2)
coeffs2 <- output2$coefficients

lm3 <- lm(lprice ~ cbrt_carat + x + clarity, data = dat)
output3 <- summary(lm3)
coeffs3 <- output3$coefficients

output1
output2
output3

