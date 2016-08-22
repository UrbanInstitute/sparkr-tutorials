##### Quantile-Quantile Residual Plot
library(ggplot2)

residuals <- "res"
nbins <- 10000

# Confirm that there are no missing residual values
nrow(filter(df, isNull(df$res)))

resdf <- select(df, residuals)

# Compute values needed to fit Q-Q plot
n <- nrow(resdf)
mean.res <- collect(agg(resdf, avg(resdf[[residuals]])))[[1]]
sd.res <- collect(agg(resdf, stddev(resdf[[residuals]])))[[1]]
min.res <- collect(agg(resdf, min(resdf[[residuals]])))[[1]]
max.res <- collect(agg(resdf, max(resdf[[residuals]])))[[1]]

# set n points in the interval (0,1)
# use the formula k/(n+1), for k = 1,..,n
# this is a vector of the n probabilities
probs <- seq(0, 1, length = nbins)

# calculate normal quantiles using mean and standard deviation from "ozone"
norm_quantiles = qnorm(probs, mean = mean.res, sd = sd.res)
res_quantiles <- unlist(approxQuantile(resdf, col = residuals, probabilities = probs, relativeError = 0.001))

dat <- data.frame(sort(norm_quantiles), sort(res_quantiles))

p <- ggplot(dat, aes(norm_quantiles, res_quantiles))
p + geom_point() + geom_abline(intercept = 0, slope = 1) + xlab("Theoretical Quantiles") + ylab("Sample Quantiles")

