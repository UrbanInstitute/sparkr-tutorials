##############################################
## Normal Probability Plot of the Residuals ##
##############################################
# Sarah Armstrong, Urban Institute
# August 23, 2016

# Summary: Function that returns a quantile-quantile plot of the residual values from linear model, i.e. a plot that fits quantile values of the residuals against those of a Gaussian distribution with mean and standard deviation equal to those of the residuals.

# Inputs:

# (*) df: a SparkR DF
# (*) residuals: the column name assigned to the residual values (a string)
# (*) qn: the number of quantiles plotted (default is 100)
# (*) error: relativeError value used in the `approxQuantile` SparkR operation

# Returns: a ggplot object displaying the Q-Q plot, including axis labels and horizontal dashed lines, annotating the extremum values of the residuals

# p1 <- qqres_plot.SparkR(df = df, residuals = "res", qn = 100, error = 0.0001)
# p1 + ggtitle("This is a title")

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