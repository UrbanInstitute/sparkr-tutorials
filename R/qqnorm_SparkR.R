#############################################################
## qqnorm.SparkR: Normal Probability Plot of the Residuals ##
#############################################################
# Sarah Armstrong, Urban Institute
# August 23, 2016
# Last Updated: August 24, 2016

# Summary: Function that returns a quantile-quantile plot of the residual values from linear model, i.e. a plot that fits quantile values of the standardized residuals against those of a standard normal distribution.

# Inputs:

# (*) df: a SparkR DF
# (*) residuals: the column name assigned to the residual values (a string); note: the function will standardize these during execution
# (*) qn: the number of quantiles plotted (default is 100)
# (*) error: relativeError value used in the `approxQuantile` SparkR operation

# Returns: a ggplot object displaying the Q-Q plot, including axis labels and horizontal dashed lines, annotating the extremum values of the standardized residuals

# p <- qqnorm.SparkR(df = df, residuals = "res", qn = 100, error = 0.0001)
# p + ggtitle("This is a title")

qqnorm.SparkR <- function(df, residuals, qn = 100, error){
  
  resdf <- select(df, residuals)
  
  sd.res <- collect(agg(resdf, stddev(resdf[[residuals]])))[[1]]
  
  resdf <- withColumn(resdf, "stdres", resdf[[residuals]] / sd.res)
  
  probs <- seq(0, 1, length = qn)
  
  norm_quantiles <- qnorm(probs, mean = 0, sd = 1)
  stdres_quantiles <- unlist(approxQuantile(resdf, col = "stdres", probabilities = probs, relativeError = error))
  
  dat <- data.frame(sort(norm_quantiles), sort(stdres_quantiles))
  
  p_ <- ggplot(dat, aes(norm_quantiles, stdres_quantiles))

  p <- p_ + geom_point(color = "#FF3333") + geom_abline(intercept = 0, slope = 1) + xlab("Normal Scores") + ylab("Standardized Residuals") + geom_hline(aes(yintercept = min(dat$sort.stdres_quantiles.), linetype = "1st & qnth Quantile Values"), show.legend = TRUE) + geom_hline(yintercept = max(dat$sort.stdres_quantiles.), linetype = "dotted") + scale_linetype_manual(values = c(name = "none", "1st & qnth Quantile Values" = "dotted")) + guides(linetype = guide_legend("")) + theme(legend.position = "bottom")
  
  return(p)
  
}