#############################################################
## confint.SparkR: Normal Distribution Confidence Interval ##
#############################################################
# Sarah Armstrong, Urban Institute
# August 31, 2016

# Summary: Function that returns a confidence intervals for parameter estimates of a GLM (Gaussian distribution family, identity link function) model.

# Inputs:

# (*) object: a SparkR GLM model, fit with `spark.glm` operation
# (*) level: level of confidence for CI

# Returns: a local data.frame, detailing the CIs for each parameter estimate

# ci <- confint.SparkR(object = lm, level = 0.975)
# ci


confint.SparkR <- function(object, level){
  
  coef <- unname(unlist(summary(object)$coefficients[,1]))
  
  err <- unname(unlist(summary(object)$coefficients[,2]))
  
  ci <- as.data.frame(cbind(names(unlist(summary(object)$coefficients[,1])), coef - err*qt(level, summary(object)$df.null), coef + err*qt(0.975, summary(object)$df.null)))
  
  colnames(ci) <- c("","Lower Bound", "Upper Bound")
  
  return(ci)
  
}