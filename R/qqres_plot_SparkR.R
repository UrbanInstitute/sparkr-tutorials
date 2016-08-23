##### Quantile-Quantile Residual Plot

qqres_plot.SparkR <- function(df, residuals, qn, error){

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

    p + geom_point(color = "#FF3333") + geom_abline(intercept = 0, slope = 1) + xlab("Theoretical Quantiles") + ylab("Sample Quantiles") + geom_abline(intercept = min(dat$sort.res_quantiles.), slope = 0, linetype = "dotted", show.legend = TRUE)+ geom_abline(intercept = max(dat$sort.res_quantiles.), slope = 0, linetype = "dotted")

}





