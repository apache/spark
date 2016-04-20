#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# mllib.R: Provides methods for MLlib integration

#' @title S4 class that represents a generalized linear model
#' @param jobj a Java object reference to the backing Scala GeneralizedLinearRegressionWrapper
#' @export
setClass("GeneralizedLinearRegressionModel", representation(jobj = "jobj"))

#' @title S4 class that represents a NaiveBayesModel
#' @param jobj a Java object reference to the backing Scala NaiveBayesWrapper
#' @export
setClass("NaiveBayesModel", representation(jobj = "jobj"))

#' @title S4 class that represents a AFTSurvivalRegressionModel
#' @param jobj a Java object reference to the backing Scala AFTSurvivalRegressionWrapper
#' @export
setClass("AFTSurvivalRegressionModel", representation(jobj = "jobj"))

#' @title S4 class that represents a KMeansModel
#' @param jobj a Java object reference to the backing Scala KMeansModel
#' @export
setClass("KMeansModel", representation(jobj = "jobj"))

#' Fits a generalized linear model
#'
#' Fits a generalized linear model, similarly to R's glm().
#'
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param data DataFrame for training.
#' @param family A description of the error distribution and link function to be used in the model.
#'               This can be a character string naming a family function, a family function or
#'               the result of a call to a family function. Refer R family at
#'               \url{https://stat.ethz.ch/R-manual/R-devel/library/stats/html/family.html}.
#' @param epsilon Positive convergence tolerance of iterations.
#' @param maxit Integer giving the maximal number of IRLS iterations.
#' @return a fitted generalized linear model
#' @rdname glm
#' @export
#' @examples
#' \dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' data(iris)
#' df <- createDataFrame(sqlContext, iris)
#' model <- glm(Sepal_Length ~ Sepal_Width, df, family="gaussian")
#' summary(model)
#' }
setMethod("glm", signature(formula = "formula", family = "ANY", data = "DataFrame"),
          function(formula, family = gaussian, data, epsilon = 1e-06, maxit = 25) {
            if (is.character(family)) {
              family <- get(family, mode = "function", envir = parent.frame())
            }
            if (is.function(family)) {
              family <- family()
            }
            if (is.null(family$family)) {
              print(family)
              stop("'family' not recognized")
            }

            formula <- paste(deparse(formula), collapse = "")

            jobj <- callJStatic("org.apache.spark.ml.r.GeneralizedLinearRegressionWrapper",
                                "fit", formula, data@sdf, family$family, family$link,
                                epsilon, as.integer(maxit))
            return(new("GeneralizedLinearRegressionModel", jobj = jobj))
          })

#' Get the summary of a generalized linear model
#'
#' Returns the summary of a model produced by glm(), similarly to R's summary().
#'
#' @param object A fitted generalized linear model
#' @return coefficients the model's coefficients, intercept
#' @rdname summary
#' @export
#' @examples
#' \dontrun{
#' model <- glm(y ~ x, trainingData)
#' summary(model)
#' }
setMethod("summary", signature(object = "GeneralizedLinearRegressionModel"),
          function(object, ...) {
            jobj <- object@jobj
            features <- callJMethod(jobj, "rFeatures")
            coefficients <- callJMethod(jobj, "rCoefficients")
            deviance.resid <- callJMethod(jobj, "rDevianceResiduals")
            dispersion <- callJMethod(jobj, "rDispersion")
            null.deviance <- callJMethod(jobj, "rNullDeviance")
            deviance <- callJMethod(jobj, "rDeviance")
            df.null <- callJMethod(jobj, "rResidualDegreeOfFreedomNull")
            df.residual <- callJMethod(jobj, "rResidualDegreeOfFreedom")
            aic <- callJMethod(jobj, "rAic")
            iter <- callJMethod(jobj, "rNumIterations")
            family <- callJMethod(jobj, "rFamily")

            deviance.resid <- dataFrame(deviance.resid)
            coefficients <- matrix(coefficients, ncol = 4)
            colnames(coefficients) <- c("Estimate", "Std. Error", "t value", "Pr(>|t|)")
            rownames(coefficients) <- unlist(features)
            ans <- list(deviance.resid = deviance.resid, coefficients = coefficients,
                        dispersion = dispersion, null.deviance = null.deviance,
                        deviance = deviance, df.null = df.null, df.residual = df.residual,
                        aic = aic, iter = iter, family = family)
            class(ans) <- "summary.GeneralizedLinearRegressionModel"
            return(ans)
          })

#' Print the summary of GeneralizedLinearRegressionModel
#'
#' @rdname print
#' @name print.summary.GeneralizedLinearRegressionModel
#' @export
print.summary.GeneralizedLinearRegressionModel <- function(x, ...) {
  x$deviance.resid <- setNames(unlist(approxQuantile(x$deviance.resid, "devianceResiduals",
    c(0.0, 0.25, 0.5, 0.75, 1.0), 0.01)), c("Min", "1Q", "Median", "3Q", "Max"))
  x$deviance.resid <- zapsmall(x$deviance.resid, 5L)
  cat("\nDeviance Residuals: \n")
  cat("(Note: These are approximate quantiles with relative error <= 0.01)\n")
  print.default(x$deviance.resid, digits = 5L, na.print = "", print.gap = 2L)

  cat("\nCoefficients:\n")
  print.default(x$coefficients, digits = 5L, na.print = "", print.gap = 2L)

  cat("\n(Dispersion parameter for ", x$family, " family taken to be ", format(x$dispersion),
    ")\n\n", apply(cbind(paste(format(c("Null", "Residual"), justify = "right"), "deviance:"),
    format(unlist(x[c("null.deviance", "deviance")]), digits = 5L),
    " on", format(unlist(x[c("df.null", "df.residual")])), " degrees of freedom\n"),
    1L, paste, collapse = " "), sep = "")
  cat("AIC: ", format(x$aic, digits = 4L), "\n\n",
    "Number of Fisher Scoring iterations: ", x$iter, "\n", sep = "")
  cat("\n")
  invisible(x)
  }

#' Make predictions from a generalized linear model
#'
#' Makes predictions from a generalized linear model produced by glm(), similarly to R's predict().
#'
#' @param object A fitted generalized linear model
#' @param newData DataFrame for testing
#' @return DataFrame containing predicted labels in a column named "prediction"
#' @rdname predict
#' @export
#' @examples
#' \dontrun{
#' model <- glm(y ~ x, trainingData)
#' predicted <- predict(model, testData)
#' showDF(predicted)
#' }
setMethod("predict", signature(object = "GeneralizedLinearRegressionModel"),
          function(object, newData) {
            return(dataFrame(callJMethod(object@jobj, "transform", newData@sdf)))
          })

#' Make predictions from a naive Bayes model
#'
#' Makes predictions from a model produced by naiveBayes(), similarly to R package e1071's predict.
#'
#' @param object A fitted naive Bayes model
#' @param newData DataFrame for testing
#' @return DataFrame containing predicted labels in a column named "prediction"
#' @rdname predict
#' @export
#' @examples
#' \dontrun{
#' model <- naiveBayes(y ~ x, trainingData)
#' predicted <- predict(model, testData)
#' showDF(predicted)
#'}
setMethod("predict", signature(object = "NaiveBayesModel"),
          function(object, newData) {
            return(dataFrame(callJMethod(object@jobj, "transform", newData@sdf)))
          })

#' Get the summary of a naive Bayes model
#'
#' Returns the summary of a naive Bayes model produced by naiveBayes(), similarly to R's summary().
#'
#' @param object A fitted MLlib model
#' @return a list containing 'apriori', the label distribution, and 'tables', conditional
#          probabilities given the target label
#' @rdname summary
#' @export
#' @examples
#' \dontrun{
#' model <- naiveBayes(y ~ x, trainingData)
#' summary(model)
#'}
setMethod("summary", signature(object = "NaiveBayesModel"),
          function(object, ...) {
            jobj <- object@jobj
            features <- callJMethod(jobj, "features")
            labels <- callJMethod(jobj, "labels")
            apriori <- callJMethod(jobj, "apriori")
            apriori <- t(as.matrix(unlist(apriori)))
            colnames(apriori) <- unlist(labels)
            tables <- callJMethod(jobj, "tables")
            tables <- matrix(tables, nrow = length(labels))
            rownames(tables) <- unlist(labels)
            colnames(tables) <- unlist(features)
            return(list(apriori = apriori, tables = tables))
          })

#' Fit a k-means model
#'
#' Fit a k-means model, similarly to R's kmeans().
#'
#' @param x DataFrame for training
#' @param centers Number of centers
#' @param iter.max Maximum iteration number
#' @param algorithm Algorithm choosen to fit the model
#' @return A fitted k-means model
#' @rdname kmeans
#' @export
#' @examples
#' \dontrun{
#' model <- kmeans(x, centers = 2, algorithm="random")
#' }
setMethod("kmeans", signature(x = "DataFrame"),
          function(x, centers, iter.max = 10, algorithm = c("random", "k-means||")) {
            columnNames <- as.array(colnames(x))
            algorithm <- match.arg(algorithm)
            jobj <- callJStatic("org.apache.spark.ml.r.KMeansWrapper", "fit", x@sdf,
                                centers, iter.max, algorithm, columnNames)
            return(new("KMeansModel", jobj = jobj))
         })

#' Get fitted result from a k-means model
#'
#' Get fitted result from a k-means model, similarly to R's fitted().
#'
#' @param object A fitted k-means model
#' @return DataFrame containing fitted values
#' @rdname fitted
#' @export
#' @examples
#' \dontrun{
#' model <- kmeans(trainingData, 2)
#' fitted.model <- fitted(model)
#' showDF(fitted.model)
#'}
setMethod("fitted", signature(object = "KMeansModel"),
          function(object, method = c("centers", "classes"), ...) {
            method <- match.arg(method)
            return(dataFrame(callJMethod(object@jobj, "fitted", method)))
          })

#' Get the summary of a k-means model
#'
#' Returns the summary of a k-means model produced by kmeans(),
#' similarly to R's summary().
#'
#' @param object a fitted k-means model
#' @return the model's coefficients, size and cluster
#' @rdname summary
#' @export
#' @examples
#' \dontrun{
#' model <- kmeans(trainingData, 2)
#' summary(model)
#' }
setMethod("summary", signature(object = "KMeansModel"),
          function(object, ...) {
            jobj <- object@jobj
            features <- callJMethod(jobj, "features")
            coefficients <- callJMethod(jobj, "coefficients")
            cluster <- callJMethod(jobj, "cluster")
            k <- callJMethod(jobj, "k")
            size <- callJMethod(jobj, "size")
            coefficients <- t(matrix(coefficients, ncol = k))
            colnames(coefficients) <- unlist(features)
            rownames(coefficients) <- 1:k
            return(list(coefficients = coefficients, size = size, cluster = dataFrame(cluster)))
          })

#' Make predictions from a k-means model
#'
#' Make predictions from a model produced by kmeans().
#'
#' @param object A fitted k-means model
#' @param newData DataFrame for testing
#' @return DataFrame containing predicted labels in a column named "prediction"
#' @rdname predict
#' @export
#' @examples
#' \dontrun{
#' model <- kmeans(trainingData, 2)
#' predicted <- predict(model, testData)
#' showDF(predicted)
#' }
setMethod("predict", signature(object = "KMeansModel"),
          function(object, newData) {
            return(dataFrame(callJMethod(object@jobj, "transform", newData@sdf)))
          })

#' Fit a Bernoulli naive Bayes model
#'
#' Fit a Bernoulli naive Bayes model, similarly to R package e1071's naiveBayes() while only
#' categorical features are supported. The input should be a DataFrame of observations instead of a
#' contingency table.
#'
#' @param object A symbolic description of the model to be fitted. Currently only a few formula
#'               operators are supported, including '~', '.', ':', '+', and '-'.
#' @param data DataFrame for training
#' @param laplace Smoothing parameter
#' @return a fitted naive Bayes model
#' @rdname naiveBayes
#' @seealso e1071: \url{https://cran.r-project.org/web/packages/e1071/}
#' @export
#' @examples
#' \dontrun{
#' df <- createDataFrame(sqlContext, infert)
#' model <- naiveBayes(education ~ ., df, laplace = 0)
#'}
setMethod("naiveBayes", signature(formula = "formula", data = "DataFrame"),
          function(formula, data, laplace = 0, ...) {
            formula <- paste(deparse(formula), collapse = "")
            jobj <- callJStatic("org.apache.spark.ml.r.NaiveBayesWrapper", "fit",
                                 formula, data@sdf, laplace)
            return(new("NaiveBayesModel", jobj = jobj))
          })

#' Fit an accelerated failure time (AFT) survival regression model.
#'
#' Fit an accelerated failure time (AFT) survival regression model, similarly to R's survreg().
#'
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', ':', '+', and '-'.
#'                Note that operator '.' is not supported currently.
#' @param data DataFrame for training.
#' @return a fitted AFT survival regression model
#' @rdname survreg
#' @seealso survival: \url{https://cran.r-project.org/web/packages/survival/}
#' @export
#' @examples
#' \dontrun{
#' df <- createDataFrame(sqlContext, ovarian)
#' model <- survreg(Surv(futime, fustat) ~ ecog_ps + rx, df)
#' }
setMethod("survreg", signature(formula = "formula", data = "DataFrame"),
          function(formula, data, ...) {
            formula <- paste(deparse(formula), collapse = "")
            jobj <- callJStatic("org.apache.spark.ml.r.AFTSurvivalRegressionWrapper",
                                "fit", formula, data@sdf)
            return(new("AFTSurvivalRegressionModel", jobj = jobj))
          })

#' Get the summary of an AFT survival regression model
#'
#' Returns the summary of an AFT survival regression model produced by survreg(),
#' similarly to R's summary().
#'
#' @param object a fitted AFT survival regression model
#' @return coefficients the model's coefficients, intercept and log(scale).
#' @rdname summary
#' @export
#' @examples
#' \dontrun{
#' model <- survreg(Surv(futime, fustat) ~ ecog_ps + rx, trainingData)
#' summary(model)
#' }
setMethod("summary", signature(object = "AFTSurvivalRegressionModel"),
          function(object, ...) {
            jobj <- object@jobj
            features <- callJMethod(jobj, "rFeatures")
            coefficients <- callJMethod(jobj, "rCoefficients")
            coefficients <- as.matrix(unlist(coefficients))
            colnames(coefficients) <- c("Value")
            rownames(coefficients) <- unlist(features)
            return(list(coefficients = coefficients))
          })

#' Make predictions from an AFT survival regression model
#'
#' Make predictions from a model produced by survreg(), similarly to R package survival's predict.
#'
#' @param object A fitted AFT survival regression model
#' @param newData DataFrame for testing
#' @return DataFrame containing predicted labels in a column named "prediction"
#' @rdname predict
#' @export
#' @examples
#' \dontrun{
#' model <- survreg(Surv(futime, fustat) ~ ecog_ps + rx, trainingData)
#' predicted <- predict(model, testData)
#' showDF(predicted)
#' }
setMethod("predict", signature(object = "AFTSurvivalRegressionModel"),
          function(object, newData) {
            return(dataFrame(callJMethod(object@jobj, "transform", newData@sdf)))
          })
