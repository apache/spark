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

# mllib_regression.R: Provides methods for MLlib regression algorithms
#                     (except for tree-based algorithms) integration

#' S4 class that represents a AFTSurvivalRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala AFTSurvivalRegressionWrapper
#' @note AFTSurvivalRegressionModel since 2.0.0
setClass("AFTSurvivalRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a generalized linear model
#'
#' @param jobj a Java object reference to the backing Scala GeneralizedLinearRegressionWrapper
#' @note GeneralizedLinearRegressionModel since 2.0.0
setClass("GeneralizedLinearRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents an IsotonicRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala IsotonicRegressionModel
#' @note IsotonicRegressionModel since 2.1.0
setClass("IsotonicRegressionModel", representation(jobj = "jobj"))

#' Generalized Linear Models
#'
#' Fits generalized linear model against a SparkDataFrame.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param family a description of the error distribution and link function to be used in the model.
#'               This can be a character string naming a family function, a family function or
#'               the result of a call to a family function. Refer R family at
#'               \url{https://stat.ethz.ch/R-manual/R-devel/library/stats/html/family.html}.
#'               Currently these families are supported: \code{binomial}, \code{gaussian},
#'               \code{Gamma}, \code{poisson} and \code{tweedie}.
#'
#'               Note that there are two ways to specify the tweedie family.
#'               \itemize{
#'                \item Set \code{family = "tweedie"} and specify the var.power and link.power;
#'                \item When package \code{statmod} is loaded, the tweedie family is specified
#'                using the family definition therein, i.e., \code{tweedie(var.power, link.power)}.
#'               }
#' @param tol positive convergence tolerance of iterations.
#' @param maxIter integer giving the maximal number of IRLS iterations.
#' @param weightCol the weight column name. If this is not set or \code{NULL}, we treat all instance
#'                  weights as 1.0.
#' @param regParam regularization parameter for L2 regularization.
#' @param var.power the power in the variance function of the Tweedie distribution which provides
#'                      the relationship between the variance and mean of the distribution. Only
#'                      applicable to the Tweedie family.
#' @param link.power the index in the power link function. Only applicable to the Tweedie family.
#' @param stringIndexerOrderType how to order categories of a string feature column. This is used to
#'                               decide the base level of a string feature as the last category
#'                               after ordering is dropped when encoding strings. Supported options
#'                               are "frequencyDesc", "frequencyAsc", "alphabetDesc", and
#'                               "alphabetAsc". The default value is "frequencyDesc". When the
#'                               ordering is set to "alphabetDesc", this drops the same category
#'                               as R when encoding strings.
#' @param offsetCol the offset column name. If this is not set or empty, we treat all instance
#'                  offsets as 0.0. The feature specified as offset has a constant coefficient of
#'                  1.0.
#' @param ... additional arguments passed to the method.
#' @aliases spark.glm,SparkDataFrame,formula-method
#' @return \code{spark.glm} returns a fitted generalized linear model.
#' @rdname spark.glm
#' @name spark.glm
#' @examples
#' \dontrun{
#' sparkR.session()
#' t <- as.data.frame(Titanic, stringsAsFactors = FALSE)
#' df <- createDataFrame(t)
#' model <- spark.glm(df, Freq ~ Sex + Age, family = "gaussian")
#' summary(model)
#'
#' # fitted values on training data
#' fitted <- predict(model, df)
#' head(select(fitted, "Freq", "prediction"))
#'
#' # save fitted model to input path
#' path <- "path/to/model"
#' write.ml(model, path)
#'
#' # can also read back the saved model and print
#' savedModel <- read.ml(path)
#' summary(savedModel)
#'
#' # note that the default string encoding is different from R's glm
#' model2 <- glm(Freq ~ Sex + Age, family = "gaussian", data = t)
#' summary(model2)
#' # use stringIndexerOrderType = "alphabetDesc" to force string encoding
#' # to be consistent with R
#' model3 <- spark.glm(df, Freq ~ Sex + Age, family = "gaussian",
#'                    stringIndexerOrderType = "alphabetDesc")
#' summary(model3)
#'
#' # fit tweedie model
#' model <- spark.glm(df, Freq ~ Sex + Age, family = "tweedie",
#'                    var.power = 1.2, link.power = 0)
#' summary(model)
#'
#' # use the tweedie family from statmod
#' library(statmod)
#' model <- spark.glm(df, Freq ~ Sex + Age, family = tweedie(1.2, 0))
#' summary(model)
#' }
#' @note spark.glm since 2.0.0
#' @seealso \link{glm}, \link{read.ml}
setMethod("spark.glm", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, family = gaussian, tol = 1e-6, maxIter = 25, weightCol = NULL,
                   regParam = 0.0, var.power = 0.0, link.power = 1.0 - var.power,
                   stringIndexerOrderType = c("frequencyDesc", "frequencyAsc",
                                              "alphabetDesc", "alphabetAsc"),
                   offsetCol = NULL) {

            stringIndexerOrderType <- match.arg(stringIndexerOrderType)
            if (is.character(family)) {
              # Handle when family = "tweedie"
              if (tolower(family) == "tweedie") {
                family <- list(family = "tweedie", link = NULL)
              } else {
                family <- get(family, mode = "function", envir = parent.frame())
              }
            }
            if (is.function(family)) {
              family <- family()
            }
            if (is.null(family$family)) {
              print(family)
              stop("'family' not recognized")
            }
            # Handle when family = statmod::tweedie()
            if (tolower(family$family) == "tweedie" && !is.null(family$variance)) {
              var.power <- log(family$variance(exp(1)))
              link.power <- log(family$linkfun(exp(1)))
              family <- list(family = "tweedie", link = NULL)
            }

            formula <- paste(deparse(formula), collapse = "")
            if (!is.null(weightCol) && weightCol == "") {
              weightCol <- NULL
            } else if (!is.null(weightCol)) {
              weightCol <- as.character(weightCol)
            }

            if (!is.null(offsetCol)) {
              offsetCol <- as.character(offsetCol)
              if (nchar(offsetCol) == 0) {
                offsetCol <- NULL
              }
            }

            # For known families, Gamma is upper-cased
            jobj <- callJStatic("org.apache.spark.ml.r.GeneralizedLinearRegressionWrapper",
                                "fit", formula, data@sdf, tolower(family$family), family$link,
                                tol, as.integer(maxIter), weightCol, regParam,
                                as.double(var.power), as.double(link.power),
                                stringIndexerOrderType, offsetCol)
            new("GeneralizedLinearRegressionModel", jobj = jobj)
          })

#' Generalized Linear Models (R-compliant)
#'
#' Fits a generalized linear model, similarly to R's glm().
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param data a SparkDataFrame or R's glm data for training.
#' @param family a description of the error distribution and link function to be used in the model.
#'               This can be a character string naming a family function, a family function or
#'               the result of a call to a family function. Refer R family at
#'               \url{https://stat.ethz.ch/R-manual/R-devel/library/stats/html/family.html}.
#'               Currently these families are supported: \code{binomial}, \code{gaussian},
#'               \code{poisson}, \code{Gamma}, and \code{tweedie}.
#' @param weightCol the weight column name. If this is not set or \code{NULL}, we treat all instance
#'                  weights as 1.0.
#' @param epsilon positive convergence tolerance of iterations.
#' @param maxit integer giving the maximal number of IRLS iterations.
#' @param var.power the index of the power variance function in the Tweedie family.
#' @param link.power the index of the power link function in the Tweedie family.
#' @param stringIndexerOrderType how to order categories of a string feature column. This is used to
#'                               decide the base level of a string feature as the last category
#'                               after ordering is dropped when encoding strings. Supported options
#'                               are "frequencyDesc", "frequencyAsc", "alphabetDesc", and
#'                               "alphabetAsc". The default value is "frequencyDesc". When the
#'                               ordering is set to "alphabetDesc", this drops the same category
#'                               as R when encoding strings.
#' @param offsetCol the offset column name. If this is not set or empty, we treat all instance
#'                  offsets as 0.0. The feature specified as offset has a constant coefficient of
#'                  1.0.
#' @return \code{glm} returns a fitted generalized linear model.
#' @rdname glm
#' @aliases glm
#' @examples
#' \dontrun{
#' sparkR.session()
#' t <- as.data.frame(Titanic)
#' df <- createDataFrame(t)
#' model <- glm(Freq ~ Sex + Age, df, family = "gaussian")
#' summary(model)
#' }
#' @note glm since 1.5.0
#' @seealso \link{spark.glm}
setMethod("glm", signature(formula = "formula", family = "ANY", data = "SparkDataFrame"),
          function(formula, family = gaussian, data, epsilon = 1e-6, maxit = 25, weightCol = NULL,
                   var.power = 0.0, link.power = 1.0 - var.power,
                   stringIndexerOrderType = c("frequencyDesc", "frequencyAsc",
                                              "alphabetDesc", "alphabetAsc"),
                   offsetCol = NULL) {
            spark.glm(data, formula, family, tol = epsilon, maxIter = maxit, weightCol = weightCol,
                      var.power = var.power, link.power = link.power,
                      stringIndexerOrderType = stringIndexerOrderType,
                      offsetCol = offsetCol)
          })

#  Returns the summary of a model produced by glm() or spark.glm(), similarly to R's summary().

#' @param object a fitted generalized linear model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list of components includes at least the \code{coefficients} (coefficients matrix,
#'         which includes coefficients, standard error of coefficients, t value and p value),
#'         \code{null.deviance} (null/residual degrees of freedom), \code{aic} (AIC)
#'         and \code{iter} (number of iterations IRLS takes). If there are collinear columns in
#'         the data, the coefficients matrix only provides coefficients.
#' @rdname spark.glm
#' @note summary(GeneralizedLinearRegressionModel) since 2.0.0
setMethod("summary", signature(object = "GeneralizedLinearRegressionModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            features <- callJMethod(jobj, "rFeatures")
            coefficients <- callJMethod(jobj, "rCoefficients")
            dispersion <- callJMethod(jobj, "rDispersion")
            null.deviance <- callJMethod(jobj, "rNullDeviance")
            deviance <- callJMethod(jobj, "rDeviance")
            df.null <- callJMethod(jobj, "rResidualDegreeOfFreedomNull")
            df.residual <- callJMethod(jobj, "rResidualDegreeOfFreedom")
            iter <- callJMethod(jobj, "rNumIterations")
            family <- callJMethod(jobj, "rFamily")
            aic <- callJMethod(jobj, "rAic")
            if (family == "tweedie" && aic == 0) aic <- NA
            deviance.resid <- if (is.loaded) {
              NULL
            } else {
              dataFrame(callJMethod(jobj, "rDevianceResiduals"))
            }
            # If the underlying WeightedLeastSquares using "normal" solver, we can provide
            # coefficients, standard error of coefficients, t value and p value. Otherwise,
            # it will be fitted by local "l-bfgs", we can only provide coefficients.
            if (length(features) == length(coefficients)) {
              coefficients <- matrix(unlist(coefficients), ncol = 1)
              colnames(coefficients) <- c("Estimate")
              rownames(coefficients) <- unlist(features)
            } else {
              coefficients <- matrix(unlist(coefficients), ncol = 4)
              colnames(coefficients) <- c("Estimate", "Std. Error", "t value", "Pr(>|t|)")
              rownames(coefficients) <- unlist(features)
            }
            ans <- list(deviance.resid = deviance.resid, coefficients = coefficients,
                        dispersion = dispersion, null.deviance = null.deviance,
                        deviance = deviance, df.null = df.null, df.residual = df.residual,
                        aic = aic, iter = iter, family = family, is.loaded = is.loaded)
            class(ans) <- "summary.GeneralizedLinearRegressionModel"
            ans
          })

#  Prints the summary of GeneralizedLinearRegressionModel

#' @rdname spark.glm
#' @param x summary object of fitted generalized linear model returned by \code{summary} function.
#' @note print.summary.GeneralizedLinearRegressionModel since 2.0.0
print.summary.GeneralizedLinearRegressionModel <- function(x, ...) {
  if (x$is.loaded) {
    cat("\nSaved-loaded model does not support output 'Deviance Residuals'.\n")
  } else {
    x$deviance.resid <- setNames(unlist(approxQuantile(x$deviance.resid, "devianceResiduals",
    c(0.0, 0.25, 0.5, 0.75, 1.0), 0.01)), c("Min", "1Q", "Median", "3Q", "Max"))
    x$deviance.resid <- zapsmall(x$deviance.resid, 5L)
    cat("\nDeviance Residuals: \n")
    cat("(Note: These are approximate quantiles with relative error <= 0.01)\n")
    print.default(x$deviance.resid, digits = 5L, na.print = "", print.gap = 2L)
  }

  cat("\nCoefficients:\n")
  print.default(x$coefficients, digits = 5L, na.print = "", print.gap = 2L)

  cat("\n(Dispersion parameter for ", x$family, " family taken to be ", format(x$dispersion),
    ")\n\n", apply(cbind(paste(format(c("Null", "Residual"), justify = "right"), "deviance:"),
    format(unlist(x[c("null.deviance", "deviance")]), digits = 5L),
    " on", format(unlist(x[c("df.null", "df.residual")])), " degrees of freedom\n"),
    1L, paste, collapse = " "), sep = "")
  cat("AIC: ", format(x$aic, digits = 4L), "\n\n",
    "Number of Fisher Scoring iterations: ", x$iter, "\n\n", sep = "")
  invisible(x)
  }

#  Makes predictions from a generalized linear model produced by glm() or spark.glm(),
#  similarly to R's predict().

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labels in a column named
#'         "prediction".
#' @rdname spark.glm
#' @note predict(GeneralizedLinearRegressionModel) since 1.5.0
setMethod("predict", signature(object = "GeneralizedLinearRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Saves the generalized linear model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.glm
#' @note write.ml(GeneralizedLinearRegressionModel, character) since 2.0.0
setMethod("write.ml", signature(object = "GeneralizedLinearRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' Isotonic Regression Model
#'
#' Fits an Isotonic Regression model against a SparkDataFrame, similarly to R's isoreg().
#' Users can print, make predictions on the produced model and save the model to the input path.
#'
#' @param data SparkDataFrame for training.
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param isotonic Whether the output sequence should be isotonic/increasing (TRUE) or
#'                 antitonic/decreasing (FALSE).
#' @param featureIndex The index of the feature if \code{featuresCol} is a vector column
#'                     (default: 0), no effect otherwise.
#' @param weightCol The weight column name.
#' @param ... additional arguments passed to the method.
#' @return \code{spark.isoreg} returns a fitted Isotonic Regression model.
#' @rdname spark.isoreg
#' @aliases spark.isoreg,SparkDataFrame,formula-method
#' @name spark.isoreg
#' @examples
#' \dontrun{
#' sparkR.session()
#' data <- list(list(7.0, 0.0), list(5.0, 1.0), list(3.0, 2.0),
#'         list(5.0, 3.0), list(1.0, 4.0))
#' df <- createDataFrame(data, c("label", "feature"))
#' model <- spark.isoreg(df, label ~ feature, isotonic = FALSE)
#' # return model boundaries and prediction as lists
#' result <- summary(model, df)
#' # prediction based on fitted model
#' predict_data <- list(list(-2.0), list(-1.0), list(0.5),
#'                 list(0.75), list(1.0), list(2.0), list(9.0))
#' predict_df <- createDataFrame(predict_data, c("feature"))
#' # get prediction column
#' predict_result <- collect(select(predict(model, predict_df), "prediction"))
#'
#' # save fitted model to input path
#' path <- "path/to/model"
#' write.ml(model, path)
#'
#' # can also read back the saved model and print
#' savedModel <- read.ml(path)
#' summary(savedModel)
#' }
#' @note spark.isoreg since 2.1.0
setMethod("spark.isoreg", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, isotonic = TRUE, featureIndex = 0, weightCol = NULL) {
            formula <- paste(deparse(formula), collapse = "")

            if (!is.null(weightCol) && weightCol == "") {
              weightCol <- NULL
            } else if (!is.null(weightCol)) {
              weightCol <- as.character(weightCol)
            }

            jobj <- callJStatic("org.apache.spark.ml.r.IsotonicRegressionWrapper", "fit",
                                data@sdf, formula, as.logical(isotonic), as.integer(featureIndex),
                                weightCol)
            new("IsotonicRegressionModel", jobj = jobj)
          })

#  Get the summary of an IsotonicRegressionModel model

#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes model's \code{boundaries} (boundaries in increasing order)
#'         and \code{predictions} (predictions associated with the boundaries at the same index).
#' @rdname spark.isoreg
#' @aliases summary,IsotonicRegressionModel-method
#' @note summary(IsotonicRegressionModel) since 2.1.0
setMethod("summary", signature(object = "IsotonicRegressionModel"),
          function(object) {
            jobj <- object@jobj
            boundaries <- callJMethod(jobj, "boundaries")
            predictions <- callJMethod(jobj, "predictions")
            list(boundaries = boundaries, predictions = predictions)
          })

#  Predicted values based on an isotonicRegression model

#' @param object a fitted IsotonicRegressionModel.
#' @param newData SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted values.
#' @rdname spark.isoreg
#' @aliases predict,IsotonicRegressionModel,SparkDataFrame-method
#' @note predict(IsotonicRegressionModel) since 2.1.0
setMethod("predict", signature(object = "IsotonicRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Save fitted IsotonicRegressionModel to the input path

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.isoreg
#' @aliases write.ml,IsotonicRegressionModel,character-method
#' @note write.ml(IsotonicRegression, character) since 2.1.0
setMethod("write.ml", signature(object = "IsotonicRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' Accelerated Failure Time (AFT) Survival Regression Model
#'
#' \code{spark.survreg} fits an accelerated failure time (AFT) survival regression model on
#' a SparkDataFrame. Users can call \code{summary} to get a summary of the fitted AFT model,
#' \code{predict} to make predictions on new data, and \code{write.ml}/\code{read.ml} to
#' save/load fitted models.
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', ':', '+', and '-'.
#'                Note that operator '.' is not supported currently.
#' @param aggregationDepth The depth for treeAggregate (greater than or equal to 2). If the
#'                         dimensions of features or the number of partitions are large, this
#'                         param could be adjusted to a larger size. This is an expert parameter.
#'                         Default value should be good for most cases.
#' @param stringIndexerOrderType how to order categories of a string feature column. This is used to
#'                               decide the base level of a string feature as the last category
#'                               after ordering is dropped when encoding strings. Supported options
#'                               are "frequencyDesc", "frequencyAsc", "alphabetDesc", and
#'                               "alphabetAsc". The default value is "frequencyDesc". When the
#'                               ordering is set to "alphabetDesc", this drops the same category
#'                               as R when encoding strings.
#' @param ... additional arguments passed to the method.
#' @return \code{spark.survreg} returns a fitted AFT survival regression model.
#' @rdname spark.survreg
#' @seealso survival: \url{https://cran.r-project.org/package=survival}
#' @examples
#' \dontrun{
#' df <- createDataFrame(ovarian)
#' model <- spark.survreg(df, Surv(futime, fustat) ~ ecog_ps + rx)
#'
#' # get a summary of the model
#' summary(model)
#'
#' # make predictions
#' predicted <- predict(model, df)
#' showDF(predicted)
#'
#' # save and load the model
#' path <- "path/to/model"
#' write.ml(model, path)
#' savedModel <- read.ml(path)
#' summary(savedModel)
#' }
#' @note spark.survreg since 2.0.0
setMethod("spark.survreg", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, aggregationDepth = 2,
                   stringIndexerOrderType = c("frequencyDesc", "frequencyAsc",
                                              "alphabetDesc", "alphabetAsc")) {
            stringIndexerOrderType <- match.arg(stringIndexerOrderType)
            formula <- paste(deparse(formula), collapse = "")
            jobj <- callJStatic("org.apache.spark.ml.r.AFTSurvivalRegressionWrapper",
                                "fit", formula, data@sdf, as.integer(aggregationDepth),
                                stringIndexerOrderType)
            new("AFTSurvivalRegressionModel", jobj = jobj)
          })

#  Returns a summary of the AFT survival regression model produced by spark.survreg,
#  similarly to R's summary().

#' @param object a fitted AFT survival regression model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes the model's \code{coefficients} (features, coefficients,
#'         intercept and log(scale)).
#' @rdname spark.survreg
#' @note summary(AFTSurvivalRegressionModel) since 2.0.0
setMethod("summary", signature(object = "AFTSurvivalRegressionModel"),
          function(object) {
            jobj <- object@jobj
            features <- callJMethod(jobj, "rFeatures")
            coefficients <- callJMethod(jobj, "rCoefficients")
            coefficients <- as.matrix(unlist(coefficients))
            colnames(coefficients) <- c("Value")
            rownames(coefficients) <- unlist(features)
            list(coefficients = coefficients)
          })

#  Makes predictions from an AFT survival regression model or a model produced by
#  spark.survreg, similarly to R package survival's predict.

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted values
#'         on the original scale of the data (mean predicted value at scale = 1.0).
#' @rdname spark.survreg
#' @note predict(AFTSurvivalRegressionModel) since 2.0.0
setMethod("predict", signature(object = "AFTSurvivalRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Saves the AFT survival regression model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#' @rdname spark.survreg
#' @note write.ml(AFTSurvivalRegressionModel, character) since 2.0.0
#' @seealso \link{write.ml}
setMethod("write.ml", signature(object = "AFTSurvivalRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })
