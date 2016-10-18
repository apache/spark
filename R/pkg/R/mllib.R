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

# Integration with R's standard functions.
# Most of MLlib's argorithms are provided in two flavours:
# - a specialization of the default R methods (glm). These methods try to respect
#   the inputs and the outputs of R's method to the largest extent, but some small differences
#   may exist.
# - a set of methods that reflect the arguments of the other languages supported by Spark. These
#   methods are prefixed with the `spark.` prefix: spark.glm, spark.kmeans, etc.

#' S4 class that represents a generalized linear model
#'
#' @param jobj a Java object reference to the backing Scala GeneralizedLinearRegressionWrapper
#' @export
#' @note GeneralizedLinearRegressionModel since 2.0.0
setClass("GeneralizedLinearRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a NaiveBayesModel
#'
#' @param jobj a Java object reference to the backing Scala NaiveBayesWrapper
#' @export
#' @note NaiveBayesModel since 2.0.0
setClass("NaiveBayesModel", representation(jobj = "jobj"))

#' S4 class that represents a AFTSurvivalRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala AFTSurvivalRegressionWrapper
#' @export
#' @note AFTSurvivalRegressionModel since 2.0.0
setClass("AFTSurvivalRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a KMeansModel
#'
#' @param jobj a Java object reference to the backing Scala KMeansModel
#' @export
#' @note KMeansModel since 2.0.0
setClass("KMeansModel", representation(jobj = "jobj"))

#' Saves the MLlib model to the input path
#'
#' Saves the MLlib model to the input path. For more information, see the specific
#' MLlib model below.
#' @rdname write.ml
#' @name write.ml
#' @export
#' @seealso \link{spark.glm}, \link{glm}
#' @seealso \link{spark.kmeans}, \link{spark.naiveBayes}, \link{spark.survreg}
#' @seealso \link{read.ml}
NULL

#' Makes predictions from a MLlib model
#'
#' Makes predictions from a MLlib model. For more information, see the specific
#' MLlib model below.
#' @rdname predict
#' @name predict
#' @export
#' @seealso \link{spark.glm}, \link{glm}
#' @seealso \link{spark.kmeans}, \link{spark.naiveBayes}, \link{spark.survreg}
NULL

#' Generalized Linear Models
#'
#' Fits generalized linear model against a Spark DataFrame.
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
#' @param tol positive convergence tolerance of iterations.
#' @param maxIter integer giving the maximal number of IRLS iterations.
#' @param ... additional arguments passed to the method.
#' @aliases spark.glm,SparkDataFrame,formula-method
#' @return \code{spark.glm} returns a fitted generalized linear model
#' @rdname spark.glm
#' @name spark.glm
#' @export
#' @examples
#' \dontrun{
#' sparkR.session()
#' data(iris)
#' df <- createDataFrame(iris)
#' model <- spark.glm(df, Sepal_Length ~ Sepal_Width, family = "gaussian")
#' summary(model)
#'
#' # fitted values on training data
#' fitted <- predict(model, df)
#' head(select(fitted, "Sepal_Length", "prediction"))
#'
#' # save fitted model to input path
#' path <- "path/to/model"
#' write.ml(model, path)
#'
#' # can also read back the saved model and print
#' savedModel <- read.ml(path)
#' summary(savedModel)
#' }
#' @note spark.glm since 2.0.0
#' @seealso \link{glm}, \link{read.ml}
setMethod("spark.glm", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, family = gaussian, tol = 1e-6, maxIter = 25) {
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
                                tol, as.integer(maxIter))
            return(new("GeneralizedLinearRegressionModel", jobj = jobj))
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
#' @param epsilon positive convergence tolerance of iterations.
#' @param maxit integer giving the maximal number of IRLS iterations.
#' @return \code{glm} returns a fitted generalized linear model.
#' @rdname glm
#' @export
#' @examples
#' \dontrun{
#' sparkR.session()
#' data(iris)
#' df <- createDataFrame(iris)
#' model <- glm(Sepal_Length ~ Sepal_Width, df, family = "gaussian")
#' summary(model)
#' }
#' @note glm since 1.5.0
#' @seealso \link{spark.glm}
setMethod("glm", signature(formula = "formula", family = "ANY", data = "SparkDataFrame"),
          function(formula, family = gaussian, data, epsilon = 1e-6, maxit = 25) {
            spark.glm(data, formula, family, tol = epsilon, maxIter = maxit)
          })

#  Returns the summary of a model produced by glm() or spark.glm(), similarly to R's summary().

#' @param object a fitted generalized linear model.
#' @return \code{summary} returns a summary object of the fitted model, a list of components
#'         including at least the coefficients, null/residual deviance, null/residual degrees
#'         of freedom, AIC and number of iterations IRLS takes.
#'
#' @rdname spark.glm
#' @export
#' @note summary(GeneralizedLinearRegressionModel) since 2.0.0
setMethod("summary", signature(object = "GeneralizedLinearRegressionModel"),
          function(object, ...) {
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            features <- callJMethod(jobj, "rFeatures")
            coefficients <- callJMethod(jobj, "rCoefficients")
            dispersion <- callJMethod(jobj, "rDispersion")
            null.deviance <- callJMethod(jobj, "rNullDeviance")
            deviance <- callJMethod(jobj, "rDeviance")
            df.null <- callJMethod(jobj, "rResidualDegreeOfFreedomNull")
            df.residual <- callJMethod(jobj, "rResidualDegreeOfFreedom")
            aic <- callJMethod(jobj, "rAic")
            iter <- callJMethod(jobj, "rNumIterations")
            family <- callJMethod(jobj, "rFamily")
            deviance.resid <- if (is.loaded) {
              NULL
            } else {
              dataFrame(callJMethod(jobj, "rDevianceResiduals"))
            }
            coefficients <- matrix(coefficients, ncol = 4)
            colnames(coefficients) <- c("Estimate", "Std. Error", "t value", "Pr(>|t|)")
            rownames(coefficients) <- unlist(features)
            ans <- list(deviance.resid = deviance.resid, coefficients = coefficients,
                        dispersion = dispersion, null.deviance = null.deviance,
                        deviance = deviance, df.null = df.null, df.residual = df.residual,
                        aic = aic, iter = iter, family = family, is.loaded = is.loaded)
            class(ans) <- "summary.GeneralizedLinearRegressionModel"
            return(ans)
          })

#  Prints the summary of GeneralizedLinearRegressionModel

#' @rdname spark.glm
#' @param x summary object of fitted generalized linear model returned by \code{summary} function
#' @export
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
    "Number of Fisher Scoring iterations: ", x$iter, "\n", sep = "")
  cat("\n")
  invisible(x)
  }

#  Makes predictions from a generalized linear model produced by glm() or spark.glm(),
#  similarly to R's predict().

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labels in a column named
#'         "prediction"
#' @rdname spark.glm
#' @export
#' @note predict(GeneralizedLinearRegressionModel) since 1.5.0
setMethod("predict", signature(object = "GeneralizedLinearRegressionModel"),
          function(object, newData) {
            return(dataFrame(callJMethod(object@jobj, "transform", newData@sdf)))
          })

# Makes predictions from a naive Bayes model or a model produced by spark.naiveBayes(),
# similarly to R package e1071's predict.

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labeled in a column named
#' "prediction"
#' @rdname spark.naiveBayes
#' @export
#' @note predict(NaiveBayesModel) since 2.0.0
setMethod("predict", signature(object = "NaiveBayesModel"),
          function(object, newData) {
            return(dataFrame(callJMethod(object@jobj, "transform", newData@sdf)))
          })

# Returns the summary of a naive Bayes model produced by \code{spark.naiveBayes}

#' @param object a naive Bayes model fitted by \code{spark.naiveBayes}.
#' @return \code{summary} returns a list containing \code{apriori}, the label distribution, and
#'         \code{tables}, conditional probabilities given the target label.
#' @rdname spark.naiveBayes
#' @export
#' @note summary(NaiveBayesModel) since 2.0.0
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

#' K-Means Clustering Model
#'
#' Fits a k-means clustering model against a Spark DataFrame, similarly to R's kmeans().
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#'                Note that the response variable of formula is empty in spark.kmeans.
#' @param k number of centers.
#' @param maxIter maximum iteration number.
#' @param initMode the initialization algorithm choosen to fit the model.
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.kmeans} returns a fitted k-means model.
#' @rdname spark.kmeans
#' @aliases spark.kmeans,SparkDataFrame,formula-method
#' @name spark.kmeans
#' @export
#' @examples
#' \dontrun{
#' sparkR.session()
#' data(iris)
#' df <- createDataFrame(iris)
#' model <- spark.kmeans(df, Sepal_Length ~ Sepal_Width, k = 4, initMode = "random")
#' summary(model)
#'
#' # fitted values on training data
#' fitted <- predict(model, df)
#' head(select(fitted, "Sepal_Length", "prediction"))
#'
#' # save fitted model to input path
#' path <- "path/to/model"
#' write.ml(model, path)
#'
#' # can also read back the saved model and print
#' savedModel <- read.ml(path)
#' summary(savedModel)
#' }
#' @note spark.kmeans since 2.0.0
#' @seealso \link{predict}, \link{read.ml}, \link{write.ml}
setMethod("spark.kmeans", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, k = 2, maxIter = 20, initMode = c("k-means||", "random")) {
            formula <- paste(deparse(formula), collapse = "")
            initMode <- match.arg(initMode)
            jobj <- callJStatic("org.apache.spark.ml.r.KMeansWrapper", "fit", data@sdf, formula,
                                as.integer(k), as.integer(maxIter), initMode)
            return(new("KMeansModel", jobj = jobj))
          })

#' Get fitted result from a k-means model
#'
#' Get fitted result from a k-means model, similarly to R's fitted().
#' Note: A saved-loaded model does not support this method.
#'
#' @param object a fitted k-means model.
#' @param method type of fitted results, \code{"centers"} for cluster centers
#'        or \code{"classes"} for assigned classes.
#' @param ... additional argument(s) passed to the method.
#' @return \code{fitted} returns a SparkDataFrame containing fitted values.
#' @rdname fitted
#' @export
#' @examples
#' \dontrun{
#' model <- spark.kmeans(trainingData, ~ ., 2)
#' fitted.model <- fitted(model)
#' showDF(fitted.model)
#'}
#' @note fitted since 2.0.0
setMethod("fitted", signature(object = "KMeansModel"),
          function(object, method = c("centers", "classes"), ...) {
            method <- match.arg(method)
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            if (is.loaded) {
              stop(paste("Saved-loaded k-means model does not support 'fitted' method"))
            } else {
              return(dataFrame(callJMethod(jobj, "fitted", method)))
            }
          })

#  Get the summary of a k-means model

#' @param object a fitted k-means model.
#' @return \code{summary} returns the model's coefficients, size and cluster.
#' @rdname spark.kmeans
#' @export
#' @note summary(KMeansModel) since 2.0.0
setMethod("summary", signature(object = "KMeansModel"),
          function(object, ...) {
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            features <- callJMethod(jobj, "features")
            coefficients <- callJMethod(jobj, "coefficients")
            k <- callJMethod(jobj, "k")
            size <- callJMethod(jobj, "size")
            coefficients <- t(matrix(coefficients, ncol = k))
            colnames(coefficients) <- unlist(features)
            rownames(coefficients) <- 1:k
            cluster <- if (is.loaded) {
              NULL
            } else {
              dataFrame(callJMethod(jobj, "cluster"))
            }
            return(list(coefficients = coefficients, size = size,
                   cluster = cluster, is.loaded = is.loaded))
          })

#  Predicted values based on a k-means model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns the predicted values based on a k-means model.
#' @rdname spark.kmeans
#' @export
#' @note predict(KMeansModel) since 2.0.0
setMethod("predict", signature(object = "KMeansModel"),
          function(object, newData) {
            return(dataFrame(callJMethod(object@jobj, "transform", newData@sdf)))
          })

#' Naive Bayes Models
#'
#' \code{spark.naiveBayes} fits a Bernoulli naive Bayes model against a SparkDataFrame.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#' Only categorical data is supported.
#'
#' @param data a \code{SparkDataFrame} of observations and labels for model fitting.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'               operators are supported, including '~', '.', ':', '+', and '-'.
#' @param smoothing smoothing parameter.
#' @param ... additional argument(s) passed to the method. Currently only \code{smoothing}.
#' @return \code{spark.naiveBayes} returns a fitted naive Bayes model.
#' @rdname spark.naiveBayes
#' @aliases spark.naiveBayes,SparkDataFrame,formula-method
#' @name spark.naiveBayes
#' @seealso e1071: \url{https://cran.r-project.org/package=e1071}
#' @export
#' @examples
#' \dontrun{
#' data <- as.data.frame(UCBAdmissions)
#' df <- createDataFrame(data)
#'
#' # fit a Bernoulli naive Bayes model
#' model <- spark.naiveBayes(df, Admit ~ Gender + Dept, smoothing = 0)
#'
#' # get the summary of the model
#' summary(model)
#'
#' # make predictions
#' predictions <- predict(model, df)
#'
#' # save and load the model
#' path <- "path/to/model"
#' write.ml(model, path)
#' savedModel <- read.ml(path)
#' summary(savedModel)
#' }
#' @note spark.naiveBayes since 2.0.0
setMethod("spark.naiveBayes", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, smoothing = 1.0, ...) {
            formula <- paste(deparse(formula), collapse = "")
            jobj <- callJStatic("org.apache.spark.ml.r.NaiveBayesWrapper", "fit",
            formula, data@sdf, smoothing)
            return(new("NaiveBayesModel", jobj = jobj))
          })

# Saves the Bernoulli naive Bayes model to the input path.

#' @param path the directory where the model is saved
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.naiveBayes
#' @export
#' @seealso \link{read.ml}
#' @note write.ml(NaiveBayesModel, character) since 2.0.0
setMethod("write.ml", signature(object = "NaiveBayesModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            writer <- callJMethod(object@jobj, "write")
            if (overwrite) {
              writer <- callJMethod(writer, "overwrite")
            }
            invisible(callJMethod(writer, "save", path))
          })

# Saves the AFT survival regression model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#' @rdname spark.survreg
#' @export
#' @note write.ml(AFTSurvivalRegressionModel, character) since 2.0.0
#' @seealso \link{read.ml}
setMethod("write.ml", signature(object = "AFTSurvivalRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            writer <- callJMethod(object@jobj, "write")
            if (overwrite) {
              writer <- callJMethod(writer, "overwrite")
            }
            invisible(callJMethod(writer, "save", path))
          })

#  Saves the generalized linear model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.glm
#' @export
#' @note write.ml(GeneralizedLinearRegressionModel, character) since 2.0.0
setMethod("write.ml", signature(object = "GeneralizedLinearRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            writer <- callJMethod(object@jobj, "write")
            if (overwrite) {
              writer <- callJMethod(writer, "overwrite")
            }
            invisible(callJMethod(writer, "save", path))
          })

#  Save fitted MLlib model to the input path

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.kmeans
#' @export
#' @note write.ml(KMeansModel, character) since 2.0.0
setMethod("write.ml", signature(object = "KMeansModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            writer <- callJMethod(object@jobj, "write")
            if (overwrite) {
              writer <- callJMethod(writer, "overwrite")
            }
            invisible(callJMethod(writer, "save", path))
          })

#' Load a fitted MLlib model from the input path.
#'
#' @param path path of the model to read.
#' @return A fitted MLlib model.
#' @rdname read.ml
#' @name read.ml
#' @export
#' @seealso \link{write.ml}
#' @examples
#' \dontrun{
#' path <- "path/to/model"
#' model <- read.ml(path)
#' }
#' @note read.ml since 2.0.0
read.ml <- function(path) {
  path <- suppressWarnings(normalizePath(path))
  jobj <- callJStatic("org.apache.spark.ml.r.RWrappers", "load", path)
  if (isInstanceOf(jobj, "org.apache.spark.ml.r.NaiveBayesWrapper")) {
    return(new("NaiveBayesModel", jobj = jobj))
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.AFTSurvivalRegressionWrapper")) {
    return(new("AFTSurvivalRegressionModel", jobj = jobj))
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.GeneralizedLinearRegressionWrapper")) {
      return(new("GeneralizedLinearRegressionModel", jobj = jobj))
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.KMeansWrapper")) {
      return(new("KMeansModel", jobj = jobj))
  } else {
    stop(paste("Unsupported model: ", jobj))
  }
}

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
#' @return \code{spark.survreg} returns a fitted AFT survival regression model.
#' @rdname spark.survreg
#' @seealso survival: \url{https://cran.r-project.org/package=survival}
#' @export
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
          function(data, formula) {
            formula <- paste(deparse(formula), collapse = "")
            jobj <- callJStatic("org.apache.spark.ml.r.AFTSurvivalRegressionWrapper",
                                "fit", formula, data@sdf)
            return(new("AFTSurvivalRegressionModel", jobj = jobj))
          })

# Returns a summary of the AFT survival regression model produced by spark.survreg,
# similarly to R's summary().

#' @param object a fitted AFT survival regression model.
#' @return \code{summary} returns a list containing the model's coefficients,
#' intercept and log(scale)
#' @rdname spark.survreg
#' @export
#' @note summary(AFTSurvivalRegressionModel) since 2.0.0
setMethod("summary", signature(object = "AFTSurvivalRegressionModel"),
          function(object) {
            jobj <- object@jobj
            features <- callJMethod(jobj, "rFeatures")
            coefficients <- callJMethod(jobj, "rCoefficients")
            coefficients <- as.matrix(unlist(coefficients))
            colnames(coefficients) <- c("Value")
            rownames(coefficients) <- unlist(features)
            return(list(coefficients = coefficients))
          })

# Makes predictions from an AFT survival regression model or a model produced by
# spark.survreg, similarly to R package survival's predict.

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted values
#' on the original scale of the data (mean predicted value at scale = 1.0).
#' @rdname spark.survreg
#' @export
#' @note predict(AFTSurvivalRegressionModel) since 2.0.0
setMethod("predict", signature(object = "AFTSurvivalRegressionModel"),
          function(object, newData) {
            return(dataFrame(callJMethod(object@jobj, "transform", newData@sdf)))
          })
