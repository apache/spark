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

#' S4 class that represents an LDAModel
#'
#' @param jobj a Java object reference to the backing Scala LDAWrapper
#' @export
#' @note LDAModel since 2.1.0
setClass("LDAModel", representation(jobj = "jobj"))

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

#' S4 class that represents a MultilayerPerceptronClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala MultilayerPerceptronClassifierWrapper
#' @export
#' @note MultilayerPerceptronClassificationModel since 2.1.0
setClass("MultilayerPerceptronClassificationModel", representation(jobj = "jobj"))

#' S4 class that represents an IsotonicRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala IsotonicRegressionModel
#' @export
#' @note IsotonicRegressionModel since 2.1.0
setClass("IsotonicRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a GaussianMixtureModel
#'
#' @param jobj a Java object reference to the backing Scala GaussianMixtureModel
#' @export
#' @note GaussianMixtureModel since 2.1.0
setClass("GaussianMixtureModel", representation(jobj = "jobj"))

#' S4 class that represents an ALSModel
#'
#' @param jobj a Java object reference to the backing Scala ALSWrapper
#' @export
#' @note ALSModel since 2.1.0
setClass("ALSModel", representation(jobj = "jobj"))

#' S4 class that represents an KSTest
#'
#' @param jobj a Java object reference to the backing Scala KSTestWrapper
#' @export
#' @note KSTest since 2.1.0
setClass("KSTest", representation(jobj = "jobj"))

#' S4 class that represents an LogisticRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala LogisticRegressionModel
#' @export
#' @note LogisticRegressionModel since 2.1.0
setClass("LogisticRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a RandomForestRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala RandomForestRegressionModel
#' @export
#' @note RandomForestRegressionModel since 2.1.0
setClass("RandomForestRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a RandomForestClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala RandomForestClassificationModel
#' @export
#' @note RandomForestClassificationModel since 2.1.0
setClass("RandomForestClassificationModel", representation(jobj = "jobj"))

#' S4 class that represents a GBTRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala GBTRegressionModel
#' @export
#' @note GBTRegressionModel since 2.1.0
setClass("GBTRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a GBTClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala GBTClassificationModel
#' @export
#' @note GBTClassificationModel since 2.1.0
setClass("GBTClassificationModel", representation(jobj = "jobj"))

#' Saves the MLlib model to the input path
#'
#' Saves the MLlib model to the input path. For more information, see the specific
#' MLlib model below.
#' @rdname write.ml
#' @name write.ml
#' @export
#' @seealso \link{spark.glm}, \link{glm},
#' @seealso \link{spark.als}, \link{spark.gaussianMixture}, \link{spark.gbt}, \link{spark.isoreg},
#' @seealso \link{spark.kmeans},
#' @seealso \link{spark.lda}, \link{spark.logit}, \link{spark.mlp}, \link{spark.naiveBayes},
#' @seealso \link{spark.randomForest}, \link{spark.survreg},
#' @seealso \link{read.ml}
NULL

#' Makes predictions from a MLlib model
#'
#' Makes predictions from a MLlib model. For more information, see the specific
#' MLlib model below.
#' @rdname predict
#' @name predict
#' @export
#' @seealso \link{spark.glm}, \link{glm},
#' @seealso \link{spark.als}, \link{spark.gaussianMixture}, \link{spark.gbt}, \link{spark.isoreg},
#' @seealso \link{spark.kmeans},
#' @seealso \link{spark.logit}, \link{spark.mlp}, \link{spark.naiveBayes},
#' @seealso \link{spark.randomForest}, \link{spark.survreg}
NULL

write_internal <- function(object, path, overwrite = FALSE) {
  writer <- callJMethod(object@jobj, "write")
  if (overwrite) {
    writer <- callJMethod(writer, "overwrite")
  }
  invisible(callJMethod(writer, "save", path))
}

predict_internal <- function(object, newData) {
  dataFrame(callJMethod(object@jobj, "transform", newData@sdf))
}

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
#'               \code{Gamma}, and \code{poisson}.
#' @param tol positive convergence tolerance of iterations.
#' @param maxIter integer giving the maximal number of IRLS iterations.
#' @param weightCol the weight column name. If this is not set or \code{NULL}, we treat all instance
#'                  weights as 1.0.
#' @param regParam regularization parameter for L2 regularization.
#' @param ... additional arguments passed to the method.
#' @aliases spark.glm,SparkDataFrame,formula-method
#' @return \code{spark.glm} returns a fitted generalized linear model.
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
          function(data, formula, family = gaussian, tol = 1e-6, maxIter = 25, weightCol = NULL,
                   regParam = 0.0) {
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
            if (is.null(weightCol)) {
              weightCol <- ""
            }

            # For known families, Gamma is upper-cased
            jobj <- callJStatic("org.apache.spark.ml.r.GeneralizedLinearRegressionWrapper",
                                "fit", formula, data@sdf, tolower(family$family), family$link,
                                tol, as.integer(maxIter), as.character(weightCol), regParam)
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
#'               \code{Gamma}, and \code{poisson}.
#' @param weightCol the weight column name. If this is not set or \code{NULL}, we treat all instance
#'                  weights as 1.0.
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
          function(formula, family = gaussian, data, epsilon = 1e-6, maxit = 25, weightCol = NULL) {
            spark.glm(data, formula, family, tol = epsilon, maxIter = maxit, weightCol = weightCol)
          })

#  Returns the summary of a model produced by glm() or spark.glm(), similarly to R's summary().

#' @param object a fitted generalized linear model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list of components includes at least the \code{coefficients} (coefficients matrix, which includes
#'         coefficients, standard error of coefficients, t value and p value),
#'         \code{null.deviance} (null/residual degrees of freedom), \code{aic} (AIC)
#'         and \code{iter} (number of iterations IRLS takes). If there are collinear columns in the data,
#'         the coefficients matrix only provides coefficients.
#' @rdname spark.glm
#' @export
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
            aic <- callJMethod(jobj, "rAic")
            iter <- callJMethod(jobj, "rNumIterations")
            family <- callJMethod(jobj, "rFamily")
            deviance.resid <- if (is.loaded) {
              NULL
            } else {
              dataFrame(callJMethod(jobj, "rDevianceResiduals"))
            }
            # If the underlying WeightedLeastSquares using "normal" solver, we can provide
            # coefficients, standard error of coefficients, t value and p value. Otherwise,
            # it will be fitted by local "l-bfgs", we can only provide coefficients.
            if (length(features) == length(coefficients)) {
              coefficients <- matrix(coefficients, ncol = 1)
              colnames(coefficients) <- c("Estimate")
              rownames(coefficients) <- unlist(features)
            } else {
              coefficients <- matrix(coefficients, ncol = 4)
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
    "Number of Fisher Scoring iterations: ", x$iter, "\n\n", sep = "")
  invisible(x)
  }

#  Makes predictions from a generalized linear model produced by glm() or spark.glm(),
#  similarly to R's predict().

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labels in a column named
#'         "prediction".
#' @rdname spark.glm
#' @export
#' @note predict(GeneralizedLinearRegressionModel) since 1.5.0
setMethod("predict", signature(object = "GeneralizedLinearRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

# Makes predictions from a naive Bayes model or a model produced by spark.naiveBayes(),
# similarly to R package e1071's predict.

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labeled in a column named
#' "prediction".
#' @rdname spark.naiveBayes
#' @export
#' @note predict(NaiveBayesModel) since 2.0.0
setMethod("predict", signature(object = "NaiveBayesModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

# Returns the summary of a naive Bayes model produced by \code{spark.naiveBayes}

#' @param object a naive Bayes model fitted by \code{spark.naiveBayes}.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{apriori} (the label distribution) and
#'         \code{tables} (conditional probabilities given the target label).
#' @rdname spark.naiveBayes
#' @export
#' @note summary(NaiveBayesModel) since 2.0.0
setMethod("summary", signature(object = "NaiveBayesModel"),
          function(object) {
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
            list(apriori = apriori, tables = tables)
          })

# Returns posterior probabilities from a Latent Dirichlet Allocation model produced by spark.lda()

#' @param newData A SparkDataFrame for testing.
#' @return \code{spark.posterior} returns a SparkDataFrame containing posterior probabilities
#'         vectors named "topicDistribution".
#' @rdname spark.lda
#' @aliases spark.posterior,LDAModel,SparkDataFrame-method
#' @export
#' @note spark.posterior(LDAModel) since 2.1.0
setMethod("spark.posterior", signature(object = "LDAModel", newData = "SparkDataFrame"),
          function(object, newData) {
            predict_internal(object, newData)
          })

# Returns the summary of a Latent Dirichlet Allocation model produced by \code{spark.lda}

#' @param object A Latent Dirichlet Allocation model fitted by \code{spark.lda}.
#' @param maxTermsPerTopic Maximum number of terms to collect for each topic. Default value of 10.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes
#'         \item{\code{docConcentration}}{concentration parameter commonly named \code{alpha} for
#'               the prior placed on documents distributions over topics \code{theta}}
#'         \item{\code{topicConcentration}}{concentration parameter commonly named \code{beta} or
#'               \code{eta} for the prior placed on topic distributions over terms}
#'         \item{\code{logLikelihood}}{log likelihood of the entire corpus}
#'         \item{\code{logPerplexity}}{log perplexity}
#'         \item{\code{isDistributed}}{TRUE for distributed model while FALSE for local model}
#'         \item{\code{vocabSize}}{number of terms in the corpus}
#'         \item{\code{topics}}{top 10 terms and their weights of all topics}
#'         \item{\code{vocabulary}}{whole terms of the training corpus, NULL if libsvm format file
#'               used as training set}
#' @rdname spark.lda
#' @aliases summary,LDAModel-method
#' @export
#' @note summary(LDAModel) since 2.1.0
setMethod("summary", signature(object = "LDAModel"),
          function(object, maxTermsPerTopic) {
            maxTermsPerTopic <- as.integer(ifelse(missing(maxTermsPerTopic), 10, maxTermsPerTopic))
            jobj <- object@jobj
            docConcentration <- callJMethod(jobj, "docConcentration")
            topicConcentration <- callJMethod(jobj, "topicConcentration")
            logLikelihood <- callJMethod(jobj, "logLikelihood")
            logPerplexity <- callJMethod(jobj, "logPerplexity")
            isDistributed <- callJMethod(jobj, "isDistributed")
            vocabSize <- callJMethod(jobj, "vocabSize")
            topics <- dataFrame(callJMethod(jobj, "topics", maxTermsPerTopic))
            vocabulary <- callJMethod(jobj, "vocabulary")
            list(docConcentration = unlist(docConcentration),
                 topicConcentration = topicConcentration,
                 logLikelihood = logLikelihood, logPerplexity = logPerplexity,
                 isDistributed = isDistributed, vocabSize = vocabSize,
                 topics = topics, vocabulary = unlist(vocabulary))
          })

# Returns the log perplexity of a Latent Dirichlet Allocation model produced by \code{spark.lda}

#' @return \code{spark.perplexity} returns the log perplexity of given SparkDataFrame, or the log
#'         perplexity of the training data if missing argument "data".
#' @rdname spark.lda
#' @aliases spark.perplexity,LDAModel-method
#' @export
#' @note spark.perplexity(LDAModel) since 2.1.0
setMethod("spark.perplexity", signature(object = "LDAModel", data = "SparkDataFrame"),
          function(object, data) {
            ifelse(missing(data), callJMethod(object@jobj, "logPerplexity"),
                   callJMethod(object@jobj, "computeLogPerplexity", data@sdf))
         })

# Saves the Latent Dirichlet Allocation model to the input path.

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.lda
#' @aliases write.ml,LDAModel,character-method
#' @export
#' @seealso \link{read.ml}
#' @note write.ml(LDAModel, character) since 2.1.0
setMethod("write.ml", signature(object = "LDAModel", path = "character"),
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
#' @export
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

            if (is.null(weightCol)) {
              weightCol <- ""
            }

            jobj <- callJStatic("org.apache.spark.ml.r.IsotonicRegressionWrapper", "fit",
                                data@sdf, formula, as.logical(isotonic), as.integer(featureIndex),
                                as.character(weightCol))
            new("IsotonicRegressionModel", jobj = jobj)
          })

#  Predicted values based on an isotonicRegression model

#' @param object a fitted IsotonicRegressionModel.
#' @param newData SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted values.
#' @rdname spark.isoreg
#' @aliases predict,IsotonicRegressionModel,SparkDataFrame-method
#' @export
#' @note predict(IsotonicRegressionModel) since 2.1.0
setMethod("predict", signature(object = "IsotonicRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Get the summary of an IsotonicRegressionModel model

#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes model's \code{boundaries} (boundaries in increasing order)
#'         and \code{predictions} (predictions associated with the boundaries at the same index).
#' @rdname spark.isoreg
#' @aliases summary,IsotonicRegressionModel-method
#' @export
#' @note summary(IsotonicRegressionModel) since 2.1.0
setMethod("summary", signature(object = "IsotonicRegressionModel"),
          function(object) {
            jobj <- object@jobj
            boundaries <- callJMethod(jobj, "boundaries")
            predictions <- callJMethod(jobj, "predictions")
            list(boundaries = boundaries, predictions = predictions)
          })

#' K-Means Clustering Model
#'
#' Fits a k-means clustering model against a SparkDataFrame, similarly to R's kmeans().
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
#' @param seed the random seed for cluster initialization
#' @param initSteps the number of steps for the k-means|| initialization mode.
#'                  This is an advanced setting, the default of 2 is almost always enough. Must be > 0.
#' @param tol convergence tolerance of iterations.
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
          function(data, formula, k = 2, maxIter = 20, initMode = c("k-means||", "random"),
                   seed = NULL, initSteps = 2, tol = 1E-4) {
            formula <- paste(deparse(formula), collapse = "")
            initMode <- match.arg(initMode)
            if (!is.null(seed)) {
              seed <- as.character(as.integer(seed))
            }
            jobj <- callJStatic("org.apache.spark.ml.r.KMeansWrapper", "fit", data@sdf, formula,
                                as.integer(k), as.integer(maxIter), initMode, seed,
                                as.integer(initSteps), as.numeric(tol))
            new("KMeansModel", jobj = jobj)
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
          function(object, method = c("centers", "classes")) {
            method <- match.arg(method)
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            if (is.loaded) {
              stop("Saved-loaded k-means model does not support 'fitted' method")
            } else {
              dataFrame(callJMethod(jobj, "fitted", method))
            }
          })

#  Get the summary of a k-means model

#' @param object a fitted k-means model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes the model's \code{k} (the configured number of cluster centers),
#'         \code{coefficients} (model cluster centers),
#'         \code{size} (number of data points in each cluster), \code{cluster}
#'         (cluster centers of the transformed data), {is.loaded} (whether the model is loaded
#'         from a saved file), and \code{clusterSize}
#'         (the actual number of cluster centers. When using initMode = "random",
#'         \code{clusterSize} may not equal to \code{k}).
#' @rdname spark.kmeans
#' @export
#' @note summary(KMeansModel) since 2.0.0
setMethod("summary", signature(object = "KMeansModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            features <- callJMethod(jobj, "features")
            coefficients <- callJMethod(jobj, "coefficients")
            k <- callJMethod(jobj, "k")
            size <- callJMethod(jobj, "size")
            clusterSize <- callJMethod(jobj, "clusterSize")
            coefficients <- t(matrix(coefficients, ncol = clusterSize))
            colnames(coefficients) <- unlist(features)
            rownames(coefficients) <- 1:clusterSize
            cluster <- if (is.loaded) {
              NULL
            } else {
              dataFrame(callJMethod(jobj, "cluster"))
            }
            list(k = k, coefficients = coefficients, size = size,
                 cluster = cluster, is.loaded = is.loaded, clusterSize = clusterSize)
          })

#  Predicted values based on a k-means model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns the predicted values based on a k-means model.
#' @rdname spark.kmeans
#' @export
#' @note predict(KMeansModel) since 2.0.0
setMethod("predict", signature(object = "KMeansModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' Logistic Regression Model
#'
#' Fits an logistic regression model against a SparkDataFrame. It supports "binomial": Binary logistic regression
#' with pivoting; "multinomial": Multinomial logistic (softmax) regression without pivoting, similar to glmnet.
#' Users can print, make predictions on the produced model and save the model to the input path.
#'
#' @param data SparkDataFrame for training.
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param regParam the regularization parameter.
#' @param elasticNetParam the ElasticNet mixing parameter. For alpha = 0.0, the penalty is an L2 penalty.
#'                        For alpha = 1.0, it is an L1 penalty. For 0.0 < alpha < 1.0, the penalty is a combination
#'                        of L1 and L2. Default is 0.0 which is an L2 penalty.
#' @param maxIter maximum iteration number.
#' @param tol convergence tolerance of iterations.
#' @param family the name of family which is a description of the label distribution to be used in the model.
#'               Supported options:
#'                 \itemize{
#'                   \item{"auto": Automatically select the family based on the number of classes:
#'                           If number of classes == 1 || number of classes == 2, set to "binomial".
#'                           Else, set to "multinomial".}
#'                   \item{"binomial": Binary logistic regression with pivoting.}
#'                   \item{"multinomial": Multinomial logistic (softmax) regression without pivoting.}
#'                 }
#' @param standardization whether to standardize the training features before fitting the model. The coefficients
#'                        of models will be always returned on the original scale, so it will be transparent for
#'                        users. Note that with/without standardization, the models should be always converged
#'                        to the same solution when no regularization is applied. Default is TRUE, same as glmnet.
#' @param thresholds in binary classification, in range [0, 1]. If the estimated probability of class label 1
#'                  is > threshold, then predict 1, else 0. A high threshold encourages the model to predict 0
#'                  more often; a low threshold encourages the model to predict 1 more often. Note: Setting this with
#'                  threshold p is equivalent to setting thresholds c(1-p, p). In multiclass (or binary) classification to adjust the probability of
#'                  predicting each class. Array must have length equal to the number of classes, with values > 0,
#'                  excepting that at most one value may be 0. The class with largest value p/t is predicted, where p
#'                  is the original probability of that class and t is the class's threshold.
#' @param weightCol The weight column name.
#' @param ... additional arguments passed to the method.
#' @return \code{spark.logit} returns a fitted logistic regression model.
#' @rdname spark.logit
#' @aliases spark.logit,SparkDataFrame,formula-method
#' @name spark.logit
#' @export
#' @examples
#' \dontrun{
#' sparkR.session()
#' # binary logistic regression
#' df <- createDataFrame(iris)
#' training <- df[df$Species %in% c("versicolor", "virginica"), ]
#' model <- spark.logit(training, Species ~ ., regParam = 0.5)
#' summary <- summary(model)
#'
#' # fitted values on training data
#' fitted <- predict(model, training)
#'
#' # save fitted model to input path
#' path <- "path/to/model"
#' write.ml(model, path)
#'
#' # can also read back the saved model and predict
#' # Note that summary deos not work on loaded model
#' savedModel <- read.ml(path)
#' summary(savedModel)
#'
#' # multinomial logistic regression
#'
#' df <- createDataFrame(iris)
#' model <- spark.logit(df, Species ~ ., regParam = 0.5)
#' summary <- summary(model)
#'
#' }
#' @note spark.logit since 2.1.0
setMethod("spark.logit", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, regParam = 0.0, elasticNetParam = 0.0, maxIter = 100,
                   tol = 1E-6, family = "auto", standardization = TRUE,
                   thresholds = 0.5, weightCol = NULL) {
            formula <- paste(deparse(formula), collapse = "")

            if (is.null(weightCol)) {
              weightCol <- ""
            }

            jobj <- callJStatic("org.apache.spark.ml.r.LogisticRegressionWrapper", "fit",
                                data@sdf, formula, as.numeric(regParam),
                                as.numeric(elasticNetParam), as.integer(maxIter),
                                as.numeric(tol), as.character(family),
                                as.logical(standardization), as.array(thresholds),
                                as.character(weightCol))
            new("LogisticRegressionModel", jobj = jobj)
          })

#  Predicted values based on an LogisticRegressionModel model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns the predicted values based on an LogisticRegressionModel.
#' @rdname spark.logit
#' @aliases predict,LogisticRegressionModel,SparkDataFrame-method
#' @export
#' @note predict(LogisticRegressionModel) since 2.1.0
setMethod("predict", signature(object = "LogisticRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Get the summary of an LogisticRegressionModel

#' @param object an LogisticRegressionModel fitted by \code{spark.logit}.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{coefficients} (coefficients matrix of the fitted model).
#' @rdname spark.logit
#' @aliases summary,LogisticRegressionModel-method
#' @export
#' @note summary(LogisticRegressionModel) since 2.1.0
setMethod("summary", signature(object = "LogisticRegressionModel"),
          function(object) {
            jobj <- object@jobj
            features <- callJMethod(jobj, "rFeatures")
            labels <- callJMethod(jobj, "labels")
            coefficients <- callJMethod(jobj, "rCoefficients")
            nCol <- length(coefficients) / length(features)
            coefficients <- matrix(coefficients, ncol = nCol)
            # If nCol == 1, means this is a binomial logistic regression model with pivoting.
            # Otherwise, it's a multinomial logistic regression model without pivoting.
            if (nCol == 1) {
              colnames(coefficients) <- c("Estimate")
            } else {
              colnames(coefficients) <- unlist(labels)
            }
            rownames(coefficients) <- unlist(features)

            list(coefficients = coefficients)
          })

#' Multilayer Perceptron Classification Model
#'
#' \code{spark.mlp} fits a multi-layer perceptron neural network model against a SparkDataFrame.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#' Only categorical data is supported.
#' For more details, see
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html}{
#'   Multilayer Perceptron}
#'
#' @param data a \code{SparkDataFrame} of observations and labels for model fitting.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param blockSize blockSize parameter.
#' @param layers integer vector containing the number of nodes for each layer.
#' @param solver solver parameter, supported options: "gd" (minibatch gradient descent) or "l-bfgs".
#' @param maxIter maximum iteration number.
#' @param tol convergence tolerance of iterations.
#' @param stepSize stepSize parameter.
#' @param seed seed parameter for weights initialization.
#' @param initialWeights initialWeights parameter for weights initialization, it should be a
#' numeric vector.
#' @param ... additional arguments passed to the method.
#' @return \code{spark.mlp} returns a fitted Multilayer Perceptron Classification Model.
#' @rdname spark.mlp
#' @aliases spark.mlp,SparkDataFrame,formula-method
#' @name spark.mlp
#' @seealso \link{read.ml}
#' @export
#' @examples
#' \dontrun{
#' df <- read.df("data/mllib/sample_multiclass_classification_data.txt", source = "libsvm")
#'
#' # fit a Multilayer Perceptron Classification Model
#' model <- spark.mlp(df, label ~ features, blockSize = 128, layers = c(4, 3), solver = "l-bfgs",
#'                    maxIter = 100, tol = 0.5, stepSize = 1, seed = 1,
#'                    initialWeights = c(0, 0, 0, 0, 0, 5, 5, 5, 5, 5, 9, 9, 9, 9, 9))
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
#' @note spark.mlp since 2.1.0
setMethod("spark.mlp", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, layers, blockSize = 128, solver = "l-bfgs", maxIter = 100,
                   tol = 1E-6, stepSize = 0.03, seed = NULL, initialWeights = NULL) {
            formula <- paste(deparse(formula), collapse = "")
            if (is.null(layers)) {
              stop ("layers must be a integer vector with length > 1.")
            }
            layers <- as.integer(na.omit(layers))
            if (length(layers) <= 1) {
              stop ("layers must be a integer vector with length > 1.")
            }
            if (!is.null(seed)) {
              seed <- as.character(as.integer(seed))
            }
            if (!is.null(initialWeights)) {
              initialWeights <- as.array(as.numeric(na.omit(initialWeights)))
            }
            jobj <- callJStatic("org.apache.spark.ml.r.MultilayerPerceptronClassifierWrapper",
                                "fit", data@sdf, formula, as.integer(blockSize), as.array(layers),
                                as.character(solver), as.integer(maxIter), as.numeric(tol),
                                as.numeric(stepSize), seed, initialWeights)
            new("MultilayerPerceptronClassificationModel", jobj = jobj)
          })

# Makes predictions from a model produced by spark.mlp().

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labeled in a column named
#' "prediction".
#' @rdname spark.mlp
#' @aliases predict,MultilayerPerceptronClassificationModel-method
#' @export
#' @note predict(MultilayerPerceptronClassificationModel) since 2.1.0
setMethod("predict", signature(object = "MultilayerPerceptronClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

# Returns the summary of a Multilayer Perceptron Classification Model produced by \code{spark.mlp}

#' @param object a Multilayer Perceptron Classification Model fitted by \code{spark.mlp}
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{numOfInputs} (number of inputs), \code{numOfOutputs}
#'         (number of outputs), \code{layers} (array of layer sizes including input
#'         and output layers), and \code{weights} (the weights of layers).
#'         For \code{weights}, it is a numeric vector with length equal to the expected
#'         given the architecture (i.e., for 8-10-2 network, 112 connection weights).
#' @rdname spark.mlp
#' @export
#' @aliases summary,MultilayerPerceptronClassificationModel-method
#' @note summary(MultilayerPerceptronClassificationModel) since 2.1.0
setMethod("summary", signature(object = "MultilayerPerceptronClassificationModel"),
          function(object) {
            jobj <- object@jobj
            layers <- unlist(callJMethod(jobj, "layers"))
            numOfInputs <- head(layers, n = 1)
            numOfOutputs <- tail(layers, n = 1)
            weights <- callJMethod(jobj, "weights")
            list(numOfInputs = numOfInputs, numOfOutputs = numOfOutputs,
                 layers = layers, weights = weights)
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
          function(data, formula, smoothing = 1.0) {
            formula <- paste(deparse(formula), collapse = "")
            jobj <- callJStatic("org.apache.spark.ml.r.NaiveBayesWrapper", "fit",
            formula, data@sdf, smoothing)
            new("NaiveBayesModel", jobj = jobj)
          })

# Saves the Bernoulli naive Bayes model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.naiveBayes
#' @export
#' @seealso \link{write.ml}
#' @note write.ml(NaiveBayesModel, character) since 2.0.0
setMethod("write.ml", signature(object = "NaiveBayesModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

# Saves the AFT survival regression model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#' @rdname spark.survreg
#' @export
#' @note write.ml(AFTSurvivalRegressionModel, character) since 2.0.0
#' @seealso \link{write.ml}
setMethod("write.ml", signature(object = "AFTSurvivalRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
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
            write_internal(object, path, overwrite)
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
            write_internal(object, path, overwrite)
          })

# Saves the Multilayer Perceptron Classification Model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.mlp
#' @aliases write.ml,MultilayerPerceptronClassificationModel,character-method
#' @export
#' @seealso \link{write.ml}
#' @note write.ml(MultilayerPerceptronClassificationModel, character) since 2.1.0
setMethod("write.ml", signature(object = "MultilayerPerceptronClassificationModel",
          path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#  Save fitted IsotonicRegressionModel to the input path

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.isoreg
#' @aliases write.ml,IsotonicRegressionModel,character-method
#' @export
#' @note write.ml(IsotonicRegression, character) since 2.1.0
setMethod("write.ml", signature(object = "IsotonicRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#  Save fitted LogisticRegressionModel to the input path

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.logit
#' @aliases write.ml,LogisticRegressionModel,character-method
#' @export
#' @note write.ml(LogisticRegression, character) since 2.1.0
setMethod("write.ml", signature(object = "LogisticRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#  Save fitted MLlib model to the input path

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @aliases write.ml,GaussianMixtureModel,character-method
#' @rdname spark.gaussianMixture
#' @export
#' @note write.ml(GaussianMixtureModel, character) since 2.1.0
setMethod("write.ml", signature(object = "GaussianMixtureModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
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
    new("NaiveBayesModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.AFTSurvivalRegressionWrapper")) {
    new("AFTSurvivalRegressionModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.GeneralizedLinearRegressionWrapper")) {
    new("GeneralizedLinearRegressionModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.KMeansWrapper")) {
    new("KMeansModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.LDAWrapper")) {
    new("LDAModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.MultilayerPerceptronClassifierWrapper")) {
    new("MultilayerPerceptronClassificationModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.IsotonicRegressionWrapper")) {
    new("IsotonicRegressionModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.GaussianMixtureWrapper")) {
    new("GaussianMixtureModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.ALSWrapper")) {
    new("ALSModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.LogisticRegressionWrapper")) {
    new("LogisticRegressionModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.RandomForestRegressorWrapper")) {
    new("RandomForestRegressionModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.RandomForestClassifierWrapper")) {
    new("RandomForestClassificationModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.GBTRegressorWrapper")) {
    new("GBTRegressionModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.GBTClassifierWrapper")) {
    new("GBTClassificationModel", jobj = jobj)
  } else {
    stop("Unsupported model: ", jobj)
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
            new("AFTSurvivalRegressionModel", jobj = jobj)
          })

#' Latent Dirichlet Allocation
#'
#' \code{spark.lda} fits a Latent Dirichlet Allocation model on a SparkDataFrame. Users can call
#' \code{summary} to get a summary of the fitted LDA model, \code{spark.posterior} to compute
#' posterior probabilities on new data, \code{spark.perplexity} to compute log perplexity on new
#' data and \code{write.ml}/\code{read.ml} to save/load fitted models.
#'
#' @param data A SparkDataFrame for training.
#' @param features Features column name. Either libSVM-format column or character-format column is
#'        valid.
#' @param k Number of topics.
#' @param maxIter Maximum iterations.
#' @param optimizer Optimizer to train an LDA model, "online" or "em", default is "online".
#' @param subsamplingRate (For online optimizer) Fraction of the corpus to be sampled and used in
#'        each iteration of mini-batch gradient descent, in range (0, 1].
#' @param topicConcentration concentration parameter (commonly named \code{beta} or \code{eta}) for
#'        the prior placed on topic distributions over terms, default -1 to set automatically on the
#'        Spark side. Use \code{summary} to retrieve the effective topicConcentration. Only 1-size
#'        numeric is accepted.
#' @param docConcentration concentration parameter (commonly named \code{alpha}) for the
#'        prior placed on documents distributions over topics (\code{theta}), default -1 to set
#'        automatically on the Spark side. Use \code{summary} to retrieve the effective
#'        docConcentration. Only 1-size or \code{k}-size numeric is accepted.
#' @param customizedStopWords stopwords that need to be removed from the given corpus. Ignore the
#'        parameter if libSVM-format column is used as the features column.
#' @param maxVocabSize maximum vocabulary size, default 1 << 18
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.lda} returns a fitted Latent Dirichlet Allocation model.
#' @rdname spark.lda
#' @aliases spark.lda,SparkDataFrame-method
#' @seealso topicmodels: \url{https://cran.r-project.org/package=topicmodels}
#' @export
#' @examples
#' \dontrun{
#' # nolint start
#' # An example "path/to/file" can be
#' # paste0(Sys.getenv("SPARK_HOME"), "/data/mllib/sample_lda_libsvm_data.txt")
#' # nolint end
#' text <- read.df("path/to/file", source = "libsvm")
#' model <- spark.lda(data = text, optimizer = "em")
#'
#' # get a summary of the model
#' summary(model)
#'
#' # compute posterior probabilities
#' posterior <- spark.posterior(model, text)
#' showDF(posterior)
#'
#' # compute perplexity
#' perplexity <- spark.perplexity(model, text)
#'
#' # save and load the model
#' path <- "path/to/model"
#' write.ml(model, path)
#' savedModel <- read.ml(path)
#' summary(savedModel)
#' }
#' @note spark.lda since 2.1.0
setMethod("spark.lda", signature(data = "SparkDataFrame"),
          function(data, features = "features", k = 10, maxIter = 20, optimizer = c("online", "em"),
                   subsamplingRate = 0.05, topicConcentration = -1, docConcentration = -1,
                   customizedStopWords = "", maxVocabSize = bitwShiftL(1, 18)) {
            optimizer <- match.arg(optimizer)
            jobj <- callJStatic("org.apache.spark.ml.r.LDAWrapper", "fit", data@sdf, features,
                                as.integer(k), as.integer(maxIter), optimizer,
                                as.numeric(subsamplingRate), topicConcentration,
                                as.array(docConcentration), as.array(customizedStopWords),
                                maxVocabSize)
            new("LDAModel", jobj = jobj)
          })

# Returns a summary of the AFT survival regression model produced by spark.survreg,
# similarly to R's summary().

#' @param object a fitted AFT survival regression model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes the model's \code{coefficients} (features, coefficients,
#'         intercept and log(scale)).
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
            list(coefficients = coefficients)
          })

# Makes predictions from an AFT survival regression model or a model produced by
# spark.survreg, similarly to R package survival's predict.

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted values
#'         on the original scale of the data (mean predicted value at scale = 1.0).
#' @rdname spark.survreg
#' @export
#' @note predict(AFTSurvivalRegressionModel) since 2.0.0
setMethod("predict", signature(object = "AFTSurvivalRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' Multivariate Gaussian Mixture Model (GMM)
#'
#' Fits multivariate gaussian mixture model against a SparkDataFrame, similarly to R's
#' mvnormalmixEM(). Users can call \code{summary} to print a summary of the fitted model,
#' \code{predict} to make predictions on new data, and \code{write.ml}/\code{read.ml}
#' to save/load fitted models.
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#'                Note that the response variable of formula is empty in spark.gaussianMixture.
#' @param k number of independent Gaussians in the mixture model.
#' @param maxIter maximum iteration number.
#' @param tol the convergence tolerance.
#' @param ... additional arguments passed to the method.
#' @aliases spark.gaussianMixture,SparkDataFrame,formula-method
#' @return \code{spark.gaussianMixture} returns a fitted multivariate gaussian mixture model.
#' @rdname spark.gaussianMixture
#' @name spark.gaussianMixture
#' @seealso mixtools: \url{https://cran.r-project.org/package=mixtools}
#' @export
#' @examples
#' \dontrun{
#' sparkR.session()
#' library(mvtnorm)
#' set.seed(100)
#' a <- rmvnorm(4, c(0, 0))
#' b <- rmvnorm(6, c(3, 4))
#' data <- rbind(a, b)
#' df <- createDataFrame(as.data.frame(data))
#' model <- spark.gaussianMixture(df, ~ V1 + V2, k = 2)
#' summary(model)
#'
#' # fitted values on training data
#' fitted <- predict(model, df)
#' head(select(fitted, "V1", "prediction"))
#'
#' # save fitted model to input path
#' path <- "path/to/model"
#' write.ml(model, path)
#'
#' # can also read back the saved model and print
#' savedModel <- read.ml(path)
#' summary(savedModel)
#' }
#' @note spark.gaussianMixture since 2.1.0
#' @seealso \link{predict}, \link{read.ml}, \link{write.ml}
setMethod("spark.gaussianMixture", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, k = 2, maxIter = 100, tol = 0.01) {
            formula <- paste(deparse(formula), collapse = "")
            jobj <- callJStatic("org.apache.spark.ml.r.GaussianMixtureWrapper", "fit", data@sdf,
                                formula, as.integer(k), as.integer(maxIter), as.numeric(tol))
            new("GaussianMixtureModel", jobj = jobj)
          })

#  Get the summary of a multivariate gaussian mixture model

#' @param object a fitted gaussian mixture model.
#' @return \code{summary} returns summary of the fitted model, which is a list.
#'         The list includes the model's \code{lambda} (lambda), \code{mu} (mu),
#'         \code{sigma} (sigma), and \code{posterior} (posterior).
#' @aliases spark.gaussianMixture,SparkDataFrame,formula-method
#' @rdname spark.gaussianMixture
#' @export
#' @note summary(GaussianMixtureModel) since 2.1.0
setMethod("summary", signature(object = "GaussianMixtureModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            lambda <- unlist(callJMethod(jobj, "lambda"))
            muList <- callJMethod(jobj, "mu")
            sigmaList <- callJMethod(jobj, "sigma")
            k <- callJMethod(jobj, "k")
            dim <- callJMethod(jobj, "dim")
            mu <- c()
            for (i in 1 : k) {
              start <- (i - 1) * dim + 1
              end <- i * dim
              mu[[i]] <- unlist(muList[start : end])
            }
            sigma <- c()
            for (i in 1 : k) {
              start <- (i - 1) * dim * dim + 1
              end <- i * dim * dim
              sigma[[i]] <- t(matrix(sigmaList[start : end], ncol = dim))
            }
            posterior <- if (is.loaded) {
              NULL
            } else {
              dataFrame(callJMethod(jobj, "posterior"))
            }
            list(lambda = lambda, mu = mu, sigma = sigma,
                 posterior = posterior, is.loaded = is.loaded)
          })

#  Predicted values based on a gaussian mixture model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labels in a column named
#'         "prediction".
#' @aliases predict,GaussianMixtureModel,SparkDataFrame-method
#' @rdname spark.gaussianMixture
#' @export
#' @note predict(GaussianMixtureModel) since 2.1.0
setMethod("predict", signature(object = "GaussianMixtureModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' Alternating Least Squares (ALS) for Collaborative Filtering
#'
#' \code{spark.als} learns latent factors in collaborative filtering via alternating least
#' squares. Users can call \code{summary} to obtain fitted latent factors, \code{predict}
#' to make predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#'
#' For more details, see
#' \href{http://spark.apache.org/docs/latest/ml-collaborative-filtering.html}{MLlib:
#' Collaborative Filtering}.
#'
#' @param data a SparkDataFrame for training.
#' @param ratingCol column name for ratings.
#' @param userCol column name for user ids. Ids must be (or can be coerced into) integers.
#' @param itemCol column name for item ids. Ids must be (or can be coerced into) integers.
#' @param rank rank of the matrix factorization (> 0).
#' @param regParam regularization parameter (>= 0).
#' @param maxIter maximum number of iterations (>= 0).
#' @param nonnegative logical value indicating whether to apply nonnegativity constraints.
#' @param implicitPrefs logical value indicating whether to use implicit preference.
#' @param alpha alpha parameter in the implicit preference formulation (>= 0).
#' @param seed integer seed for random number generation.
#' @param numUserBlocks number of user blocks used to parallelize computation (> 0).
#' @param numItemBlocks number of item blocks used to parallelize computation (> 0).
#' @param checkpointInterval number of checkpoint intervals (>= 1) or disable checkpoint (-1).
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.als} returns a fitted ALS model.
#' @rdname spark.als
#' @aliases spark.als,SparkDataFrame-method
#' @name spark.als
#' @export
#' @examples
#' \dontrun{
#' ratings <- list(list(0, 0, 4.0), list(0, 1, 2.0), list(1, 1, 3.0), list(1, 2, 4.0),
#'                 list(2, 1, 1.0), list(2, 2, 5.0))
#' df <- createDataFrame(ratings, c("user", "item", "rating"))
#' model <- spark.als(df, "rating", "user", "item")
#'
#' # extract latent factors
#' stats <- summary(model)
#' userFactors <- stats$userFactors
#' itemFactors <- stats$itemFactors
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
#'
#' # set other arguments
#' modelS <- spark.als(df, "rating", "user", "item", rank = 20,
#'                     regParam = 0.1, nonnegative = TRUE)
#' statsS <- summary(modelS)
#' }
#' @note spark.als since 2.1.0
setMethod("spark.als", signature(data = "SparkDataFrame"),
          function(data, ratingCol = "rating", userCol = "user", itemCol = "item",
                   rank = 10, regParam = 0.1, maxIter = 10, nonnegative = FALSE,
                   implicitPrefs = FALSE, alpha = 1.0, numUserBlocks = 10, numItemBlocks = 10,
                   checkpointInterval = 10, seed = 0) {

            if (!is.numeric(rank) || rank <= 0) {
              stop("rank should be a positive number.")
            }
            if (!is.numeric(regParam) || regParam < 0) {
              stop("regParam should be a nonnegative number.")
            }
            if (!is.numeric(maxIter) || maxIter <= 0) {
              stop("maxIter should be a positive number.")
            }

            jobj <- callJStatic("org.apache.spark.ml.r.ALSWrapper",
                                "fit", data@sdf, ratingCol, userCol, itemCol, as.integer(rank),
                                regParam, as.integer(maxIter), implicitPrefs, alpha, nonnegative,
                                as.integer(numUserBlocks), as.integer(numItemBlocks),
                                as.integer(checkpointInterval), as.integer(seed))
            new("ALSModel", jobj = jobj)
          })

# Returns a summary of the ALS model produced by spark.als.

#' @param object a fitted ALS model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{user} (the names of the user column),
#'         \code{item} (the item column), \code{rating} (the rating column), \code{userFactors}
#'         (the estimated user factors), \code{itemFactors} (the estimated item factors),
#'         and \code{rank} (rank of the matrix factorization model).
#' @rdname spark.als
#' @aliases summary,ALSModel-method
#' @export
#' @note summary(ALSModel) since 2.1.0
setMethod("summary", signature(object = "ALSModel"),
          function(object) {
            jobj <- object@jobj
            user <- callJMethod(jobj, "userCol")
            item <- callJMethod(jobj, "itemCol")
            rating <- callJMethod(jobj, "ratingCol")
            userFactors <- dataFrame(callJMethod(jobj, "userFactors"))
            itemFactors <- dataFrame(callJMethod(jobj, "itemFactors"))
            rank <- callJMethod(jobj, "rank")
            list(user = user, item = item, rating = rating, userFactors = userFactors,
                 itemFactors = itemFactors, rank = rank)
          })


# Makes predictions from an ALS model or a model produced by spark.als.

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted values.
#' @rdname spark.als
#' @aliases predict,ALSModel-method
#' @export
#' @note predict(ALSModel) since 2.1.0
setMethod("predict", signature(object = "ALSModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })


# Saves the ALS model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite logical value indicating whether to overwrite if the output path
#'                  already exists. Default is FALSE which means throw exception
#'                  if the output path exists.
#'
#' @rdname spark.als
#' @aliases write.ml,ALSModel,character-method
#' @export
#' @seealso \link{read.ml}
#' @note write.ml(ALSModel, character) since 2.1.0
setMethod("write.ml", signature(object = "ALSModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' (One-Sample) Kolmogorov-Smirnov Test
#'
#' @description
#' \code{spark.kstest} Conduct the two-sided Kolmogorov-Smirnov (KS) test for data sampled from a
#' continuous distribution.
#'
#' By comparing the largest difference between the empirical cumulative
#' distribution of the sample data and the theoretical distribution we can provide a test for the
#' the null hypothesis that the sample data comes from that theoretical distribution.
#'
#' Users can call \code{summary} to obtain a summary of the test, and \code{print.summary.KSTest}
#' to print out a summary result.
#'
#' @param data a SparkDataFrame of user data.
#' @param testCol column name where the test data is from. It should be a column of double type.
#' @param nullHypothesis name of the theoretical distribution tested against. Currently only
#'                       \code{"norm"} for normal distribution is supported.
#' @param distParams parameters(s) of the distribution. For \code{nullHypothesis = "norm"},
#'                   we can provide as a vector the mean and standard deviation of
#'                   the distribution. If none is provided, then standard normal will be used.
#'                   If only one is provided, then the standard deviation will be set to be one.
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.kstest} returns a test result object.
#' @rdname spark.kstest
#' @aliases spark.kstest,SparkDataFrame-method
#' @name spark.kstest
#' @seealso \href{http://spark.apache.org/docs/latest/mllib-statistics.html#hypothesis-testing}{
#'          MLlib: Hypothesis Testing}
#' @export
#' @examples
#' \dontrun{
#' data <- data.frame(test = c(0.1, 0.15, 0.2, 0.3, 0.25))
#' df <- createDataFrame(data)
#' test <- spark.kstest(df, "test", "norm", c(0, 1))
#'
#' # get a summary of the test result
#' testSummary <- summary(test)
#' testSummary
#'
#' # print out the summary in an organized way
#' print.summary.KSTest(testSummary)
#' }
#' @note spark.kstest since 2.1.0
setMethod("spark.kstest", signature(data = "SparkDataFrame"),
          function(data, testCol = "test", nullHypothesis = c("norm"), distParams = c(0, 1)) {
            tryCatch(match.arg(nullHypothesis),
                     error = function(e) {
                       msg <- paste("Distribution", nullHypothesis, "is not supported.")
                       stop(msg)
                     })
            if (nullHypothesis == "norm") {
              distParams <- as.numeric(distParams)
              mu <- ifelse(length(distParams) < 1, 0, distParams[1])
              sigma <- ifelse(length(distParams) < 2, 1, distParams[2])
              jobj <- callJStatic("org.apache.spark.ml.r.KSTestWrapper",
                                  "test", data@sdf, testCol, nullHypothesis,
                                  as.array(c(mu, sigma)))
              new("KSTest", jobj = jobj)
            }
})

#  Get the summary of Kolmogorov-Smirnov (KS) Test.
#' @param object test result object of KSTest by \code{spark.kstest}.
#' @return \code{summary} returns summary information of KSTest object, which is a list.
#'         The list includes the \code{p.value} (p-value), \code{statistic} (test statistic
#'         computed for the test), \code{nullHypothesis} (the null hypothesis with its
#'         parameters tested against) and \code{degreesOfFreedom} (degrees of freedom of the test).
#' @rdname spark.kstest
#' @aliases summary,KSTest-method
#' @export
#' @note summary(KSTest) since 2.1.0
setMethod("summary", signature(object = "KSTest"),
          function(object) {
            jobj <- object@jobj
            pValue <- callJMethod(jobj, "pValue")
            statistic <- callJMethod(jobj, "statistic")
            nullHypothesis <- callJMethod(jobj, "nullHypothesis")
            distName <- callJMethod(jobj, "distName")
            distParams <- unlist(callJMethod(jobj, "distParams"))
            degreesOfFreedom <- callJMethod(jobj, "degreesOfFreedom")

            ans <- list(p.value = pValue, statistic = statistic, nullHypothesis = nullHypothesis,
                        nullHypothesis.name = distName, nullHypothesis.parameters = distParams,
                        degreesOfFreedom = degreesOfFreedom, jobj = jobj)
            class(ans) <- "summary.KSTest"
            ans
          })

#  Prints the summary of KSTest

#' @rdname spark.kstest
#' @param x summary object of KSTest returned by \code{summary}.
#' @export
#' @note print.summary.KSTest since 2.1.0
print.summary.KSTest <- function(x, ...) {
  jobj <- x$jobj
  summaryStr <- callJMethod(jobj, "summary")
  cat(summaryStr, "\n")
  invisible(x)
}

#' Random Forest Model for Regression and Classification
#'
#' \code{spark.randomForest} fits a Random Forest Regression model or Classification model on
#' a SparkDataFrame. Users can call \code{summary} to get a summary of the fitted Random Forest
#' model, \code{predict} to make predictions on new data, and \code{write.ml}/\code{read.ml} to
#' save/load fitted models.
#' For more details, see
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression}{
#' Random Forest Regression} and
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-classifier}{
#' Random Forest Classification}
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', ':', '+', and '-'.
#' @param type type of model, one of "regression" or "classification", to fit
#' @param maxDepth Maximum depth of the tree (>= 0).
#' @param maxBins Maximum number of bins used for discretizing continuous features and for choosing
#'                how to split on features at each node. More bins give higher granularity. Must be
#'                >= 2 and >= number of categories in any categorical feature.
#' @param numTrees Number of trees to train (>= 1).
#' @param impurity Criterion used for information gain calculation.
#'                 For regression, must be "variance". For classification, must be one of
#'                 "entropy" and "gini", default is "gini".
#' @param featureSubsetStrategy The number of features to consider for splits at each tree node.
#'        Supported options: "auto", "all", "onethird", "sqrt", "log2", (0.0-1.0], [1-n].
#' @param seed integer seed for random number generation.
#' @param subsamplingRate Fraction of the training data used for learning each decision tree, in
#'                        range (0, 1].
#' @param minInstancesPerNode Minimum number of instances each child must have after split.
#' @param minInfoGain Minimum information gain for a split to be considered at a tree node.
#' @param checkpointInterval Param for set checkpoint interval (>= 1) or disable checkpoint (-1).
#' @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation.
#' @param cacheNodeIds If FALSE, the algorithm will pass trees to executors to match instances with
#'                     nodes. If TRUE, the algorithm will cache node IDs for each instance. Caching
#'                     can speed up training of deeper trees. Users can set how often should the
#'                     cache be checkpointed or disable it by setting checkpointInterval.
#' @param ... additional arguments passed to the method.
#' @aliases spark.randomForest,SparkDataFrame,formula-method
#' @return \code{spark.randomForest} returns a fitted Random Forest model.
#' @rdname spark.randomForest
#' @name spark.randomForest
#' @export
#' @examples
#' \dontrun{
#' # fit a Random Forest Regression Model
#' df <- createDataFrame(longley)
#' model <- spark.randomForest(df, Employed ~ ., type = "regression", maxDepth = 5, maxBins = 16)
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
#'
#' # fit a Random Forest Classification Model
#' df <- createDataFrame(iris)
#' model <- spark.randomForest(df, Species ~ Petal_Length + Petal_Width, "classification")
#' }
#' @note spark.randomForest since 2.1.0
setMethod("spark.randomForest", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, type = c("regression", "classification"),
                   maxDepth = 5, maxBins = 32, numTrees = 20, impurity = NULL,
                   featureSubsetStrategy = "auto", seed = NULL, subsamplingRate = 1.0,
                   minInstancesPerNode = 1, minInfoGain = 0.0, checkpointInterval = 10,
                   maxMemoryInMB = 256, cacheNodeIds = FALSE) {
            type <- match.arg(type)
            formula <- paste(deparse(formula), collapse = "")
            if (!is.null(seed)) {
              seed <- as.character(as.integer(seed))
            }
            switch(type,
                   regression = {
                     if (is.null(impurity)) impurity <- "variance"
                     impurity <- match.arg(impurity, "variance")
                     jobj <- callJStatic("org.apache.spark.ml.r.RandomForestRegressorWrapper",
                                         "fit", data@sdf, formula, as.integer(maxDepth),
                                         as.integer(maxBins), as.integer(numTrees),
                                         impurity, as.integer(minInstancesPerNode),
                                         as.numeric(minInfoGain), as.integer(checkpointInterval),
                                         as.character(featureSubsetStrategy), seed,
                                         as.numeric(subsamplingRate),
                                         as.integer(maxMemoryInMB), as.logical(cacheNodeIds))
                     new("RandomForestRegressionModel", jobj = jobj)
                   },
                   classification = {
                     if (is.null(impurity)) impurity <- "gini"
                     impurity <- match.arg(impurity, c("gini", "entropy"))
                     jobj <- callJStatic("org.apache.spark.ml.r.RandomForestClassifierWrapper",
                                         "fit", data@sdf, formula, as.integer(maxDepth),
                                         as.integer(maxBins), as.integer(numTrees),
                                         impurity, as.integer(minInstancesPerNode),
                                         as.numeric(minInfoGain), as.integer(checkpointInterval),
                                         as.character(featureSubsetStrategy), seed,
                                         as.numeric(subsamplingRate),
                                         as.integer(maxMemoryInMB), as.logical(cacheNodeIds))
                     new("RandomForestClassificationModel", jobj = jobj)
                   }
            )
          })

# Makes predictions from a Random Forest Regression model or Classification model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labeled in a column named
#'         "prediction".
#' @rdname spark.randomForest
#' @aliases predict,RandomForestRegressionModel-method
#' @export
#' @note predict(RandomForestRegressionModel) since 2.1.0
setMethod("predict", signature(object = "RandomForestRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' @rdname spark.randomForest
#' @aliases predict,RandomForestClassificationModel-method
#' @export
#' @note predict(RandomForestClassificationModel) since 2.1.0
setMethod("predict", signature(object = "RandomForestClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

# Save the Random Forest Regression or Classification model to the input path.

#' @param object A fitted Random Forest regression model or classification model.
#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @aliases write.ml,RandomForestRegressionModel,character-method
#' @rdname spark.randomForest
#' @export
#' @note write.ml(RandomForestRegressionModel, character) since 2.1.0
setMethod("write.ml", signature(object = "RandomForestRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' @aliases write.ml,RandomForestClassificationModel,character-method
#' @rdname spark.randomForest
#' @export
#' @note write.ml(RandomForestClassificationModel, character) since 2.1.0
setMethod("write.ml", signature(object = "RandomForestClassificationModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

# Create the summary of a tree ensemble model (eg. Random Forest, GBT)
summary.treeEnsemble <- function(model) {
  jobj <- model@jobj
  formula <- callJMethod(jobj, "formula")
  numFeatures <- callJMethod(jobj, "numFeatures")
  features <-  callJMethod(jobj, "features")
  featureImportances <- callJMethod(callJMethod(jobj, "featureImportances"), "toString")
  numTrees <- callJMethod(jobj, "numTrees")
  treeWeights <- callJMethod(jobj, "treeWeights")
  list(formula = formula,
       numFeatures = numFeatures,
       features = features,
       featureImportances = featureImportances,
       numTrees = numTrees,
       treeWeights = treeWeights,
       jobj = jobj)
}

#  Get the summary of a Random Forest Regression Model

#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list of components includes \code{formula} (formula),
#'         \code{numFeatures} (number of features), \code{features} (list of features),
#'         \code{featureImportances} (feature importances), \code{numTrees} (number of trees),
#'         and \code{treeWeights} (tree weights).
#' @rdname spark.randomForest
#' @aliases summary,RandomForestRegressionModel-method
#' @export
#' @note summary(RandomForestRegressionModel) since 2.1.0
setMethod("summary", signature(object = "RandomForestRegressionModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.RandomForestRegressionModel"
            ans
          })

#  Get the summary of a Random Forest Classification Model

#' @rdname spark.randomForest
#' @aliases summary,RandomForestClassificationModel-method
#' @export
#' @note summary(RandomForestClassificationModel) since 2.1.0
setMethod("summary", signature(object = "RandomForestClassificationModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.RandomForestClassificationModel"
            ans
          })

#  Prints the summary of tree ensemble models (eg. Random Forest, GBT)
print.summary.treeEnsemble <- function(x) {
  jobj <- x$jobj
  cat("Formula: ", x$formula)
  cat("\nNumber of features: ", x$numFeatures)
  cat("\nFeatures: ", unlist(x$features))
  cat("\nFeature importances: ", x$featureImportances)
  cat("\nNumber of trees: ", x$numTrees)
  cat("\nTree weights: ", unlist(x$treeWeights))

  summaryStr <- callJMethod(jobj, "summary")
  cat("\n", summaryStr, "\n")
  invisible(x)
}

#  Prints the summary of Random Forest Regression Model

#' @param x summary object of Random Forest regression model or classification model
#'          returned by \code{summary}.
#' @rdname spark.randomForest
#' @export
#' @note print.summary.RandomForestRegressionModel since 2.1.0
print.summary.RandomForestRegressionModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

#  Prints the summary of Random Forest Classification Model

#' @rdname spark.randomForest
#' @export
#' @note print.summary.RandomForestClassificationModel since 2.1.0
print.summary.RandomForestClassificationModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

#' Gradient Boosted Tree Model for Regression and Classification
#'
#' \code{spark.gbt} fits a Gradient Boosted Tree Regression model or Classification model on a
#' SparkDataFrame. Users can call \code{summary} to get a summary of the fitted
#' Gradient Boosted Tree model, \code{predict} to make predictions on new data, and
#' \code{write.ml}/\code{read.ml} to save/load fitted models.
#' For more details, see
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-regression}{
#' GBT Regression} and
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier}{
#' GBT Classification}
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', ':', '+', and '-'.
#' @param type type of model, one of "regression" or "classification", to fit
#' @param maxDepth Maximum depth of the tree (>= 0).
#' @param maxBins Maximum number of bins used for discretizing continuous features and for choosing
#'                how to split on features at each node. More bins give higher granularity. Must be
#'                >= 2 and >= number of categories in any categorical feature.
#' @param maxIter Param for maximum number of iterations (>= 0).
#' @param stepSize Param for Step size to be used for each iteration of optimization.
#' @param lossType Loss function which GBT tries to minimize.
#'                 For classification, must be "logistic". For regression, must be one of
#'                 "squared" (L2) and "absolute" (L1), default is "squared".
#' @param seed integer seed for random number generation.
#' @param subsamplingRate Fraction of the training data used for learning each decision tree, in
#'                        range (0, 1].
#' @param minInstancesPerNode Minimum number of instances each child must have after split. If a
#'                            split causes the left or right child to have fewer than
#'                            minInstancesPerNode, the split will be discarded as invalid. Should be
#'                            >= 1.
#' @param minInfoGain Minimum information gain for a split to be considered at a tree node.
#' @param checkpointInterval Param for set checkpoint interval (>= 1) or disable checkpoint (-1).
#' @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation.
#' @param cacheNodeIds If FALSE, the algorithm will pass trees to executors to match instances with
#'                     nodes. If TRUE, the algorithm will cache node IDs for each instance. Caching
#'                     can speed up training of deeper trees. Users can set how often should the
#'                     cache be checkpointed or disable it by setting checkpointInterval.
#' @param ... additional arguments passed to the method.
#' @aliases spark.gbt,SparkDataFrame,formula-method
#' @return \code{spark.gbt} returns a fitted Gradient Boosted Tree model.
#' @rdname spark.gbt
#' @name spark.gbt
#' @export
#' @examples
#' \dontrun{
#' # fit a Gradient Boosted Tree Regression Model
#' df <- createDataFrame(longley)
#' model <- spark.gbt(df, Employed ~ ., type = "regression", maxDepth = 5, maxBins = 16)
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
#'
#' # fit a Gradient Boosted Tree Classification Model
#' # label must be binary - Only binary classification is supported for GBT.
#' df <- createDataFrame(iris[iris$Species != "virginica", ])
#' model <- spark.gbt(df, Species ~ Petal_Length + Petal_Width, "classification")
#'
#' # numeric label is also supported
#' iris2 <- iris[iris$Species != "virginica", ]
#' iris2$NumericSpecies <- ifelse(iris2$Species == "setosa", 0, 1)
#' df <- createDataFrame(iris2)
#' model <- spark.gbt(df, NumericSpecies ~ ., type = "classification")
#' }
#' @note spark.gbt since 2.1.0
setMethod("spark.gbt", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, type = c("regression", "classification"),
                   maxDepth = 5, maxBins = 32, maxIter = 20, stepSize = 0.1, lossType = NULL,
                   seed = NULL, subsamplingRate = 1.0, minInstancesPerNode = 1, minInfoGain = 0.0,
                   checkpointInterval = 10, maxMemoryInMB = 256, cacheNodeIds = FALSE) {
            type <- match.arg(type)
            formula <- paste(deparse(formula), collapse = "")
            if (!is.null(seed)) {
              seed <- as.character(as.integer(seed))
            }
            switch(type,
                   regression = {
                     if (is.null(lossType)) lossType <- "squared"
                     lossType <- match.arg(lossType, c("squared", "absolute"))
                     jobj <- callJStatic("org.apache.spark.ml.r.GBTRegressorWrapper",
                                         "fit", data@sdf, formula, as.integer(maxDepth),
                                         as.integer(maxBins), as.integer(maxIter),
                                         as.numeric(stepSize), as.integer(minInstancesPerNode),
                                         as.numeric(minInfoGain), as.integer(checkpointInterval),
                                         lossType, seed, as.numeric(subsamplingRate),
                                         as.integer(maxMemoryInMB), as.logical(cacheNodeIds))
                     new("GBTRegressionModel", jobj = jobj)
                   },
                   classification = {
                     if (is.null(lossType)) lossType <- "logistic"
                     lossType <- match.arg(lossType, "logistic")
                     jobj <- callJStatic("org.apache.spark.ml.r.GBTClassifierWrapper",
                                         "fit", data@sdf, formula, as.integer(maxDepth),
                                         as.integer(maxBins), as.integer(maxIter),
                                         as.numeric(stepSize), as.integer(minInstancesPerNode),
                                         as.numeric(minInfoGain), as.integer(checkpointInterval),
                                         lossType, seed, as.numeric(subsamplingRate),
                                         as.integer(maxMemoryInMB), as.logical(cacheNodeIds))
                     new("GBTClassificationModel", jobj = jobj)
                   }
            )
          })

# Makes predictions from a Gradient Boosted Tree Regression model or Classification model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labeled in a column named
#'         "prediction".
#' @rdname spark.gbt
#' @aliases predict,GBTRegressionModel-method
#' @export
#' @note predict(GBTRegressionModel) since 2.1.0
setMethod("predict", signature(object = "GBTRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' @rdname spark.gbt
#' @aliases predict,GBTClassificationModel-method
#' @export
#' @note predict(GBTClassificationModel) since 2.1.0
setMethod("predict", signature(object = "GBTClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

# Save the Gradient Boosted Tree Regression or Classification model to the input path.

#' @param object A fitted Gradient Boosted Tree regression model or classification model.
#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#' @aliases write.ml,GBTRegressionModel,character-method
#' @rdname spark.gbt
#' @export
#' @note write.ml(GBTRegressionModel, character) since 2.1.0
setMethod("write.ml", signature(object = "GBTRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' @aliases write.ml,GBTClassificationModel,character-method
#' @rdname spark.gbt
#' @export
#' @note write.ml(GBTClassificationModel, character) since 2.1.0
setMethod("write.ml", signature(object = "GBTClassificationModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#  Get the summary of a Gradient Boosted Tree Regression Model

#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list of components includes \code{formula} (formula),
#'         \code{numFeatures} (number of features), \code{features} (list of features),
#'         \code{featureImportances} (feature importances), \code{numTrees} (number of trees),
#'         and \code{treeWeights} (tree weights).
#' @rdname spark.gbt
#' @aliases summary,GBTRegressionModel-method
#' @export
#' @note summary(GBTRegressionModel) since 2.1.0
setMethod("summary", signature(object = "GBTRegressionModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.GBTRegressionModel"
            ans
          })

#  Get the summary of a Gradient Boosted Tree Classification Model

#' @rdname spark.gbt
#' @aliases summary,GBTClassificationModel-method
#' @export
#' @note summary(GBTClassificationModel) since 2.1.0
setMethod("summary", signature(object = "GBTClassificationModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.GBTClassificationModel"
            ans
          })

#  Prints the summary of Gradient Boosted Tree Regression Model

#' @param x summary object of Gradient Boosted Tree regression model or classification model
#'          returned by \code{summary}.
#' @rdname spark.gbt
#' @export
#' @note print.summary.GBTRegressionModel since 2.1.0
print.summary.GBTRegressionModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

#  Prints the summary of Gradient Boosted Tree Classification Model

#' @rdname spark.gbt
#' @export
#' @note print.summary.GBTClassificationModel since 2.1.0
print.summary.GBTClassificationModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}
