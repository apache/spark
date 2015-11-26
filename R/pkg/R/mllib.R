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

#' @title S4 class that represents a PipelineModel
#' @param model A Java object reference to the backing Scala PipelineModel
#' @export
setClass("PipelineModel", representation(model = "jobj"))

#' Fits a generalized linear model
#'
#' Fits a generalized linear model, similarly to R's glm(). Also see the glmnet package.
#'
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param data DataFrame for training
#' @param family Error distribution. "gaussian" -> linear regression, "binomial" -> logistic reg.
#' @param lambda Regularization parameter
#' @param alpha Elastic-net mixing parameter (see glmnet's documentation for details)
#' @param standardize Whether to standardize features before training
#' @param solver The solver algorithm used for optimization, this can be "l-bfgs", "normal" and
#'               "auto". "l-bfgs" denotes Limited-memory BFGS which is a limited-memory
#'               quasi-Newton optimization method. "normal" denotes using Normal Equation as an
#'               analytical solution to the linear regression problem. The default value is "auto"
#'               which means that the solver algorithm is selected automatically.
#' @return a fitted MLlib model
#' @rdname glm
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' data(iris)
#' df <- createDataFrame(sqlContext, iris)
#' model <- glm(Sepal_Length ~ Sepal_Width, df, family="gaussian")
#' summary(model)
#'}
setMethod("glm", signature(formula = "formula", family = "ANY", data = "DataFrame"),
          function(formula, family = c("gaussian", "binomial"), data, lambda = 0, alpha = 0,
            standardize = TRUE, solver = "auto") {
            family <- match.arg(family)
            formula <- paste(deparse(formula), collapse="")
            model <- callJStatic("org.apache.spark.ml.api.r.SparkRWrappers",
                                 "fitRModelFormula", formula, data@sdf, family, lambda,
                                 alpha, standardize, solver)
            return(new("PipelineModel", model = model))
          })

#' Make predictions from a model
#'
#' Makes predictions from a model produced by glm(), similarly to R's predict().
#'
#' @param object A fitted MLlib model
#' @param newData DataFrame for testing
#' @return DataFrame containing predicted values
#' @rdname predict
#' @export
#' @examples
#'\dontrun{
#' model <- glm(y ~ x, trainingData)
#' predicted <- predict(model, testData)
#' showDF(predicted)
#'}
setMethod("predict", signature(object = "PipelineModel"),
          function(object, newData) {
            return(dataFrame(callJMethod(object@model, "transform", newData@sdf)))
          })

#' Get the summary of a model
#'
#' Returns the summary of a model produced by glm(), similarly to R's summary().
#'
#' @param object A fitted MLlib model
#' @return a list with 'devianceResiduals' and 'coefficients' components for gaussian family
#'         or a list with 'coefficients' component for binomial family. \cr
#'         For gaussian family: the 'devianceResiduals' gives the min/max deviance residuals
#'         of the estimation, the 'coefficients' gives the estimated coefficients and their
#'         estimated standard errors, t values and p-values. (It only available when model
#'         fitted by normal solver.) \cr
#'         For binomial family: the 'coefficients' gives the estimated coefficients.
#'         See summary.glm for more information. \cr
#' @rdname summary
#' @export
#' @examples
#'\dontrun{
#' model <- glm(y ~ x, trainingData)
#' summary(model)
#'}
setMethod("summary", signature(object = "PipelineModel"),
          function(object, ...) {
            modelName <- callJStatic("org.apache.spark.ml.api.r.SparkRWrappers",
                                   "getModelName", object@model)
            features <- callJStatic("org.apache.spark.ml.api.r.SparkRWrappers",
                                   "getModelFeatures", object@model)
            coefficients <- callJStatic("org.apache.spark.ml.api.r.SparkRWrappers",
                                   "getModelCoefficients", object@model)
            if (modelName == "LinearRegressionModel") {
              devianceResiduals <- callJStatic("org.apache.spark.ml.api.r.SparkRWrappers",
                                               "getModelDevianceResiduals", object@model)
              devianceResiduals <- matrix(devianceResiduals, nrow = 1)
              colnames(devianceResiduals) <- c("Min", "Max")
              rownames(devianceResiduals) <- rep("", times = 1)
              coefficients <- matrix(coefficients, ncol = 4)
              colnames(coefficients) <- c("Estimate", "Std. Error", "t value", "Pr(>|t|)")
              rownames(coefficients) <- unlist(features)
              return(list(devianceResiduals = devianceResiduals, coefficients = coefficients))
            } else {
              coefficients <- as.matrix(unlist(coefficients))
              colnames(coefficients) <- c("Estimate")
              rownames(coefficients) <- unlist(features)
              return(list(coefficients = coefficients))
            }
          })
