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
#'                operators are supported, including '~', '+', '-', and '.'.
#' @param data DataFrame for training
#' @param family Error distribution. "gaussian" -> linear regression, "binomial" -> logistic reg.
#' @param lambda Regularization parameter
#' @param alpha Elastic-net mixing parameter (see glmnet's documentation for details)
#' @return a fitted MLlib model
#' @rdname glm
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' data(iris)
#' df <- createDataFrame(sqlContext, iris)
#' model <- glm(Sepal_Length ~ Sepal_Width, df)
#'}
setMethod("glm", signature(formula = "formula", family = "ANY", data = "DataFrame"),
          function(formula, family = c("gaussian", "binomial"), data, lambda = 0, alpha = 0) {
            family <- match.arg(family)
            model <- callJStatic("org.apache.spark.ml.api.r.SparkRWrappers",
                                 "fitRModelFormula", deparse(formula), data@sdf, family, lambda,
                                 alpha)
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
#' @param x A fitted MLlib model
#' @return a list with a 'coefficient' component, which is the matrix of coefficients. See
#'         summary.glm for more information.
#' @rdname summary
#' @export
#' @examples
#'\dontrun{
#' model <- glm(y ~ x, trainingData)
#' summary(model)
#'}
setMethod("summary", signature(x = "PipelineModel"),
          function(x, ...) {
            features <- callJStatic("org.apache.spark.ml.api.r.SparkRWrappers",
                                   "getModelFeatures", x@model)
            weights <- callJStatic("org.apache.spark.ml.api.r.SparkRWrappers",
                                   "getModelWeights", x@model)
            coefficients <- as.matrix(unlist(weights))
            colnames(coefficients) <- c("Estimate")
            rownames(coefficients) <- unlist(features)
            return(list(coefficients = coefficients))
          })
