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

# mllib.R: Provides methods for MLLib integration

#' Fits a generalized linear model similar to R's glm().
#' @param formula A symbolic description of the model to be fitted
#' @param data DataFrame for training
#' @param family Error distribution. "gaussian" -> linear regression, "binomial" -> logistic reg.
#' @param lambda Regularization parameter
#' @param alpha Elastic-net mixing parmaeter (see glmnet's documentation for details)
#' @return a fitted MLLib model
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df <- createDataFrame(sqlContext, iris)
#' model <- SparkR:::glm(Sepal_Length ~ Sepal_Width, df)
#'}
glm <- function(formula, data, family = c("gaussian", "binomial"), lambda = 0, alpha = 0) {
  family <- match.arg(family)
  model <- callJStatic("org.apache.spark.mllib.api.r.MLUtils",
                       "fitRModelFormula", deparse(formula), data@sdf, family, lambda, alpha)
  return(model)
}

#' Fits a generalized linear model similar to R's glm().
#' @param model A fitted MLLib model
#' @param newData DataFrame for testing
#' @return DataFrame containing predicted values
#' @examples
#'\dontrun{
#' model <- SparkR:::glm(y ~ x, trainingData)
#' predicted <- SparkR:::predict(model, testData)
#' showDF(predicted)
#'}
predict <- function(model, newData) {
  return(dataFrame(callJMethod(model, "transform", newData@sdf)))
}
