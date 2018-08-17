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

# mllib_utils.R: Utilities for MLlib integration

# Integration with R's standard functions.
# Most of MLlib's argorithms are provided in two flavours:
# - a specialization of the default R methods (glm). These methods try to respect
#   the inputs and the outputs of R's method to the largest extent, but some small differences
#   may exist.
# - a set of methods that reflect the arguments of the other languages supported by Spark. These
#   methods are prefixed with the `spark.` prefix: spark.glm, spark.kmeans, etc.

#' Saves the MLlib model to the input path
#'
#' Saves the MLlib model to the input path. For more information, see the specific
#' MLlib model below.
#' @rdname write.ml
#' @name write.ml
#' @seealso \link{spark.als}, \link{spark.bisectingKmeans}, \link{spark.decisionTree},
#' @seealso \link{spark.gaussianMixture}, \link{spark.gbt},
#' @seealso \link{spark.glm}, \link{glm}, \link{spark.isoreg},
#' @seealso \link{spark.kmeans},
#' @seealso \link{spark.lda}, \link{spark.logit},
#' @seealso \link{spark.mlp}, \link{spark.naiveBayes},
#' @seealso \link{spark.randomForest}, \link{spark.survreg}, \link{spark.svmLinear},
#' @seealso \link{read.ml}
NULL

#' Makes predictions from a MLlib model
#'
#' Makes predictions from a MLlib model. For more information, see the specific
#' MLlib model below.
#' @rdname predict
#' @name predict
#' @seealso \link{spark.als}, \link{spark.bisectingKmeans}, \link{spark.decisionTree},
#' @seealso \link{spark.gaussianMixture}, \link{spark.gbt},
#' @seealso \link{spark.glm}, \link{glm}, \link{spark.isoreg},
#' @seealso \link{spark.kmeans},
#' @seealso \link{spark.logit}, \link{spark.mlp}, \link{spark.naiveBayes},
#' @seealso \link{spark.randomForest}, \link{spark.survreg}, \link{spark.svmLinear}
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

#' Load a fitted MLlib model from the input path.
#'
#' @param path path of the model to read.
#' @return A fitted MLlib model.
#' @rdname read.ml
#' @name read.ml
#' @seealso \link{write.ml}
#' @examples
#' \dontrun{
#' path <- "path/to/model"
#' model <- read.ml(path)
#' }
#' @note read.ml since 2.0.0
read.ml <- function(path) {
  path <- suppressWarnings(normalizePath(path))
  sparkSession <- getSparkSession()
  callJStatic("org.apache.spark.ml.r.RWrappers", "session", sparkSession)
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
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.DecisionTreeRegressorWrapper")) {
    new("DecisionTreeRegressionModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.DecisionTreeClassifierWrapper")) {
    new("DecisionTreeClassificationModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.GBTRegressorWrapper")) {
    new("GBTRegressionModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.GBTClassifierWrapper")) {
    new("GBTClassificationModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.BisectingKMeansWrapper")) {
    new("BisectingKMeansModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.LinearSVCWrapper")) {
    new("LinearSVCModel", jobj = jobj)
  } else if (isInstanceOf(jobj, "org.apache.spark.ml.r.FPGrowthWrapper")) {
    new("FPGrowthModel", jobj = jobj)
  } else {
    stop("Unsupported model: ", jobj)
  }
}
