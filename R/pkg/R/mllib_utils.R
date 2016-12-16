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

#  Saves the AFT survival regression model to the input path.

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

#  Saves the Multilayer Perceptron Classification Model to the input path.

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

#  Saves the Bernoulli naive Bayes model to the input path.

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

#  Saves the Latent Dirichlet Allocation model to the input path.

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

#  Save the Gradient Boosted Tree Regression or Classification model to the input path.

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

#  Save the Random Forest Regression or Classification model to the input path.

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

#  Saves the ALS model to the input path.

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
