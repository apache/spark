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

# mllib_recommendation.R: Provides methods for MLlib recommendation algorithms integration

#' S4 class that represents an ALSModel
#'
#' @param jobj a Java object reference to the backing Scala ALSWrapper
#' @note ALSModel since 2.1.0
setClass("ALSModel", representation(jobj = "jobj"))

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
#'                           Note: this setting will be ignored if the checkpoint directory is not
#'                           set.
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.als} returns a fitted ALS model.
#' @rdname spark.als
#' @aliases spark.als,SparkDataFrame-method
#' @name spark.als
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

#  Returns a summary of the ALS model produced by spark.als.

#' @param object a fitted ALS model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{user} (the names of the user column),
#'         \code{item} (the item column), \code{rating} (the rating column), \code{userFactors}
#'         (the estimated user factors), \code{itemFactors} (the estimated item factors),
#'         and \code{rank} (rank of the matrix factorization model).
#' @rdname spark.als
#' @aliases summary,ALSModel-method
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

#  Makes predictions from an ALS model or a model produced by spark.als.

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted values.
#' @rdname spark.als
#' @aliases predict,ALSModel-method
#' @note predict(ALSModel) since 2.1.0
setMethod("predict", signature(object = "ALSModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Saves the ALS model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite logical value indicating whether to overwrite if the output path
#'                  already exists. Default is FALSE which means throw exception
#'                  if the output path exists.
#'
#' @rdname spark.als
#' @aliases write.ml,ALSModel,character-method
#' @seealso \link{read.ml}
#' @note write.ml(ALSModel, character) since 2.1.0
setMethod("write.ml", signature(object = "ALSModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })
