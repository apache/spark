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

# mllib_fpm.R: Provides methods for MLlib frequent pattern mining algorithms integration

#' S4 class that represents a FPGrowthModel
#'
#' @param jobj a Java object reference to the backing Scala FPGrowthModel
#' @export
#' @note FPGrowthModel since 2.2.0
setClass("FPGrowthModel", slots = list(jobj = "jobj"))

#' FP-growth
#'
#' A parallel FP-growth algorithm to mine frequent itemsets.
#' \code{spark.fpGrowth} fits a FP-growth model on a SparkDataFrame. Users can
#' \code{spark.freqItemsets} to get frequent itemsets, \code{spark.associationRules} to get
#' association rules, \code{predict} to make predictions on new data based on generated association
#' rules, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#' For more details, see
#' \href{https://spark.apache.org/docs/latest/mllib-frequent-pattern-mining.html#fp-growth}{
#' FP-growth}.
#'
#' @param data A SparkDataFrame for training.
#' @param minSupport Minimal support level.
#' @param minConfidence Minimal confidence level.
#' @param itemsCol Features column name.
#' @param numPartitions Number of partitions used for fitting.
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.fpGrowth} returns a fitted FPGrowth model.
#' @rdname spark.fpGrowth
#' @name spark.fpGrowth
#' @aliases spark.fpGrowth,SparkDataFrame-method
#' @export
#' @examples
#' \dontrun{
#' raw_data <- read.df(
#'   "data/mllib/sample_fpgrowth.txt",
#'   source = "csv",
#'   schema = structType(structField("raw_items", "string")))
#'
#' data <- selectExpr(raw_data, "split(raw_items, ' ') as items")
#' model <- spark.fpGrowth(data)
#'
#' # Show frequent itemsets
#' frequent_itemsets <- spark.freqItemsets(model)
#' showDF(frequent_itemsets)
#'
#' # Show association rules
#' association_rules <- spark.associationRules(model)
#' showDF(association_rules)
#'
#' # Predict on new data
#' new_itemsets <- data.frame(items = c("t", "t,s"))
#' new_data <- selectExpr(createDataFrame(new_itemsets), "split(items, ',') as items")
#' predict(model, new_data)
#'
#' # Save and load model
#' path <- "/path/to/model"
#' write.ml(model, path)
#' read.ml(path)
#'
#' # Optional arguments
#' baskets_data <- selectExpr(createDataFrame(itemsets), "split(items, ',') as baskets")
#' another_model <- spark.fpGrowth(data, minSupport = 0.1, minConfidence = 0.5,
#'                                 itemsCol = "baskets", numPartitions = 10)
#' }
#' @note spark.fpGrowth since 2.2.0
setMethod("spark.fpGrowth", signature(data = "SparkDataFrame"),
          function(data, minSupport = 0.3, minConfidence = 0.8,
                   itemsCol = "items", numPartitions = NULL) {
            if (!is.numeric(minSupport) || minSupport < 0 || minSupport > 1) {
              stop("minSupport should be a number [0, 1].")
            }
            if (!is.numeric(minConfidence) || minConfidence < 0 || minConfidence > 1) {
              stop("minConfidence should be a number [0, 1].")
            }
            if (!is.null(numPartitions)) {
              numPartitions <- as.integer(numPartitions)
              stopifnot(numPartitions > 0)
            }

            jobj <- callJStatic("org.apache.spark.ml.r.FPGrowthWrapper", "fit",
                                data@sdf, as.numeric(minSupport), as.numeric(minConfidence),
                                itemsCol, numPartitions)
            new("FPGrowthModel", jobj = jobj)
          })

# Get frequent itemsets.

#' @param object a fitted FPGrowth model.
#' @return A \code{SparkDataFrame} with frequent itemsets.
#'         The \code{SparkDataFrame} contains two columns:
#'         \code{items} (an array of the same type as the input column)
#'         and \code{freq} (frequency of the itemset).
#' @rdname spark.fpGrowth
#' @aliases freqItemsets,FPGrowthModel-method
#' @export
#' @note spark.freqItemsets(FPGrowthModel) since 2.2.0
setMethod("spark.freqItemsets", signature(object = "FPGrowthModel"),
          function(object) {
            dataFrame(callJMethod(object@jobj, "freqItemsets"))
          })

# Get association rules.

#' @return A \code{SparkDataFrame} with association rules.
#'         The \code{SparkDataFrame} contains three columns:
#'         \code{antecedent} (an array of the same type as the input column),
#'         \code{consequent} (an array of the same type as the input column),
#'         and \code{condfidence} (confidence).
#' @rdname spark.fpGrowth
#' @aliases associationRules,FPGrowthModel-method
#' @export
#' @note spark.associationRules(FPGrowthModel) since 2.2.0
setMethod("spark.associationRules", signature(object = "FPGrowthModel"),
          function(object) {
            dataFrame(callJMethod(object@jobj, "associationRules"))
          })

#  Makes predictions based on generated association rules

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted values.
#' @rdname spark.fpGrowth
#' @aliases predict,FPGrowthModel-method
#' @export
#' @note predict(FPGrowthModel) since 2.2.0
setMethod("predict", signature(object = "FPGrowthModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Saves the FPGrowth model to the output path.

#' @param path the directory where the model is saved.
#' @param overwrite logical value indicating whether to overwrite if the output path
#'                  already exists. Default is FALSE which means throw exception
#'                  if the output path exists.
#' @rdname spark.fpGrowth
#' @aliases write.ml,FPGrowthModel,character-method
#' @export
#' @seealso \link{read.ml}
#' @note write.ml(FPGrowthModel, character) since 2.2.0
setMethod("write.ml", signature(object = "FPGrowthModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })
