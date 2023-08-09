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

# mllib_tree.R: Provides methods for MLlib tree-based algorithms integration

#' S4 class that represents a GBTRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala GBTRegressionModel
#' @note GBTRegressionModel since 2.1.0
setClass("GBTRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a GBTClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala GBTClassificationModel
#' @note GBTClassificationModel since 2.1.0
setClass("GBTClassificationModel", representation(jobj = "jobj"))

#' S4 class that represents a RandomForestRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala RandomForestRegressionModel
#' @note RandomForestRegressionModel since 2.1.0
setClass("RandomForestRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a RandomForestClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala RandomForestClassificationModel
#' @note RandomForestClassificationModel since 2.1.0
setClass("RandomForestClassificationModel", representation(jobj = "jobj"))

#' S4 class that represents a DecisionTreeRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala DecisionTreeRegressionModel
#' @note DecisionTreeRegressionModel since 2.3.0
setClass("DecisionTreeRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a DecisionTreeClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala DecisionTreeClassificationModel
#' @note DecisionTreeClassificationModel since 2.3.0
setClass("DecisionTreeClassificationModel", representation(jobj = "jobj"))

# Create the summary of a tree ensemble model (e.g. Random Forest, GBT)
summary.treeEnsemble <- function(model) {
  jobj <- model@jobj
  formula <- callJMethod(jobj, "formula")
  numFeatures <- callJMethod(jobj, "numFeatures")
  features <-  callJMethod(jobj, "features")
  featureImportances <- callJMethod(callJMethod(jobj, "featureImportances"), "toString")
  maxDepth <- callJMethod(jobj, "maxDepth")
  numTrees <- callJMethod(jobj, "numTrees")
  treeWeights <- callJMethod(jobj, "treeWeights")
  list(formula = formula,
       numFeatures = numFeatures,
       features = features,
       featureImportances = featureImportances,
       maxDepth = maxDepth,
       numTrees = numTrees,
       treeWeights = treeWeights,
       jobj = jobj)
}

# Prints the summary of tree ensemble models (e.g. Random Forest, GBT)
print.summary.treeEnsemble <- function(x) {
  jobj <- x$jobj
  cat("Formula: ", x$formula)
  cat("\nNumber of features: ", x$numFeatures)
  cat("\nFeatures: ", unlist(x$features))
  cat("\nFeature importances: ", x$featureImportances)
  cat("\nMax Depth: ", x$maxDepth)
  cat("\nNumber of trees: ", x$numTrees)
  cat("\nTree weights: ", unlist(x$treeWeights))

  summaryStr <- callJMethod(jobj, "summary")
  cat("\n", summaryStr, "\n")
  invisible(x)
}

# Create the summary of a decision tree model
summary.decisionTree <- function(model) {
  jobj <- model@jobj
  formula <- callJMethod(jobj, "formula")
  numFeatures <- callJMethod(jobj, "numFeatures")
  features <-  callJMethod(jobj, "features")
  featureImportances <- callJMethod(callJMethod(jobj, "featureImportances"), "toString")
  maxDepth <- callJMethod(jobj, "maxDepth")
  list(formula = formula,
       numFeatures = numFeatures,
       features = features,
       featureImportances = featureImportances,
       maxDepth = maxDepth,
       jobj = jobj)
}

# Prints the summary of decision tree models
print.summary.decisionTree <- function(x) {
  jobj <- x$jobj
  cat("Formula: ", x$formula)
  cat("\nNumber of features: ", x$numFeatures)
  cat("\nFeatures: ", unlist(x$features))
  cat("\nFeature importances: ", x$featureImportances)
  cat("\nMax Depth: ", x$maxDepth)

  summaryStr <- callJMethod(jobj, "summary")
  cat("\n", summaryStr, "\n")
  invisible(x)
}

#' Gradient Boosted Tree Model for Regression and Classification
#'
#' \code{spark.gbt} fits a Gradient Boosted Tree Regression model or Classification model on a
#' SparkDataFrame. Users can call \code{summary} to get a summary of the fitted
#' Gradient Boosted Tree model, \code{predict} to make predictions on new data, and
#' \code{write.ml}/\code{read.ml} to save/load fitted models.
#' For more details, see
# nolint start
#' \href{https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-regression}{
#' GBT Regression} and
#' \href{https://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier}{
#' GBT Classification}
# nolint end
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', ':', '+', '-', '*', and '^'.
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
#'                           Note: this setting will be ignored if the checkpoint directory is not
#'                           set.
#' @param maxMemoryInMB Maximum memory in MiB allocated to histogram aggregation.
#' @param cacheNodeIds If FALSE, the algorithm will pass trees to executors to match instances with
#'                     nodes. If TRUE, the algorithm will cache node IDs for each instance. Caching
#'                     can speed up training of deeper trees. Users can set how often should the
#'                     cache be checkpointed or disable it by setting checkpointInterval.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type in classification model.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @aliases spark.gbt,SparkDataFrame,formula-method
#' @return \code{spark.gbt} returns a fitted Gradient Boosted Tree model.
#' @rdname spark.gbt
#' @name spark.gbt
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
#' t <- as.data.frame(Titanic)
#' df <- createDataFrame(t)
#' model <- spark.gbt(df, Survived ~ Age + Freq, "classification")
#'
#' # numeric label is also supported
#' t2 <- as.data.frame(Titanic)
#' t2$NumericGender <- ifelse(t2$Sex == "Male", 0, 1)
#' df <- createDataFrame(t2)
#' model <- spark.gbt(df, NumericGender ~ ., type = "classification")
#' }
#' @note spark.gbt since 2.1.0
setMethod("spark.gbt", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, type = c("regression", "classification"),
                   maxDepth = 5, maxBins = 32, maxIter = 20, stepSize = 0.1, lossType = NULL,
                   seed = NULL, subsamplingRate = 1.0, minInstancesPerNode = 1, minInfoGain = 0.0,
                   checkpointInterval = 10, maxMemoryInMB = 256, cacheNodeIds = FALSE,
                   handleInvalid = c("error", "keep", "skip")) {
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
                     handleInvalid <- match.arg(handleInvalid)
                     if (is.null(lossType)) lossType <- "logistic"
                     lossType <- match.arg(lossType, "logistic")
                     jobj <- callJStatic("org.apache.spark.ml.r.GBTClassifierWrapper",
                                         "fit", data@sdf, formula, as.integer(maxDepth),
                                         as.integer(maxBins), as.integer(maxIter),
                                         as.numeric(stepSize), as.integer(minInstancesPerNode),
                                         as.numeric(minInfoGain), as.integer(checkpointInterval),
                                         lossType, seed, as.numeric(subsamplingRate),
                                         as.integer(maxMemoryInMB), as.logical(cacheNodeIds),
                                         handleInvalid)
                     new("GBTClassificationModel", jobj = jobj)
                   }
            )
          })

#  Get the summary of a Gradient Boosted Tree Regression Model

#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list of components includes \code{formula} (formula),
#'         \code{numFeatures} (number of features), \code{features} (list of features),
#'         \code{featureImportances} (feature importances), \code{maxDepth} (max depth of trees),
#'         \code{numTrees} (number of trees), and \code{treeWeights} (tree weights).
#' @rdname spark.gbt
#' @aliases summary,GBTRegressionModel-method
#' @note summary(GBTRegressionModel) since 2.1.0
setMethod("summary", signature(object = "GBTRegressionModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.GBTRegressionModel"
            ans
          })

#  Prints the summary of Gradient Boosted Tree Regression Model

#' @param x summary object of Gradient Boosted Tree regression model or classification model
#'          returned by \code{summary}.
#' @rdname spark.gbt
#' @note print.summary.GBTRegressionModel since 2.1.0
print.summary.GBTRegressionModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

#  Get the summary of a Gradient Boosted Tree Classification Model

#' @rdname spark.gbt
#' @aliases summary,GBTClassificationModel-method
#' @note summary(GBTClassificationModel) since 2.1.0
setMethod("summary", signature(object = "GBTClassificationModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.GBTClassificationModel"
            ans
          })

#  Prints the summary of Gradient Boosted Tree Classification Model

#' @rdname spark.gbt
#' @note print.summary.GBTClassificationModel since 2.1.0
print.summary.GBTClassificationModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

#  Makes predictions from a Gradient Boosted Tree Regression model or Classification model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labeled in a column named
#'         "prediction".
#' @rdname spark.gbt
#' @aliases predict,GBTRegressionModel-method
#' @note predict(GBTRegressionModel) since 2.1.0
setMethod("predict", signature(object = "GBTRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' @rdname spark.gbt
#' @aliases predict,GBTClassificationModel-method
#' @note predict(GBTClassificationModel) since 2.1.0
setMethod("predict", signature(object = "GBTClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Save the Gradient Boosted Tree Regression or Classification model to the input path.

#' @param object A fitted Gradient Boosted Tree regression model or classification model.
#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#' @aliases write.ml,GBTRegressionModel,character-method
#' @rdname spark.gbt
#' @note write.ml(GBTRegressionModel, character) since 2.1.0
setMethod("write.ml", signature(object = "GBTRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' @aliases write.ml,GBTClassificationModel,character-method
#' @rdname spark.gbt
#' @note write.ml(GBTClassificationModel, character) since 2.1.0
setMethod("write.ml", signature(object = "GBTClassificationModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' Random Forest Model for Regression and Classification
#'
#' \code{spark.randomForest} fits a Random Forest Regression model or Classification model on
#' a SparkDataFrame. Users can call \code{summary} to get a summary of the fitted Random Forest
#' model, \code{predict} to make predictions on new data, and \code{write.ml}/\code{read.ml} to
#' save/load fitted models.
#' For more details, see
# nolint start
#' \href{https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression}{
#' Random Forest Regression} and
#' \href{https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-classifier}{
#' Random Forest Classification}
# nolint end
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
#'                              Supported options: "auto" (choose automatically for task: If
#'                                                 numTrees == 1, set to "all." If numTrees > 1
#'                                                 (forest), set to "sqrt" for classification and
#'                                                 to "onethird" for regression),
#'                                                 "all" (use all features),
#'                                                 "onethird" (use 1/3 of the features),
#'                                                 "sqrt" (use sqrt(number of features)),
#'                                                 "log2" (use log2(number of features)),
#'                                                 "n": (when n is in the range (0, 1.0], use
#'                                                 n * number of features. When n is in the range
#'                                                 (1, number of features), use n features).
#'                                                 Default is "auto".
#' @param seed integer seed for random number generation.
#' @param subsamplingRate Fraction of the training data used for learning each decision tree, in
#'                        range (0, 1].
#' @param minInstancesPerNode Minimum number of instances each child must have after split.
#' @param minInfoGain Minimum information gain for a split to be considered at a tree node.
#' @param checkpointInterval Param for set checkpoint interval (>= 1) or disable checkpoint (-1).
#'                           Note: this setting will be ignored if the checkpoint directory is not
#'                           set.
#' @param maxMemoryInMB Maximum memory in MiB allocated to histogram aggregation.
#' @param cacheNodeIds If FALSE, the algorithm will pass trees to executors to match instances with
#'                     nodes. If TRUE, the algorithm will cache node IDs for each instance. Caching
#'                     can speed up training of deeper trees. Users can set how often should the
#'                     cache be checkpointed or disable it by setting checkpointInterval.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type in classification model.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param bootstrap Whether bootstrap samples are used when building trees.
#' @param ... additional arguments passed to the method.
#' @aliases spark.randomForest,SparkDataFrame,formula-method
#' @return \code{spark.randomForest} returns a fitted Random Forest model.
#' @rdname spark.randomForest
#' @name spark.randomForest
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
#' t <- as.data.frame(Titanic)
#' df <- createDataFrame(t)
#' model <- spark.randomForest(df, Survived ~ Freq + Age, "classification")
#' }
#' @note spark.randomForest since 2.1.0
setMethod("spark.randomForest", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, type = c("regression", "classification"),
                   maxDepth = 5, maxBins = 32, numTrees = 20, impurity = NULL,
                   featureSubsetStrategy = "auto", seed = NULL, subsamplingRate = 1.0,
                   minInstancesPerNode = 1, minInfoGain = 0.0, checkpointInterval = 10,
                   maxMemoryInMB = 256, cacheNodeIds = FALSE,
                   handleInvalid = c("error", "keep", "skip"),
                   bootstrap = TRUE) {
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
                                         as.integer(maxMemoryInMB), as.logical(cacheNodeIds),
                                         as.logical(bootstrap))
                     new("RandomForestRegressionModel", jobj = jobj)
                   },
                   classification = {
                     handleInvalid <- match.arg(handleInvalid)
                     if (is.null(impurity)) impurity <- "gini"
                     impurity <- match.arg(impurity, c("gini", "entropy"))
                     jobj <- callJStatic("org.apache.spark.ml.r.RandomForestClassifierWrapper",
                                         "fit", data@sdf, formula, as.integer(maxDepth),
                                         as.integer(maxBins), as.integer(numTrees),
                                         impurity, as.integer(minInstancesPerNode),
                                         as.numeric(minInfoGain), as.integer(checkpointInterval),
                                         as.character(featureSubsetStrategy), seed,
                                         as.numeric(subsamplingRate),
                                         as.integer(maxMemoryInMB), as.logical(cacheNodeIds),
                                         handleInvalid, as.logical(bootstrap))
                     new("RandomForestClassificationModel", jobj = jobj)
                   }
            )
          })

#  Get the summary of a Random Forest Regression Model

#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list of components includes \code{formula} (formula),
#'         \code{numFeatures} (number of features), \code{features} (list of features),
#'         \code{featureImportances} (feature importances), \code{maxDepth} (max depth of trees),
#'         \code{numTrees} (number of trees), and \code{treeWeights} (tree weights).
#' @rdname spark.randomForest
#' @aliases summary,RandomForestRegressionModel-method
#' @note summary(RandomForestRegressionModel) since 2.1.0
setMethod("summary", signature(object = "RandomForestRegressionModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.RandomForestRegressionModel"
            ans
          })

#  Prints the summary of Random Forest Regression Model

#' @param x summary object of Random Forest regression model or classification model
#'          returned by \code{summary}.
#' @rdname spark.randomForest
#' @note print.summary.RandomForestRegressionModel since 2.1.0
print.summary.RandomForestRegressionModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

#  Get the summary of a Random Forest Classification Model

#' @rdname spark.randomForest
#' @aliases summary,RandomForestClassificationModel-method
#' @note summary(RandomForestClassificationModel) since 2.1.0
setMethod("summary", signature(object = "RandomForestClassificationModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.RandomForestClassificationModel"
            ans
          })

#  Prints the summary of Random Forest Classification Model

#' @rdname spark.randomForest
#' @note print.summary.RandomForestClassificationModel since 2.1.0
print.summary.RandomForestClassificationModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

#  Makes predictions from a Random Forest Regression model or Classification model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labeled in a column named
#'         "prediction".
#' @rdname spark.randomForest
#' @aliases predict,RandomForestRegressionModel-method
#' @note predict(RandomForestRegressionModel) since 2.1.0
setMethod("predict", signature(object = "RandomForestRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' @rdname spark.randomForest
#' @aliases predict,RandomForestClassificationModel-method
#' @note predict(RandomForestClassificationModel) since 2.1.0
setMethod("predict", signature(object = "RandomForestClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Save the Random Forest Regression or Classification model to the input path.

#' @param object A fitted Random Forest regression model or classification model.
#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @aliases write.ml,RandomForestRegressionModel,character-method
#' @rdname spark.randomForest
#' @note write.ml(RandomForestRegressionModel, character) since 2.1.0
setMethod("write.ml", signature(object = "RandomForestRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' @aliases write.ml,RandomForestClassificationModel,character-method
#' @rdname spark.randomForest
#' @note write.ml(RandomForestClassificationModel, character) since 2.1.0
setMethod("write.ml", signature(object = "RandomForestClassificationModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' Decision Tree Model for Regression and Classification
#'
#' \code{spark.decisionTree} fits a Decision Tree Regression model or Classification model on
#' a SparkDataFrame. Users can call \code{summary} to get a summary of the fitted Decision Tree
#' model, \code{predict} to make predictions on new data, and \code{write.ml}/\code{read.ml} to
#' save/load fitted models.
#' For more details, see
# nolint start
#' \href{https://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-regression}{
#' Decision Tree Regression} and
#' \href{https://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-classifier}{
#' Decision Tree Classification}
# nolint end
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', ':', '+', and '-'.
#' @param type type of model, one of "regression" or "classification", to fit
#' @param maxDepth Maximum depth of the tree (>= 0).
#' @param maxBins Maximum number of bins used for discretizing continuous features and for choosing
#'                how to split on features at each node. More bins give higher granularity. Must be
#'                >= 2 and >= number of categories in any categorical feature.
#' @param impurity Criterion used for information gain calculation.
#'                 For regression, must be "variance". For classification, must be one of
#'                 "entropy" and "gini", default is "gini".
#' @param seed integer seed for random number generation.
#' @param minInstancesPerNode Minimum number of instances each child must have after split.
#' @param minInfoGain Minimum information gain for a split to be considered at a tree node.
#' @param checkpointInterval Param for set checkpoint interval (>= 1) or disable checkpoint (-1).
#'                           Note: this setting will be ignored if the checkpoint directory is not
#'                           set.
#' @param maxMemoryInMB Maximum memory in MiB allocated to histogram aggregation.
#' @param cacheNodeIds If FALSE, the algorithm will pass trees to executors to match instances with
#'                     nodes. If TRUE, the algorithm will cache node IDs for each instance. Caching
#'                     can speed up training of deeper trees. Users can set how often should the
#'                     cache be checkpointed or disable it by setting checkpointInterval.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type in classification model.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @aliases spark.decisionTree,SparkDataFrame,formula-method
#' @return \code{spark.decisionTree} returns a fitted Decision Tree model.
#' @rdname spark.decisionTree
#' @name spark.decisionTree
#' @examples
#' \dontrun{
#' # fit a Decision Tree Regression Model
#' df <- createDataFrame(longley)
#' model <- spark.decisionTree(df, Employed ~ ., type = "regression", maxDepth = 5, maxBins = 16)
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
#' # fit a Decision Tree Classification Model
#' t <- as.data.frame(Titanic)
#' df <- createDataFrame(t)
#' model <- spark.decisionTree(df, Survived ~ Freq + Age, "classification")
#' }
#' @note spark.decisionTree since 2.3.0
setMethod("spark.decisionTree", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, type = c("regression", "classification"),
                   maxDepth = 5, maxBins = 32, impurity = NULL, seed = NULL,
                   minInstancesPerNode = 1, minInfoGain = 0.0, checkpointInterval = 10,
                   maxMemoryInMB = 256, cacheNodeIds = FALSE,
                   handleInvalid = c("error", "keep", "skip")) {
            type <- match.arg(type)
            formula <- paste(deparse(formula), collapse = "")
            if (!is.null(seed)) {
              seed <- as.character(as.integer(seed))
            }
            switch(type,
                   regression = {
                     if (is.null(impurity)) impurity <- "variance"
                     impurity <- match.arg(impurity, "variance")
                     jobj <- callJStatic("org.apache.spark.ml.r.DecisionTreeRegressorWrapper",
                                         "fit", data@sdf, formula, as.integer(maxDepth),
                                         as.integer(maxBins), impurity,
                                         as.integer(minInstancesPerNode), as.numeric(minInfoGain),
                                         as.integer(checkpointInterval), seed,
                                         as.integer(maxMemoryInMB), as.logical(cacheNodeIds))
                     new("DecisionTreeRegressionModel", jobj = jobj)
                   },
                   classification = {
                     handleInvalid <- match.arg(handleInvalid)
                     if (is.null(impurity)) impurity <- "gini"
                     impurity <- match.arg(impurity, c("gini", "entropy"))
                     jobj <- callJStatic("org.apache.spark.ml.r.DecisionTreeClassifierWrapper",
                                         "fit", data@sdf, formula, as.integer(maxDepth),
                                         as.integer(maxBins), impurity,
                                         as.integer(minInstancesPerNode), as.numeric(minInfoGain),
                                         as.integer(checkpointInterval), seed,
                                         as.integer(maxMemoryInMB), as.logical(cacheNodeIds),
                                         handleInvalid)
                     new("DecisionTreeClassificationModel", jobj = jobj)
                   }
            )
          })

#  Get the summary of a Decision Tree Regression Model

#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list of components includes \code{formula} (formula),
#'         \code{numFeatures} (number of features), \code{features} (list of features),
#'         \code{featureImportances} (feature importances), and \code{maxDepth} (max depth of
#'         trees).
#' @rdname spark.decisionTree
#' @aliases summary,DecisionTreeRegressionModel-method
#' @note summary(DecisionTreeRegressionModel) since 2.3.0
setMethod("summary", signature(object = "DecisionTreeRegressionModel"),
          function(object) {
            ans <- summary.decisionTree(object)
            class(ans) <- "summary.DecisionTreeRegressionModel"
            ans
          })

#  Prints the summary of Decision Tree Regression Model

#' @param x summary object of Decision Tree regression model or classification model
#'          returned by \code{summary}.
#' @rdname spark.decisionTree
#' @note print.summary.DecisionTreeRegressionModel since 2.3.0
print.summary.DecisionTreeRegressionModel <- function(x, ...) {
  print.summary.decisionTree(x)
}

#  Get the summary of a Decision Tree Classification Model

#' @rdname spark.decisionTree
#' @aliases summary,DecisionTreeClassificationModel-method
#' @note summary(DecisionTreeClassificationModel) since 2.3.0
setMethod("summary", signature(object = "DecisionTreeClassificationModel"),
          function(object) {
            ans <- summary.decisionTree(object)
            class(ans) <- "summary.DecisionTreeClassificationModel"
            ans
          })

#  Prints the summary of Decision Tree Classification Model

#' @rdname spark.decisionTree
#' @note print.summary.DecisionTreeClassificationModel since 2.3.0
print.summary.DecisionTreeClassificationModel <- function(x, ...) {
  print.summary.decisionTree(x)
}

#  Makes predictions from a Decision Tree Regression model or Classification model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labeled in a column named
#'         "prediction".
#' @rdname spark.decisionTree
#' @aliases predict,DecisionTreeRegressionModel-method
#' @note predict(DecisionTreeRegressionModel) since 2.3.0
setMethod("predict", signature(object = "DecisionTreeRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' @rdname spark.decisionTree
#' @aliases predict,DecisionTreeClassificationModel-method
#' @note predict(DecisionTreeClassificationModel) since 2.3.0
setMethod("predict", signature(object = "DecisionTreeClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Save the Decision Tree Regression or Classification model to the input path.

#' @param object A fitted Decision Tree regression model or classification model.
#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @aliases write.ml,DecisionTreeRegressionModel,character-method
#' @rdname spark.decisionTree
#' @note write.ml(DecisionTreeRegressionModel, character) since 2.3.0
setMethod("write.ml", signature(object = "DecisionTreeRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' @aliases write.ml,DecisionTreeClassificationModel,character-method
#' @rdname spark.decisionTree
#' @note write.ml(DecisionTreeClassificationModel, character) since 2.3.0
setMethod("write.ml", signature(object = "DecisionTreeClassificationModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })
