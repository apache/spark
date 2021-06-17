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

# mllib_regression.R: Provides methods for MLlib classification algorithms
#                     (except for tree-based algorithms) integration

#' S4 class that represents an LinearSVCModel
#'
#' @param jobj a Java object reference to the backing Scala LinearSVCModel
#' @note LinearSVCModel since 2.2.0
setClass("LinearSVCModel", representation(jobj = "jobj"))

#' S4 class that represents an LogisticRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala LogisticRegressionModel
#' @note LogisticRegressionModel since 2.1.0
setClass("LogisticRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a MultilayerPerceptronClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala MultilayerPerceptronClassifierWrapper
#' @note MultilayerPerceptronClassificationModel since 2.1.0
setClass("MultilayerPerceptronClassificationModel", representation(jobj = "jobj"))

#' S4 class that represents a NaiveBayesModel
#'
#' @param jobj a Java object reference to the backing Scala NaiveBayesWrapper
#' @note NaiveBayesModel since 2.0.0
setClass("NaiveBayesModel", representation(jobj = "jobj"))

#' S4 class that represents a FMClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala FMClassifierWrapper
#' @note FMClassificationModel since 3.1.0
setClass("FMClassificationModel", representation(jobj = "jobj"))

#' Linear SVM Model
#'
#' Fits a linear SVM model against a SparkDataFrame, similar to svm in e1071 package.
#' Currently only supports binary classification model with linear kernel.
#' Users can print, make predictions on the produced model and save the model to the input path.
#'
#' @param data SparkDataFrame for training.
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', '-', '*', and '^'.
#' @param regParam The regularization parameter. Only supports L2 regularization currently.
#' @param maxIter Maximum iteration number.
#' @param tol Convergence tolerance of iterations.
#' @param standardization Whether to standardize the training features before fitting the model.
#'                        The coefficients of models will be always returned on the original scale,
#'                        so it will be transparent for users. Note that with/without
#'                        standardization, the models should be always converged to the same
#'                        solution when no regularization is applied.
#' @param threshold The threshold in binary classification applied to the linear model prediction.
#'                  This threshold can be any real number, where Inf will make all predictions 0.0
#'                  and -Inf will make all predictions 1.0.
#' @param weightCol The weight column name.
#' @param aggregationDepth The depth for treeAggregate (greater than or equal to 2). If the
#'                         dimensions of features or the number of partitions are large, this param
#'                         could be adjusted to a larger size.
#'                         This is an expert parameter. Default value should be good for most cases.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @return \code{spark.svmLinear} returns a fitted linear SVM model.
#' @rdname spark.svmLinear
#' @aliases spark.svmLinear,SparkDataFrame,formula-method
#' @name spark.svmLinear
#' @examples
#' \dontrun{
#' sparkR.session()
#' t <- as.data.frame(Titanic)
#' training <- createDataFrame(t)
#' model <- spark.svmLinear(training, Survived ~ ., regParam = 0.5)
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
#' }
#' @note spark.svmLinear since 2.2.0
setMethod("spark.svmLinear", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, regParam = 0.0, maxIter = 100, tol = 1E-6, standardization = TRUE,
                   threshold = 0.0, weightCol = NULL, aggregationDepth = 2,
                   handleInvalid = c("error", "keep", "skip")) {
            formula <- paste(deparse(formula), collapse = "")

            if (!is.null(weightCol) && weightCol == "") {
              weightCol <- NULL
            } else if (!is.null(weightCol)) {
              weightCol <- as.character(weightCol)
            }

            handleInvalid <- match.arg(handleInvalid)

            jobj <- callJStatic("org.apache.spark.ml.r.LinearSVCWrapper", "fit",
                                data@sdf, formula, as.numeric(regParam), as.integer(maxIter),
                                as.numeric(tol), as.logical(standardization), as.numeric(threshold),
                                weightCol, as.integer(aggregationDepth), handleInvalid)
            new("LinearSVCModel", jobj = jobj)
          })

#  Predicted values based on a LinearSVCModel model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns the predicted values based on a LinearSVCModel.
#' @rdname spark.svmLinear
#' @aliases predict,LinearSVCModel,SparkDataFrame-method
#' @note predict(LinearSVCModel) since 2.2.0
setMethod("predict", signature(object = "LinearSVCModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Get the summary of a LinearSVCModel

#' @param object a LinearSVCModel fitted by \code{spark.svmLinear}.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{coefficients} (coefficients of the fitted model),
#'         \code{numClasses} (number of classes), \code{numFeatures} (number of features).
#' @rdname spark.svmLinear
#' @aliases summary,LinearSVCModel-method
#' @note summary(LinearSVCModel) since 2.2.0
setMethod("summary", signature(object = "LinearSVCModel"),
          function(object) {
            jobj <- object@jobj
            features <- callJMethod(jobj, "rFeatures")
            coefficients <- callJMethod(jobj, "rCoefficients")
            coefficients <- as.matrix(unlist(coefficients))
            colnames(coefficients) <- c("Estimate")
            rownames(coefficients) <- unlist(features)
            numClasses <- callJMethod(jobj, "numClasses")
            numFeatures <- callJMethod(jobj, "numFeatures")
            list(coefficients = coefficients, numClasses = numClasses, numFeatures = numFeatures)
          })

#  Save fitted LinearSVCModel to the input path

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.svmLinear
#' @aliases write.ml,LinearSVCModel,character-method
#' @note write.ml(LogisticRegression, character) since 2.2.0
setMethod("write.ml", signature(object = "LinearSVCModel", path = "character"),
function(object, path, overwrite = FALSE) {
    write_internal(object, path, overwrite)
})

#' Logistic Regression Model
#'
#' Fits an logistic regression model against a SparkDataFrame. It supports "binomial": Binary
#' logistic regression with pivoting; "multinomial": Multinomial logistic (softmax) regression
#' without pivoting, similar to glmnet. Users can print, make predictions on the produced model
#' and save the model to the input path.
#'
#' @param data SparkDataFrame for training.
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param regParam the regularization parameter.
#' @param elasticNetParam the ElasticNet mixing parameter. For alpha = 0.0, the penalty is an L2
#'                        penalty. For alpha = 1.0, it is an L1 penalty. For 0.0 < alpha < 1.0,
#'                        the penalty is a combination of L1 and L2. Default is 0.0 which is an
#'                        L2 penalty.
#' @param maxIter maximum iteration number.
#' @param tol convergence tolerance of iterations.
#' @param family the name of family which is a description of the label distribution to be used
#'               in the model.
#'               Supported options:
#'                 \itemize{
#'                   \item{"auto": Automatically select the family based on the number of classes:
#'                           If number of classes == 1 || number of classes == 2, set to "binomial".
#'                           Else, set to "multinomial".}
#'                   \item{"binomial": Binary logistic regression with pivoting.}
#'                   \item{"multinomial": Multinomial logistic (softmax) regression without
#'                           pivoting.}
#'                 }
#' @param standardization whether to standardize the training features before fitting the model.
#'                        The coefficients of models will be always returned on the original scale,
#'                        so it will be transparent for users. Note that with/without
#'                        standardization, the models should be always converged to the same
#'                        solution when no regularization is applied. Default is TRUE, same as
#'                        glmnet.
#' @param thresholds in binary classification, in range [0, 1]. If the estimated probability of
#'                   class label 1 is > threshold, then predict 1, else 0. A high threshold
#'                   encourages the model to predict 0 more often; a low threshold encourages the
#'                   model to predict 1 more often. Note: Setting this with threshold p is
#'                   equivalent to setting thresholds c(1-p, p). In multiclass (or binary)
#'                   classification to adjust the probability of predicting each class. Array must
#'                   have length equal to the number of classes, with values > 0, excepting that
#'                   at most one value may be 0. The class with largest value p/t is predicted,
#'                   where p is the original probability of that class and t is the class's
#'                   threshold.
#' @param weightCol The weight column name.
#' @param aggregationDepth The depth for treeAggregate (greater than or equal to 2). If the
#'                         dimensions of features or the number of partitions are large, this param
#'                         could be adjusted to a larger size. This is an expert parameter. Default
#'                         value should be good for most cases.
#' @param lowerBoundsOnCoefficients The lower bounds on coefficients if fitting under bound
#'                                  constrained optimization.
#'                                  The bound matrix must be compatible with the shape (1, number
#'                                  of features) for binomial regression, or (number of classes,
#'                                  number of features) for multinomial regression.
#'                                  It is a R matrix.
#' @param upperBoundsOnCoefficients The upper bounds on coefficients if fitting under bound
#'                                  constrained optimization.
#'                                  The bound matrix must be compatible with the shape (1, number
#'                                  of features) for binomial regression, or (number of classes,
#'                                  number of features) for multinomial regression.
#'                                  It is a R matrix.
#' @param lowerBoundsOnIntercepts The lower bounds on intercepts if fitting under bound constrained
#'                                optimization.
#'                                The bounds vector size must be equal to 1 for binomial regression,
#'                                or the number
#'                                of classes for multinomial regression.
#' @param upperBoundsOnIntercepts The upper bounds on intercepts if fitting under bound constrained
#'                                optimization.
#'                                The bound vector size must be equal to 1 for binomial regression,
#'                                or the number of classes for multinomial regression.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @return \code{spark.logit} returns a fitted logistic regression model.
#' @rdname spark.logit
#' @aliases spark.logit,SparkDataFrame,formula-method
#' @name spark.logit
#' @examples
#' \dontrun{
#' sparkR.session()
#' # binary logistic regression
#' t <- as.data.frame(Titanic)
#' training <- createDataFrame(t)
#' model <- spark.logit(training, Survived ~ ., regParam = 0.5)
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
#' # binary logistic regression against two classes with
#' # upperBoundsOnCoefficients and upperBoundsOnIntercepts
#' ubc <- matrix(c(1.0, 0.0, 1.0, 0.0), nrow = 1, ncol = 4)
#' model <- spark.logit(training, Species ~ .,
#'                       upperBoundsOnCoefficients = ubc,
#'                       upperBoundsOnIntercepts = 1.0)
#'
#' # multinomial logistic regression
#' model <- spark.logit(training, Class ~ ., regParam = 0.5)
#' summary <- summary(model)
#'
#' # multinomial logistic regression with
#' # lowerBoundsOnCoefficients and lowerBoundsOnIntercepts
#' lbc <- matrix(c(0.0, -1.0, 0.0, -1.0, 0.0, -1.0, 0.0, -1.0), nrow = 2, ncol = 4)
#' lbi <- as.array(c(0.0, 0.0))
#' model <- spark.logit(training, Species ~ ., family = "multinomial",
#'                      lowerBoundsOnCoefficients = lbc,
#'                      lowerBoundsOnIntercepts = lbi)
#' }
#' @note spark.logit since 2.1.0
setMethod("spark.logit", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, regParam = 0.0, elasticNetParam = 0.0, maxIter = 100,
                   tol = 1E-6, family = "auto", standardization = TRUE,
                   thresholds = 0.5, weightCol = NULL, aggregationDepth = 2,
                   lowerBoundsOnCoefficients = NULL, upperBoundsOnCoefficients = NULL,
                   lowerBoundsOnIntercepts = NULL, upperBoundsOnIntercepts = NULL,
                   handleInvalid = c("error", "keep", "skip")) {
            formula <- paste(deparse(formula), collapse = "")
            row <- 0
            col <- 0

            if (!is.null(weightCol) && weightCol == "") {
              weightCol <- NULL
            } else if (!is.null(weightCol)) {
              weightCol <- as.character(weightCol)
            }

            if (!is.null(lowerBoundsOnIntercepts)) {
                lowerBoundsOnIntercepts <- as.array(lowerBoundsOnIntercepts)
            }

            if (!is.null(upperBoundsOnIntercepts)) {
                upperBoundsOnIntercepts <- as.array(upperBoundsOnIntercepts)
            }

            if (!is.null(lowerBoundsOnCoefficients)) {
              if (class(lowerBoundsOnCoefficients) != "matrix") {
                stop("lowerBoundsOnCoefficients must be a matrix.")
              }
              row <- nrow(lowerBoundsOnCoefficients)
              col <- ncol(lowerBoundsOnCoefficients)
              lowerBoundsOnCoefficients <- as.array(as.vector(lowerBoundsOnCoefficients))
            }

            if (!is.null(upperBoundsOnCoefficients)) {
              if (class(upperBoundsOnCoefficients) != "matrix") {
                stop("upperBoundsOnCoefficients must be a matrix.")
              }

              if (!is.null(lowerBoundsOnCoefficients) && (row != nrow(upperBoundsOnCoefficients)
                || col != ncol(upperBoundsOnCoefficients))) {
                stop("dimension of upperBoundsOnCoefficients ",
                     "is not the same as lowerBoundsOnCoefficients")
              }

              if (is.null(lowerBoundsOnCoefficients)) {
                row <- nrow(upperBoundsOnCoefficients)
                col <- ncol(upperBoundsOnCoefficients)
              }

              upperBoundsOnCoefficients <- as.array(as.vector(upperBoundsOnCoefficients))
            }

            handleInvalid <- match.arg(handleInvalid)

            jobj <- callJStatic("org.apache.spark.ml.r.LogisticRegressionWrapper", "fit",
                                data@sdf, formula, as.numeric(regParam),
                                as.numeric(elasticNetParam), as.integer(maxIter),
                                as.numeric(tol), as.character(family),
                                as.logical(standardization), as.array(thresholds),
                                weightCol, as.integer(aggregationDepth),
                                as.integer(row), as.integer(col),
                                lowerBoundsOnCoefficients, upperBoundsOnCoefficients,
                                lowerBoundsOnIntercepts, upperBoundsOnIntercepts,
                                handleInvalid)
            new("LogisticRegressionModel", jobj = jobj)
          })

#  Get the summary of an LogisticRegressionModel

#' @param object an LogisticRegressionModel fitted by \code{spark.logit}.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{coefficients} (coefficients matrix of the fitted model).
#' @rdname spark.logit
#' @aliases summary,LogisticRegressionModel-method
#' @note summary(LogisticRegressionModel) since 2.1.0
setMethod("summary", signature(object = "LogisticRegressionModel"),
          function(object) {
            jobj <- object@jobj
            features <- callJMethod(jobj, "rFeatures")
            labels <- callJMethod(jobj, "labels")
            coefficients <- callJMethod(jobj, "rCoefficients")
            nCol <- length(coefficients) / length(features)
            coefficients <- matrix(unlist(coefficients), ncol = nCol)
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

#  Predicted values based on an LogisticRegressionModel model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns the predicted values based on an LogisticRegressionModel.
#' @rdname spark.logit
#' @aliases predict,LogisticRegressionModel,SparkDataFrame-method
#' @note predict(LogisticRegressionModel) since 2.1.0
setMethod("predict", signature(object = "LogisticRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Save fitted LogisticRegressionModel to the input path

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.logit
#' @aliases write.ml,LogisticRegressionModel,character-method
#' @note write.ml(LogisticRegression, character) since 2.1.0
setMethod("write.ml", signature(object = "LogisticRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' Multilayer Perceptron Classification Model
#'
#' \code{spark.mlp} fits a multi-layer perceptron neural network model against a SparkDataFrame.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#' Only categorical data is supported.
#' For more details, see
#' \href{https://spark.apache.org/docs/latest/ml-classification-regression.html}{
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
#'        numeric vector.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @return \code{spark.mlp} returns a fitted Multilayer Perceptron Classification Model.
#' @rdname spark.mlp
#' @aliases spark.mlp,SparkDataFrame,formula-method
#' @name spark.mlp
#' @seealso \link{read.ml}
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
                   tol = 1E-6, stepSize = 0.03, seed = NULL, initialWeights = NULL,
                   handleInvalid = c("error", "keep", "skip")) {
            formula <- paste(deparse(formula), collapse = "")
            if (is.null(layers)) {
              stop("layers must be a integer vector with length > 1.")
            }
            layers <- as.integer(na.omit(layers))
            if (length(layers) <= 1) {
              stop("layers must be a integer vector with length > 1.")
            }
            if (!is.null(seed)) {
              seed <- as.character(as.integer(seed))
            }
            if (!is.null(initialWeights)) {
              initialWeights <- as.array(as.numeric(na.omit(initialWeights)))
            }
            handleInvalid <- match.arg(handleInvalid)
            jobj <- callJStatic("org.apache.spark.ml.r.MultilayerPerceptronClassifierWrapper",
                                "fit", data@sdf, formula, as.integer(blockSize), as.array(layers),
                                as.character(solver), as.integer(maxIter), as.numeric(tol),
                                as.numeric(stepSize), seed, initialWeights, handleInvalid)
            new("MultilayerPerceptronClassificationModel", jobj = jobj)
          })

#  Returns the summary of a Multilayer Perceptron Classification Model produced by \code{spark.mlp}

#' @param object a Multilayer Perceptron Classification Model fitted by \code{spark.mlp}
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{numOfInputs} (number of inputs), \code{numOfOutputs}
#'         (number of outputs), \code{layers} (array of layer sizes including input
#'         and output layers), and \code{weights} (the weights of layers).
#'         For \code{weights}, it is a numeric vector with length equal to the expected
#'         given the architecture (i.e., for 8-10-2 network, 112 connection weights).
#' @rdname spark.mlp
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

#  Makes predictions from a model produced by spark.mlp().

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labeled in a column named
#' "prediction".
#' @rdname spark.mlp
#' @aliases predict,MultilayerPerceptronClassificationModel-method
#' @note predict(MultilayerPerceptronClassificationModel) since 2.1.0
setMethod("predict", signature(object = "MultilayerPerceptronClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Saves the Multilayer Perceptron Classification Model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.mlp
#' @aliases write.ml,MultilayerPerceptronClassificationModel,character-method
#' @seealso \link{write.ml}
#' @note write.ml(MultilayerPerceptronClassificationModel, character) since 2.1.0
setMethod("write.ml", signature(object = "MultilayerPerceptronClassificationModel",
          path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
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
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional argument(s) passed to the method. Currently only \code{smoothing}.
#' @return \code{spark.naiveBayes} returns a fitted naive Bayes model.
#' @rdname spark.naiveBayes
#' @aliases spark.naiveBayes,SparkDataFrame,formula-method
#' @name spark.naiveBayes
#' @seealso e1071: \url{https://cran.r-project.org/package=e1071}
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
          function(data, formula, smoothing = 1.0,
                   handleInvalid = c("error", "keep", "skip")) {
            formula <- paste(deparse(formula), collapse = "")
            handleInvalid <- match.arg(handleInvalid)
            jobj <- callJStatic("org.apache.spark.ml.r.NaiveBayesWrapper", "fit",
                                formula, data@sdf, smoothing, handleInvalid)
            new("NaiveBayesModel", jobj = jobj)
          })

#  Returns the summary of a naive Bayes model produced by \code{spark.naiveBayes}

#' @param object a naive Bayes model fitted by \code{spark.naiveBayes}.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{apriori} (the label distribution) and
#'         \code{tables} (conditional probabilities given the target label).
#' @rdname spark.naiveBayes
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

#  Makes predictions from a naive Bayes model or a model produced by spark.naiveBayes(),
#  similarly to R package e1071's predict.

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labeled in a column named
#' "prediction".
#' @rdname spark.naiveBayes
#' @note predict(NaiveBayesModel) since 2.0.0
setMethod("predict", signature(object = "NaiveBayesModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Saves the Bernoulli naive Bayes model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.naiveBayes
#' @seealso \link{write.ml}
#' @note write.ml(NaiveBayesModel, character) since 2.0.0
setMethod("write.ml", signature(object = "NaiveBayesModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' Factorization Machines Classification Model
#'
#' \code{spark.fmClassifier} fits a factorization classification model against a SparkDataFrame.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#' Only categorical data is supported.
#'
#' @param data a \code{SparkDataFrame} of observations and labels for model fitting.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param factorSize dimensionality of the factors.
#' @param fitLinear whether to fit linear term.  # TODO Can we express this with formula?
#' @param regParam the regularization parameter.
#' @param miniBatchFraction the mini-batch fraction parameter.
#' @param initStd the standard deviation of initial coefficients.
#' @param maxIter maximum iteration number.
#' @param stepSize stepSize parameter.
#' @param tol convergence tolerance of iterations.
#' @param solver solver parameter, supported options: "gd" (minibatch gradient descent) or "adamW".
#' @param thresholds in binary classification, in range [0, 1]. If the estimated probability of
#'                   class label 1 is > threshold, then predict 1, else 0. A high threshold
#'                   encourages the model to predict 0 more often; a low threshold encourages the
#'                   model to predict 1 more often. Note: Setting this with threshold p is
#'                   equivalent to setting thresholds c(1-p, p).
#' @param seed seed parameter for weights initialization.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @return \code{spark.fmClassifier} returns a fitted Factorization Machines Classification Model.
#' @rdname spark.fmClassifier
#' @aliases spark.fmClassifier,SparkDataFrame,formula-method
#' @name spark.fmClassifier
#' @seealso \link{read.ml}
#' @examples
#' \dontrun{
#' df <- read.df("data/mllib/sample_binary_classification_data.txt", source = "libsvm")
#'
#' # fit Factorization Machines Classification Model
#' model <- spark.fmClassifier(
#'            df, label ~ features,
#'            regParam = 0.01, maxIter = 10, fitLinear = TRUE
#'          )
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
#' @note spark.fmClassifier since 3.1.0
setMethod("spark.fmClassifier", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, factorSize = 8, fitLinear = TRUE, regParam = 0.0,
                   miniBatchFraction = 1.0, initStd = 0.01, maxIter = 100, stepSize=1.0,
                   tol = 1e-6, solver = c("adamW", "gd"), thresholds = NULL, seed = NULL,
                   handleInvalid = c("error", "keep", "skip")) {

            formula <- paste(deparse(formula), collapse = "")

            if (!is.null(seed)) {
              seed <- as.character(as.integer(seed))
            }

            if (!is.null(thresholds)) {
              thresholds <- as.list(thresholds)
            }

            solver <- match.arg(solver)
            handleInvalid <- match.arg(handleInvalid)

            jobj <- callJStatic("org.apache.spark.ml.r.FMClassifierWrapper",
                                "fit",
                                data@sdf,
                                formula,
                                as.integer(factorSize),
                                as.logical(fitLinear),
                                as.numeric(regParam),
                                as.numeric(miniBatchFraction),
                                as.numeric(initStd),
                                as.integer(maxIter),
                                as.numeric(stepSize),
                                as.numeric(tol),
                                solver,
                                seed,
                                thresholds,
                                handleInvalid)
            new("FMClassificationModel", jobj = jobj)
          })

#  Returns the summary of a FM Classification model produced by \code{spark.fmClassifier}

#' @param object a FM Classification model fitted by \code{spark.fmClassifier}.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#' @rdname spark.fmClassifier
#' @note summary(FMClassificationModel) since 3.1.0
setMethod("summary", signature(object = "FMClassificationModel"),
          function(object) {
            jobj <- object@jobj
            features <- callJMethod(jobj, "rFeatures")
            coefficients <- callJMethod(jobj, "rCoefficients")
            coefficients <- as.matrix(unlist(coefficients))
            colnames(coefficients) <- c("Estimate")
            rownames(coefficients) <- unlist(features)
            numClasses <- callJMethod(jobj, "numClasses")
            numFeatures <- callJMethod(jobj, "numFeatures")
            raw_factors <- unlist(callJMethod(jobj, "rFactors"))
            factor_size <- callJMethod(jobj, "factorSize")

            list(
              coefficients = coefficients,
              factors = matrix(raw_factors, ncol = factor_size),
              numClasses = numClasses, numFeatures = numFeatures,
              factorSize = factor_size
            )
          })

#  Predicted values based on an FMClassificationModel model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns the predicted values based on a FM Classification model.
#' @rdname spark.fmClassifier
#' @aliases predict,FMClassificationModel,SparkDataFrame-method
#' @note predict(FMClassificationModel) since 3.1.0
setMethod("predict", signature(object = "FMClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Save fitted FMClassificationModel to the input path

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.fmClassifier
#' @aliases write.ml,FMClassificationModel,character-method
#' @note write.ml(FMClassificationModel, character) since 3.1.0
setMethod("write.ml", signature(object = "FMClassificationModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })
