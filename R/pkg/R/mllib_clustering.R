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

# mllib_clustering.R: Provides methods for MLlib clustering algorithms integration

#' S4 class that represents a BisectingKMeansModel
#'
#' @param jobj a Java object reference to the backing Scala BisectingKMeansModel
#' @note BisectingKMeansModel since 2.2.0
setClass("BisectingKMeansModel", representation(jobj = "jobj"))

#' S4 class that represents a GaussianMixtureModel
#'
#' @param jobj a Java object reference to the backing Scala GaussianMixtureModel
#' @note GaussianMixtureModel since 2.1.0
setClass("GaussianMixtureModel", representation(jobj = "jobj"))

#' S4 class that represents a KMeansModel
#'
#' @param jobj a Java object reference to the backing Scala KMeansModel
#' @note KMeansModel since 2.0.0
setClass("KMeansModel", representation(jobj = "jobj"))

#' S4 class that represents an LDAModel
#'
#' @param jobj a Java object reference to the backing Scala LDAWrapper
#' @note LDAModel since 2.1.0
setClass("LDAModel", representation(jobj = "jobj"))

#' S4 class that represents a PowerIterationClustering
#'
#' @param jobj a Java object reference to the backing Scala PowerIterationClustering
#' @note PowerIterationClustering since 3.0.0
setClass("PowerIterationClustering", slots = list(jobj = "jobj"))

#' Bisecting K-Means Clustering Model
#'
#' Fits a bisecting k-means clustering model against a SparkDataFrame.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', '-', '*', and '^'.
#'                Note that the response variable of formula is empty in spark.bisectingKmeans.
#' @param k the desired number of leaf clusters. Must be > 1.
#'          The actual number could be smaller if there are no divisible leaf clusters.
#' @param maxIter maximum iteration number.
#' @param seed the random seed.
#' @param minDivisibleClusterSize The minimum number of points (if greater than or equal to 1.0)
#'                                or the minimum proportion of points (if less than 1.0) of a
#'                                divisible cluster. Note that it is an expert parameter. The
#'                                default value should be good enough for most cases.
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.bisectingKmeans} returns a fitted bisecting k-means model.
#' @rdname spark.bisectingKmeans
#' @aliases spark.bisectingKmeans,SparkDataFrame,formula-method
#' @name spark.bisectingKmeans
#' @examples
#' \dontrun{
#' sparkR.session()
#' t <- as.data.frame(Titanic)
#' df <- createDataFrame(t)
#' model <- spark.bisectingKmeans(df, Class ~ Survived, k = 4)
#' summary(model)
#'
#' # get fitted result from a bisecting k-means model
#' fitted.model <- fitted(model, "centers")
#' showDF(fitted.model)
#'
#' # fitted values on training data
#' fitted <- predict(model, df)
#' head(select(fitted, "Class", "prediction"))
#'
#' # save fitted model to input path
#' path <- "path/to/model"
#' write.ml(model, path)
#'
#' # can also read back the saved model and print
#' savedModel <- read.ml(path)
#' summary(savedModel)
#' }
#' @note spark.bisectingKmeans since 2.2.0
#' @seealso \link{predict}, \link{read.ml}, \link{write.ml}
setMethod("spark.bisectingKmeans", signature(data = "SparkDataFrame", formula = "formula"),
          function(data, formula, k = 4, maxIter = 20, seed = NULL, minDivisibleClusterSize = 1.0) {
            formula <- paste0(deparse(formula), collapse = "")
            if (!is.null(seed)) {
              seed <- as.character(as.integer(seed))
            }
            jobj <- callJStatic("org.apache.spark.ml.r.BisectingKMeansWrapper", "fit",
                                data@sdf, formula, as.integer(k), as.integer(maxIter),
                                seed, as.numeric(minDivisibleClusterSize))
            new("BisectingKMeansModel", jobj = jobj)
          })

#  Get the summary of a bisecting k-means model

#' @param object a fitted bisecting k-means model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes the model's \code{k} (number of cluster centers),
#'         \code{coefficients} (model cluster centers),
#'         \code{size} (number of data points in each cluster), \code{cluster}
#'         (cluster centers of the transformed data; cluster is NULL if is.loaded is TRUE),
#'         and \code{is.loaded} (whether the model is loaded from a saved file).
#' @rdname spark.bisectingKmeans
#' @note summary(BisectingKMeansModel) since 2.2.0
setMethod("summary", signature(object = "BisectingKMeansModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            features <- callJMethod(jobj, "features")
            coefficients <- callJMethod(jobj, "coefficients")
            k <- callJMethod(jobj, "k")
            size <- callJMethod(jobj, "size")
            coefficients <- t(matrix(coefficients, ncol = k))
            colnames(coefficients) <- unlist(features)
            rownames(coefficients) <- 1:k
            cluster <- if (is.loaded) {
              NULL
            } else {
              dataFrame(callJMethod(jobj, "cluster"))
            }
            list(k = k, coefficients = coefficients, size = size,
            cluster = cluster, is.loaded = is.loaded)
          })

#  Predicted values based on a bisecting k-means model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns the predicted values based on a bisecting k-means model.
#' @rdname spark.bisectingKmeans
#' @note predict(BisectingKMeansModel) since 2.2.0
setMethod("predict", signature(object = "BisectingKMeansModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' Get fitted result from a bisecting k-means model
#'
#' Get fitted result from a bisecting k-means model.
#' Note: A saved-loaded model does not support this method.
#'
#' @param method type of fitted results, \code{"centers"} for cluster centers
#'        or \code{"classes"} for assigned classes.
#' @return \code{fitted} returns a SparkDataFrame containing fitted values.
#' @rdname spark.bisectingKmeans
#' @note fitted since 2.2.0
setMethod("fitted", signature(object = "BisectingKMeansModel"),
          function(object, method = c("centers", "classes")) {
            method <- match.arg(method)
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            if (is.loaded) {
              stop("Saved-loaded bisecting k-means model does not support 'fitted' method")
            } else {
              dataFrame(callJMethod(jobj, "fitted", method))
            }
          })

#  Save fitted MLlib model to the input path

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.bisectingKmeans
#' @note write.ml(BisectingKMeansModel, character) since 2.2.0
setMethod("write.ml", signature(object = "BisectingKMeansModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
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
#'         \code{sigma} (sigma), \code{loglik} (loglik), and \code{posterior} (posterior).
#' @aliases spark.gaussianMixture,SparkDataFrame,formula-method
#' @rdname spark.gaussianMixture
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
            loglik <- callJMethod(jobj, "logLikelihood")
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
            list(lambda = lambda, mu = mu, sigma = sigma, loglik = loglik,
                 posterior = posterior, is.loaded = is.loaded)
          })

#  Predicted values based on a gaussian mixture model

#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted labels in a column named
#'         "prediction".
#' @aliases predict,GaussianMixtureModel,SparkDataFrame-method
#' @rdname spark.gaussianMixture
#' @note predict(GaussianMixtureModel) since 2.1.0
setMethod("predict", signature(object = "GaussianMixtureModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Save fitted MLlib model to the input path

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @aliases write.ml,GaussianMixtureModel,character-method
#' @rdname spark.gaussianMixture
#' @note write.ml(GaussianMixtureModel, character) since 2.1.0
setMethod("write.ml", signature(object = "GaussianMixtureModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
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
#' @param initMode the initialization algorithm chosen to fit the model.
#' @param seed the random seed for cluster initialization.
#' @param initSteps the number of steps for the k-means|| initialization mode.
#'                  This is an advanced setting, the default of 2 is almost always enough.
#'                  Must be > 0.
#' @param tol convergence tolerance of iterations.
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.kmeans} returns a fitted k-means model.
#' @rdname spark.kmeans
#' @aliases spark.kmeans,SparkDataFrame,formula-method
#' @name spark.kmeans
#' @examples
#' \dontrun{
#' sparkR.session()
#' t <- as.data.frame(Titanic)
#' df <- createDataFrame(t)
#' model <- spark.kmeans(df, Class ~ Survived, k = 4, initMode = "random")
#' summary(model)
#'
#' # fitted values on training data
#' fitted <- predict(model, df)
#' head(select(fitted, "Class", "prediction"))
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
            coefficients <- t(matrix(unlist(coefficients), ncol = clusterSize))
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
#' @note predict(KMeansModel) since 2.0.0
setMethod("predict", signature(object = "KMeansModel"),
          function(object, newData) {
            predict_internal(object, newData)
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

#  Save fitted MLlib model to the input path

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.kmeans
#' @note write.ml(KMeansModel, character) since 2.0.0
setMethod("write.ml", signature(object = "KMeansModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
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
#' @examples
#' \dontrun{
#' text <- read.df("data/mllib/sample_lda_libsvm_data.txt", source = "libsvm")
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

#  Returns the summary of a Latent Dirichlet Allocation model produced by \code{spark.lda}

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
#'         \item{\code{trainingLogLikelihood}}{Log likelihood of the observed tokens in the
#'               training set, given the current parameter estimates:
#'               log P(docs | topics, topic distributions for docs, Dirichlet hyperparameters)
#'               It is only for distributed LDA model (i.e., optimizer = "em")}
#'         \item{\code{logPrior}}{Log probability of the current parameter estimate:
#'               log P(topics, topic distributions for docs | Dirichlet hyperparameters)
#'               It is only for distributed LDA model (i.e., optimizer = "em")}
#' @rdname spark.lda
#' @aliases summary,LDAModel-method
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
            trainingLogLikelihood <- if (isDistributed) {
              callJMethod(jobj, "trainingLogLikelihood")
            } else {
              NA
            }
            logPrior <- if (isDistributed) {
              callJMethod(jobj, "logPrior")
            } else {
              NA
            }
            list(docConcentration = unlist(docConcentration),
                 topicConcentration = topicConcentration,
                 logLikelihood = logLikelihood, logPerplexity = logPerplexity,
                 isDistributed = isDistributed, vocabSize = vocabSize,
                 topics = topics, vocabulary = unlist(vocabulary),
                 trainingLogLikelihood = trainingLogLikelihood, logPrior = logPrior)
          })

#  Returns the log perplexity of a Latent Dirichlet Allocation model produced by \code{spark.lda}

#' @return \code{spark.perplexity} returns the log perplexity of given SparkDataFrame, or the log
#'         perplexity of the training data if missing argument "data".
#' @rdname spark.lda
#' @aliases spark.perplexity,LDAModel-method
#' @note spark.perplexity(LDAModel) since 2.1.0
setMethod("spark.perplexity", signature(object = "LDAModel", data = "SparkDataFrame"),
          function(object, data) {
            ifelse(missing(data), callJMethod(object@jobj, "logPerplexity"),
                   callJMethod(object@jobj, "computeLogPerplexity", data@sdf))
         })

#  Returns posterior probabilities from a Latent Dirichlet Allocation model produced by spark.lda()

#' @param newData A SparkDataFrame for testing.
#' @return \code{spark.posterior} returns a SparkDataFrame containing posterior probabilities
#'         vectors named "topicDistribution".
#' @rdname spark.lda
#' @aliases spark.posterior,LDAModel,SparkDataFrame-method
#' @note spark.posterior(LDAModel) since 2.1.0
setMethod("spark.posterior", signature(object = "LDAModel", newData = "SparkDataFrame"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Saves the Latent Dirichlet Allocation model to the input path.

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname spark.lda
#' @aliases write.ml,LDAModel,character-method
#' @seealso \link{read.ml}
#' @note write.ml(LDAModel, character) since 2.1.0
setMethod("write.ml", signature(object = "LDAModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' PowerIterationClustering
#'
#' A scalable graph clustering algorithm. Users can call \code{spark.assignClusters} to
#' return a cluster assignment for each input vertex.
#' Run the PIC algorithm and returns a cluster assignment for each input vertex.
#' @param data a SparkDataFrame.
#' @param k the number of clusters to create.
#' @param initMode the initialization algorithm; "random" or "degree"
#' @param maxIter the maximum number of iterations.
#' @param sourceCol the name of the input column for source vertex IDs.
#' @param destinationCol the name of the input column for destination vertex IDs
#' @param weightCol weight column name. If this is not set or \code{NULL},
#'                  we treat all instance weights as 1.0.
#' @param ... additional argument(s) passed to the method.
#' @return A dataset that contains columns of vertex id and the corresponding cluster for the id.
#'         The schema of it will be: \code{id: integer}, \code{cluster: integer}
#' @rdname spark.powerIterationClustering
#' @aliases spark.assignClusters,SparkDataFrame-method
#' @examples
#' \dontrun{
#' df <- createDataFrame(list(list(0L, 1L, 1.0), list(0L, 2L, 1.0),
#'                            list(1L, 2L, 1.0), list(3L, 4L, 1.0),
#'                            list(4L, 0L, 0.1)),
#'                       schema = c("src", "dst", "weight"))
#' clusters <- spark.assignClusters(df, initMode = "degree", weightCol = "weight")
#' showDF(clusters)
#' }
#' @note spark.assignClusters(SparkDataFrame) since 3.0.0
setMethod("spark.assignClusters",
          signature(data = "SparkDataFrame"),
          function(data, k = 2L, initMode = c("random", "degree"), maxIter = 20L,
            sourceCol = "src", destinationCol = "dst", weightCol = NULL) {
            if (!is.integer(k) || k < 1) {
              stop("k should be a number with value >= 1.")
            }
            if (!is.integer(maxIter) || maxIter <= 0) {
              stop("maxIter should be a number with value > 0.")
            }
            initMode <- match.arg(initMode)
            if (!is.null(weightCol) && weightCol == "") {
              weightCol <- NULL
            } else if (!is.null(weightCol)) {
              weightCol <- as.character(weightCol)
            }
            jobj <- callJStatic("org.apache.spark.ml.r.PowerIterationClusteringWrapper",
                                "getPowerIterationClustering",
                                as.integer(k), initMode,
                                as.integer(maxIter), as.character(sourceCol),
                                as.character(destinationCol), weightCol)
            object <- new("PowerIterationClustering", jobj = jobj)
            dataFrame(callJMethod(object@jobj, "assignClusters", data@sdf))
          })
