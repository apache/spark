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

library(testthat)

context("MLlib clustering algorithms")

# Tests for MLlib clustering algorithms in SparkR
sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

absoluteSparkPath <- function(x) {
  sparkHome <- sparkR.conf("spark.home")
  file.path(sparkHome, x)
}

test_that("spark.bisectingKmeans", {
  newIris <- iris
  newIris$Species <- NULL
  training <- suppressWarnings(createDataFrame(newIris))

  take(training, 1)

  model <- spark.bisectingKmeans(data = training, ~ .)
  sample <- take(select(predict(model, training), "prediction"), 1)
  expect_equal(typeof(sample$prediction), "integer")
  expect_equal(sample$prediction, 1)

  # Test fitted works on Bisecting KMeans
  fitted.model <- fitted(model)
  expect_equal(sort(collect(distinct(select(fitted.model, "prediction")))$prediction),
               c(0, 1, 2, 3))

  # Test summary works on KMeans
  summary.model <- summary(model)
  cluster <- summary.model$cluster
  k <- summary.model$k
  expect_equal(k, 4)
  expect_equal(sort(collect(distinct(select(cluster, "prediction")))$prediction),
               c(0, 1, 2, 3))

  # Test model save/load
  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-bisectingkmeans", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    summary2 <- summary(model2)
    expect_equal(sort(unlist(summary.model$size)), sort(unlist(summary2$size)))
    expect_equal(summary.model$coefficients, summary2$coefficients)
    expect_true(!summary.model$is.loaded)
    expect_true(summary2$is.loaded)

    unlink(modelPath)
  }
})

test_that("spark.gaussianMixture", {
  # R code to reproduce the result.
  # nolint start
  #' library(mvtnorm)
  #' set.seed(1)
  #' a <- rmvnorm(7, c(0, 0))
  #' b <- rmvnorm(8, c(10, 10))
  #' data <- rbind(a, b)
  #' model <- mvnormalmixEM(data, k = 2)
  #' model$lambda
  #
  #  [1] 0.4666667 0.5333333
  #
  #' model$mu
  #
  #  [1] 0.11731091 -0.06192351
  #  [1] 10.363673  9.897081
  #
  #' model$sigma
  #
  #  [[1]]
  #             [,1]       [,2]
  #  [1,] 0.62049934 0.06880802
  #  [2,] 0.06880802 1.27431874
  #
  #  [[2]]
  #            [,1]     [,2]
  #  [1,] 0.2961543 0.160783
  #  [2,] 0.1607830 1.008878
  #
  #' model$loglik
  #
  #  [1] -46.89499
  # nolint end
  data <- list(list(-0.6264538, 0.1836433), list(-0.8356286, 1.5952808),
               list(0.3295078, -0.8204684), list(0.4874291, 0.7383247),
               list(0.5757814, -0.3053884), list(1.5117812, 0.3898432),
               list(-0.6212406, -2.2146999), list(11.1249309, 9.9550664),
               list(9.9838097, 10.9438362), list(10.8212212, 10.5939013),
               list(10.9189774, 10.7821363), list(10.0745650, 8.0106483),
               list(10.6198257, 9.9438713), list(9.8442045, 8.5292476),
               list(9.5218499, 10.4179416))
  df <- createDataFrame(data, c("x1", "x2"))
  model <- spark.gaussianMixture(df, ~ x1 + x2, k = 2)
  stats <- summary(model)
  rLambda <- c(0.4666667, 0.5333333)
  rMu <- c(0.11731091, -0.06192351, 10.363673, 9.897081)
  rSigma <- c(0.62049934, 0.06880802, 0.06880802, 1.27431874,
              0.2961543, 0.160783, 0.1607830, 1.008878)
  rLoglik <- -46.89499
  expect_equal(stats$lambda, rLambda, tolerance = 1e-3)
  expect_equal(unlist(stats$mu), rMu, tolerance = 1e-3)
  expect_equal(unlist(stats$sigma), rSigma, tolerance = 1e-3)
  expect_equal(unlist(stats$loglik), rLoglik, tolerance = 1e-3)
  p <- collect(select(predict(model, df), "prediction"))
  expect_equal(p$prediction, c(0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1))

  # Test model save/load
  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-gaussianMixture", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    stats2 <- summary(model2)
    expect_equal(stats$lambda, stats2$lambda)
    expect_equal(unlist(stats$mu), unlist(stats2$mu))
    expect_equal(unlist(stats$sigma), unlist(stats2$sigma))
    expect_equal(unlist(stats$loglik), unlist(stats2$loglik))

    unlink(modelPath)
  }
})

test_that("spark.kmeans", {
  newIris <- iris
  newIris$Species <- NULL
  training <- suppressWarnings(createDataFrame(newIris))

  take(training, 1)

  model <- spark.kmeans(data = training, ~ ., k = 2, maxIter = 10, initMode = "random")
  sample <- take(select(predict(model, training), "prediction"), 1)
  expect_equal(typeof(sample$prediction), "integer")
  expect_equal(sample$prediction, 0)

  # Test stats::kmeans is working
  statsModel <- kmeans(x = newIris, centers = 2)
  expect_equal(sort(unique(statsModel$cluster)), c(1, 2))

  # Test fitted works on KMeans
  fitted.model <- fitted(model)
  expect_equal(sort(collect(distinct(select(fitted.model, "prediction")))$prediction), c(0, 1))

  # Test summary works on KMeans
  summary.model <- summary(model)
  cluster <- summary.model$cluster
  k <- summary.model$k
  expect_equal(k, 2)
  expect_equal(sort(collect(distinct(select(cluster, "prediction")))$prediction), c(0, 1))

  # test summary coefficients return matrix type
  expect_true(class(summary.model$coefficients) == "matrix")
  expect_true(class(summary.model$coefficients[1, ]) == "numeric")

  # Test model save/load
  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-kmeans", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    summary2 <- summary(model2)
    expect_equal(sort(unlist(summary.model$size)), sort(unlist(summary2$size)))
    expect_equal(summary.model$coefficients, summary2$coefficients)
    expect_true(!summary.model$is.loaded)
    expect_true(summary2$is.loaded)

    unlink(modelPath)
  }

  # Test Kmeans on dataset that is sensitive to seed value
  col1 <- c(1, 2, 3, 4, 0, 1, 2, 3, 4, 0)
  col2 <- c(1, 2, 3, 4, 0, 1, 2, 3, 4, 0)
  col3 <- c(1, 2, 3, 4, 0, 1, 2, 3, 4, 0)
  cols <- as.data.frame(cbind(col1, col2, col3))
  df <- createDataFrame(cols)

  model1 <- spark.kmeans(data = df, ~ ., k = 5, maxIter = 10,
                         initMode = "random", seed = 1, tol = 1E-5)
  model2 <- spark.kmeans(data = df, ~ ., k = 5, maxIter = 10,
                         initMode = "random", seed = 22222, tol = 1E-5)

  summary.model1 <- summary(model1)
  summary.model2 <- summary(model2)
  cluster1 <- summary.model1$cluster
  cluster2 <- summary.model2$cluster
  clusterSize1 <- summary.model1$clusterSize
  clusterSize2 <- summary.model2$clusterSize

  # The predicted clusters are different
  expect_equal(sort(collect(distinct(select(cluster1, "prediction")))$prediction),
             c(0, 1, 2, 3))
  expect_equal(sort(collect(distinct(select(cluster2, "prediction")))$prediction),
             c(0, 1, 2))
  expect_equal(clusterSize1, 4)
  expect_equal(clusterSize2, 3)
})

test_that("spark.lda with libsvm", {
  text <- read.df(absoluteSparkPath("data/mllib/sample_lda_libsvm_data.txt"), source = "libsvm")
  model <- spark.lda(text, optimizer = "em")

  stats <- summary(model, 10)
  isDistributed <- stats$isDistributed
  logLikelihood <- stats$logLikelihood
  logPerplexity <- stats$logPerplexity
  vocabSize <- stats$vocabSize
  topics <- stats$topicTopTerms
  weights <- stats$topicTopTermsWeights
  vocabulary <- stats$vocabulary
  trainingLogLikelihood <- stats$trainingLogLikelihood
  logPrior <- stats$logPrior

  expect_true(isDistributed)
  expect_true(logLikelihood <= 0 & is.finite(logLikelihood))
  expect_true(logPerplexity >= 0 & is.finite(logPerplexity))
  expect_equal(vocabSize, 11)
  expect_true(is.null(vocabulary))
  expect_true(trainingLogLikelihood <= 0 & !is.na(trainingLogLikelihood))
  expect_true(logPrior <= 0 & !is.na(logPrior))

  # Test model save/load
  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-lda", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    stats2 <- summary(model2)

    expect_true(stats2$isDistributed)
    expect_equal(logLikelihood, stats2$logLikelihood)
    expect_equal(logPerplexity, stats2$logPerplexity)
    expect_equal(vocabSize, stats2$vocabSize)
    expect_equal(vocabulary, stats2$vocabulary)
    expect_equal(trainingLogLikelihood, stats2$trainingLogLikelihood)
    expect_equal(logPrior, stats2$logPrior)

    unlink(modelPath)
  }
})

test_that("spark.lda with text input", {
  text <- read.text(absoluteSparkPath("data/mllib/sample_lda_data.txt"))
  model <- spark.lda(text, optimizer = "online", features = "value")

  stats <- summary(model)
  isDistributed <- stats$isDistributed
  logLikelihood <- stats$logLikelihood
  logPerplexity <- stats$logPerplexity
  vocabSize <- stats$vocabSize
  topics <- stats$topicTopTerms
  weights <- stats$topicTopTermsWeights
  vocabulary <- stats$vocabulary
  trainingLogLikelihood <- stats$trainingLogLikelihood
  logPrior <- stats$logPrior

  expect_false(isDistributed)
  expect_true(logLikelihood <= 0 & is.finite(logLikelihood))
  expect_true(logPerplexity >= 0 & is.finite(logPerplexity))
  expect_equal(vocabSize, 10)
  expect_true(setequal(stats$vocabulary, c("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")))
  expect_true(is.na(trainingLogLikelihood))
  expect_true(is.na(logPrior))

  # Test model save/load
  modelPath <- tempfile(pattern = "spark-lda-text", fileext = ".tmp")
  write.ml(model, modelPath)
  expect_error(write.ml(model, modelPath))
  write.ml(model, modelPath, overwrite = TRUE)
  model2 <- read.ml(modelPath)
  stats2 <- summary(model2)

  expect_false(stats2$isDistributed)
  expect_equal(logLikelihood, stats2$logLikelihood)
  expect_equal(logPerplexity, stats2$logPerplexity)
  expect_equal(vocabSize, stats2$vocabSize)
  expect_true(all.equal(vocabulary, stats2$vocabulary))
  expect_true(is.na(stats2$trainingLogLikelihood))
  expect_true(is.na(stats2$logPrior))

  unlink(modelPath)
})

test_that("spark.posterior and spark.perplexity", {
  text <- read.text(absoluteSparkPath("data/mllib/sample_lda_data.txt"))
  model <- spark.lda(text, features = "value", k = 3)

  # Assert perplexities are equal
  stats <- summary(model)
  logPerplexity <- spark.perplexity(model, text)
  expect_equal(logPerplexity, stats$logPerplexity)

  # Assert the sum of every topic distribution is equal to 1
  posterior <- spark.posterior(model, text)
  local.posterior <- collect(posterior)$topicDistribution
  expect_equal(length(local.posterior), sum(unlist(local.posterior)))
})

test_that("spark.assignClusters", {
  df <- createDataFrame(list(list(0L, 1L, 1.0), list(0L, 2L, 1.0),
                             list(1L, 2L, 1.0), list(3L, 4L, 1.0),
                             list(4L, 0L, 0.1)),
                        schema = c("src", "dst", "weight"))
  clusters <- spark.assignClusters(df, initMode = "degree", weightCol = "weight")
  expected_result <- createDataFrame(list(list(4L, 1L), list(0L, 0L),
                                          list(1L, 0L), list(3L, 1L),
                                          list(2L, 0L)),
                                     schema = c("id", "cluster"))
  expect_equivalent(expected_result, clusters)
})

sparkR.session.stop()
