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

context("MLlib functions")

# Tests for MLlib functions in SparkR
sparkSession <- sparkR.session(enableHiveSupport = FALSE)

absoluteSparkPath <- function(x) {
  sparkHome <- sparkR.conf("spark.home")
  file.path(sparkHome, x)
}

test_that("formula of spark.glm", {
  training <- suppressWarnings(createDataFrame(iris))
  # directly calling the spark API
  # dot minus and intercept vs native glm
  model <- spark.glm(training, Sepal_Width ~ . - Species + 0)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ . - Species + 0, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)

  # feature interaction vs native glm
  model <- spark.glm(training, Sepal_Width ~ Species:Sepal_Length)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Species:Sepal.Length, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)

  # glm should work with long formula
  training <- suppressWarnings(createDataFrame(iris))
  training$LongLongLongLongLongName <- training$Sepal_Width
  training$VeryLongLongLongLonLongName <- training$Sepal_Length
  training$AnotherLongLongLongLongName <- training$Species
  model <- spark.glm(training, LongLongLongLongLongName ~ VeryLongLongLongLonLongName +
    AnotherLongLongLongLongName)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Sepal.Length + Species, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)
})

test_that("spark.glm and predict", {
  training <- suppressWarnings(createDataFrame(iris))
  # gaussian family
  model <- spark.glm(training, Sepal_Width ~ Sepal_Length + Species)
  prediction <- predict(model, training)
  expect_equal(typeof(take(select(prediction, "prediction"), 1)$prediction), "double")
  vals <- collect(select(prediction, "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Sepal.Length + Species, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)

  # poisson family
  model <- spark.glm(training, Sepal_Width ~ Sepal_Length + Species,
  family = poisson(link = identity))
  prediction <- predict(model, training)
  expect_equal(typeof(take(select(prediction, "prediction"), 1)$prediction), "double")
  vals <- collect(select(prediction, "prediction"))
  rVals <- suppressWarnings(predict(glm(Sepal.Width ~ Sepal.Length + Species,
  data = iris, family = poisson(link = identity)), iris))
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)

  # Test stats::predict is working
  x <- rnorm(15)
  y <- x + rnorm(15)
  expect_equal(length(predict(lm(y ~ x))), 15)
})

test_that("spark.glm summary", {
  # gaussian family
  training <- suppressWarnings(createDataFrame(iris))
  stats <- summary(spark.glm(training, Sepal_Width ~ Sepal_Length + Species))

  rStats <- summary(glm(Sepal.Width ~ Sepal.Length + Species, data = iris))

  coefs <- unlist(stats$coefficients)
  rCoefs <- unlist(rStats$coefficients)
  expect_true(all(abs(rCoefs - coefs) < 1e-4))
  expect_true(all(
    rownames(stats$coefficients) ==
    c("(Intercept)", "Sepal_Length", "Species_versicolor", "Species_virginica")))
  expect_equal(stats$dispersion, rStats$dispersion)
  expect_equal(stats$null.deviance, rStats$null.deviance)
  expect_equal(stats$deviance, rStats$deviance)
  expect_equal(stats$df.null, rStats$df.null)
  expect_equal(stats$df.residual, rStats$df.residual)
  expect_equal(stats$aic, rStats$aic)

  out <- capture.output(print(stats))
  expect_match(out[2], "Deviance Residuals:")
  expect_true(any(grepl("AIC: 59.22", out)))

  # binomial family
  df <- suppressWarnings(createDataFrame(iris))
  training <- df[df$Species %in% c("versicolor", "virginica"), ]
  stats <- summary(spark.glm(training, Species ~ Sepal_Length + Sepal_Width,
    family = binomial(link = "logit")))

  rTraining <- iris[iris$Species %in% c("versicolor", "virginica"), ]
  rStats <- summary(glm(Species ~ Sepal.Length + Sepal.Width, data = rTraining,
  family = binomial(link = "logit")))

  coefs <- unlist(stats$coefficients)
  rCoefs <- unlist(rStats$coefficients)
  expect_true(all(abs(rCoefs - coefs) < 1e-4))
  expect_true(all(
    rownames(stats$coefficients) ==
    c("(Intercept)", "Sepal_Length", "Sepal_Width")))
  expect_equal(stats$dispersion, rStats$dispersion)
  expect_equal(stats$null.deviance, rStats$null.deviance)
  expect_equal(stats$deviance, rStats$deviance)
  expect_equal(stats$df.null, rStats$df.null)
  expect_equal(stats$df.residual, rStats$df.residual)
  expect_equal(stats$aic, rStats$aic)

  # Test spark.glm works with weighted dataset
  a1 <- c(0, 1, 2, 3)
  a2 <- c(5, 2, 1, 3)
  w <- c(1, 2, 3, 4)
  b <- c(1, 0, 1, 0)
  data <- as.data.frame(cbind(a1, a2, w, b))
  df <- suppressWarnings(createDataFrame(data))

  stats <- summary(spark.glm(df, b ~ a1 + a2, family = "binomial", weightCol = "w"))
  rStats <- summary(glm(b ~ a1 + a2, family = "binomial", data = data, weights = w))

  coefs <- unlist(stats$coefficients)
  rCoefs <- unlist(rStats$coefficients)
  expect_true(all(abs(rCoefs - coefs) < 1e-3))
  expect_true(all(rownames(stats$coefficients) == c("(Intercept)", "a1", "a2")))
  expect_equal(stats$dispersion, rStats$dispersion)
  expect_equal(stats$null.deviance, rStats$null.deviance)
  expect_equal(stats$deviance, rStats$deviance)
  expect_equal(stats$df.null, rStats$df.null)
  expect_equal(stats$df.residual, rStats$df.residual)
  expect_equal(stats$aic, rStats$aic)

  # Test summary works on base GLM models
  baseModel <- stats::glm(Sepal.Width ~ Sepal.Length + Species, data = iris)
  baseSummary <- summary(baseModel)
  expect_true(abs(baseSummary$deviance - 12.19313) < 1e-4)

  # Test spark.glm works with regularization parameter
  data <- as.data.frame(cbind(a1, a2, b))
  df <- suppressWarnings(createDataFrame(data))
  regStats <- summary(spark.glm(df, b ~ a1 + a2, regParam = 1.0))
  expect_equal(regStats$aic, 13.32836, tolerance = 1e-4) # 13.32836 is from summary() result
})

test_that("spark.glm save/load", {
  training <- suppressWarnings(createDataFrame(iris))
  m <- spark.glm(training, Sepal_Width ~ Sepal_Length + Species)
  s <- summary(m)

  modelPath <- tempfile(pattern = "spark-glm", fileext = ".tmp")
  write.ml(m, modelPath)
  expect_error(write.ml(m, modelPath))
  write.ml(m, modelPath, overwrite = TRUE)
  m2 <- read.ml(modelPath)
  s2 <- summary(m2)

  expect_equal(s$coefficients, s2$coefficients)
  expect_equal(rownames(s$coefficients), rownames(s2$coefficients))
  expect_equal(s$dispersion, s2$dispersion)
  expect_equal(s$null.deviance, s2$null.deviance)
  expect_equal(s$deviance, s2$deviance)
  expect_equal(s$df.null, s2$df.null)
  expect_equal(s$df.residual, s2$df.residual)
  expect_equal(s$aic, s2$aic)
  expect_equal(s$iter, s2$iter)
  expect_true(!s$is.loaded)
  expect_true(s2$is.loaded)

  unlink(modelPath)
})



test_that("formula of glm", {
  training <- suppressWarnings(createDataFrame(iris))
  # dot minus and intercept vs native glm
  model <- glm(Sepal_Width ~ . - Species + 0, data = training)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ . - Species + 0, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)

  # feature interaction vs native glm
  model <- glm(Sepal_Width ~ Species:Sepal_Length, data = training)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Species:Sepal.Length, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)

  # glm should work with long formula
  training <- suppressWarnings(createDataFrame(iris))
  training$LongLongLongLongLongName <- training$Sepal_Width
  training$VeryLongLongLongLonLongName <- training$Sepal_Length
  training$AnotherLongLongLongLongName <- training$Species
  model <- glm(LongLongLongLongLongName ~ VeryLongLongLongLonLongName + AnotherLongLongLongLongName,
               data = training)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Sepal.Length + Species, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)
})

test_that("glm and predict", {
  training <- suppressWarnings(createDataFrame(iris))
  # gaussian family
  model <- glm(Sepal_Width ~ Sepal_Length + Species, data = training)
  prediction <- predict(model, training)
  expect_equal(typeof(take(select(prediction, "prediction"), 1)$prediction), "double")
  vals <- collect(select(prediction, "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Sepal.Length + Species, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)

  # poisson family
  model <- glm(Sepal_Width ~ Sepal_Length + Species, data = training,
               family = poisson(link = identity))
  prediction <- predict(model, training)
  expect_equal(typeof(take(select(prediction, "prediction"), 1)$prediction), "double")
  vals <- collect(select(prediction, "prediction"))
  rVals <- suppressWarnings(predict(glm(Sepal.Width ~ Sepal.Length + Species,
           data = iris, family = poisson(link = identity)), iris))
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)

  # Test stats::predict is working
  x <- rnorm(15)
  y <- x + rnorm(15)
  expect_equal(length(predict(lm(y ~ x))), 15)
})

test_that("glm summary", {
  # gaussian family
  training <- suppressWarnings(createDataFrame(iris))
  stats <- summary(glm(Sepal_Width ~ Sepal_Length + Species, data = training))

  rStats <- summary(glm(Sepal.Width ~ Sepal.Length + Species, data = iris))

  coefs <- unlist(stats$coefficients)
  rCoefs <- unlist(rStats$coefficients)
  expect_true(all(abs(rCoefs - coefs) < 1e-4))
  expect_true(all(
    rownames(stats$coefficients) ==
    c("(Intercept)", "Sepal_Length", "Species_versicolor", "Species_virginica")))
  expect_equal(stats$dispersion, rStats$dispersion)
  expect_equal(stats$null.deviance, rStats$null.deviance)
  expect_equal(stats$deviance, rStats$deviance)
  expect_equal(stats$df.null, rStats$df.null)
  expect_equal(stats$df.residual, rStats$df.residual)
  expect_equal(stats$aic, rStats$aic)

  # binomial family
  df <- suppressWarnings(createDataFrame(iris))
  training <- df[df$Species %in% c("versicolor", "virginica"), ]
  stats <- summary(glm(Species ~ Sepal_Length + Sepal_Width, data = training,
    family = binomial(link = "logit")))

  rTraining <- iris[iris$Species %in% c("versicolor", "virginica"), ]
  rStats <- summary(glm(Species ~ Sepal.Length + Sepal.Width, data = rTraining,
    family = binomial(link = "logit")))

  coefs <- unlist(stats$coefficients)
  rCoefs <- unlist(rStats$coefficients)
  expect_true(all(abs(rCoefs - coefs) < 1e-4))
  expect_true(all(
    rownames(stats$coefficients) ==
    c("(Intercept)", "Sepal_Length", "Sepal_Width")))
  expect_equal(stats$dispersion, rStats$dispersion)
  expect_equal(stats$null.deviance, rStats$null.deviance)
  expect_equal(stats$deviance, rStats$deviance)
  expect_equal(stats$df.null, rStats$df.null)
  expect_equal(stats$df.residual, rStats$df.residual)
  expect_equal(stats$aic, rStats$aic)

  # Test summary works on base GLM models
  baseModel <- stats::glm(Sepal.Width ~ Sepal.Length + Species, data = iris)
  baseSummary <- summary(baseModel)
  expect_true(abs(baseSummary$deviance - 12.19313) < 1e-4)
})

test_that("glm save/load", {
  training <- suppressWarnings(createDataFrame(iris))
  m <- glm(Sepal_Width ~ Sepal_Length + Species, data = training)
  s <- summary(m)

  modelPath <- tempfile(pattern = "glm", fileext = ".tmp")
  write.ml(m, modelPath)
  expect_error(write.ml(m, modelPath))
  write.ml(m, modelPath, overwrite = TRUE)
  m2 <- read.ml(modelPath)
  s2 <- summary(m2)

  expect_equal(s$coefficients, s2$coefficients)
  expect_equal(rownames(s$coefficients), rownames(s2$coefficients))
  expect_equal(s$dispersion, s2$dispersion)
  expect_equal(s$null.deviance, s2$null.deviance)
  expect_equal(s$deviance, s2$deviance)
  expect_equal(s$df.null, s2$df.null)
  expect_equal(s$df.residual, s2$df.residual)
  expect_equal(s$aic, s2$aic)
  expect_equal(s$iter, s2$iter)
  expect_true(!s$is.loaded)
  expect_true(s2$is.loaded)

  unlink(modelPath)
})

test_that("spark.kmeans", {
  newIris <- iris
  newIris$Species <- NULL
  training <- suppressWarnings(createDataFrame(newIris))

  take(training, 1)

  model <- spark.kmeans(data = training, ~ ., k = 2, maxIter = 10, initMode = "random")
  sample <- take(select(predict(model, training), "prediction"), 1)
  expect_equal(typeof(sample$prediction), "integer")
  expect_equal(sample$prediction, 1)

  # Test stats::kmeans is working
  statsModel <- kmeans(x = newIris, centers = 2)
  expect_equal(sort(unique(statsModel$cluster)), c(1, 2))

  # Test fitted works on KMeans
  fitted.model <- fitted(model)
  expect_equal(sort(collect(distinct(select(fitted.model, "prediction")))$prediction), c(0, 1))

  # Test summary works on KMeans
  summary.model <- summary(model)
  cluster <- summary.model$cluster
  expect_equal(sort(collect(distinct(select(cluster, "prediction")))$prediction), c(0, 1))

  # Test model save/load
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
})

test_that("spark.mlp", {
  df <- read.df(absoluteSparkPath("data/mllib/sample_multiclass_classification_data.txt"),
                source = "libsvm")
  model <- spark.mlp(df, blockSize = 128, layers = c(4, 5, 4, 3), solver = "l-bfgs", maxIter = 100,
                     tol = 0.5, stepSize = 1, seed = 1)

  # Test summary method
  summary <- summary(model)
  expect_equal(summary$labelCount, 3)
  expect_equal(summary$layers, c(4, 5, 4, 3))
  expect_equal(length(summary$weights), 64)
  expect_equal(head(summary$weights, 5), list(-0.878743, 0.2154151, -1.16304, -0.6583214, 1.009825),
               tolerance = 1e-6)

  # Test predict method
  mlpTestDF <- df
  mlpPredictions <- collect(select(predict(model, mlpTestDF), "prediction"))
  expect_equal(head(mlpPredictions$prediction, 6), c(0, 1, 1, 1, 1, 1))

  # Test model save/load
  modelPath <- tempfile(pattern = "spark-mlp", fileext = ".tmp")
  write.ml(model, modelPath)
  expect_error(write.ml(model, modelPath))
  write.ml(model, modelPath, overwrite = TRUE)
  model2 <- read.ml(modelPath)
  summary2 <- summary(model2)

  expect_equal(summary2$labelCount, 3)
  expect_equal(summary2$layers, c(4, 5, 4, 3))
  expect_equal(length(summary2$weights), 64)

  unlink(modelPath)

  # Test default parameter
  model <- spark.mlp(df, layers = c(4, 5, 4, 3))
  mlpPredictions <- collect(select(predict(model, mlpTestDF), "prediction"))
  expect_equal(head(mlpPredictions$prediction, 10), c(1, 1, 1, 1, 0, 1, 2, 2, 1, 0))

  # Test illegal parameter
  expect_error(spark.mlp(df, layers = NULL), "layers must be a integer vector with length > 1.")
  expect_error(spark.mlp(df, layers = c()), "layers must be a integer vector with length > 1.")
  expect_error(spark.mlp(df, layers = c(3)), "layers must be a integer vector with length > 1.")

  # Test random seed
  # default seed
  model <- spark.mlp(df, layers = c(4, 5, 4, 3), maxIter = 10)
  mlpPredictions <- collect(select(predict(model, mlpTestDF), "prediction"))
  expect_equal(head(mlpPredictions$prediction, 12), c(1, 1, 1, 1, 0, 1, 2, 2, 1, 2, 0, 1))
  # seed equals 10
  model <- spark.mlp(df, layers = c(4, 5, 4, 3), maxIter = 10, seed = 10)
  mlpPredictions <- collect(select(predict(model, mlpTestDF), "prediction"))
  expect_equal(head(mlpPredictions$prediction, 12), c(1, 1, 1, 1, 2, 1, 2, 2, 1, 0, 0, 1))
})

test_that("spark.naiveBayes", {
  # R code to reproduce the result.
  # We do not support instance weights yet. So we ignore the frequencies.
  #
  #' library(e1071)
  #' t <- as.data.frame(Titanic)
  #' t1 <- t[t$Freq > 0, -5]
  #' m <- naiveBayes(Survived ~ ., data = t1)
  #' m
  #' predict(m, t1)
  #
  # -- output of 'm'
  #
  # A-priori probabilities:
  # Y
  #        No       Yes
  # 0.4166667 0.5833333
  #
  # Conditional probabilities:
  #      Class
  # Y           1st       2nd       3rd      Crew
  #   No  0.2000000 0.2000000 0.4000000 0.2000000
  #   Yes 0.2857143 0.2857143 0.2857143 0.1428571
  #
  #      Sex
  # Y     Male Female
  #   No   0.5    0.5
  #   Yes  0.5    0.5
  #
  #      Age
  # Y         Child     Adult
  #   No  0.2000000 0.8000000
  #   Yes 0.4285714 0.5714286
  #
  # -- output of 'predict(m, t1)'
  #
  # Yes Yes Yes Yes No  No  Yes Yes No  No  Yes Yes Yes Yes Yes Yes Yes Yes No  No  Yes Yes No  No
  #

  t <- as.data.frame(Titanic)
  t1 <- t[t$Freq > 0, -5]
  df <- suppressWarnings(createDataFrame(t1))
  m <- spark.naiveBayes(df, Survived ~ ., smoothing = 0.0)
  s <- summary(m)
  expect_equal(as.double(s$apriori[1, "Yes"]), 0.5833333, tolerance = 1e-6)
  expect_equal(sum(s$apriori), 1)
  expect_equal(as.double(s$tables["Yes", "Age_Adult"]), 0.5714286, tolerance = 1e-6)
  p <- collect(select(predict(m, df), "prediction"))
  expect_equal(p$prediction, c("Yes", "Yes", "Yes", "Yes", "No", "No", "Yes", "Yes", "No", "No",
                               "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "No", "No",
                               "Yes", "Yes", "No", "No"))

  # Test model save/load
  modelPath <- tempfile(pattern = "spark-naiveBayes", fileext = ".tmp")
  write.ml(m, modelPath)
  expect_error(write.ml(m, modelPath))
  write.ml(m, modelPath, overwrite = TRUE)
  m2 <- read.ml(modelPath)
  s2 <- summary(m2)
  expect_equal(s$apriori, s2$apriori)
  expect_equal(s$tables, s2$tables)

  unlink(modelPath)

  # Test e1071::naiveBayes
  if (requireNamespace("e1071", quietly = TRUE)) {
    expect_error(m <- e1071::naiveBayes(Survived ~ ., data = t1), NA)
    expect_equal(as.character(predict(m, t1[1, ])), "Yes")
  }
})

test_that("spark.survreg", {
  # R code to reproduce the result.
  #
  #' rData <- list(time = c(4, 3, 1, 1, 2, 2, 3), status = c(1, 1, 1, 0, 1, 1, 0),
  #'               x = c(0, 2, 1, 1, 1, 0, 0), sex = c(0, 0, 0, 0, 1, 1, 1))
  #' library(survival)
  #' model <- survreg(Surv(time, status) ~ x + sex, rData)
  #' summary(model)
  #' predict(model, data)
  #
  # -- output of 'summary(model)'
  #
  #              Value Std. Error     z        p
  # (Intercept)  1.315      0.270  4.88 1.07e-06
  # x           -0.190      0.173 -1.10 2.72e-01
  # sex         -0.253      0.329 -0.77 4.42e-01
  # Log(scale)  -1.160      0.396 -2.93 3.41e-03
  #
  # -- output of 'predict(model, data)'
  #
  #        1        2        3        4        5        6        7
  # 3.724591 2.545368 3.079035 3.079035 2.390146 2.891269 2.891269
  #
  data <- list(list(4, 1, 0, 0), list(3, 1, 2, 0), list(1, 1, 1, 0),
          list(1, 0, 1, 0), list(2, 1, 1, 1), list(2, 1, 0, 1), list(3, 0, 0, 1))
  df <- createDataFrame(data, c("time", "status", "x", "sex"))
  model <- spark.survreg(df, Surv(time, status) ~ x + sex)
  stats <- summary(model)
  coefs <- as.vector(stats$coefficients[, 1])
  rCoefs <- c(1.3149571, -0.1903409, -0.2532618, -1.1599800)
  expect_equal(coefs, rCoefs, tolerance = 1e-4)
  expect_true(all(
    rownames(stats$coefficients) ==
    c("(Intercept)", "x", "sex", "Log(scale)")))
  p <- collect(select(predict(model, df), "prediction"))
  expect_equal(p$prediction, c(3.724591, 2.545368, 3.079035, 3.079035,
               2.390146, 2.891269, 2.891269), tolerance = 1e-4)

  # Test model save/load
  modelPath <- tempfile(pattern = "spark-survreg", fileext = ".tmp")
  write.ml(model, modelPath)
  expect_error(write.ml(model, modelPath))
  write.ml(model, modelPath, overwrite = TRUE)
  model2 <- read.ml(modelPath)
  stats2 <- summary(model2)
  coefs2 <- as.vector(stats2$coefficients[, 1])
  expect_equal(coefs, coefs2)
  expect_equal(rownames(stats$coefficients), rownames(stats2$coefficients))

  unlink(modelPath)

  # Test survival::survreg
  if (requireNamespace("survival", quietly = TRUE)) {
    rData <- list(time = c(4, 3, 1, 1, 2, 2, 3), status = c(1, 1, 1, 0, 1, 1, 0),
                 x = c(0, 2, 1, 1, 1, 0, 0), sex = c(0, 0, 0, 0, 1, 1, 1))
    expect_error(
      model <- survival::survreg(formula = survival::Surv(time, status) ~ x + sex, data = rData),
      NA)
    expect_equal(predict(model, rData)[[1]], 3.724591, tolerance = 1e-4)
  }
})

test_that("spark.isotonicRegression", {
  label <- c(7.0, 5.0, 3.0, 5.0, 1.0)
  feature <- c(0.0, 1.0, 2.0, 3.0, 4.0)
  weight <- c(1.0, 1.0, 1.0, 1.0, 1.0)
  data <- as.data.frame(cbind(label, feature, weight))
  df <- suppressWarnings(createDataFrame(data))

  model <- spark.isoreg(df, label ~ feature, isotonic = FALSE,
                        weightCol = "weight")
  # only allow one variable on the right hand side of the formula
  expect_error(model2 <- spark.isoreg(df, ~., isotonic = FALSE))
  result <- summary(model)
  expect_equal(result$predictions, list(7, 5, 4, 4, 1))

  # Test model prediction
  predict_data <- list(list(-2.0), list(-1.0), list(0.5),
                       list(0.75), list(1.0), list(2.0), list(9.0))
  predict_df <- createDataFrame(predict_data, c("feature"))
  predict_result <- collect(select(predict(model, predict_df), "prediction"))
  expect_equal(predict_result$prediction, c(7.0, 7.0, 6.0, 5.5, 5.0, 4.0, 1.0))

  # Test model save/load
  modelPath <- tempfile(pattern = "spark-isotonicRegression", fileext = ".tmp")
  write.ml(model, modelPath)
  expect_error(write.ml(model, modelPath))
  write.ml(model, modelPath, overwrite = TRUE)
  model2 <- read.ml(modelPath)
  expect_equal(result, summary(model2))

  unlink(modelPath)
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
  expect_equal(stats$lambda, rLambda, tolerance = 1e-3)
  expect_equal(unlist(stats$mu), rMu, tolerance = 1e-3)
  expect_equal(unlist(stats$sigma), rSigma, tolerance = 1e-3)
  p <- collect(select(predict(model, df), "prediction"))
  expect_equal(p$prediction, c(0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1))

  # Test model save/load
  modelPath <- tempfile(pattern = "spark-gaussianMixture", fileext = ".tmp")
  write.ml(model, modelPath)
  expect_error(write.ml(model, modelPath))
  write.ml(model, modelPath, overwrite = TRUE)
  model2 <- read.ml(modelPath)
  stats2 <- summary(model2)
  expect_equal(stats$lambda, stats2$lambda)
  expect_equal(unlist(stats$mu), unlist(stats2$mu))
  expect_equal(unlist(stats$sigma), unlist(stats2$sigma))

  unlink(modelPath)
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

  expect_false(isDistributed)
  expect_true(logLikelihood <= 0 & is.finite(logLikelihood))
  expect_true(logPerplexity >= 0 & is.finite(logPerplexity))
  expect_equal(vocabSize, 11)
  expect_true(is.null(vocabulary))

  # Test model save/load
  modelPath <- tempfile(pattern = "spark-lda", fileext = ".tmp")
  write.ml(model, modelPath)
  expect_error(write.ml(model, modelPath))
  write.ml(model, modelPath, overwrite = TRUE)
  model2 <- read.ml(modelPath)
  stats2 <- summary(model2)

  expect_false(stats2$isDistributed)
  expect_equal(logLikelihood, stats2$logLikelihood)
  expect_equal(logPerplexity, stats2$logPerplexity)
  expect_equal(vocabSize, stats2$vocabSize)
  expect_equal(vocabulary, stats2$vocabulary)

  unlink(modelPath)
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

  expect_false(isDistributed)
  expect_true(logLikelihood <= 0 & is.finite(logLikelihood))
  expect_true(logPerplexity >= 0 & is.finite(logPerplexity))
  expect_equal(vocabSize, 10)
  expect_true(setequal(stats$vocabulary, c("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")))

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

test_that("spark.als", {
  data <- list(list(0, 0, 4.0), list(0, 1, 2.0), list(1, 1, 3.0), list(1, 2, 4.0),
  list(2, 1, 1.0), list(2, 2, 5.0))
  df <- createDataFrame(data, c("user", "item", "score"))
  model <- spark.als(df, ratingCol = "score", userCol = "user", itemCol = "item",
  rank = 10, maxIter = 5, seed = 0, reg = 0.1)
  stats <- summary(model)
  expect_equal(stats$rank, 10)
  test <- createDataFrame(list(list(0, 2), list(1, 0), list(2, 0)), c("user", "item"))
  predictions <- collect(predict(model, test))

  expect_equal(predictions$prediction, c(-0.1380762, 2.6258414, -1.5018409),
  tolerance = 1e-4)

  # Test model save/load
  modelPath <- tempfile(pattern = "spark-als", fileext = ".tmp")
  write.ml(model, modelPath)
  expect_error(write.ml(model, modelPath))
  write.ml(model, modelPath, overwrite = TRUE)
  model2 <- read.ml(modelPath)
  stats2 <- summary(model2)
  expect_equal(stats2$rating, "score")
  userFactors <- collect(stats$userFactors)
  itemFactors <- collect(stats$itemFactors)
  userFactors2 <- collect(stats2$userFactors)
  itemFactors2 <- collect(stats2$itemFactors)

  orderUser <- order(userFactors$id)
  orderUser2 <- order(userFactors2$id)
  expect_equal(userFactors$id[orderUser], userFactors2$id[orderUser2])
  expect_equal(userFactors$features[orderUser], userFactors2$features[orderUser2])

  orderItem <- order(itemFactors$id)
  orderItem2 <- order(itemFactors2$id)
  expect_equal(itemFactors$id[orderItem], itemFactors2$id[orderItem2])
  expect_equal(itemFactors$features[orderItem], itemFactors2$features[orderItem2])

  unlink(modelPath)
})

test_that("spark.kstest", {
  data <- data.frame(test = c(0.1, 0.15, 0.2, 0.3, 0.25, -1, -0.5))
  df <- createDataFrame(data)
  testResult <- spark.kstest(df, "test", "norm")
  stats <- summary(testResult)

  rStats <- ks.test(data$test, "pnorm", alternative = "two.sided")

  expect_equal(stats$p.value, rStats$p.value, tolerance = 1e-4)
  expect_equal(stats$statistic, unname(rStats$statistic), tolerance = 1e-4)
  expect_match(capture.output(stats)[1], "Kolmogorov-Smirnov test summary:")

  testResult <- spark.kstest(df, "test", "norm", -0.5)
  stats <- summary(testResult)

  rStats <- ks.test(data$test, "pnorm", -0.5, 1, alternative = "two.sided")

  expect_equal(stats$p.value, rStats$p.value, tolerance = 1e-4)
  expect_equal(stats$statistic, unname(rStats$statistic), tolerance = 1e-4)
  expect_match(capture.output(stats)[1], "Kolmogorov-Smirnov test summary:")
})

sparkR.session.stop()
