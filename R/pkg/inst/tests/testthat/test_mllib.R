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
    expect_that(m <- e1071::naiveBayes(Survived ~ ., data = t1), not(throws_error()))
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
  result <- summary(model, df)
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
  expect_equal(result, summary(model2, df))

  unlink(modelPath)
})

test_that("spark.gaussianMixture", {
  # R code to reproduce the result.
  # nolint start
  #' library(mvtnorm)
  #' set.seed(100)
  #' a <- rmvnorm(4, c(0, 0))
  #' b <- rmvnorm(6, c(3, 4))
  #' data <- rbind(a, b)
  #' model <- mvnormalmixEM(data, k = 2)
  #' model$lambda
  #
  #  [1] 0.4 0.6
  #
  #' model$mu
  #
  #  [1] -0.2614822  0.5128697
  #  [1] 2.647284 4.544682
  #
  #' model$sigma
  #
  #  [[1]]
  #  [,1]       [,2]
  #  [1,] 0.08427399 0.00548772
  #  [2,] 0.00548772 0.09090715
  #
  #  [[2]]
  #  [,1]       [,2]
  #  [1,]  0.1641373 -0.1673806
  #  [2,] -0.1673806  0.7508951
  # nolint end
  data <- list(list(-0.50219235, 0.1315312), list(-0.07891709, 0.8867848),
               list(0.11697127, 0.3186301), list(-0.58179068, 0.7145327),
               list(2.17474057, 3.6401379), list(3.08988614, 4.0962745),
               list(2.79836605, 4.7398405), list(3.12337950, 3.9706833),
               list(2.61114575, 4.5108563), list(2.08618581, 6.3102968))
  df <- createDataFrame(data, c("x1", "x2"))
  model <- spark.gaussianMixture(df, ~ x1 + x2, k = 2)
  stats <- summary(model)
  rLambda <- c(0.50861, 0.49139)
  rMu <- c(0.267, 1.195, 2.743, 4.730)
  rSigma <- c(1.099, 1.339, 1.339, 1.798,
              0.145, -0.309, -0.309, 0.716)
  expect_equal(stats$lambda, rLambda, tolerance = 1e-3)
  expect_equal(unlist(stats$mu), rMu, tolerance = 1e-3)
  expect_equal(unlist(stats$sigma), rSigma, tolerance = 1e-3)
  p <- collect(select(predict(model, df), "prediction"))
  expect_equal(p$prediction, c(0, 0, 0, 0, 0, 1, 1, 1, 1, 1))

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
  text <- read.df("data/mllib/sample_lda_libsvm_data.txt", source = "libsvm")
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
  text <- read.text("data/mllib/sample_lda_data.txt")
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
  text <- read.text("data/mllib/sample_lda_data.txt")
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

sparkR.session.stop()
