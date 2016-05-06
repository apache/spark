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

sc <- sparkR.init()

sqlContext <- sparkRSQL.init(sc)

test_that("formula of spark.glm", {
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  df <- suppressWarnings(createDataFrame(sqlContext, iris))
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

  # Test summary works on base GLM models
  baseModel <- stats::glm(Sepal.Width ~ Sepal.Length + Species, data = iris)
  baseSummary <- summary(baseModel)
  expect_true(abs(baseSummary$deviance - 12.19313) < 1e-4)
})

test_that("spark.glm save/load", {
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  df <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
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
  training <- suppressWarnings(createDataFrame(sqlContext, newIris))

  take(training, 1)

  model <- spark.kmeans(data = training, ~ ., k = 2)
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
  df <- suppressWarnings(createDataFrame(sqlContext, t1))
  m <- spark.naiveBayes(df, Survived ~ .)
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
  df <- createDataFrame(sqlContext, data, c("time", "status", "x", "sex"))
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
