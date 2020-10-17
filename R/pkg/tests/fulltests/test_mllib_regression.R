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

context("MLlib regression algorithms, except for tree-based algorithms")

# Tests for MLlib regression algorithms in SparkR
sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

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

  # Gamma family
  x <- runif(100, -1, 1)
  y <- rgamma(100, rate = 10 / exp(0.5 + 1.2 * x), shape = 10)
  df <- as.DataFrame(as.data.frame(list(x = x, y = y)))
  model <- glm(y ~ x, family = Gamma, df)
  out <- capture.output(print(summary(model)))
  expect_true(any(grepl("Dispersion parameter for gamma family", out)))

  # tweedie family
  model <- spark.glm(training, Sepal_Width ~ Sepal_Length + Species,
                     family = "tweedie", var.power = 1.2, link.power = 0.0)
  prediction <- predict(model, training)
  expect_equal(typeof(take(select(prediction, "prediction"), 1)$prediction), "double")
  vals <- collect(select(prediction, "prediction"))

  # manual calculation of the R predicted values to avoid dependence on statmod
  #' library(statmod)
  #' rModel <- glm(Sepal.Width ~ Sepal.Length + Species, data = iris,
  #'             family = tweedie(var.power = 1.2, link.power = 0.0))
  #' print(coef(rModel))

  rCoef <- c(0.6455409, 0.1169143, -0.3224752, -0.3282174)
  rVals <- exp(as.numeric(model.matrix(Sepal.Width ~ Sepal.Length + Species,
                                       data = iris) %*% rCoef))
  expect_true(all(abs(rVals - vals) < 1e-5), rVals - vals)

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

  # test summary coefficients return matrix type
  expect_true(any(class(stats$coefficients) == "matrix"))
  expect_true(class(stats$coefficients[, 1]) == "numeric")

  coefs <- stats$coefficients
  rCoefs <- rStats$coefficients
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

  coefs <- stats$coefficients
  rCoefs <- rStats$coefficients
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
  df <- createDataFrame(data)

  stats <- summary(spark.glm(df, b ~ a1 + a2, family = "binomial", weightCol = "w"))
  rStats <- summary(glm(b ~ a1 + a2, family = "binomial", data = data, weights = w))

  coefs <- stats$coefficients
  rCoefs <- rStats$coefficients
  expect_true(all(abs(rCoefs - coefs) < 1e-3))
  expect_true(all(rownames(stats$coefficients) == c("(Intercept)", "a1", "a2")))
  expect_equal(stats$dispersion, rStats$dispersion)
  expect_equal(stats$null.deviance, rStats$null.deviance)
  expect_equal(stats$deviance, rStats$deviance)
  expect_equal(stats$df.null, rStats$df.null)
  expect_equal(stats$df.residual, rStats$df.residual)
  expect_equal(stats$aic, rStats$aic)

  # Test spark.glm works with offset
  training <- suppressWarnings(createDataFrame(iris))
  stats <- summary(spark.glm(training, Sepal_Width ~ Sepal_Length + Species,
                             family = poisson(), offsetCol = "Petal_Length"))
  rStats <- suppressWarnings(summary(glm(Sepal.Width ~ Sepal.Length + Species,
                        data = iris, family = poisson(), offset = iris$Petal.Length)))
  expect_true(all(abs(rStats$coefficients - stats$coefficients) < 1e-3))

  # Test summary works on base GLM models
  baseModel <- stats::glm(Sepal.Width ~ Sepal.Length + Species, data = iris)
  baseSummary <- summary(baseModel)
  expect_true(abs(baseSummary$deviance - 12.19313) < 1e-4)

  # Test spark.glm works with regularization parameter
  data <- as.data.frame(cbind(a1, a2, b))
  df <- suppressWarnings(createDataFrame(data))
  regStats <- summary(spark.glm(df, b ~ a1 + a2, regParam = 1.0))
  expect_equal(regStats$aic, 13.32836, tolerance = 1e-4) # 13.32836 is from summary() result

  # Test spark.glm works on collinear data
  A <- matrix(c(1, 2, 3, 4, 2, 4, 6, 8), 4, 2)
  b <- c(1, 2, 3, 4)
  data <- as.data.frame(cbind(A, b))
  df <- createDataFrame(data)
  stats <- summary(spark.glm(df, b ~ . - 1))
  coefs <- stats$coefficients
  expect_true(all(abs(c(0.5, 0.25) - coefs) < 1e-4))
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

  # tweedie family
  model <- glm(Sepal_Width ~ Sepal_Length + Species, data = training,
               family = "tweedie", var.power = 1.2, link.power = 0.0)
  prediction <- predict(model, training)
  expect_equal(typeof(take(select(prediction, "prediction"), 1)$prediction), "double")
  vals <- collect(select(prediction, "prediction"))

  # manual calculation of the R predicted values to avoid dependence on statmod
  #' library(statmod)
  #' rModel <- glm(Sepal.Width ~ Sepal.Length + Species, data = iris,
  #'             family = tweedie(var.power = 1.2, link.power = 0.0))
  #' print(coef(rModel))

  rCoef <- c(0.6455409, 0.1169143, -0.3224752, -0.3282174)
  rVals <- exp(as.numeric(model.matrix(Sepal.Width ~ Sepal.Length + Species,
                                   data = iris) %*% rCoef))
  expect_true(all(abs(rVals - vals) < 1e-5), rVals - vals)

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

  coefs <- stats$coefficients
  rCoefs <- rStats$coefficients
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

  coefs <- stats$coefficients
  rCoefs <- rStats$coefficients
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

test_that("spark.glm and glm with string encoding", {
  t <- as.data.frame(Titanic, stringsAsFactors = FALSE)
  df <- createDataFrame(t)

  # base R
  rm <- stats::glm(Freq ~ Sex + Age, family = "gaussian", data = t)
  # spark.glm with default stringIndexerOrderType = "frequencyDesc"
  sm0 <- spark.glm(df, Freq ~ Sex + Age, family = "gaussian")
  # spark.glm with stringIndexerOrderType = "alphabetDesc"
  sm1 <- spark.glm(df, Freq ~ Sex + Age, family = "gaussian",
                   stringIndexerOrderType = "alphabetDesc")
  # glm with stringIndexerOrderType = "alphabetDesc"
  sm2 <- glm(Freq ~ Sex + Age, family = "gaussian", data = df,
                stringIndexerOrderType = "alphabetDesc")

  rStats <- summary(rm)
  rCoefs <- rStats$coefficients
  sStats <- lapply(list(sm0, sm1, sm2), summary)
  # order by coefficient size since column rendering may be different
  o <- order(rCoefs[, 1])

  # default encoding does not produce same results as R
  expect_false(all(abs(rCoefs[o, ] - sStats[[1]]$coefficients[o, ]) < 1e-4))

  # all estimates should be the same as R with stringIndexerOrderType = "alphabetDesc"
  test <- lapply(sStats[2:3], function(stats) {
    expect_true(all(abs(rCoefs[o, ] - stats$coefficients[o, ]) < 1e-4))
    expect_equal(stats$dispersion, rStats$dispersion)
    expect_equal(stats$null.deviance, rStats$null.deviance)
    expect_equal(stats$deviance, rStats$deviance)
    expect_equal(stats$df.null, rStats$df.null)
    expect_equal(stats$df.residual, rStats$df.residual)
    expect_equal(stats$aic, rStats$aic)
  })

  # fitted values should be equal regardless of string encoding
  rVals <- predict(rm, t)
  test <- lapply(list(sm0, sm1, sm2), function(sm) {
    vals <- collect(select(predict(sm, df), "prediction"))
    expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)
  })
})

test_that("spark.isoreg", {
  label <- c(7.0, 5.0, 3.0, 5.0, 1.0)
  feature <- c(0.0, 1.0, 2.0, 3.0, 4.0)
  weight <- c(1.0, 1.0, 1.0, 1.0, 1.0)
  data <- as.data.frame(cbind(label, feature, weight))
  df <- createDataFrame(data)

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
  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-isoreg", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    expect_equal(result, summary(model2))

    unlink(modelPath)
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
  if (windows_with_hadoop()) {
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
  }

  # Test survival::survreg
  if (requireNamespace("survival", quietly = TRUE)) {
    rData <- list(time = c(4, 3, 1, 1, 2, 2, 3), status = c(1, 1, 1, 0, 1, 1, 0),
                 x = c(0, 2, 1, 1, 1, 0, 0), sex = c(0, 0, 0, 0, 1, 1, 1))
    expect_error(
      model <- survival::survreg(formula = survival::Surv(time, status) ~ x + sex, data = rData),
                                 NA)
    expect_equal(predict(model, rData)[[1]], 3.724591, tolerance = 1e-4)

    # Test stringIndexerOrderType
    rData <- as.data.frame(rData)
    rData$sex2 <- c("female", "male")[rData$sex + 1]
    df <- createDataFrame(rData)
    expect_error(
      rModel <- survival::survreg(survival::Surv(time, status) ~ x + sex2, rData), NA)
    rCoefs <- as.numeric(summary(rModel)$table[, 1])
    model <- spark.survreg(df, Surv(time, status) ~ x + sex2)
    coefs <- as.vector(summary(model)$coefficients[, 1])
    o <- order(rCoefs)
    # stringIndexerOrderType = "frequencyDesc" produces different estimates from R
    expect_false(all(abs(rCoefs[o] - coefs[o]) < 1e-4))

    # stringIndexerOrderType = "alphabetDesc" produces the same estimates as R
    model <- spark.survreg(df, Surv(time, status) ~ x + sex2,
                           stringIndexerOrderType = "alphabetDesc")
    coefs <- as.vector(summary(model)$coefficients[, 1])
    expect_true(all(abs(rCoefs[o] - coefs[o]) < 1e-4))
  }
})

sparkR.session.stop()
