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

test_that("glm and predict", {
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
  test <- select(training, "Sepal_Length")
  model <- glm(Sepal_Width ~ Sepal_Length, training, family = "gaussian")
  prediction <- predict(model, test)
  expect_equal(typeof(take(select(prediction, "prediction"), 1)$prediction), "double")

  # Test stats::predict is working
  x <- rnorm(15)
  y <- x + rnorm(15)
  expect_equal(length(predict(lm(y ~ x))), 15)
})

test_that("glm should work with long formula", {
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

test_that("predictions match with native glm", {
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
  model <- glm(Sepal_Width ~ Sepal_Length + Species, data = training)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Sepal.Length + Species, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)
})

test_that("dot minus and intercept vs native glm", {
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
  model <- glm(Sepal_Width ~ . - Species + 0, data = training)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ . - Species + 0, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)
})

test_that("feature interaction vs native glm", {
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
  model <- glm(Sepal_Width ~ Species:Sepal_Length, data = training)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Species:Sepal.Length, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)
})

test_that("summary coefficients match with native glm", {
  training <- suppressWarnings(createDataFrame(sqlContext, iris))
  stats <- summary(glm(Sepal_Width ~ Sepal_Length + Species, data = training, solver = "normal"))
  coefs <- unlist(stats$coefficients)
  devianceResiduals <- unlist(stats$devianceResiduals)

  rStats <- summary(glm(Sepal.Width ~ Sepal.Length + Species, data = iris))
  rCoefs <- unlist(rStats$coefficients)
  rDevianceResiduals <- c(-0.95096, 0.72918)

  expect_true(all(abs(rCoefs - coefs) < 1e-5))
  expect_true(all(abs(rDevianceResiduals - devianceResiduals) < 1e-5))
  expect_true(all(
    rownames(stats$coefficients) ==
    c("(Intercept)", "Sepal_Length", "Species_versicolor", "Species_virginica")))
})

test_that("summary coefficients match with native glm of family 'binomial'", {
  df <- suppressWarnings(createDataFrame(sqlContext, iris))
  training <- filter(df, df$Species != "setosa")
  stats <- summary(glm(Species ~ Sepal_Length + Sepal_Width, data = training,
    family = "binomial"))
  coefs <- as.vector(stats$coefficients[,1])

  rTraining <- iris[iris$Species %in% c("versicolor","virginica"),]
  rCoefs <- as.vector(coef(glm(Species ~ Sepal.Length + Sepal.Width, data = rTraining,
    family = binomial(link = "logit"))))

  expect_true(all(abs(rCoefs - coefs) < 1e-4))
  expect_true(all(
    rownames(stats$coefficients) ==
    c("(Intercept)", "Sepal_Length", "Sepal_Width")))
})

test_that("summary works on base GLM models", {
  baseModel <- stats::glm(Sepal.Width ~ Sepal.Length + Species, data = iris)
  baseSummary <- summary(baseModel)
  expect_true(abs(baseSummary$deviance - 12.19313) < 1e-4)
})
