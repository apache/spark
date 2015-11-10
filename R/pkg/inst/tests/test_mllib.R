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
  training <- createDataFrame(sqlContext, iris)
  test <- select(training, "Sepal_Length")
  model <- glm(Sepal_Width ~ Sepal_Length, training, family = "gaussian")
  prediction <- predict(model, test)
  expect_equal(typeof(take(select(prediction, "prediction"), 1)$prediction), "double")
})

test_that("glm should work with long formula", {
  training <- createDataFrame(sqlContext, iris)
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
  training <- createDataFrame(sqlContext, iris)
  model <- glm(Sepal_Width ~ Sepal_Length + Species, data = training)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Sepal.Length + Species, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)
})

test_that("dot minus and intercept vs native glm", {
  training <- createDataFrame(sqlContext, iris)
  model <- glm(Sepal_Width ~ . - Species + 0, data = training)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ . - Species + 0, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)
})

test_that("feature interaction vs native glm", {
  training <- createDataFrame(sqlContext, iris)
  model <- glm(Sepal_Width ~ Species:Sepal_Length, data = training)
  vals <- collect(select(predict(model, training), "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Species:Sepal.Length, data = iris), iris)
  expect_true(all(abs(rVals - vals) < 1e-6), rVals - vals)
})

test_that("summary coefficients match with native glm", {
  training <- createDataFrame(sqlContext, iris)
  stats <- summary(glm(Sepal_Width ~ Sepal_Length + Species, data = training, solver = "normal"))
  coefs <- unlist(stats$Coefficients)
  devianceResiduals <- unlist(stats$DevianceResiduals)

  rCoefs <- as.vector(coef(glm(Sepal.Width ~ Sepal.Length + Species, data = iris)))
  rStdError <- c(0.23536, 0.04630, 0.07207, 0.09331)
  rTValue <- c(7.123, 7.557, -13.644, -10.798)
  rPValue <- c(0.0, 0.0, 0.0, 0.0)
  rDevianceResiduals <- c(-0.95096, 0.72918)

  expect_true(all(abs(rCoefs - coefs[1:4]) < 1e-6))
  expect_true(all(abs(rStdError - coefs[5:8]) < 1e-5))
  expect_true(all(abs(rTValue - coefs[9:12]) < 1e-3))
  expect_true(all(abs(rPValue - coefs[13:16]) < 1e-6))
  expect_true(all(abs(rDevianceResiduals - devianceResiduals) < 1e-5))
  expect_true(all(
    rownames(stats$Coefficients) ==
    c("(Intercept)", "Sepal_Length", "Species_versicolor", "Species_virginica")))
})

test_that("summary coefficients match with native glm of family 'binomial'", {
  df <- createDataFrame(sqlContext, iris)
  training <- filter(df, df$Species != "setosa")
  stats <- summary(glm(Species ~ Sepal_Length + Sepal_Width, data = training,
    family = "binomial"))
  coefs <- as.vector(stats$Coefficients)

  rTraining <- iris[iris$Species %in% c("versicolor","virginica"),]
  rCoefs <- as.vector(coef(glm(Species ~ Sepal.Length + Sepal.Width, data = rTraining,
    family = binomial(link = "logit"))))
  rStdError <- c(3.0974, 0.5169, 0.8628)
  rTValue <- c(-4.212, 3.680, 0.469)
  rPValue <- c(0.000, 0.000, 0.639)

  expect_true(all(abs(rCoefs - coefs[1:3]) < 1e-4))
  expect_true(all(abs(rStdError - coefs[4:6]) < 1e-4))
  expect_true(all(abs(rTValue - coefs[7:9]) < 1e-3))
  expect_true(all(abs(rPValue - coefs[10:12]) < 1e-3))
  expect_true(all(
    rownames(stats$Coefficients) ==
    c("(Intercept)", "Sepal_Length", "Sepal_Width")))
})
