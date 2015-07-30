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

test_that("summary coefficients match with native glm", {
  training <- createDataFrame(sqlContext, iris)
  stats <- summary(glm(Sepal_Width ~ Sepal_Length + Species, data = training))
  coefs <- as.vector(stats$coefficients)
  rCoefs <- as.vector(coef(glm(Sepal.Width ~ Sepal.Length + Species, data = iris)))
  expect_true(all(abs(rCoefs - coefs) < 1e-6))
  expect_true(all(
    as.character(stats$features) ==
    c("(Intercept)", "Sepal_Length", "Species__versicolor", "Species__virginica")))
})
