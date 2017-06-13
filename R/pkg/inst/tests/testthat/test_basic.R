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

context("basic tests for CRAN")

test_that("create DataFrame from list or data.frame", {
  sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

  i <- 4
  df <- createDataFrame(data.frame(dummy = 1:i))
  expect_equal(count(df), i)

  l <- list(list(a = 1, b = 2), list(a = 3, b = 4))
  df <- createDataFrame(l)
  expect_equal(columns(df), c("a", "b"))

  a <- 1:3
  b <- c("a", "b", "c")
  ldf <- data.frame(a, b)
  df <- createDataFrame(ldf)
  expect_equal(columns(df), c("a", "b"))
  expect_equal(dtypes(df), list(c("a", "int"), c("b", "string")))
  expect_equal(count(df), 3)
  ldf2 <- collect(df)
  expect_equal(ldf$a, ldf2$a)

  mtcarsdf <- createDataFrame(mtcars)
  expect_equivalent(collect(mtcarsdf), mtcars)

  bytes <- as.raw(c(1, 2, 3))
  df <- createDataFrame(list(list(bytes)))
  expect_equal(collect(df)[[1]][[1]], bytes)

  sparkR.session.stop()
})

test_that("spark.glm and predict", {
  sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

  training <- suppressWarnings(createDataFrame(iris))
  # gaussian family
  model <- spark.glm(training, Sepal_Width ~ Sepal_Length + Species)
  prediction <- predict(model, training)
  expect_equal(typeof(take(select(prediction, "prediction"), 1)$prediction), "double")
  vals <- collect(select(prediction, "prediction"))
  rVals <- predict(glm(Sepal.Width ~ Sepal.Length + Species, data = iris), iris)
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

  sparkR.session.stop()
})
