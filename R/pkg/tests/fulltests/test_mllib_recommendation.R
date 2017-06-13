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

context("MLlib recommendation algorithms")

# Tests for MLlib recommendation algorithms in SparkR
sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

test_that("spark.als", {
  data <- list(list(0, 0, 4.0), list(0, 1, 2.0), list(1, 1, 3.0), list(1, 2, 4.0),
               list(2, 1, 1.0), list(2, 2, 5.0))
  df <- createDataFrame(data, c("user", "item", "score"))
  model <- spark.als(df, ratingCol = "score", userCol = "user", itemCol = "item",
                     rank = 10, maxIter = 5, seed = 0, regParam = 0.1)
  stats <- summary(model)
  expect_equal(stats$rank, 10)
  test <- createDataFrame(list(list(0, 2), list(1, 0), list(2, 0)), c("user", "item"))
  predictions <- collect(predict(model, test))

  expect_equal(predictions$prediction, c(-0.1380762, 2.6258414, -1.5018409),
  tolerance = 1e-4)

  # Test model save/load
  if (windows_with_hadoop()) {
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
  }
})

sparkR.session.stop()
