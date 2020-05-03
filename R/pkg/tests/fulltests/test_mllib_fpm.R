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

context("MLlib frequent pattern mining")

# Tests for MLlib frequent pattern mining algorithms in SparkR
sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

test_that("spark.fpGrowth", {
  data <- selectExpr(createDataFrame(data.frame(items = c(
    "1,2",
    "1,2",
    "1,2,3",
    "1,3"
  ))), "split(items, ',') as items")

  model <- spark.fpGrowth(data, minSupport = 0.3, minConfidence = 0.8, numPartitions = 1)

  itemsets <- collect(spark.freqItemsets(model))

  expected_itemsets <- data.frame(
    items = I(list(list("3"), list("3", "1"), list("2"), list("2", "1"), list("1"))),
    freq = c(2, 2, 3, 3, 4)
  )

  expect_equivalent(expected_itemsets, itemsets)

  expected_association_rules <- data.frame(
    antecedent = I(list(list("2"), list("3"))),
    consequent = I(list(list("1"), list("1"))),
    confidence = c(1, 1),
    lift = c(1, 1)
  )

  expect_equivalent(expected_association_rules, collect(spark.associationRules(model)))

  new_data <- selectExpr(createDataFrame(data.frame(items = c(
    "1,2",
    "1,3",
    "2,3"
  ))), "split(items, ',') as items")

  expected_predictions <- data.frame(
    items = I(list(list("1", "2"), list("1", "3"), list("2", "3"))),
    prediction = I(list(list(), list(), list("1")))
  )

  expect_equivalent(expected_predictions, collect(predict(model, new_data)))

  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-fpm", fileext = ".tmp")
    write.ml(model, modelPath, overwrite = TRUE)
    loaded_model <- read.ml(modelPath)

    expect_equivalent(
      itemsets,
      collect(spark.freqItemsets(loaded_model)))

    unlink(modelPath)
  }

  model_without_numpartitions <- spark.fpGrowth(data, minSupport = 0.3, minConfidence = 0.8)
  expect_equal(
    count(spark.freqItemsets(model_without_numpartitions)),
    count(spark.freqItemsets(model))
  )

})

test_that("spark.prefixSpan", {
  df <- createDataFrame(list(list(list(list(1L, 2L), list(3L))),
                             list(list(list(1L), list(3L, 2L), list(1L, 2L))),
                             list(list(list(1L, 2L), list(5L))),
                             list(list(list(6L)))),
                        schema = c("sequence"))
  result <- spark.findFrequentSequentialPatterns(df, minSupport = 0.5, maxPatternLength = 5L,
                                                 maxLocalProjDBSize = 32000000L)

  expected_result <- createDataFrame(list(list(list(list(1L)), 3L), list(list(list(3L)), 2L),
                                          list(list(list(2L)), 3L), list(list(list(1L, 2L)), 3L),
                                          list(list(list(1L), list(3L)), 2L)),
                                     schema = c("sequence", "freq"))

  expect_equivalent(expected_result, result)
})

sparkR.session.stop()
