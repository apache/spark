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

context("partitionBy, groupByKey, reduceByKey etc.")

# JavaSparkContext handle
sparkSession <- sparkR.session()
sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)

# Data
intPairs <- list(list(1L, -1), list(2L, 100), list(2L, 1), list(1L, 200))
intRdd <- parallelize(sc, intPairs, 2L)

doublePairs <- list(list(1.5, -1), list(2.5, 100), list(2.5, 1), list(1.5, 200))
doubleRdd <- parallelize(sc, doublePairs, 2L)

numPairs <- list(list(1L, 100), list(2L, 200), list(4L, -1), list(3L, 1),
                 list(3L, 0))
numPairsRdd <- parallelize(sc, numPairs, length(numPairs))

strList <- list("Dexter Morgan: Blood. Sometimes it sets my teeth on edge and ",
                "Dexter Morgan: Harry and Dorris Morgan did a wonderful job ")
strListRDD <- parallelize(sc, strList, 4)

test_that("groupByKey for integers", {
  grouped <- groupByKey(intRdd, 2L)

  actual <- collect(grouped)

  expected <- list(list(2L, list(100, 1)), list(1L, list(-1, 200)))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("groupByKey for doubles", {
  grouped <- groupByKey(doubleRdd, 2L)

  actual <- collect(grouped)

  expected <- list(list(1.5, list(-1, 200)), list(2.5, list(100, 1)))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("reduceByKey for ints", {
  reduced <- reduceByKey(intRdd, "+", 2L)

  actual <- collect(reduced)

  expected <- list(list(2L, 101), list(1L, 199))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("reduceByKey for doubles", {
  reduced <- reduceByKey(doubleRdd, "+", 2L)
  actual <- collect(reduced)

  expected <- list(list(1.5, 199), list(2.5, 101))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("combineByKey for ints", {
  reduced <- combineByKey(intRdd, function(x) { x }, "+", "+", 2L)

  actual <- collect(reduced)

  expected <- list(list(2L, 101), list(1L, 199))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("combineByKey for doubles", {
  reduced <- combineByKey(doubleRdd, function(x) { x }, "+", "+", 2L)
  actual <- collect(reduced)

  expected <- list(list(1.5, 199), list(2.5, 101))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("combineByKey for characters", {
  stringKeyRDD <- parallelize(sc,
                              list(list("max", 1L), list("min", 2L),
                                   list("other", 3L), list("max", 4L)), 2L)
  reduced <- combineByKey(stringKeyRDD,
                          function(x) { x }, "+", "+", 2L)
  actual <- collect(reduced)

  expected <- list(list("max", 5L), list("min", 2L), list("other", 3L))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("aggregateByKey", {
  # test aggregateByKey for int keys
  rdd <- parallelize(sc, list(list(1, 1), list(1, 2), list(2, 3), list(2, 4)))

  zeroValue <- list(0, 0)
  seqOp <- function(x, y) { list(x[[1]] + y, x[[2]] + 1) }
  combOp <- function(x, y) { list(x[[1]] + y[[1]], x[[2]] + y[[2]]) }
  aggregatedRDD <- aggregateByKey(rdd, zeroValue, seqOp, combOp, 2L)

  actual <- collect(aggregatedRDD)

  expected <- list(list(1, list(3, 2)), list(2, list(7, 2)))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))

  # test aggregateByKey for string keys
  rdd <- parallelize(sc, list(list("a", 1), list("a", 2), list("b", 3), list("b", 4)))

  zeroValue <- list(0, 0)
  seqOp <- function(x, y) { list(x[[1]] + y, x[[2]] + 1) }
  combOp <- function(x, y) { list(x[[1]] + y[[1]], x[[2]] + y[[2]]) }
  aggregatedRDD <- aggregateByKey(rdd, zeroValue, seqOp, combOp, 2L)

  actual <- collect(aggregatedRDD)

  expected <- list(list("a", list(3, 2)), list("b", list(7, 2)))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("foldByKey", {
  # test foldByKey for int keys
  folded <- foldByKey(intRdd, 0, "+", 2L)

  actual <- collect(folded)

  expected <- list(list(2L, 101), list(1L, 199))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))

  # test foldByKey for double keys
  folded <- foldByKey(doubleRdd, 0, "+", 2L)

  actual <- collect(folded)

  expected <- list(list(1.5, 199), list(2.5, 101))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))

  # test foldByKey for string keys
  stringKeyPairs <- list(list("a", -1), list("b", 100), list("b", 1), list("a", 200))

  stringKeyRDD <- parallelize(sc, stringKeyPairs)
  folded <- foldByKey(stringKeyRDD, 0, "+", 2L)

  actual <- collect(folded)

  expected <- list(list("b", 101), list("a", 199))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))

  # test foldByKey for empty pair RDD
  rdd <- parallelize(sc, list())
  folded <- foldByKey(rdd, 0, "+", 2L)
  actual <- collect(folded)
  expected <- list()
  expect_equal(actual, expected)

  # test foldByKey for RDD with only 1 pair
  rdd <- parallelize(sc,  list(list(1, 1)))
  folded <- foldByKey(rdd, 0, "+", 2L)
  actual <- collect(folded)
  expected <- list(list(1, 1))
  expect_equal(actual, expected)
})

test_that("partitionBy() partitions data correctly", {
  # Partition by magnitude
  partitionByMagnitude <- function(key) { if (key >= 3) 1 else 0 }

  resultRDD <- partitionBy(numPairsRdd, 2L, partitionByMagnitude)

  expected_first <- list(list(1, 100), list(2, 200)) # key less than 3
  expected_second <- list(list(4, -1), list(3, 1), list(3, 0)) # key greater than or equal 3
  actual_first <- collectPartition(resultRDD, 0L)
  actual_second <- collectPartition(resultRDD, 1L)

  expect_equal(sortKeyValueList(actual_first), sortKeyValueList(expected_first))
  expect_equal(sortKeyValueList(actual_second), sortKeyValueList(expected_second))
})

test_that("partitionBy works with dependencies", {
  kOne <- 1
  partitionByParity <- function(key) { if (key %% 2 == kOne) 7 else 4 }

  # Partition by parity
  resultRDD <- partitionBy(numPairsRdd, numPartitions = 2L, partitionByParity)

  # keys even; 100 %% 2 == 0
  expected_first <- list(list(2, 200), list(4, -1))
  # keys odd; 3 %% 2 == 1
  expected_second <- list(list(1, 100), list(3, 1), list(3, 0))
  actual_first <- collectPartition(resultRDD, 0L)
  actual_second <- collectPartition(resultRDD, 1L)

  expect_equal(sortKeyValueList(actual_first), sortKeyValueList(expected_first))
  expect_equal(sortKeyValueList(actual_second), sortKeyValueList(expected_second))
})

test_that("test partitionBy with string keys", {
  words <- flatMap(strListRDD, function(line) { strsplit(line, " ")[[1]] })
  wordCount <- lapply(words, function(word) { list(word, 1L) })

  resultRDD <- partitionBy(wordCount, 2L)
  expected_first <- list(list("Dexter", 1), list("Dexter", 1))
  expected_second <- list(list("and", 1), list("and", 1))

  actual_first <- Filter(function(item) { item[[1]] == "Dexter" },
                         collectPartition(resultRDD, 0L))
  actual_second <- Filter(function(item) { item[[1]] == "and" },
                          collectPartition(resultRDD, 1L))

  expect_equal(sortKeyValueList(actual_first), sortKeyValueList(expected_first))
  expect_equal(sortKeyValueList(actual_second), sortKeyValueList(expected_second))
})
