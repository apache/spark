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

context("binary functions")

# JavaSparkContext handle
sc <- sparkR.init()

# Data
nums <- 1:10
rdd <- parallelize(sc, nums, 2L)

# File content
mockFile <- c("Spark is pretty.", "Spark is awesome.")

test_that("union on two RDDs", {
  actual <- collect(unionRDD(rdd, rdd))
  expect_equal(actual, as.list(rep(nums, 2)))

  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName)

  text.rdd <- textFile(sc, fileName)
  union.rdd <- unionRDD(rdd, text.rdd)
  actual <- collect(union.rdd)
  expect_equal(actual, c(as.list(nums), mockFile))
  expect_equal(getSerializedMode(union.rdd), "byte")

  rdd <- map(text.rdd, function(x) {x})
  union.rdd <- unionRDD(rdd, text.rdd)
  actual <- collect(union.rdd)
  expect_equal(actual, as.list(c(mockFile, mockFile)))
  expect_equal(getSerializedMode(union.rdd), "byte")

  unlink(fileName)
})

test_that("cogroup on two RDDs", {
  rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
  rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
  cogroup.rdd <- cogroup(rdd1, rdd2, numPartitions = 2L)
  actual <- collect(cogroup.rdd)
  expect_equal(actual,
               list(list(1, list(list(1), list(2, 3))), list(2, list(list(4), list()))))

  rdd1 <- parallelize(sc, list(list("a", 1), list("a", 4)))
  rdd2 <- parallelize(sc, list(list("b", 2), list("a", 3)))
  cogroup.rdd <- cogroup(rdd1, rdd2, numPartitions = 2L)
  actual <- collect(cogroup.rdd)

  expected <- list(list("b", list(list(), list(2))), list("a", list(list(1, 4), list(3))))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))
})

test_that("zipPartitions() on RDDs", {
  rdd1 <- parallelize(sc, 1:2, 2L)  # 1, 2
  rdd2 <- parallelize(sc, 1:4, 2L)  # 1:2, 3:4
  rdd3 <- parallelize(sc, 1:6, 2L)  # 1:3, 4:6
  actual <- collect(zipPartitions(rdd1, rdd2, rdd3,
                                  func = function(x, y, z) { list(list(x, y, z))} ))
  expect_equal(actual,
               list(list(1, c(1, 2), c(1, 2, 3)), list(2, c(3, 4), c(4, 5, 6))))

  mockFile <- c("Spark is pretty.", "Spark is awesome.")
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName, 1)
  actual <- collect(zipPartitions(rdd, rdd,
                                  func = function(x, y) { list(paste(x, y, sep = "\n")) }))
  expected <- list(paste(mockFile, mockFile, sep = "\n"))
  expect_equal(actual, expected)

  rdd1 <- parallelize(sc, 0:1, 1)
  actual <- collect(zipPartitions(rdd1, rdd,
                                  func = function(x, y) { list(x + nchar(y)) }))
  expected <- list(0:1 + nchar(mockFile))
  expect_equal(actual, expected)

  rdd <- map(rdd, function(x) { x })
  actual <- collect(zipPartitions(rdd, rdd1,
                                  func = function(x, y) { list(y + nchar(x)) }))
  expect_equal(actual, expected)

  unlink(fileName)
})
