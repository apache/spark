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
  
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName)

  text.rdd <- textFile(sc, fileName)
  union.rdd <- unionRDD(rdd, text.rdd)
  actual <- collect(union.rdd)
  expect_equal(actual, c(as.list(nums), mockFile))
  expect_true(getSerializedMode(union.rdd) == "byte")

  rdd<- map(text.rdd, function(x) {x})
  union.rdd <- unionRDD(rdd, text.rdd)
  actual <- collect(union.rdd)
  expect_equal(actual, as.list(c(mockFile, mockFile)))
  expect_true(getSerializedMode(union.rdd) == "byte")

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
