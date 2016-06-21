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

context("broadcast variables")

# JavaSparkContext handle
sparkSession <- sparkR.session()
sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)

# Partitioned data
nums <- 1:2
rrdd <- parallelize(sc, nums, 2L)

test_that("using broadcast variable", {
  randomMat <- matrix(nrow = 10, ncol = 10, data = rnorm(100))
  randomMatBr <- broadcast(sc, randomMat)

  useBroadcast <- function(x) {
    sum(SparkR:::value(randomMatBr) * x)
  }
  actual <- collect(lapply(rrdd, useBroadcast))
  expected <- list(sum(randomMat) * 1, sum(randomMat) * 2)
  expect_equal(actual, expected)
})

test_that("without using broadcast variable", {
  randomMat <- matrix(nrow = 10, ncol = 10, data = rnorm(100))

  useBroadcast <- function(x) {
    sum(randomMat * x)
  }
  actual <- collect(lapply(rrdd, useBroadcast))
  expected <- list(sum(randomMat) * 1, sum(randomMat) * 2)
  expect_equal(actual, expected)
})
