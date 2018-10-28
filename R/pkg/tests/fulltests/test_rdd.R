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

context("basic RDD functions")

# JavaSparkContext handle
sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)
sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)

# Data
nums <- 1:10
rdd <- parallelize(sc, nums, 2L)

intPairs <- list(list(1L, -1), list(2L, 100), list(2L, 1), list(1L, 200))
intRdd <- parallelize(sc, intPairs, 2L)

test_that("get number of partitions in RDD", {
  expect_equal(getNumPartitionsRDD(rdd), 2)
  expect_equal(getNumPartitionsRDD(intRdd), 2)
})

test_that("first on RDD", {
  expect_equal(firstRDD(rdd), 1)
  newrdd <- lapply(rdd, function(x) x + 1)
  expect_equal(firstRDD(newrdd), 2)
})

test_that("Test correct concurrency of RRDD.compute()", {
  rdd <- parallelize(sc, 1:1000, 100)
  jrdd <- getJRDD(lapply(rdd, function(x) { x }), "row")
  zrdd <- callJMethod(jrdd, "zip", jrdd)
  count <- callJMethod(zrdd, "count")
  expect_equal(count, 1000)
})

sparkR.session.stop()
