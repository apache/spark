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

context("parallelize() and collect()")

# Mock data
numVector <- c(-10:97)
numList <- list(sqrt(1), sqrt(2), sqrt(3), 4 ** 10)
strVector <- c("Dexter Morgan: I suppose I should be upset, even feel",
               "violated, but I'm not. No, in fact, I think this is a friendly",
               "message, like \"Hey, wanna play?\" and yes, I want to play. ",
               "I really, really do.")
strList <- list("Dexter Morgan: Blood. Sometimes it sets my teeth on edge, ",
                "other times it helps me control the chaos.",
                "Dexter Morgan: Harry and Dorris Morgan did a wonderful job ",
                "raising me. But they're both dead now. I didn't kill them. Honest.")

numPairs <- list(list(1, 1), list(1, 2), list(2, 2), list(2, 3))
strPairs <- list(list(strList, strList), list(strList, strList))

# JavaSparkContext handle
sparkSession <- sparkR.session(enableHiveSupport = FALSE)
jsc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)

# Tests

test_that("parallelize() on simple vectors and lists returns an RDD", {
  numVectorRDD <- parallelize(jsc, numVector, 1)
  numVectorRDD2 <- parallelize(jsc, numVector, 10)
  numListRDD <- parallelize(jsc, numList, 1)
  numListRDD2 <- parallelize(jsc, numList, 4)
  strVectorRDD <- parallelize(jsc, strVector, 2)
  strVectorRDD2 <- parallelize(jsc, strVector, 3)
  strListRDD <- parallelize(jsc, strList, 4)
  strListRDD2 <- parallelize(jsc, strList, 1)

  rdds <- c(numVectorRDD,
             numVectorRDD2,
             numListRDD,
             numListRDD2,
             strVectorRDD,
             strVectorRDD2,
             strListRDD,
             strListRDD2)

  for (rdd in rdds) {
    expect_is(rdd, "RDD")
    expect_true(.hasSlot(rdd, "jrdd")
                && inherits(rdd@jrdd, "jobj")
                && isInstanceOf(rdd@jrdd, "org.apache.spark.api.java.JavaRDD"))
  }
})

test_that("collect(), following a parallelize(), gives back the original collections", {
  numVectorRDD <- parallelize(jsc, numVector, 10)
  expect_equal(collectRDD(numVectorRDD), as.list(numVector))

  numListRDD <- parallelize(jsc, numList, 1)
  numListRDD2 <- parallelize(jsc, numList, 4)
  expect_equal(collectRDD(numListRDD), as.list(numList))
  expect_equal(collectRDD(numListRDD2), as.list(numList))

  strVectorRDD <- parallelize(jsc, strVector, 2)
  strVectorRDD2 <- parallelize(jsc, strVector, 3)
  expect_equal(collectRDD(strVectorRDD), as.list(strVector))
  expect_equal(collectRDD(strVectorRDD2), as.list(strVector))

  strListRDD <- parallelize(jsc, strList, 4)
  strListRDD2 <- parallelize(jsc, strList, 1)
  expect_equal(collectRDD(strListRDD), as.list(strList))
  expect_equal(collectRDD(strListRDD2), as.list(strList))
})

test_that("regression: collect() following a parallelize() does not drop elements", {
  # 10 %/% 6 = 1, ceiling(10 / 6) = 2
  collLen <- 10
  numPart <- 6
  expected <- runif(collLen)
  actual <- collectRDD(parallelize(jsc, expected, numPart))
  expect_equal(actual, as.list(expected))
})

test_that("parallelize() and collect() work for lists of pairs (pairwise data)", {
  # use the pairwise logical to indicate pairwise data
  numPairsRDDD1 <- parallelize(jsc, numPairs, 1)
  numPairsRDDD2 <- parallelize(jsc, numPairs, 2)
  numPairsRDDD3 <- parallelize(jsc, numPairs, 3)
  expect_equal(collectRDD(numPairsRDDD1), numPairs)
  expect_equal(collectRDD(numPairsRDDD2), numPairs)
  expect_equal(collectRDD(numPairsRDDD3), numPairs)
  # can also leave out the parameter name, if the params are supplied in order
  strPairsRDDD1 <- parallelize(jsc, strPairs, 1)
  strPairsRDDD2 <- parallelize(jsc, strPairs, 2)
  expect_equal(collectRDD(strPairsRDDD1), strPairs)
  expect_equal(collectRDD(strPairsRDDD2), strPairs)
})

sparkR.session.stop()
