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

context("SerDe functionality")

sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

test_that("SerDe of primitive types", {
  x <- callJStatic("SparkRHandler", "echo", 1L)
  expect_equal(x, 1L)
  expect_equal(class(x), "integer")

  x <- callJStatic("SparkRHandler", "echo", 1)
  expect_equal(x, 1)
  expect_equal(class(x), "numeric")

  x <- callJStatic("SparkRHandler", "echo", TRUE)
  expect_true(x)
  expect_equal(class(x), "logical")

  x <- callJStatic("SparkRHandler", "echo", "abc")
  expect_equal(x, "abc")
  expect_equal(class(x), "character")
})

test_that("SerDe of multi-element primitive vectors inside R data.frame", {
  # vector of integers embedded in R data.frame
  indices <- 1L:3L
  myDf <- data.frame(indices)
  myDf$data <- list(rep(0L, 3L))
  mySparkDf <- as.DataFrame(myDf)
  myResultingDf <- collect(mySparkDf)
  myDfListedData <- data.frame(indices)
  myDfListedData$data <- list(as.list(rep(0L, 3L)))
  expect_equal(myResultingDf, myDfListedData)
  expect_equal(class(myResultingDf[["data"]][[1]]), "list")
  expect_equal(class(myResultingDf[["data"]][[1]][[1]]), "integer")

  # vector of numeric embedded in R data.frame
  myDf <- data.frame(indices)
  myDf$data <- list(rep(0, 3L))
  mySparkDf <- as.DataFrame(myDf)
  myResultingDf <- collect(mySparkDf)
  myDfListedData <- data.frame(indices)
  myDfListedData$data <- list(as.list(rep(0, 3L)))
  expect_equal(myResultingDf, myDfListedData)
  expect_equal(class(myResultingDf[["data"]][[1]]), "list")
  expect_equal(class(myResultingDf[["data"]][[1]][[1]]), "numeric")

  # vector of logical embedded in R data.frame
  myDf <- data.frame(indices)
  myDf$data <- list(rep(TRUE, 3L))
  mySparkDf <- as.DataFrame(myDf)
  myResultingDf <- collect(mySparkDf)
  myDfListedData <- data.frame(indices)
  myDfListedData$data <- list(as.list(rep(TRUE, 3L)))
  expect_equal(myResultingDf, myDfListedData)
  expect_equal(class(myResultingDf[["data"]][[1]]), "list")
  expect_equal(class(myResultingDf[["data"]][[1]][[1]]), "logical")

  # vector of character embedded in R data.frame
  myDf <- data.frame(indices)
  myDf$data <- list(rep("abc", 3L))
  mySparkDf <- as.DataFrame(myDf)
  myResultingDf <- collect(mySparkDf)
  myDfListedData <- data.frame(indices)
  myDfListedData$data <- list(as.list(rep("abc", 3L)))
  expect_equal(myResultingDf, myDfListedData)
  expect_equal(class(myResultingDf[["data"]][[1]]), "list")
  expect_equal(class(myResultingDf[["data"]][[1]][[1]]), "character")
})

test_that("SerDe of list of primitive types", {
  x <- list(1L, 2L, 3L)
  y <- callJStatic("SparkRHandler", "echo", x)
  expect_equal(x, y)
  expect_equal(class(y[[1]]), "integer")

  x <- list(1, 2, 3)
  y <- callJStatic("SparkRHandler", "echo", x)
  expect_equal(x, y)
  expect_equal(class(y[[1]]), "numeric")

  x <- list(TRUE, FALSE)
  y <- callJStatic("SparkRHandler", "echo", x)
  expect_equal(x, y)
  expect_equal(class(y[[1]]), "logical")

  x <- list("a", "b", "c")
  y <- callJStatic("SparkRHandler", "echo", x)
  expect_equal(x, y)
  expect_equal(class(y[[1]]), "character")

  # Empty list
  x <- list()
  y <- callJStatic("SparkRHandler", "echo", x)
  expect_equal(x, y)
})

test_that("SerDe of list of lists", {
  x <- list(list(1L, 2L, 3L), list(1, 2, 3),
            list(TRUE, FALSE), list("a", "b", "c"))
  y <- callJStatic("SparkRHandler", "echo", x)
  expect_equal(x, y)

  # List of empty lists
  x <- list(list(), list())
  y <- callJStatic("SparkRHandler", "echo", x)
  expect_equal(x, y)
})

sparkR.session.stop()

# Note that this test should be at the end of tests since the configruations used here are not
# specific to sessions, and the Spark context is restarted.
test_that("createDataFrame large objects", {
  for (encryptionEnabled in list("true", "false")) {
    # To simulate a large object scenario, we set spark.r.maxAllocationLimit to a smaller value
    conf <- list(spark.r.maxAllocationLimit = "100",
                 spark.io.encryption.enabled = encryptionEnabled)

    suppressWarnings(sparkR.session(master = sparkRTestMaster,
                                    sparkConfig = conf,
                                    enableHiveSupport = FALSE))

    sc <- getSparkContext()
    actual <- callJStatic("org.apache.spark.api.r.RUtils", "isEncryptionEnabled", sc)
    expected <- as.logical(encryptionEnabled)
    expect_equal(actual, expected)

    tryCatch({
      # suppress warnings from dot in the field names. See also SPARK-21536.
      df <- suppressWarnings(createDataFrame(iris, numPartitions = 3))
      expect_equal(getNumPartitions(df), 3)
      expect_equal(dim(df), dim(iris))

      df <- createDataFrame(cars, numPartitions = 3)
      expect_equal(collect(df), cars)
    },
    finally = {
      sparkR.stop()
    })
  }
})
