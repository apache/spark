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

context("SparkSQL Arrow optimization")

sparkSession <- sparkR.session(
  master = sparkRTestMaster,
  enableHiveSupport = FALSE,
  sparkConfig = list(spark.sql.execution.arrow.sparkr.enabled = "true"))

test_that("createDataFrame/collect Arrow optimization", {
  skip_if_not_installed("arrow")

  conf <- callJMethod(sparkSession, "conf")
  arrowEnabled <- sparkR.conf("spark.sql.execution.arrow.sparkr.enabled")[[1]]

  callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", "false")
  tryCatch({
    expected <- collect(createDataFrame(mtcars))
  },
  finally = {
    callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", arrowEnabled)
  })

  expect_equal(collect(createDataFrame(mtcars)), expected)
})

test_that("createDataFrame/collect Arrow optimization - many partitions (partition order test)", {
  skip_if_not_installed("arrow")
  expect_equal(collect(createDataFrame(mtcars, numPartitions = 32)),
               collect(createDataFrame(mtcars, numPartitions = 1)))
})

test_that("createDataFrame/collect Arrow optimization - type specification", {
  skip_if_not_installed("arrow")
  rdf <- data.frame(list(list(a = 1,
                              b = "a",
                              c = TRUE,
                              d = 1.1,
                              e = 1L,
                              f = as.Date("1990-02-24"),
                              g = as.POSIXct("1990-02-24 12:34:56"))))

  arrowEnabled <- sparkR.conf("spark.sql.execution.arrow.sparkr.enabled")[[1]]
  conf <- callJMethod(sparkSession, "conf")

  callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", "false")
  tryCatch({
    expected <- collect(createDataFrame(rdf))
  },
  finally = {
    callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", arrowEnabled)
  })

  expect_true(all(collect(createDataFrame(rdf)) == expected))
})

test_that("dapply() Arrow optimization", {
  skip_if_not_installed("arrow")
  df <- createDataFrame(mtcars)

  conf <- callJMethod(sparkSession, "conf")
  arrowEnabled <- sparkR.conf("spark.sql.execution.arrow.sparkr.enabled")[[1]]

  callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", "false")
  tryCatch({
    ret <- dapply(df,
                  function(rdf) {
                    stopifnot(is.data.frame(rdf))
                    rdf
                  },
                  schema(df))
    expected <- collect(ret)
  },
  finally = {
    callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", arrowEnabled)
  })

  ret <- dapply(df,
                function(rdf) {
                  stopifnot(is.data.frame(rdf))
                  # mtcars' hp is more then 50.
                  stopifnot(all(rdf$hp > 50))
                  rdf
                },
                schema(df))
  actual <- collect(ret)
  expect_equal(actual, expected)
  expect_equal(count(ret), nrow(mtcars))
})

test_that("dapply() Arrow optimization - type specification", {
  skip_if_not_installed("arrow")
  # Note that regular dapply() seems not supporting date and timestamps
  # whereas Arrow-optimized dapply() does.
  rdf <- data.frame(list(list(a = 1,
                              b = "a",
                              c = TRUE,
                              d = 1.1,
                              e = 1L)))
  # numPartitions are set to 8 intentionally to test empty partitions as well.
  df <- createDataFrame(rdf, numPartitions = 8)

  conf <- callJMethod(sparkSession, "conf")
  arrowEnabled <- sparkR.conf("spark.sql.execution.arrow.sparkr.enabled")[[1]]

  callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", "false")
  tryCatch({
    ret <- dapply(df, function(rdf) { rdf }, schema(df))
    expected <- collect(ret)
  },
  finally = {
    callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", arrowEnabled)
  })

  ret <- dapply(df, function(rdf) { rdf }, schema(df))
  actual <- collect(ret)
  expect_equal(actual, expected)
})

test_that("dapply() Arrow optimization - type specification (date and timestamp)", {
  skip_if_not_installed("arrow")
  rdf <- data.frame(list(list(a = as.Date("1990-02-24"),
                              b = as.POSIXct("1990-02-24 12:34:56"))))
  df <- createDataFrame(rdf)
  ret <- dapply(df, function(rdf) { rdf }, schema(df))
  expect_true(all(collect(ret) == rdf))
})

test_that("gapply() Arrow optimization", {
  skip_if_not_installed("arrow")
  df <- createDataFrame(mtcars)

  conf <- callJMethod(sparkSession, "conf")
  arrowEnabled <- sparkR.conf("spark.sql.execution.arrow.sparkr.enabled")[[1]]

  callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", "false")
  tryCatch({
    ret <- gapply(df,
                 "gear",
                 function(key, grouped) {
                   if (length(key) > 0) {
                     stopifnot(is.numeric(key[[1]]))
                   }
                   stopifnot(is.data.frame(grouped))
                   grouped
                 },
                 schema(df))
    expected <- collect(ret)
  },
  finally = {
    callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", arrowEnabled)
  })

  ret <- gapply(df,
               "gear",
               function(key, grouped) {
                 if (length(key) > 0) {
                   stopifnot(is.numeric(key[[1]]))
                 }
                 stopifnot(is.data.frame(grouped))
                 stopifnot(length(colnames(grouped)) == 11)
                 # mtcars' hp is more then 50.
                 stopifnot(all(grouped$hp > 50))
                 grouped
               },
               schema(df))
  actual <- collect(ret)
  expect_equal(actual, expected)
  expect_equal(count(ret), nrow(mtcars))
})

test_that("gapply() Arrow optimization - type specification", {
  skip_if_not_installed("arrow")
  # Note that regular gapply() seems not supporting date and timestamps
  # whereas Arrow-optimized gapply() does.
  rdf <- data.frame(list(list(a = 1,
                              b = "a",
                              c = TRUE,
                              d = 1.1,
                              e = 1L)))
  df <- createDataFrame(rdf)

  conf <- callJMethod(sparkSession, "conf")
  arrowEnabled <- sparkR.conf("spark.sql.execution.arrow.sparkr.enabled")[[1]]

  callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", "false")
  tryCatch({
    ret <- gapply(df,
                  "a",
                  function(key, grouped) { grouped }, schema(df))
    expected <- collect(ret)
  },
  finally = {
    callJMethod(conf, "set", "spark.sql.execution.arrow.sparkr.enabled", arrowEnabled)
  })

  ret <- gapply(df,
                "a",
                function(key, grouped) { grouped }, schema(df))
  actual <- collect(ret)
  expect_equal(actual, expected)
})

test_that("gapply() Arrow optimization - type specification (date and timestamp)", {
  skip_if_not_installed("arrow")
  rdf <- data.frame(list(list(a = as.Date("1990-02-24"),
                              b = as.POSIXct("1990-02-24 12:34:56"))))
  df <- createDataFrame(rdf)
  ret <- gapply(df,
                "a",
                function(key, grouped) { grouped }, schema(df))
  expect_true(all(collect(ret) == rdf))
})

test_that("Arrow optimization - unsupported types", {
  skip_if_not_installed("arrow")

  expect_error(checkSchemaInArrow(structType("a FLOAT")), "not support float type")
  expect_error(checkSchemaInArrow(structType("a BINARY")), "not support binary type")
  expect_error(checkSchemaInArrow(structType("a ARRAY<INT>")), "not support array type")
  expect_error(checkSchemaInArrow(structType("a MAP<INT, INT>")), "not support map type")
  expect_error(checkSchemaInArrow(structType("a STRUCT<a: INT>")),
               "not support nested struct type")
})

test_that("SPARK-32478: gapply() Arrow optimization - error message for schema mismatch", {
  skip_if_not_installed("arrow")
  df <- createDataFrame(list(list(a = 1L, b = "a")))

  expect_error(
    count(gapply(df, "a", function(key, group) { group }, structType("a int, b int"))),
    "expected IntegerType, IntegerType, got IntegerType, StringType")
})

test_that("SPARK-43789: Automatically pick the number of partitions based on Arrow batch size", {
  skip_if_not_installed("arrow")

  conf <- callJMethod(sparkSession, "conf")
  maxRecordsPerBatch <- sparkR.conf("spark.sql.execution.arrow.maxRecordsPerBatch")[[1]]

  callJMethod(conf, "set", "spark.sql.execution.arrow.maxRecordsPerBatch", "10")
  tryCatch({
    expect_equal(getNumPartitionsRDD(toRDD(createDataFrame(mtcars))), 4)
  },
  finally = {
    callJMethod(conf, "set", "spark.sql.execution.arrow.maxRecordsPerBatch", maxRecordsPerBatch)
  })
})

sparkR.session.stop()
