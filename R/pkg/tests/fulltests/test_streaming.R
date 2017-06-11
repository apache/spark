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

context("Structured Streaming")

# Tests for Structured Streaming functions in SparkR

sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

jsonSubDir <- file.path("sparkr-test", "json", "")
if (is_windows()) {
  # file.path removes the empty separator on Windows, adds it back
  jsonSubDir <- paste0(jsonSubDir, .Platform$file.sep)
}
jsonDir <- file.path(tempdir(), jsonSubDir)
dir.create(jsonDir, recursive = TRUE)

mockLines <- c("{\"name\":\"Michael\"}",
               "{\"name\":\"Andy\", \"age\":30}",
               "{\"name\":\"Justin\", \"age\":19}")
jsonPath <- tempfile(pattern = jsonSubDir, fileext = ".tmp")
writeLines(mockLines, jsonPath)

mockLinesNa <- c("{\"name\":\"Bob\",\"age\":16,\"height\":176.5}",
                 "{\"name\":\"Alice\",\"age\":null,\"height\":164.3}",
                 "{\"name\":\"David\",\"age\":60,\"height\":null}")
jsonPathNa <- tempfile(pattern = jsonSubDir, fileext = ".tmp")

schema <- structType(structField("name", "string"),
                     structField("age", "integer"),
                     structField("count", "double"))

test_that("read.stream, write.stream, awaitTermination, stopQuery", {
  df <- read.stream("json", path = jsonDir, schema = schema, maxFilesPerTrigger = 1)
  expect_true(isStreaming(df))
  counts <- count(group_by(df, "name"))
  q <- write.stream(counts, "memory", queryName = "people", outputMode = "complete")

  expect_false(awaitTermination(q, 5 * 1000))
  callJMethod(q@ssq, "processAllAvailable")
  expect_equal(head(sql("SELECT count(*) FROM people"))[[1]], 3)

  writeLines(mockLinesNa, jsonPathNa)
  awaitTermination(q, 5 * 1000)
  callJMethod(q@ssq, "processAllAvailable")
  expect_equal(head(sql("SELECT count(*) FROM people"))[[1]], 6)

  stopQuery(q)
  expect_true(awaitTermination(q, 1))
  expect_error(awaitTermination(q), NA)
})

test_that("print from explain, lastProgress, status, isActive", {
  df <- read.stream("json", path = jsonDir, schema = schema)
  expect_true(isStreaming(df))
  counts <- count(group_by(df, "name"))
  q <- write.stream(counts, "memory", queryName = "people2", outputMode = "complete")

  awaitTermination(q, 5 * 1000)
  callJMethod(q@ssq, "processAllAvailable")

  expect_equal(capture.output(explain(q))[[1]], "== Physical Plan ==")
  expect_true(any(grepl("\"description\" : \"MemorySink\"", capture.output(lastProgress(q)))))
  expect_true(any(grepl("\"isTriggerActive\" : ", capture.output(status(q)))))

  expect_equal(queryName(q), "people2")
  expect_true(isActive(q))

  stopQuery(q)
})

test_that("Stream other format", {
  parquetPath <- tempfile(pattern = "sparkr-test", fileext = ".parquet")
  df <- read.df(jsonPath, "json", schema)
  write.df(df, parquetPath, "parquet", "overwrite")

  df <- read.stream(path = parquetPath, schema = schema)
  expect_true(isStreaming(df))
  counts <- count(group_by(df, "name"))
  q <- write.stream(counts, "memory", queryName = "people3", outputMode = "complete")

  expect_false(awaitTermination(q, 5 * 1000))
  callJMethod(q@ssq, "processAllAvailable")
  expect_equal(head(sql("SELECT count(*) FROM people3"))[[1]], 3)

  expect_equal(queryName(q), "people3")
  expect_true(any(grepl("\"description\" : \"FileStreamSource[[:print:]]+parquet",
              capture.output(lastProgress(q)))))
  expect_true(isActive(q))

  stopQuery(q)
  expect_true(awaitTermination(q, 1))
  expect_false(isActive(q))

  unlink(parquetPath)
})

test_that("Non-streaming DataFrame", {
  c <- as.DataFrame(cars)
  expect_false(isStreaming(c))

  expect_error(write.stream(c, "memory", queryName = "people", outputMode = "complete"),
               paste0(".*(writeStream : analysis error - 'writeStream' can be called only on ",
                      "streaming Dataset/DataFrame).*"))
})

test_that("Unsupported operation", {
  # memory sink without aggregation
  df <- read.stream("json", path = jsonDir, schema = schema, maxFilesPerTrigger = 1)
  expect_error(write.stream(df, "memory", queryName = "people", outputMode = "complete"),
               paste0(".*(start : analysis error - Complete output mode not supported when there ",
                      "are no streaming aggregations on streaming DataFrames/Datasets).*"))
})

test_that("Terminated by error", {
  df <- read.stream("json", path = jsonDir, schema = schema, maxFilesPerTrigger = -1)
  counts <- count(group_by(df, "name"))
  # This would not fail before returning with a StreamingQuery,
  # but could dump error log at just about the same time
  expect_error(q <- write.stream(counts, "memory", queryName = "people4", outputMode = "complete"),
               NA)

  expect_error(awaitTermination(q, 5 * 1000),
               paste0(".*(awaitTermination : streaming query error - Invalid value '-1' for option",
                      " 'maxFilesPerTrigger', must be a positive integer).*"))

  expect_true(any(grepl("\"message\" : \"Terminated with exception: Invalid value",
              capture.output(status(q)))))
  expect_true(any(grepl("Streaming query has no progress", capture.output(lastProgress(q)))))
  expect_equal(queryName(q), "people4")
  expect_false(isActive(q))

  stopQuery(q)
})

unlink(jsonPath)
unlink(jsonPathNa)

sparkR.session.stop()
