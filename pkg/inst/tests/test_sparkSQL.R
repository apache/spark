library(testthat)

context("SparkSQL functions")

# Tests for jsonFile, registerTempTable, sql, count, table

sc <- sparkR.init()

sqlCtx <- sparkRSQL.init(sc)

mockLines <- c("{\"Name\":\"Michael\"}",
               "{\"Name\":\"Andy\", \"Age\":30}",
               "{\"Name\":\"Justin\", \"Age\":19}")
jsonPath <- tempfile(pattern="sparkr-test", fileext=".tmp")
writeLines(mockLines, jsonPath)

test_that("jsonFile() on a local file returns a DataFrame", {
  df <- jsonFile(sqlCtx, jsonPath)
  expect_true(inherits(df, "DataFrame"))
  expect_true(count(df) == 3)
})

test_that("registerTempTable() results in a queryable table and sql() results in a new DataFrame", {
  registerTempTable(df, "table1")
  newdf <- sql(sqlCtx, "SELECT * FROM table1 where Name = 'Michael'")
  expect_true(inherits(newdf, "DataFrame"))
  expect_true(count(newdf) == 1)
})

test_that("table() returns a new DataFrame", {
  tabledf <- table(sqlCtx, "table1")
  expect_true(inherits(tabledf, "DataFrame"))
  expect_true(count(tabledf) == 3)
})

unlink(jsonPath)
