library(testthat)

context("SparkSQL functions")

# Tests for jsonFile, registerTempTable, sql, count, table

sc <- sparkR.init()

sqlCtx <- sparkRSQL.init(sc)

mockLines <- c("{\"name\":\"Michael\"}",
               "{\"name\":\"Andy\", \"age\":30}",
               "{\"name\":\"Justin\", \"age\":19}")
jsonPath <- tempfile(pattern="sparkr-test", fileext=".tmp")
writeLines(mockLines, jsonPath)

test_that("jsonFile() on a local file returns a DataFrame", {
  df <- jsonFile(sqlCtx, jsonPath)
  expect_true(inherits(df, "DataFrame"))
  expect_true(count(df) == 3)
})

test_that("registerTempTable() results in a queryable table and sql() results in a new DataFrame", {
  df <- jsonFile(sqlCtx, jsonPath)
  registerTempTable(df, "table1")
  newdf <- sql(sqlCtx, "SELECT * FROM table1 where name = 'Michael'")
  expect_true(inherits(newdf, "DataFrame"))
  expect_true(count(newdf) == 1)
})

test_that("table() returns a new DataFrame", {
  tabledf <- table(sqlCtx, "table1")
  expect_true(inherits(tabledf, "DataFrame"))
  expect_true(count(tabledf) == 3)
})

test_that("toRDD() returns an RRDD", {
  df <- jsonFile(sqlCtx, jsonPath)
  testRDD <- toRDD(df)
  expect_true(inherits(testRDD, "RDD"))
  expect_true(count(testRDD) == 3)
})

test_that("union on two RDDs created from DataFrames returns an RRDD", {
  df <- jsonFile(sqlCtx, jsonPath)
  RDD1 <- toRDD(df)
  RDD2 <- toRDD(df)
  unioned <- unionRDD(RDD1, RDD2)
  expect_true(inherits(unioned, "RDD"))
  expect_true(getSerializedMode(unioned) == "byte")
  expect_true(collect(unioned)[[2]]$name == "Andy")
})

test_that("union on mixed serialization types correctly returns a byte RRDD", {
  # Byte RDD
  nums <- 1:10
  rdd <- parallelize(sc, nums, 2L)
  
  # String RDD
  textLines <- c("Michael",
                 "Andy, 30",
                 "Justin, 19")
  textPath <- tempfile(pattern="sparkr-textLines", fileext=".tmp")
  writeLines(textLines, textPath)
  textRDD <- textFile(sc, textPath)
  
  df <- jsonFile(sqlCtx, jsonPath)
  dfRDD <- toRDD(df)
  
  unionByte <- unionRDD(rdd, dfRDD)
  expect_true(inherits(unionByte, "RDD"))
  expect_true(getSerializedMode(unionByte) == "byte")
  expect_true(collect(unionByte)[[1]] == 1)
  expect_true(collect(unionByte)[[12]]$name == "Andy")
  
  unionString <- unionRDD(textRDD, dfRDD)
  expect_true(inherits(unionString, "RDD"))
  expect_true(getSerializedMode(unionString) == "byte")
  expect_true(collect(unionString)[[1]] == "Michael")
  expect_true(collect(unionString)[[5]]$name == "Andy")
})

test_that("objectFile() works with row serialization", {
  objectPath <- tempfile(pattern="spark-test", fileext=".tmp")
  df <- jsonFile(sqlCtx, jsonPath)
  dfRDD <- toRDD(df)
  saveAsObjectFile(dfRDD, objectPath)
  objectIn <- objectFile(sc, objectPath)
  
  expect_true(inherits(objectIn, "RDD"))
  expect_true(getSerializedMode(objectIn) == "byte")
  expect_true(collect(objectIn)[[2]]$age == 30)
})

test_that("lapply() on a DataFrame returns an RDD with the correct columns", {
  df <- jsonFile(sqlCtx, jsonPath)
  testRDD <- lapply(df, function(row) {
    row$newCol <- row$age + 5
    row
    })
  expect_true(inherits(testRDD, "RDD"))
  collected <- collect(testRDD)
  expect_true(collected[[1]]$name == "Michael")
  expect_true(collected[[2]]$newCol == "35")
})

test_that("collect() returns a data.frame", {
  df <- jsonFile(sqlCtx, jsonPath)
  rdf <- collect(df)
  expect_true(is.data.frame(rdf))
  expect_true(names(rdf)[1] == "age")
  expect_true(nrow(rdf) == 3)
  expect_true(ncol(rdf) == 2)
})

test_that("collect() and take() on a DataFrame return the same number of rows and columns", {
  df <- jsonFile(sqlCtx, jsonPath)
  expect_true(nrow(collect(df)) == length(take(df, 10)))
  expect_true(ncol(collect(df)) == length(take(df, 10)[[1]]))
})

test_that("multiple pipeline transformations starting with a DataFrame result in an RDD with the correct values", {
  df <- jsonFile(sqlCtx, jsonPath)
  first <- lapply(df, function(row) {
    row$age <- row$age + 5
    row
  })
  second <- lapply(first, function(row) {
    row$testCol <- if (row$age == 35 && !is.na(row$age)) TRUE else FALSE
    row
  })
  expect_true(inherits(second, "RDD"))
  expect_true(count(second) == 3)
  expect_true(collect(second)[[2]]$age == 35)
  expect_true(collect(second)[[2]]$testCol)
  expect_false(collect(second)[[3]]$testCol)
})

unlink(jsonPath)
