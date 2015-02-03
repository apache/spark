library(testthat)

context("the jsonFile() function")

sc <- sparkR.init()

sqlctx <- sparkRSQL.init(sc)

jsonPath <- paste(getwd(), "/pkg/inst/tests/people.json", sep = "")

test_that("jsonFile() on a local file returns a DataFrame", {
  df <- jsonFile(sqlctx, jsonPath)
  expect_true(inherits(df, "DataFrame"))
  expect_true(count(df) > 0)
  expect_true(count(df) == 3)
})
