context("functions in utils.R")

# JavaSparkContext handle
sc <- sparkR.init()

test_that("convertJListToRList() gives back (deserializes) the original JLists
          of strings and integers", {
  # It's hard to manually create a Java List using rJava, since it does not
  # support generics well. Instead, we rely on collect() returning a
  # JList.
  nums <- as.list(1:10)
  rdd <- parallelize(sc, nums, 1L)
  jList <- callJMethod(rdd@jrdd, "collect")
  rList <- convertJListToRList(jList, flatten = TRUE)
  expect_equal(rList, nums)

  strs <- as.list("hello", "spark")
  rdd <- parallelize(sc, strs, 2L)
  jList <- callJMethod(rdd@jrdd, "collect")
  rList <- convertJListToRList(jList, flatten = TRUE)
  expect_equal(rList, strs)
})

test_that("reserialize on RDD", {
  # File content
  mockFile <- c("Spark is pretty.", "Spark is awesome.")
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName)
  
  text.rdd <- textFile(sc, fileName)
  expect_false(text.rdd@env$serialized)
  ser.rdd <- reserialize(text.rdd)
  expect_equal(collect(ser.rdd), as.list(mockFile))
  expect_true(ser.rdd@env$serialized)
  
  unlink(fileName)
})

test_that("cleanClosure on R functions", {
  y <- c(1, 2, 3)
  g <- function(x) { x + 1 }
  f <- function(x) { g(x) + y }
  newF <- cleanClosure(f)
  env <- environment(newF)
  expect_equal(length(ls(env)), 2)  # y, g
  actual <- get("y", envir = env)
  expect_equal(actual, y)
  actual <- get("g", envir = env)
  expect_equal(actual, g)
  
  # Check for nested enclosures and package variables.
  env2 <- new.env()
  funcEnv <- new.env(parent = env2)
  f <- function(x) { min(g(x) + y) }
  environment(f) <- funcEnv  # enclosing relationship: f -> funcEnv -> env2 -> .GlobalEnv
  newF <- SparkR:::cleanClosure(f)
  env <- environment(newF)
  expect_equal(length(ls(env)), 2)  # "min" should not be included
  actual <- get("y", envir = env)
  expect_equal(actual, y)
  actual <- get("g", envir = env)
  expect_equal(actual, g)

  g <- function(x) { x + y }
  f <- function(x) { lapply(x, g) + 1 }
  newF <- SparkR:::cleanClosure(f)
  env <- environment(newF)
  expect_equal(length(ls(env)), 1)  # Only "g", "y" should be in the environemnt of g.
  expect_equal(ls(env), "g")
  newG <- get("g", envir = env)
  env <- environment(newG)
  expect_equal(length(ls(env)), 1)
  actual <- get("y", envir = env)
  expect_equal(actual, y)
  
  # Test for function (and variable) definitions.
  f <- function(x) {
    g <- function(y) { y * 2 }
    g(x)
  }
  newF <- SparkR:::cleanClosure(f)
  env <- environment(newF)
  expect_equal(length(ls(env)), 0)  # "y" and "g" should not be included.
})
