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

test_that("serializeToBytes on RDD", {
  # File content
  mockFile <- c("Spark is pretty.", "Spark is awesome.")
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName)
  
  text.rdd <- textFile(sc, fileName)
  expect_true(getSerializedMode(text.rdd) == "string")
  ser.rdd <- serializeToBytes(text.rdd)
  expect_equal(collect(ser.rdd), as.list(mockFile))
  expect_true(getSerializedMode(ser.rdd) == "byte")
  
  unlink(fileName)
})

test_that("cleanClosure on R functions", {
  y <- c(1, 2, 3)
  g <- function(x) { x + 1 }
  f <- function(x) { g(x) + y }
  newF <- cleanClosure(f)
  env <- environment(newF)
  expect_equal(length(ls(env)), 2)  # y, g
  actual <- get("y", envir = env, inherits = FALSE)
  expect_equal(actual, y)
  actual <- get("g", envir = env, inherits = FALSE)
  expect_equal(actual, g)
  
  # Test for nested enclosures and package variables.
  env2 <- new.env()
  funcEnv <- new.env(parent = env2)
  f <- function(x) { log(g(x) + y) }
  environment(f) <- funcEnv  # enclosing relationship: f -> funcEnv -> env2 -> .GlobalEnv
  newF <- cleanClosure(f)
  env <- environment(newF)
  expect_equal(length(ls(env)), 2)  # "min" should not be included
  actual <- get("y", envir = env, inherits = FALSE)
  expect_equal(actual, y)
  actual <- get("g", envir = env, inherits = FALSE)
  expect_equal(actual, g)

  # Test for recursive closure capture for a free variable of a function.
  g <- function(x) { x + y }
  f <- function(x) { lapply(x, g) + 1 }
  newF <- cleanClosure(f)
  env <- environment(newF)
  expect_equal(length(ls(env)), 1)  # Only "g". "y" should be in the environemnt of g.
  expect_equal(ls(env), "g")
  newG <- get("g", envir = env, inherits = FALSE)
  env <- environment(newG)
  expect_equal(length(ls(env)), 1)
  actual <- get("y", envir = env, inherits = FALSE)
  expect_equal(actual, y)
  
  # Test for function (and variable) definitions.
  f <- function(x) {
    g <- function(y) { y * 2 }
    g(x)
  }
  newF <- cleanClosure(f)
  env <- environment(newF)
  expect_equal(length(ls(env)), 0)  # "y" and "g" should not be included.
  
  # Test for access operators `$`, `::` and `:::`.
  l <- list(a = 1)
  a <- 2
  base <- c(1, 2, 3)
  f <- function(x) {
    z <- base::as.integer(x) + 1
    l$a <- 3
    z + l$a
  }
  newF <- cleanClosure(f)
  env <- environment(newF)
  expect_equal(ls(env), "l")  # "base" and "a" should not be included.
  expect_equal(get("l", envir = env, inherits = FALSE), l)
  
  # Test for overriding variables in base namespace (Issue: SparkR-196).
  nums <- as.list(1:10)
  rdd <- parallelize(sc, nums, 2L)
  t = 4  # Override base::t in .GlobalEnv.
  f <- function(x) { x > t }
  newF <- cleanClosure(f)
  env <- environment(newF)
  expect_equal(ls(env), "t")
  expect_equal(get("t", envir = env, inherits = FALSE), t)
  actual <- collect(lapply(rdd, f))
  expected <- as.list(c(rep(FALSE, 4), rep(TRUE, 6)))
  expect_equal(actual, expected)
  
  # Test for broadcast variables.
  a <- matrix(nrow=10, ncol=10, data=rnorm(100))
  aBroadcast <- broadcast(sc, a)
  normMultiply <- function(x) { norm(aBroadcast$value) * x }
  newnormMultiply <- SparkR:::cleanClosure(normMultiply)
  env <- environment(newnormMultiply)
  expect_equal(ls(env), "aBroadcast")
  expect_equal(get("aBroadcast", envir = env, inherits = FALSE), aBroadcast)
})
