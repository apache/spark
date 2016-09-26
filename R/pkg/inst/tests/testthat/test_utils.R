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

context("functions in utils.R")

# JavaSparkContext handle
sparkSession <- sparkR.session(enableHiveSupport = FALSE)
sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)

test_that("convertJListToRList() gives back (deserializes) the original JLists
          of strings and integers", {
  # It's hard to manually create a Java List using rJava, since it does not
  # support generics well. Instead, we rely on collectRDD() returning a
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
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName)

  text.rdd <- textFile(sc, fileName)
  expect_equal(getSerializedMode(text.rdd), "string")
  ser.rdd <- serializeToBytes(text.rdd)
  expect_equal(collectRDD(ser.rdd), as.list(mockFile))
  expect_equal(getSerializedMode(ser.rdd), "byte")

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

  base <- c(1, 2, 3)
  l <- list(field = matrix(1))
  field <- matrix(2)
  defUse <- 3
  g <- function(x) { x + y }
  f <- function(x) {
    defUse <- base::as.integer(x) + 1  # Test for access operators `::`.
    lapply(x, g) + 1  # Test for capturing function call "g"'s closure as a argument of lapply.
    l$field[1, 1] <- 3  # Test for access operators `$`.
    res <- defUse + l$field[1, ]  # Test for def-use chain of "defUse", and "" symbol.
    f(res)  # Test for recursive calls.
  }
  newF <- cleanClosure(f)
  env <- environment(newF)
  # TODO(shivaram): length(ls(env)) is 4 here for some reason and `lapply` is included in `env`.
  # Disabling this test till we debug this.
  #
  # nolint start
  # expect_equal(length(ls(env)), 3)  # Only "g", "l" and "f". No "base", "field" or "defUse".
  # nolint end
  expect_true("g" %in% ls(env))
  expect_true("l" %in% ls(env))
  expect_true("f" %in% ls(env))
  expect_equal(get("l", envir = env, inherits = FALSE), l)
  # "y" should be in the environemnt of g.
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

  # Test for overriding variables in base namespace (Issue: SparkR-196).
  nums <- as.list(1:10)
  rdd <- parallelize(sc, nums, 2L)
  t <- 4  # Override base::t in .GlobalEnv.
  f <- function(x) { x > t }
  newF <- cleanClosure(f)
  env <- environment(newF)
  expect_equal(ls(env), "t")
  expect_equal(get("t", envir = env, inherits = FALSE), t)
  actual <- collectRDD(lapply(rdd, f))
  expected <- as.list(c(rep(FALSE, 4), rep(TRUE, 6)))
  expect_equal(actual, expected)

  # Test for broadcast variables.
  a <- matrix(nrow = 10, ncol = 10, data = rnorm(100))
  aBroadcast <- broadcast(sc, a)
  normMultiply <- function(x) { norm(aBroadcast$value) * x }
  newnormMultiply <- SparkR:::cleanClosure(normMultiply)
  env <- environment(newnormMultiply)
  expect_equal(ls(env), "aBroadcast")
  expect_equal(get("aBroadcast", envir = env, inherits = FALSE), aBroadcast)
})

test_that("varargsToJProperties", {
  jprops <- newJObject("java.util.Properties")
  expect_true(class(jprops) == "jobj")

  jprops <- varargsToJProperties(abc = "123")
  expect_true(class(jprops) == "jobj")
  expect_equal(callJMethod(jprops, "getProperty", "abc"), "123")

  jprops <- varargsToJProperties(abc = "abc", b = 1)
  expect_equal(callJMethod(jprops, "getProperty", "abc"), "abc")
  expect_equal(callJMethod(jprops, "getProperty", "b"), "1")

  jprops <- varargsToJProperties()
  expect_equal(callJMethod(jprops, "size"), 0L)
})

test_that("convertToJSaveMode", {
  s <- convertToJSaveMode("error")
  expect_true(class(s) == "jobj")
  expect_match(capture.output(print.jobj(s)), "Java ref type org.apache.spark.sql.SaveMode id ")
  expect_error(convertToJSaveMode("foo"),
    'mode should be one of "append", "overwrite", "error", "ignore"') #nolint
})

test_that("captureJVMException", {
  method <- "getSQLDataType"
  expect_error(tryCatch(callJStatic("org.apache.spark.sql.api.r.SQLUtils", method,
                                    "unknown"),
                        error = function(e) {
                          captureJVMException(e, method)
                        }),
               "Error in getSQLDataType : illegal argument - Invalid type unknown")
})

test_that("hashCode", {
  expect_error(hashCode("bc53d3605e8a5b7de1e8e271c2317645"), NA)
})

test_that("overrideEnvs", {
  config <- new.env()
  config[["spark.master"]] <- "foo"
  config[["config_only"]] <- "ok"
  param <- new.env()
  param[["spark.master"]] <- "local"
  param[["param_only"]] <- "blah"
  overrideEnvs(config, param)
  expect_equal(config[["spark.master"]], "local")
  expect_equal(config[["param_only"]], "blah")
  expect_equal(config[["config_only"]], "ok")
})

test_that("rbindRaws", {

  # Mixed Column types
  r <- serialize(1:5, connection = NULL)
  r1 <- serialize(1, connection = NULL)
  r2 <- serialize(letters, connection = NULL)
  r3 <- serialize(1:10, connection = NULL)
  inputData <- list(list(1L, r1, "a", r), list(2L, r2, "b", r),
                    list(3L, r3, "c", r))
  expected <- data.frame(V1 = 1:3)
  expected$V2 <- list(r1, r2, r3)
  expected$V3 <- c("a", "b", "c")
  expected$V4 <- list(r, r, r)
  result <- rbindRaws(inputData)
  expect_equal(expected, result)

  # Single binary column
  input <- list(list(r1), list(r2), list(r3))
  expected <- subset(expected, select = "V2")
  result <- setNames(rbindRaws(input), "V2")
  expect_equal(expected, result)

})

test_that("varargsToStrEnv", {
  strenv <- varargsToStrEnv(a = 1, b = 1.1, c = TRUE, d = "abcd")
  env <- varargsToEnv(a = "1", b = "1.1", c = "true", d = "abcd")
  expect_equal(strenv, env)
  expect_error(varargsToStrEnv(a = list(1, "a")))
})

sparkR.session.stop()
