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

  base <- c(1, 2, 3)
  l <- list(field = matrix(1))
  field <- matrix(2)
  defUse <- 3
  g <- function(x) { x + y }
  f <- function(x) {
    defUse <- base::as.integer(x) + 1  # Test for access operators `::`.
    lapply(x, g) + 1  # Test for capturing function call "g"'s closure as a argument of lapply.
    l$field[1,1] <- 3  # Test for access operators `$`.
    res <- defUse + l$field[1,]  # Test for def-use chain of "defUse", and "" symbol.
    f(res)  # Test for recursive calls.
  }
  newF <- cleanClosure(f)
  env <- environment(newF)
  expect_equal(length(ls(env)), 3)  # Only "g", "l" and "f". No "base", "field" or "defUse".
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
