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

sparkSession <- sparkR.session()

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
