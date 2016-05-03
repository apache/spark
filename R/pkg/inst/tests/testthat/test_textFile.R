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

context("the textFile() function")

# JavaSparkContext handle
sc <- sparkR.init()

mockFile <- c("Spark is pretty.", "Spark is awesome.")

test_that("textFile() on a local file returns an RDD", {
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName)
  expect_is(rdd, "RDD")
  expect_true(count(rdd) > 0)
  expect_equal(count(rdd), 2)

  unlink(fileName)
})

test_that("textFile() followed by a collect() returns the same content", {
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName)
  expect_equal(collect(rdd), as.list(mockFile))

  unlink(fileName)
})

test_that("textFile() word count works as expected", {
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName)

  words <- flatMap(rdd, function(line) { strsplit(line, " ")[[1]] })
  wordCount <- lapply(words, function(word) { list(word, 1L) })

  counts <- reduceByKey(wordCount, "+", 2L)
  output <- collect(counts)
  expected <- list(list("pretty.", 1), list("is", 2), list("awesome.", 1),
                   list("Spark", 2))
  expect_equal(sortKeyValueList(output), sortKeyValueList(expected))

  unlink(fileName)
})

test_that("several transformations on RDD created by textFile()", {
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName) # RDD
  for (i in 1:10) {
    # PipelinedRDD initially created from RDD
    rdd <- lapply(rdd, function(x) paste(x, x))
  }
  collect(rdd)

  unlink(fileName)
})

test_that("textFile() followed by a saveAsTextFile() returns the same content", {
  fileName1 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  fileName2 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName1)

  rdd <- textFile(sc, fileName1, 1L)
  saveAsTextFile(rdd, fileName2)
  rdd <- textFile(sc, fileName2)
  expect_equal(collect(rdd), as.list(mockFile))

  unlink(fileName1)
  unlink(fileName2)
})

test_that("saveAsTextFile() on a parallelized list works as expected", {
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  l <- list(1, 2, 3)
  rdd <- parallelize(sc, l, 1L)
  saveAsTextFile(rdd, fileName)
  rdd <- textFile(sc, fileName)
  expect_equal(collect(rdd), lapply(l, function(x) {toString(x)}))

  unlink(fileName)
})

test_that("textFile() and saveAsTextFile() word count works as expected", {
  fileName1 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  fileName2 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName1)

  rdd <- textFile(sc, fileName1)

  words <- flatMap(rdd, function(line) { strsplit(line, " ")[[1]] })
  wordCount <- lapply(words, function(word) { list(word, 1L) })

  counts <- reduceByKey(wordCount, "+", 2L)

  saveAsTextFile(counts, fileName2)
  rdd <- textFile(sc, fileName2)

  output <- collect(rdd)
  expected <- list(list("awesome.", 1), list("Spark", 2),
                   list("pretty.", 1), list("is", 2))
  expectedStr <- lapply(expected, function(x) { toString(x) })
  expect_equal(sortKeyValueList(output), sortKeyValueList(expectedStr))

  unlink(fileName1)
  unlink(fileName2)
})

test_that("textFile() on multiple paths", {
  fileName1 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  fileName2 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines("Spark is pretty.", fileName1)
  writeLines("Spark is awesome.", fileName2)

  rdd <- textFile(sc, c(fileName1, fileName2))
  expect_equal(count(rdd), 2)

  unlink(fileName1)
  unlink(fileName2)
})

test_that("Pipelined operations on RDDs created using textFile", {
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName)

  lengths <- lapply(rdd, function(x) { length(x) })
  expect_equal(collect(lengths), list(1, 1))

  lengthsPipelined <- lapply(lengths, function(x) { x + 10 })
  expect_equal(collect(lengthsPipelined), list(11, 11))

  lengths30 <- lapply(lengthsPipelined, function(x) { x + 20 })
  expect_equal(collect(lengths30), list(31, 31))

  lengths20 <- lapply(lengths, function(x) { x + 20 })
  expect_equal(collect(lengths20), list(21, 21))

  unlink(fileName)
})
