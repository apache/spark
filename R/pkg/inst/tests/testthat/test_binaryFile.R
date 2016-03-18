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

context("functions on binary files")

# JavaSparkContext handle
sc <- sparkR.init()

mockFile <- c("Spark is pretty.", "Spark is awesome.")

test_that("saveAsObjectFile()/objectFile() following textFile() works", {
  fileName1 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  fileName2 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName1)

  rdd <- textFile(sc, fileName1, 1)
  saveAsObjectFile(rdd, fileName2)
  rdd <- objectFile(sc, fileName2)
  expect_equal(collect(rdd), as.list(mockFile))

  unlink(fileName1)
  unlink(fileName2, recursive = TRUE)
})

test_that("saveAsObjectFile()/objectFile() works on a parallelized list", {
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")

  l <- list(1, 2, 3)
  rdd <- parallelize(sc, l, 1)
  saveAsObjectFile(rdd, fileName)
  rdd <- objectFile(sc, fileName)
  expect_equal(collect(rdd), l)

  unlink(fileName, recursive = TRUE)
})

test_that("saveAsObjectFile()/objectFile() following RDD transformations works", {
  fileName1 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  fileName2 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName1)

  rdd <- textFile(sc, fileName1)

  words <- flatMap(rdd, function(line) { strsplit(line, " ")[[1]] })
  wordCount <- lapply(words, function(word) { list(word, 1L) })

  counts <- reduceByKey(wordCount, "+", 2L)

  saveAsObjectFile(counts, fileName2)
  counts <- objectFile(sc, fileName2)

  output <- collect(counts)
  expected <- list(list("awesome.", 1), list("Spark", 2), list("pretty.", 1),
                    list("is", 2))
  expect_equal(sortKeyValueList(output), sortKeyValueList(expected))

  unlink(fileName1)
  unlink(fileName2, recursive = TRUE)
})

test_that("saveAsObjectFile()/objectFile() works with multiple paths", {
  fileName1 <- tempfile(pattern = "spark-test", fileext = ".tmp")
  fileName2 <- tempfile(pattern = "spark-test", fileext = ".tmp")

  rdd1 <- parallelize(sc, "Spark is pretty.")
  saveAsObjectFile(rdd1, fileName1)
  rdd2 <- parallelize(sc, "Spark is awesome.")
  saveAsObjectFile(rdd2, fileName2)

  rdd <- objectFile(sc, c(fileName1, fileName2))
  expect_equal(count(rdd), 2)

  unlink(fileName1, recursive = TRUE)
  unlink(fileName2, recursive = TRUE)
})
