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

context("basic RDD functions")

# JavaSparkContext handle
sparkSession <- sparkR.session(enableHiveSupport = FALSE)
sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)

# Data
nums <- 1:10
rdd <- parallelize(sc, nums, 2L)

intPairs <- list(list(1L, -1), list(2L, 100), list(2L, 1), list(1L, 200))
intRdd <- parallelize(sc, intPairs, 2L)

test_that("get number of partitions in RDD", {
  expect_equal(getNumPartitions(rdd), 2)
  expect_equal(getNumPartitions(intRdd), 2)
})

test_that("first on RDD", {
  expect_equal(firstRDD(rdd), 1)
  newrdd <- lapply(rdd, function(x) x + 1)
  expect_equal(firstRDD(newrdd), 2)
})

test_that("count and length on RDD", {
   expect_equal(countRDD(rdd), 10)
   expect_equal(lengthRDD(rdd), 10)
})

test_that("count by values and keys", {
  mods <- lapply(rdd, function(x) { x %% 3 })
  actual <- countByValue(mods)
  expected <- list(list(0, 3L), list(1, 4L), list(2, 3L))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))

  actual <- countByKey(intRdd)
  expected <- list(list(2L, 2L), list(1L, 2L))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("lapply on RDD", {
  multiples <- lapply(rdd, function(x) { 2 * x })
  actual <- collectRDD(multiples)
  expect_equal(actual, as.list(nums * 2))
})

test_that("lapplyPartition on RDD", {
  sums <- lapplyPartition(rdd, function(part) { sum(unlist(part)) })
  actual <- collectRDD(sums)
  expect_equal(actual, list(15, 40))
})

test_that("mapPartitions on RDD", {
  sums <- mapPartitions(rdd, function(part) { sum(unlist(part)) })
  actual <- collectRDD(sums)
  expect_equal(actual, list(15, 40))
})

test_that("flatMap() on RDDs", {
  flat <- flatMap(intRdd, function(x) { list(x, x) })
  actual <- collectRDD(flat)
  expect_equal(actual, rep(intPairs, each = 2))
})

test_that("filterRDD on RDD", {
  filtered.rdd <- filterRDD(rdd, function(x) { x %% 2 == 0 })
  actual <- collectRDD(filtered.rdd)
  expect_equal(actual, list(2, 4, 6, 8, 10))

  filtered.rdd <- Filter(function(x) { x[[2]] < 0 }, intRdd)
  actual <- collectRDD(filtered.rdd)
  expect_equal(actual, list(list(1L, -1)))

  # Filter out all elements.
  filtered.rdd <- filterRDD(rdd, function(x) { x > 10 })
  actual <- collectRDD(filtered.rdd)
  expect_equal(actual, list())
})

test_that("lookup on RDD", {
  vals <- lookup(intRdd, 1L)
  expect_equal(vals, list(-1, 200))

  vals <- lookup(intRdd, 3L)
  expect_equal(vals, list())
})

test_that("several transformations on RDD (a benchmark on PipelinedRDD)", {
  rdd2 <- rdd
  for (i in 1:12)
    rdd2 <- lapplyPartitionsWithIndex(
              rdd2, function(partIndex, part) {
                part <- as.list(unlist(part) * partIndex + i)
              })
  rdd2 <- lapply(rdd2, function(x) x + x)
  actual <- collectRDD(rdd2)
  expected <- list(24, 24, 24, 24, 24,
                   168, 170, 172, 174, 176)
  expect_equal(actual, expected)
})

test_that("PipelinedRDD support actions: cache(), persist(), unpersist(), checkpoint()", {
  # RDD
  rdd2 <- rdd
  # PipelinedRDD
  rdd2 <- lapplyPartitionsWithIndex(
            rdd2,
            function(partIndex, part) {
              part <- as.list(unlist(part) * partIndex)
            })

  cacheRDD(rdd2)
  expect_true(rdd2@env$isCached)
  rdd2 <- lapply(rdd2, function(x) x)
  expect_false(rdd2@env$isCached)

  unpersistRDD(rdd2)
  expect_false(rdd2@env$isCached)

  persistRDD(rdd2, "MEMORY_AND_DISK")
  expect_true(rdd2@env$isCached)
  rdd2 <- lapply(rdd2, function(x) x)
  expect_false(rdd2@env$isCached)

  unpersistRDD(rdd2)
  expect_false(rdd2@env$isCached)

  tempDir <- tempfile(pattern = "checkpoint")
  setCheckpointDir(sc, tempDir)
  checkpoint(rdd2)
  expect_true(rdd2@env$isCheckpointed)

  rdd2 <- lapply(rdd2, function(x) x)
  expect_false(rdd2@env$isCached)
  expect_false(rdd2@env$isCheckpointed)

  # make sure the data is collectable
  collectRDD(rdd2)

  unlink(tempDir)
})

test_that("reduce on RDD", {
  sum <- reduce(rdd, "+")
  expect_equal(sum, 55)

  # Also test with an inline function
  sumInline <- reduce(rdd, function(x, y) { x + y })
  expect_equal(sumInline, 55)
})

test_that("lapply with dependency", {
  fa <- 5
  multiples <- lapply(rdd, function(x) { fa * x })
  actual <- collectRDD(multiples)

  expect_equal(actual, as.list(nums * 5))
})

test_that("lapplyPartitionsWithIndex on RDDs", {
  func <- function(partIndex, part) { list(partIndex, Reduce("+", part)) }
  actual <- collectRDD(lapplyPartitionsWithIndex(rdd, func), flatten = FALSE)
  expect_equal(actual, list(list(0, 15), list(1, 40)))

  pairsRDD <- parallelize(sc, list(list(1, 2), list(3, 4), list(4, 8)), 1L)
  partitionByParity <- function(key) { if (key %% 2 == 1) 0 else 1 }
  mkTup <- function(partIndex, part) { list(partIndex, part) }
  actual <- collectRDD(lapplyPartitionsWithIndex(
                      partitionByRDD(pairsRDD, 2L, partitionByParity),
                      mkTup),
                    FALSE)
  expect_equal(actual, list(list(0, list(list(1, 2), list(3, 4))),
                            list(1, list(list(4, 8)))))
})

test_that("sampleRDD() on RDDs", {
  expect_equal(unlist(collectRDD(sampleRDD(rdd, FALSE, 1.0, 2014L))), nums)
})

test_that("takeSample() on RDDs", {
  # ported from RDDSuite.scala, modified seeds
  data <- parallelize(sc, 1:100, 2L)
  for (seed in 4:5) {
    s <- takeSample(data, FALSE, 20L, seed)
    expect_equal(length(s), 20L)
    expect_equal(length(unique(s)), 20L)
    for (elem in s) {
      expect_true(elem >= 1 && elem <= 100)
    }
  }
  for (seed in 4:5) {
    s <- takeSample(data, FALSE, 200L, seed)
    expect_equal(length(s), 100L)
    expect_equal(length(unique(s)), 100L)
    for (elem in s) {
      expect_true(elem >= 1 && elem <= 100)
    }
  }
  for (seed in 4:5) {
    s <- takeSample(data, TRUE, 20L, seed)
    expect_equal(length(s), 20L)
    for (elem in s) {
      expect_true(elem >= 1 && elem <= 100)
    }
  }
  for (seed in 4:5) {
    s <- takeSample(data, TRUE, 100L, seed)
    expect_equal(length(s), 100L)
    # Chance of getting all distinct elements is astronomically low, so test we
    # got less than 100
    expect_true(length(unique(s)) < 100L)
  }
  for (seed in 4:5) {
    s <- takeSample(data, TRUE, 200L, seed)
    expect_equal(length(s), 200L)
    # Chance of getting all distinct elements is still quite low, so test we
    # got less than 100
    expect_true(length(unique(s)) < 100L)
  }
})

test_that("mapValues() on pairwise RDDs", {
  multiples <- mapValues(intRdd, function(x) { x * 2 })
  actual <- collectRDD(multiples)
  expected <- lapply(intPairs, function(x) {
    list(x[[1]], x[[2]] * 2)
  })
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("flatMapValues() on pairwise RDDs", {
  l <- parallelize(sc, list(list(1, c(1, 2)), list(2, c(3, 4))))
  actual <- collectRDD(flatMapValues(l, function(x) { x }))
  expect_equal(actual, list(list(1, 1), list(1, 2), list(2, 3), list(2, 4)))

  # Generate x to x+1 for every value
  actual <- collectRDD(flatMapValues(intRdd, function(x) { x: (x + 1) }))
  expect_equal(actual,
               list(list(1L, -1), list(1L, 0), list(2L, 100), list(2L, 101),
                    list(2L, 1), list(2L, 2), list(1L, 200), list(1L, 201)))
})

test_that("reduceByKeyLocally() on PairwiseRDDs", {
  pairs <- parallelize(sc, list(list(1, 2), list(1.1, 3), list(1, 4)), 2L)
  actual <- reduceByKeyLocally(pairs, "+")
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list(1, 6), list(1.1, 3))))

  pairs <- parallelize(sc, list(list("abc", 1.2), list(1.1, 0), list("abc", 1.3),
                                list("bb", 5)), 4L)
  actual <- reduceByKeyLocally(pairs, "+")
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list("abc", 2.5), list(1.1, 0), list("bb", 5))))
})

test_that("distinct() on RDDs", {
  nums.rep2 <- rep(1:10, 2)
  rdd.rep2 <- parallelize(sc, nums.rep2, 2L)
  uniques <- distinctRDD(rdd.rep2)
  actual <- sort(unlist(collectRDD(uniques)))
  expect_equal(actual, nums)
})

test_that("maximum() on RDDs", {
  max <- maximum(rdd)
  expect_equal(max, 10)
})

test_that("minimum() on RDDs", {
  min <- minimum(rdd)
  expect_equal(min, 1)
})

test_that("sumRDD() on RDDs", {
  sum <- sumRDD(rdd)
  expect_equal(sum, 55)
})

test_that("keyBy on RDDs", {
  func <- function(x) { x * x }
  keys <- keyBy(rdd, func)
  actual <- collectRDD(keys)
  expect_equal(actual, lapply(nums, function(x) { list(func(x), x) }))
})

test_that("repartition/coalesce on RDDs", {
  rdd <- parallelize(sc, 1:20, 4L) # each partition contains 5 elements

  # repartition
  r1 <- repartitionRDD(rdd, 2)
  expect_equal(getNumPartitions(r1), 2L)
  count <- length(collectPartition(r1, 0L))
  expect_true(count >= 8 && count <= 12)

  r2 <- repartitionRDD(rdd, 6)
  expect_equal(getNumPartitions(r2), 6L)
  count <- length(collectPartition(r2, 0L))
  expect_true(count >= 0 && count <= 4)

  # coalesce
  r3 <- coalesce(rdd, 1)
  expect_equal(getNumPartitions(r3), 1L)
  count <- length(collectPartition(r3, 0L))
  expect_equal(count, 20)
})

test_that("sortBy() on RDDs", {
  sortedRdd <- sortBy(rdd, function(x) { x * x }, ascending = FALSE)
  actual <- collectRDD(sortedRdd)
  expect_equal(actual, as.list(sort(nums, decreasing = TRUE)))

  rdd2 <- parallelize(sc, sort(nums, decreasing = TRUE), 2L)
  sortedRdd2 <- sortBy(rdd2, function(x) { x * x })
  actual <- collectRDD(sortedRdd2)
  expect_equal(actual, as.list(nums))
})

test_that("takeOrdered() on RDDs", {
  l <- list(10, 1, 2, 9, 3, 4, 5, 6, 7)
  rdd <- parallelize(sc, l)
  actual <- takeOrdered(rdd, 6L)
  expect_equal(actual, as.list(sort(unlist(l)))[1:6])

  l <- list("e", "d", "c", "d", "a")
  rdd <- parallelize(sc, l)
  actual <- takeOrdered(rdd, 3L)
  expect_equal(actual, as.list(sort(unlist(l)))[1:3])
})

test_that("top() on RDDs", {
  l <- list(10, 1, 2, 9, 3, 4, 5, 6, 7)
  rdd <- parallelize(sc, l)
  actual <- top(rdd, 6L)
  expect_equal(actual, as.list(sort(unlist(l), decreasing = TRUE))[1:6])

  l <- list("e", "d", "c", "d", "a")
  rdd <- parallelize(sc, l)
  actual <- top(rdd, 3L)
  expect_equal(actual, as.list(sort(unlist(l), decreasing = TRUE))[1:3])
})

test_that("fold() on RDDs", {
  actual <- fold(rdd, 0, "+")
  expect_equal(actual, Reduce("+", nums, 0))

  rdd <- parallelize(sc, list())
  actual <- fold(rdd, 0, "+")
  expect_equal(actual, 0)
})

test_that("aggregateRDD() on RDDs", {
  rdd <- parallelize(sc, list(1, 2, 3, 4))
  zeroValue <- list(0, 0)
  seqOp <- function(x, y) { list(x[[1]] + y, x[[2]] + 1) }
  combOp <- function(x, y) { list(x[[1]] + y[[1]], x[[2]] + y[[2]]) }
  actual <- aggregateRDD(rdd, zeroValue, seqOp, combOp)
  expect_equal(actual, list(10, 4))

  rdd <- parallelize(sc, list())
  actual <- aggregateRDD(rdd, zeroValue, seqOp, combOp)
  expect_equal(actual, list(0, 0))
})

test_that("zipWithUniqueId() on RDDs", {
  rdd <- parallelize(sc, list("a", "b", "c", "d", "e"), 3L)
  actual <- collectRDD(zipWithUniqueId(rdd))
  expected <- list(list("a", 0), list("b", 3), list("c", 1),
                   list("d", 4), list("e", 2))
  expect_equal(actual, expected)

  rdd <- parallelize(sc, list("a", "b", "c", "d", "e"), 1L)
  actual <- collectRDD(zipWithUniqueId(rdd))
  expected <- list(list("a", 0), list("b", 1), list("c", 2),
                   list("d", 3), list("e", 4))
  expect_equal(actual, expected)
})

test_that("zipWithIndex() on RDDs", {
  rdd <- parallelize(sc, list("a", "b", "c", "d", "e"), 3L)
  actual <- collectRDD(zipWithIndex(rdd))
  expected <- list(list("a", 0), list("b", 1), list("c", 2),
                   list("d", 3), list("e", 4))
  expect_equal(actual, expected)

  rdd <- parallelize(sc, list("a", "b", "c", "d", "e"), 1L)
  actual <- collectRDD(zipWithIndex(rdd))
  expected <- list(list("a", 0), list("b", 1), list("c", 2),
                   list("d", 3), list("e", 4))
  expect_equal(actual, expected)
})

test_that("glom() on RDD", {
  rdd <- parallelize(sc, as.list(1:4), 2L)
  actual <- collectRDD(glom(rdd))
  expect_equal(actual, list(list(1, 2), list(3, 4)))
})

test_that("keys() on RDDs", {
  keys <- keys(intRdd)
  actual <- collectRDD(keys)
  expect_equal(actual, lapply(intPairs, function(x) { x[[1]] }))
})

test_that("values() on RDDs", {
  values <- values(intRdd)
  actual <- collectRDD(values)
  expect_equal(actual, lapply(intPairs, function(x) { x[[2]] }))
})

test_that("pipeRDD() on RDDs", {
  actual <- collectRDD(pipeRDD(rdd, "more"))
  expected <- as.list(as.character(1:10))
  expect_equal(actual, expected)

  trailed.rdd <- parallelize(sc, c("1", "", "2\n", "3\n\r\n"))
  actual <- collectRDD(pipeRDD(trailed.rdd, "sort"))
  expected <- list("", "1", "2", "3")
  expect_equal(actual, expected)

  rev.nums <- 9:0
  rev.rdd <- parallelize(sc, rev.nums, 2L)
  actual <- collectRDD(pipeRDD(rev.rdd, "sort"))
  expected <- as.list(as.character(c(5:9, 0:4)))
  expect_equal(actual, expected)
})

test_that("zipRDD() on RDDs", {
  rdd1 <- parallelize(sc, 0:4, 2)
  rdd2 <- parallelize(sc, 1000:1004, 2)
  actual <- collectRDD(zipRDD(rdd1, rdd2))
  expect_equal(actual,
               list(list(0, 1000), list(1, 1001), list(2, 1002), list(3, 1003), list(4, 1004)))

  mockFile <- c("Spark is pretty.", "Spark is awesome.")
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName, 1)
  actual <- collectRDD(zipRDD(rdd, rdd))
  expected <- lapply(mockFile, function(x) { list(x, x) })
  expect_equal(actual, expected)

  rdd1 <- parallelize(sc, 0:1, 1)
  actual <- collectRDD(zipRDD(rdd1, rdd))
  expected <- lapply(0:1, function(x) { list(x, mockFile[x + 1]) })
  expect_equal(actual, expected)

  rdd1 <- map(rdd, function(x) { x })
  actual <- collectRDD(zipRDD(rdd, rdd1))
  expected <- lapply(mockFile, function(x) { list(x, x) })
  expect_equal(actual, expected)

  unlink(fileName)
})

test_that("cartesian() on RDDs", {
  rdd <- parallelize(sc, 1:3)
  actual <- collectRDD(cartesian(rdd, rdd))
  expect_equal(sortKeyValueList(actual),
               list(
                 list(1, 1), list(1, 2), list(1, 3),
                 list(2, 1), list(2, 2), list(2, 3),
                 list(3, 1), list(3, 2), list(3, 3)))

  # test case where one RDD is empty
  emptyRdd <- parallelize(sc, list())
  actual <- collectRDD(cartesian(rdd, emptyRdd))
  expect_equal(actual, list())

  mockFile <- c("Spark is pretty.", "Spark is awesome.")
  fileName <- tempfile(pattern = "spark-test", fileext = ".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName)
  actual <- collectRDD(cartesian(rdd, rdd))
  expected <- list(
    list("Spark is awesome.", "Spark is pretty."),
    list("Spark is awesome.", "Spark is awesome."),
    list("Spark is pretty.", "Spark is pretty."),
    list("Spark is pretty.", "Spark is awesome."))
  expect_equal(sortKeyValueList(actual), expected)

  rdd1 <- parallelize(sc, 0:1)
  actual <- collectRDD(cartesian(rdd1, rdd))
  expect_equal(sortKeyValueList(actual),
               list(
                 list(0, "Spark is pretty."),
                 list(0, "Spark is awesome."),
                 list(1, "Spark is pretty."),
                 list(1, "Spark is awesome.")))

  rdd1 <- map(rdd, function(x) { x })
  actual <- collectRDD(cartesian(rdd, rdd1))
  expect_equal(sortKeyValueList(actual), expected)

  unlink(fileName)
})

test_that("subtract() on RDDs", {
  l <- list(1, 1, 2, 2, 3, 4)
  rdd1 <- parallelize(sc, l)

  # subtract by itself
  actual <- collectRDD(subtract(rdd1, rdd1))
  expect_equal(actual, list())

  # subtract by an empty RDD
  rdd2 <- parallelize(sc, list())
  actual <- collectRDD(subtract(rdd1, rdd2))
  expect_equal(as.list(sort(as.vector(actual, mode = "integer"))),
               l)

  rdd2 <- parallelize(sc, list(2, 4))
  actual <- collectRDD(subtract(rdd1, rdd2))
  expect_equal(as.list(sort(as.vector(actual, mode = "integer"))),
               list(1, 1, 3))

  l <- list("a", "a", "b", "b", "c", "d")
  rdd1 <- parallelize(sc, l)
  rdd2 <- parallelize(sc, list("b", "d"))
  actual <- collectRDD(subtract(rdd1, rdd2))
  expect_equal(as.list(sort(as.vector(actual, mode = "character"))),
               list("a", "a", "c"))
})

test_that("subtractByKey() on pairwise RDDs", {
  l <- list(list("a", 1), list("b", 4),
            list("b", 5), list("a", 2))
  rdd1 <- parallelize(sc, l)

  # subtractByKey by itself
  actual <- collectRDD(subtractByKey(rdd1, rdd1))
  expect_equal(actual, list())

  # subtractByKey by an empty RDD
  rdd2 <- parallelize(sc, list())
  actual <- collectRDD(subtractByKey(rdd1, rdd2))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(l))

  rdd2 <- parallelize(sc, list(list("a", 3), list("c", 1)))
  actual <- collectRDD(subtractByKey(rdd1, rdd2))
  expect_equal(actual,
               list(list("b", 4), list("b", 5)))

  l <- list(list(1, 1), list(2, 4),
            list(2, 5), list(1, 2))
  rdd1 <- parallelize(sc, l)
  rdd2 <- parallelize(sc, list(list(1, 3), list(3, 1)))
  actual <- collectRDD(subtractByKey(rdd1, rdd2))
  expect_equal(actual,
               list(list(2, 4), list(2, 5)))
})

test_that("intersection() on RDDs", {
  # intersection with self
  actual <- collectRDD(intersection(rdd, rdd))
  expect_equal(sort(as.integer(actual)), nums)

  # intersection with an empty RDD
  emptyRdd <- parallelize(sc, list())
  actual <- collectRDD(intersection(rdd, emptyRdd))
  expect_equal(actual, list())

  rdd1 <- parallelize(sc, list(1, 10, 2, 3, 4, 5))
  rdd2 <- parallelize(sc, list(1, 6, 2, 3, 7, 8))
  actual <- collectRDD(intersection(rdd1, rdd2))
  expect_equal(sort(as.integer(actual)), 1:3)
})

test_that("join() on pairwise RDDs", {
  rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
  rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
  actual <- collectRDD(joinRDD(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list(1, list(1, 2)), list(1, list(1, 3)))))

  rdd1 <- parallelize(sc, list(list("a", 1), list("b", 4)))
  rdd2 <- parallelize(sc, list(list("a", 2), list("a", 3)))
  actual <- collectRDD(joinRDD(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list("a", list(1, 2)), list("a", list(1, 3)))))

  rdd1 <- parallelize(sc, list(list(1, 1), list(2, 2)))
  rdd2 <- parallelize(sc, list(list(3, 3), list(4, 4)))
  actual <- collectRDD(joinRDD(rdd1, rdd2, 2L))
  expect_equal(actual, list())

  rdd1 <- parallelize(sc, list(list("a", 1), list("b", 2)))
  rdd2 <- parallelize(sc, list(list("c", 3), list("d", 4)))
  actual <- collectRDD(joinRDD(rdd1, rdd2, 2L))
  expect_equal(actual, list())
})

test_that("leftOuterJoin() on pairwise RDDs", {
  rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
  rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
  actual <- collectRDD(leftOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list(1, list(1, 2)), list(1, list(1, 3)), list(2, list(4, NULL)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list("a", 1), list("b", 4)))
  rdd2 <- parallelize(sc, list(list("a", 2), list("a", 3)))
  actual <- collectRDD(leftOuterJoin(rdd1, rdd2, 2L))
  expected <-  list(list("b", list(4, NULL)), list("a", list(1, 2)), list("a", list(1, 3)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list(1, 1), list(2, 2)))
  rdd2 <- parallelize(sc, list(list(3, 3), list(4, 4)))
  actual <- collectRDD(leftOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list(1, list(1, NULL)), list(2, list(2, NULL)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list("a", 1), list("b", 2)))
  rdd2 <- parallelize(sc, list(list("c", 3), list("d", 4)))
  actual <- collectRDD(leftOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list("b", list(2, NULL)), list("a", list(1, NULL)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))
})

test_that("rightOuterJoin() on pairwise RDDs", {
  rdd1 <- parallelize(sc, list(list(1, 2), list(1, 3)))
  rdd2 <- parallelize(sc, list(list(1, 1), list(2, 4)))
  actual <- collectRDD(rightOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list(1, list(2, 1)), list(1, list(3, 1)), list(2, list(NULL, 4)))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list("a", 2), list("a", 3)))
  rdd2 <- parallelize(sc, list(list("a", 1), list("b", 4)))
  actual <- collectRDD(rightOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list("b", list(NULL, 4)), list("a", list(2, 1)), list("a", list(3, 1)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list(1, 1), list(2, 2)))
  rdd2 <- parallelize(sc, list(list(3, 3), list(4, 4)))
  actual <- collectRDD(rightOuterJoin(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list(3, list(NULL, 3)), list(4, list(NULL, 4)))))

  rdd1 <- parallelize(sc, list(list("a", 1), list("b", 2)))
  rdd2 <- parallelize(sc, list(list("c", 3), list("d", 4)))
  actual <- collectRDD(rightOuterJoin(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list("d", list(NULL, 4)), list("c", list(NULL, 3)))))
})

test_that("fullOuterJoin() on pairwise RDDs", {
  rdd1 <- parallelize(sc, list(list(1, 2), list(1, 3), list(3, 3)))
  rdd2 <- parallelize(sc, list(list(1, 1), list(2, 4)))
  actual <- collectRDD(fullOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list(1, list(2, 1)), list(1, list(3, 1)),
                   list(2, list(NULL, 4)), list(3, list(3, NULL)))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list("a", 2), list("a", 3), list("c", 1)))
  rdd2 <- parallelize(sc, list(list("a", 1), list("b", 4)))
  actual <- collectRDD(fullOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list("b", list(NULL, 4)), list("a", list(2, 1)),
                   list("a", list(3, 1)), list("c", list(1, NULL)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list(1, 1), list(2, 2)))
  rdd2 <- parallelize(sc, list(list(3, 3), list(4, 4)))
  actual <- collectRDD(fullOuterJoin(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list(1, list(1, NULL)), list(2, list(2, NULL)),
                                     list(3, list(NULL, 3)), list(4, list(NULL, 4)))))

  rdd1 <- parallelize(sc, list(list("a", 1), list("b", 2)))
  rdd2 <- parallelize(sc, list(list("c", 3), list("d", 4)))
  actual <- collectRDD(fullOuterJoin(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list("a", list(1, NULL)), list("b", list(2, NULL)),
                                     list("d", list(NULL, 4)), list("c", list(NULL, 3)))))
})

test_that("sortByKey() on pairwise RDDs", {
  numPairsRdd <- map(rdd, function(x) { list (x, x) })
  sortedRdd <- sortByKey(numPairsRdd, ascending = FALSE)
  actual <- collectRDD(sortedRdd)
  numPairs <- lapply(nums, function(x) { list (x, x) })
  expect_equal(actual, sortKeyValueList(numPairs, decreasing = TRUE))

  rdd2 <- parallelize(sc, sort(nums, decreasing = TRUE), 2L)
  numPairsRdd2 <- map(rdd2, function(x) { list (x, x) })
  sortedRdd2 <- sortByKey(numPairsRdd2)
  actual <- collectRDD(sortedRdd2)
  expect_equal(actual, numPairs)

  # sort by string keys
  l <- list(list("a", 1), list("b", 2), list("1", 3), list("d", 4), list("2", 5))
  rdd3 <- parallelize(sc, l, 2L)
  sortedRdd3 <- sortByKey(rdd3)
  actual <- collectRDD(sortedRdd3)
  expect_equal(actual, list(list("1", 3), list("2", 5), list("a", 1), list("b", 2), list("d", 4)))

  # test on the boundary cases

  # boundary case 1: the RDD to be sorted has only 1 partition
  rdd4 <- parallelize(sc, l, 1L)
  sortedRdd4 <- sortByKey(rdd4)
  actual <- collectRDD(sortedRdd4)
  expect_equal(actual, list(list("1", 3), list("2", 5), list("a", 1), list("b", 2), list("d", 4)))

  # boundary case 2: the sorted RDD has only 1 partition
  rdd5 <- parallelize(sc, l, 2L)
  sortedRdd5 <- sortByKey(rdd5, numPartitions = 1L)
  actual <- collectRDD(sortedRdd5)
  expect_equal(actual, list(list("1", 3), list("2", 5), list("a", 1), list("b", 2), list("d", 4)))

  # boundary case 3: the RDD to be sorted has only 1 element
  l2 <- list(list("a", 1))
  rdd6 <- parallelize(sc, l2, 2L)
  sortedRdd6 <- sortByKey(rdd6)
  actual <- collectRDD(sortedRdd6)
  expect_equal(actual, l2)

  # boundary case 4: the RDD to be sorted has 0 element
  l3 <- list()
  rdd7 <- parallelize(sc, l3, 2L)
  sortedRdd7 <- sortByKey(rdd7)
  actual <- collectRDD(sortedRdd7)
  expect_equal(actual, l3)
})

test_that("collectAsMap() on a pairwise RDD", {
  rdd <- parallelize(sc, list(list(1, 2), list(3, 4)))
  vals <- collectAsMap(rdd)
  expect_equal(vals, list(`1` = 2, `3` = 4))

  rdd <- parallelize(sc, list(list("a", 1), list("b", 2)))
  vals <- collectAsMap(rdd)
  expect_equal(vals, list(a = 1, b = 2))

  rdd <- parallelize(sc, list(list(1.1, 2.2), list(1.2, 2.4)))
  vals <- collectAsMap(rdd)
  expect_equal(vals, list(`1.1` = 2.2, `1.2` = 2.4))

  rdd <- parallelize(sc, list(list(1, "a"), list(2, "b")))
  vals <- collectAsMap(rdd)
  expect_equal(vals, list(`1` = "a", `2` = "b"))
})

test_that("show()", {
  rdd <- parallelize(sc, list(1:10))
  expect_output(showRDD(rdd), "ParallelCollectionRDD\\[\\d+\\] at parallelize at RRDD\\.scala:\\d+")
})

test_that("sampleByKey() on pairwise RDDs", {
  rdd <- parallelize(sc, 1:2000)
  pairsRDD <- lapply(rdd, function(x) { if (x %% 2 == 0) list("a", x) else list("b", x) })
  fractions <- list(a = 0.2, b = 0.1)
  sample <- sampleByKey(pairsRDD, FALSE, fractions, 1618L)
  expect_equal(100 < length(lookup(sample, "a")) && 300 > length(lookup(sample, "a")), TRUE)
  expect_equal(50 < length(lookup(sample, "b")) && 150 > length(lookup(sample, "b")), TRUE)
  expect_equal(lookup(sample, "a")[which.min(lookup(sample, "a"))] >= 0, TRUE)
  expect_equal(lookup(sample, "a")[which.max(lookup(sample, "a"))] <= 2000, TRUE)
  expect_equal(lookup(sample, "b")[which.min(lookup(sample, "b"))] >= 0, TRUE)
  expect_equal(lookup(sample, "b")[which.max(lookup(sample, "b"))] <= 2000, TRUE)

  rdd <- parallelize(sc, 1:2000)
  pairsRDD <- lapply(rdd, function(x) { if (x %% 2 == 0) list(2, x) else list(3, x) })
  fractions <- list(`2` = 0.2, `3` = 0.1)
  sample <- sampleByKey(pairsRDD, TRUE, fractions, 1618L)
  expect_equal(100 < length(lookup(sample, 2)) && 300 > length(lookup(sample, 2)), TRUE)
  expect_equal(50 < length(lookup(sample, 3)) && 150 > length(lookup(sample, 3)), TRUE)
  expect_equal(lookup(sample, 2)[which.min(lookup(sample, 2))] >= 0, TRUE)
  expect_equal(lookup(sample, 2)[which.max(lookup(sample, 2))] <= 2000, TRUE)
  expect_equal(lookup(sample, 3)[which.min(lookup(sample, 3))] >= 0, TRUE)
  expect_equal(lookup(sample, 3)[which.max(lookup(sample, 3))] <= 2000, TRUE)
})

test_that("Test correct concurrency of RRDD.compute()", {
  rdd <- parallelize(sc, 1:1000, 100)
  jrdd <- getJRDD(lapply(rdd, function(x) { x }), "row")
  zrdd <- callJMethod(jrdd, "zip", jrdd)
  count <- callJMethod(zrdd, "count")
  expect_equal(count, 1000)
})

test_that("add and get file to be downloaded with Spark job on every node", {
  path <- tempfile(pattern = "hello", fileext = ".txt")
  filename <- basename(path)
  words <- "Hello World!"
  writeLines(words, path)
  addFile(sc, path)
  download_path <- sparkFiles.get(filename)
  expect_equal(readLines(download_path), words)
  unlink(path)
})

sparkR.session.stop()
