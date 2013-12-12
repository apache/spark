context("partitionBy, groupByKey, reduceByKey etc.")

# JavaSparkContext handle
sc <- sparkR.init()

# Data
intPairs <- list(list(1L, -1), list(2L, 100), list(2L, 1), list(1L, 200))
intRdd <- parallelize(sc, intPairs, 2L)

doublePairs <- list(list(1.5, -1), list(2.5, 100), list(2.5, 1), list(1.5, 200))
doubleRdd <- parallelize(sc, doublePairs, 2L)

numPairs <- list(list(1L, 100), list(2L, 200), list(4L, -1), list(3L, 1),
                 list(3L, 0))
numPairsRdd <- parallelize(sc, numPairs, length(numPairs))

strList <- list("Dexter Morgan: Blood. Sometimes it sets my teeth on edge, ",
                "other times it helps me control the chaos.",
                "Dexter Morgan: Harry and Dorris Morgan did a wonderful job ",
                "raising me. But they're both dead now. I didn't kill them. Honest.")
strListRRDD <- parallelize(sc, strList, 4)

test_that("groupByKey for integers", {
  grouped <- groupByKey(intRdd, 2L)

  actual <- collect(grouped)

  expected <- list(list(2L, list(100, 1)), list(1L, list(-1, 200)))
  expect_equal(actual, expected)
})

test_that("groupByKey for doubles", {
  grouped <- groupByKey(doubleRdd, 2L)

  actual <- collect(grouped)

  expected <- list(list(1.5, list(-1, 200)), list(2.5, list(100, 1)))
  expect_equal(actual, expected)
})

test_that("reduceByKey for ints", {
  reduced <- reduceByKey(intRdd, "+", 2L)

  actual <- collect(reduced)

  expected <- list(list(2L, 101), list(1L, 199))
  expect_equal(actual, expected)
})

test_that("reduceByKey for doubles", {
  reduced <- reduceByKey(doubleRdd, "+", 2L)
  actual <- collect(reduced)

  expected <- list(list(1.5, 199), list(2.5, 101))
  expect_equal(actual, expected)
})

test_that("partitionBy() partitions data correctly", {
  # Partition by magnitude
  partitionByMagnitude <- function(key) { if (key >= 3) 1 else 0 }

  resultRRDD <- partitionBy(numPairsRdd, 2L, partitionByMagnitude)

  expected_first <- list(list(1, 100), list(2, 200)) # key < 3
  expected_second <- list(list(4, -1), list(3, 1), list(3, 0)) # key >= 3
  actual_first <- collectPartition(resultRRDD, 0L)
  actual_second <- collectPartition(resultRRDD, 1L)

  expect_equal(actual_first, expected_first)
  expect_equal(actual_second, expected_second)
})

test_that("partitionBy works with dependencies", {
  kOne <- 1
  partitionByParity <- function(key) { if (key %% 2 == kOne) 7 else 4 }

  # Partition by parity
  resultRRDD <- partitionBy(numPairsRdd, numPartitions = 2L, partitionByParity)

  # keys even; 100 %% 2 == 0
  expected_first <- list(list(2, 200), list(4, -1))
  # keys odd; 3 %% 2 == 1
  expected_second <- list(list(1, 100), list(3, 1), list(3, 0))
  actual_first <- collectPartition(resultRRDD, 0L)
  actual_second <- collectPartition(resultRRDD, 1L)

  expect_equal(actual_first, expected_first)
  expect_equal(actual_second, expected_second)
})
