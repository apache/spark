context("partitionBy() on pairwise RRDDs")

# JavaSparkContext handle
jsc <- sparkR.init()

# Data

numPairs <- list(list(4, -1), list(1, 100), list(3, 1), list(2, 200), list(3, 0))

# Partition functions
partitionByMagnitude <- function(key) { if (key >= 3) 1 else 0 }
partitionByParity <- function(key) { if (key %% 2 == 1) 3 else 100 }

  # FIXME: this version does not work: the worker throws
  # Error in partitionFunc(tuple[[1]]) : object 'kOne' not found

  # kOne <- 1
  # partitionByParity <- function(key) { if (key %% 2 == kOne) 3 else 7 }

# Tests

test_that("partitionBy() partitions data correctly", {
  rrdd <- parallelize(jsc, numPairs, length(numPairs), pairwise = TRUE)
  resultRRDD <- partitionBy(rrdd, 2L, partitionByMagnitude)

  expected_first <- list(list(1, 100), list(2, 200)) # key < 3
  expected_second <- list(list(4, -1), list(3, 1), list(3, 0)) # key >= 3
  actual_first <- collectPartition(resultRRDD, 0L)
  actual_second <- collectPartition(resultRRDD, 1L)

  expect_equal(actual_first, expected_first)
  expect_equal(actual_second, expected_second)


  rrdd <- parallelize(jsc, numPairs, 1L, pairwise = TRUE)
  resultRRDD <- partitionBy(rrdd, numPartitions = 2L, partitionByParity)

  # keys even; 100 %% 2 == 0
  expected_first <- list(list(4, -1), list(2, 200))
  # keys odd; 3 %% 2 == 1
  expected_second <- list(list(1, 100), list(3, 1), list(3, 0))
  actual_first <- collectPartition(resultRRDD, 0L)
  actual_second <- collectPartition(resultRRDD, 1L)

  expect_equal(actual_first, expected_first)
  expect_equal(actual_second, expected_second)

})
