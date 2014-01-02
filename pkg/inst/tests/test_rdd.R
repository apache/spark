context("basic RDD functions")

# JavaSparkContext handle
sc <- sparkR.init()

# Data
nums <- 1:10
rdd <- parallelize(sc, nums, 2L)

test_that("count and length on RDD", {
   expect_equal(count(rdd), 10)
   expect_equal(length(rdd), 10)
})

test_that("lapply on RDD", {
  multiples <- lapply(rdd, function(x) { 2 * x })
  actual <- collect(multiples)
  expect_equal(actual, as.list(nums * 2))
})

test_that("lapplyPartition on RDD", {
  sums <- lapplyPartition(rdd, function(part) { sum(unlist(part)) })
  actual <- collect(sums)
  expect_equal(actual, list(15, 40))
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
  actual <- collect(multiples)

  expect_equal(actual, as.list(nums * 5))
})

test_that("lapplyPartitionsWithIndex on RDDs", {
  func <- function(splitIndex, part) { list(splitIndex, Reduce("+", part)) }
  actual <- collect(lapplyPartitionsWithIndex(rdd, func), flatten = FALSE)
  expect_equal(actual, list(list(0, 15), list(1, 40)))

  pairsRDD <- parallelize(sc, list(list(1, 2), list(3, 4), list(4, 8)), 1L)
  partitionByParity <- function(key) { if (key %% 2 == 1) 0 else 1 }
  mkTup <- function(splitIndex, part) { list(splitIndex, part) }
  actual <- collect(lapplyPartitionsWithIndex(
                      partitionBy(pairsRDD, 2L, partitionByParity),
                      mkTup),
                    FALSE)
  expect_equal(actual, list(list(0, list(list(1, 2), list(3, 4))),
                            list(1, list(list(4, 8)))))
})
