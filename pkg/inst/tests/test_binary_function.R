context("binary functions")

# JavaSparkContext handle
sc <- sparkR.init()

# Data
nums <- 1:10
rdd <- parallelize(sc, nums, 2L)

intPairs <- list(list(1L, -1), list(2L, 100), list(2L, 1), list(1L, 200))
intRdd <- parallelize(sc, intPairs, 2L)

test_that("union on two RDDs", {
  actual <- collect(unionRDD(rdd, rdd))
  expect_equal(actual, rep(nums, 2))
})