context("broadcast variables")

# JavaSparkContext handle
sc <- sparkR.init()

# Partitioned data
nums <- 1:2
rrdd <- parallelize(sc, nums, 2L)

test_that("using broadcast variable", {
  randomMat <- matrix(nrow=10, ncol=10, data=rnorm(100))
  randomMatBr <- broadcast(sc, randomMat)

  useBroadcast <- function(x) {
    sum(value(randomMatBr) * x)
  }
  actual <- collect(lapply(rrdd, useBroadcast))
  expected <- list(sum(randomMat) * 1, sum(randomMat) * 2)
  expect_equal(actual, expected)
})

test_that("without using broadcast variable", {
  randomMat <- matrix(nrow=10, ncol=10, data=rnorm(100))

  useBroadcast <- function(x) {
    sum(randomMat * x)
  }
  actual <- collect(lapply(rrdd, useBroadcast))
  expected <- list(sum(randomMat) * 1, sum(randomMat) * 2)
  expect_equal(actual, expected)
})
