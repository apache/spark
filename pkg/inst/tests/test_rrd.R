context("basic RRDD functions")

# JavaSparkContext handle
jsc <- sparkR.init()

# Data
nums <- 1:10
rrdd <- parallelize(jsc, nums, 2L)

test_that("count and length on RRDD", {
   expect_equal(count(rrdd), 10)
   expect_equal(length(rrdd), 10)
})

test_that("lapply on RRDD", {
  multiples <- lapply(rrdd, function(x) { 2 * x })
  actual <- collect(multiples)
  expect_equal(actual, as.list(nums * 2))
})

test_that("lapplyPartition on RRDD", {
  sums <- lapplyPartition(rrdd, function(part) { sum(unlist(part)) })
  actual <- collect(sums)
  expect_equal(actual, list(15, 40))
})

test_that("reduce on RRDD", {
  sum <- reduce(rrdd, "+")
  expect_equal(sum, 55)

  # Also test with an inline function
  sumInline <- reduce(rrdd, function(x, y) { x + y })
  expect_equal(sumInline, 55)
})
