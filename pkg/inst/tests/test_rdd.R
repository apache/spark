context("basic RDD functions")

# JavaSparkContext handle
sc <- sparkR.init()

# Data
nums <- 1:10
rdd <- parallelize(sc, nums, 2L)

intPairs <- list(list(1L, -1), list(2L, 100), list(2L, 1), list(1L, 200))
intRdd <- parallelize(sc, intPairs, 2L)

test_that("get number of partitions in RDD", {
  expect_equal(numPartitions(rdd), 2)
  expect_equal(numPartitions(intRdd), 2)
})

test_that("count and length on RDD", {
   expect_equal(count(rdd), 10)
   expect_equal(length(rdd), 10)
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
  actual <- collect(multiples)
  expect_equal(actual, as.list(nums * 2))
})

test_that("lapplyPartition on RDD", {
  sums <- lapplyPartition(rdd, function(part) { sum(unlist(part)) })
  actual <- collect(sums)
  expect_equal(actual, list(15, 40))
})

test_that("mapPartitions on RDD", {
  sums <- mapPartitions(rdd, function(part) { sum(unlist(part)) })
  actual <- collect(sums)
  expect_equal(actual, list(15, 40))
})

test_that("flatMap() on RDDs", {
  flat <- flatMap(intRdd, function(x) { list(x, x) })
  actual <- collect(flat)
  expect_equal(actual, rep(intPairs, each=2))
})

test_that("filterRDD on RDD", {
  filtered.rdd <- filterRDD(rdd, function(x) { x %% 2 == 0 })
  actual <- collect(filtered.rdd)
  expect_equal(actual, list(2, 4, 6, 8, 10))
  
  filtered.rdd <- Filter(function(x) { x[[2]] < 0 }, intRdd)
  actual <- collect(filtered.rdd)
  expect_equal(actual, list(list(1L, -1)))
  
  # Filter out all elements.
  filtered.rdd <- filterRDD(rdd, function(x) { x > 10 })
  actual <- collect(filtered.rdd)
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
              rdd2, function(split, part) {
                part <- as.list(unlist(part) * split + i)
              })
  rdd2 <- lapply(rdd2, function(x) x + x)
  collect(rdd2)
})

test_that("PipelinedRDD support actions: cache(), persist(), unpersist(), checkpoint()", {
  # RDD
  rdd2 <- rdd
  # PipelinedRDD
  rdd2 <- lapplyPartitionsWithIndex(
            rdd2,
            function(split, part) {
              part <- as.list(unlist(part) * split)
            })

  cache(rdd2)
  expect_true(rdd2@env$isCached)
  rdd2 <- lapply(rdd2, function(x) x)
  expect_false(rdd2@env$isCached)

  unpersist(rdd2)
  expect_false(rdd2@env$isCached)

  persist(rdd2, "MEMORY_AND_DISK")
  expect_true(rdd2@env$isCached)
  rdd2 <- lapply(rdd2, function(x) x)
  expect_false(rdd2@env$isCached)

  unpersist(rdd2)
  expect_false(rdd2@env$isCached)

  setCheckpointDir(sc, "checkpoints")
  checkpoint(rdd2)
  expect_true(rdd2@env$isCheckpointed)

  rdd2 <- lapply(rdd2, function(x) x)
  expect_false(rdd2@env$isCached)
  expect_false(rdd2@env$isCheckpointed)

  # make sure the data is collectable
  collect(rdd2)

  unlink("checkpoints")
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

test_that("sampleRDD() on RDDs", {
  expect_equal(unlist(collect(sampleRDD(rdd, FALSE, 1.0, 2014L))), nums)
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
    # got < 100
    expect_true(length(unique(s)) < 100L)
  }
  for (seed in 4:5) {
    s <- takeSample(data, TRUE, 200L, seed)
    expect_equal(length(s), 200L)
    # Chance of getting all distinct elements is still quite low, so test we
    # got < 100
    expect_true(length(unique(s)) < 100L)
  }
})

test_that("mapValues() on pairwise RDDs", {
  multiples <- mapValues(intRdd, function(x) { x * 2 })
  actual <- collect(multiples)
  expected <- lapply(intPairs, function(x) {
    list(x[[1]], x[[2]] * 2)
  })
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))
})

test_that("flatMapValues() on pairwise RDDs", {
  l <- parallelize(sc, list(list(1, c(1,2)), list(2, c(3,4))))
  actual <- collect(flatMapValues(l, function(x) { x }))
  expect_equal(actual, list(list(1,1), list(1,2), list(2,3), list(2,4)))
  
  # Generate x to x+1 for every value
  actual <- collect(flatMapValues(intRdd, function(x) { x:(x + 1) }))
  expect_equal(actual, 
               list(list(1L, -1), list(1L, 0), list(2L, 100), list(2L, 101),
                    list(2L, 1), list(2L, 2), list(1L, 200), list(1L, 201)))
})

test_that("distinct() on RDDs", {
  nums.rep2 <- rep(1:10, 2)
  rdd.rep2 <- parallelize(sc, nums.rep2, 2L)
  uniques <- distinct(rdd.rep2)
  actual <- sort(unlist(collect(uniques)))
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

test_that("keyBy on RDDs", {
  func <- function(x) { x*x }
  keys <- keyBy(rdd, func)
  actual <- collect(keys)
  expect_equal(actual, lapply(nums, function(x) { list(func(x), x) }))
})

test_that("keys() on RDDs", {
  keys <- keys(intRdd)
  actual <- collect(keys)
  expect_equal(actual, lapply(intPairs, function(x) { x[[1]] }))
})

test_that("values() on RDDs", {
  values <- values(intRdd)
  actual <- collect(values)
  expect_equal(actual, lapply(intPairs, function(x) { x[[2]] }))
})

test_that("join() on pairwise RDDs", {
  rdd1 <- parallelize(sc, list(list(1,1), list(2,4)))
  rdd2 <- parallelize(sc, list(list(1,2), list(1,3)))
  actual <- collect(join(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list(1, list(1, 2)), list(1, list(1, 3)))))

  rdd1 <- parallelize(sc, list(list("a",1), list("b",4)))
  rdd2 <- parallelize(sc, list(list("a",2), list("a",3)))
  actual <- collect(join(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list("a", list(1, 2)), list("a", list(1, 3)))))

  rdd1 <- parallelize(sc, list(list(1,1), list(2,2)))
  rdd2 <- parallelize(sc, list(list(3,3), list(4,4)))
  actual <- collect(join(rdd1, rdd2, 2L))
  expect_equal(actual, list())

  rdd1 <- parallelize(sc, list(list("a",1), list("b",2)))
  rdd2 <- parallelize(sc, list(list("c",3), list("d",4)))
  actual <- collect(join(rdd1, rdd2, 2L))
  expect_equal(actual, list())
})

test_that("leftOuterJoin() on pairwise RDDs", {
  rdd1 <- parallelize(sc, list(list(1,1), list(2,4)))
  rdd2 <- parallelize(sc, list(list(1,2), list(1,3)))
  actual <- collect(leftOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list(1, list(1, 2)), list(1, list(1, 3)), list(2, list(4, NULL)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list("a",1), list("b",4)))
  rdd2 <- parallelize(sc, list(list("a",2), list("a",3)))
  actual <- collect(leftOuterJoin(rdd1, rdd2, 2L))
  expected <-  list(list("b", list(4, NULL)), list("a", list(1, 2)), list("a", list(1, 3)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list(1,1), list(2,2)))
  rdd2 <- parallelize(sc, list(list(3,3), list(4,4)))
  actual <- collect(leftOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list(1, list(1, NULL)), list(2, list(2, NULL)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list("a",1), list("b",2)))
  rdd2 <- parallelize(sc, list(list("c",3), list("d",4)))
  actual <- collect(leftOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list("b", list(2, NULL)), list("a", list(1, NULL)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))
})

test_that("rightOuterJoin() on pairwise RDDs", {
  rdd1 <- parallelize(sc, list(list(1,2), list(1,3)))
  rdd2 <- parallelize(sc, list(list(1,1), list(2,4)))
  actual <- collect(rightOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list(1, list(2, 1)), list(1, list(3, 1)), list(2, list(NULL, 4)))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list("a",2), list("a",3)))
  rdd2 <- parallelize(sc, list(list("a",1), list("b",4)))
  actual <- collect(rightOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list("b", list(NULL, 4)), list("a", list(2, 1)), list("a", list(3, 1)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list(1,1), list(2,2)))
  rdd2 <- parallelize(sc, list(list(3,3), list(4,4)))
  actual <- collect(rightOuterJoin(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list(3, list(NULL, 3)), list(4, list(NULL, 4)))))

  rdd1 <- parallelize(sc, list(list("a",1), list("b",2)))
  rdd2 <- parallelize(sc, list(list("c",3), list("d",4)))
  actual <- collect(rightOuterJoin(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list("d", list(NULL, 4)), list("c", list(NULL, 3)))))
})

test_that("fullOuterJoin() on pairwise RDDs", {
  rdd1 <- parallelize(sc, list(list(1,2), list(1,3), list(3,3)))
  rdd2 <- parallelize(sc, list(list(1,1), list(2,4)))
  actual <- collect(fullOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list(1, list(2, 1)), list(1, list(3, 1)), list(2, list(NULL, 4)), list(3, list(3, NULL)))
  expect_equal(sortKeyValueList(actual), sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list("a",2), list("a",3), list("c", 1)))
  rdd2 <- parallelize(sc, list(list("a",1), list("b",4)))
  actual <- collect(fullOuterJoin(rdd1, rdd2, 2L))
  expected <- list(list("b", list(NULL, 4)), list("a", list(2, 1)), list("a", list(3, 1)), list("c", list(1, NULL)))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(expected))

  rdd1 <- parallelize(sc, list(list(1,1), list(2,2)))
  rdd2 <- parallelize(sc, list(list(3,3), list(4,4)))
  actual <- collect(fullOuterJoin(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list(1, list(1, NULL)), list(2, list(2, NULL)), list(3, list(NULL, 3)), list(4, list(NULL, 4)))))

  rdd1 <- parallelize(sc, list(list("a",1), list("b",2)))
  rdd2 <- parallelize(sc, list(list("c",3), list("d",4)))
  actual <- collect(fullOuterJoin(rdd1, rdd2, 2L))
  expect_equal(sortKeyValueList(actual),
               sortKeyValueList(list(list("a", list(1, NULL)), list("b", list(2, NULL)), list("d", list(NULL, 4)), list("c", list(NULL, 3)))))
})
