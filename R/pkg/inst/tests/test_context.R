context("test functions in sparkR.R")

test_that("repeatedly starting and stopping SparkR", {
  for (i in 1:4) {
    sc <- sparkR.init()
    rdd <- parallelize(sc, 1:20, 2L)
    expect_equal(count(rdd), 20)
    sparkR.stop()
  }
})

test_that("rdd GC across sparkR.stop", {
  sparkR.stop()
  sc <- sparkR.init() # sc should get id 0
  rdd1 <- parallelize(sc, 1:20, 2L) # rdd1 should get id 1
  rdd2 <- parallelize(sc, 1:10, 2L) # rdd2 should get id 2
  sparkR.stop()

  sc <- sparkR.init() # sc should get id 0 again

  # GC rdd1 before creating rdd3 and rdd2 after
  rm(rdd1)
  gc()

  rdd3 <- parallelize(sc, 1:20, 2L) # rdd3 should get id 1 now
  rdd4 <- parallelize(sc, 1:10, 2L) # rdd4 should get id 2 now

  rm(rdd2)
  gc()

  count(rdd3)
  count(rdd4)
})
