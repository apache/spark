context("binary functions")

# JavaSparkContext handle
sc <- sparkR.init()

# Data
nums <- 1:10
rdd <- parallelize(sc, nums, 2L)

# File content
mockFile <- c("Spark is pretty.", "Spark is awesome.")

test_that("union on two RDDs", {
  actual <- collect(unionRDD(rdd, rdd))
  expect_equal(actual, as.list(rep(nums, 2)))
  
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName)

  text.rdd <- textFile(sc, fileName)
  union.rdd <- unionRDD(rdd, text.rdd)
  actual <- collect(union.rdd)
  expect_equal(actual, c(as.list(nums), mockFile))
  expect_true(union.rdd@env$serialized)
  
  unlink(fileName)
})