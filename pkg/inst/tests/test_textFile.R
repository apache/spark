context("the textFile() function")

# JavaSparkContext handle
sc <- sparkR.init()

mockFile = c("Spark is pretty.", "Spark is awesome.")

test_that("textFile() on a local file returns an RRDD", {
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName)

  rrdd <- textFile(sc, fileName)
  expect_that(class(rrdd), is_equivalent_to("RRDD"))
  expect_true(count(rrdd) > 0)
  expect_true(count(rrdd) == 2)

  unlink(fileName)
})

test_that("textFile() followed by a collect() returns the same content", {
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName)

  rrdd <- textFile(sc, fileName)
  expect_equal(collect(rrdd), as.list(mockFile))

  unlink(fileName)
})
