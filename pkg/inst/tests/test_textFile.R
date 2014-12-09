context("the textFile() function")

# JavaSparkContext handle
sc <- sparkR.init()

mockFile = c("Spark is pretty.", "Spark is awesome.")

test_that("textFile() on a local file returns an RDD", {
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName)
  expect_true(inherits(rdd, "RDD"))
  expect_true(count(rdd) > 0)
  expect_true(count(rdd) == 2)

  unlink(fileName)
})

test_that("textFile() followed by a collect() returns the same content", {
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName)
  expect_equal(collect(rdd), as.list(mockFile))

  unlink(fileName)
})

test_that("textFile() word count works as expected", {
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName)

  words <- flatMap(rdd, function(line) { strsplit(line, " ")[[1]] })
  wordCount <- lapply(words, function(word) { list(word, 1L) })

  counts <- reduceByKey(wordCount, "+", 2L)
  output <- collect(counts)
  expected <- list(list("pretty.", 1), list("is", 2), list("awesome.", 1),
                   list("Spark", 2))
  expect_equal(output, expected)
  
  unlink(fileName)
})

test_that("several transformations on RDD created by textFile()", {
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName)

  rdd <- textFile(sc, fileName) # RDD
  for (i in 1:10) {
    # PipelinedRDD initially created from RDD
    rdd <- lapply(rdd, function(x) paste(x, x))
  }
  collect(rdd)

  unlink(fileName)
})

test_that("textFile() followed by a saveAsTextFile() returns the same content", {
  fileName1 <- tempfile(pattern="spark-test", fileext=".tmp")
  fileName2 <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName1)

  rdd <- textFile(sc, fileName1)
  saveAsTextFile(rdd, fileName2)
  rdd <- textFile(sc, fileName2)
  expect_equal(collect(rdd), as.list(mockFile))

  unlink(fileName1)
  unlink(fileName2)
})

test_that("saveAsTextFile() on a parallelized list works as expected", {
  fileName <- tempfile(pattern="spark-test", fileext=".tmp")
  l <- list(1, 2, 3)
  rdd <- parallelize(sc, l)
  saveAsTextFile(rdd, fileName)
  rdd <- textFile(sc, fileName)
  expect_equal(collect(rdd), lapply(l, function(x) {toString(x)}))

  unlink(fileName)
})

test_that("textFile() and saveAsTextFile() word count works as expected", {
  fileName1 <- tempfile(pattern="spark-test", fileext=".tmp")
  fileName2 <- tempfile(pattern="spark-test", fileext=".tmp")
  writeLines(mockFile, fileName1)

  rdd <- textFile(sc, fileName1)

  words <- flatMap(rdd, function(line) { strsplit(line, " ")[[1]] })
  wordCount <- lapply(words, function(word) { list(word, 1L) })

  counts <- reduceByKey(wordCount, "+", 2L)

  saveAsTextFile(counts, fileName2)
  rdd <- textFile(sc, fileName2)
   
  output <- collect(rdd)
  expected <- list(list("awesome.", 1), list("Spark", 2),
                   list("pretty.", 1), list("is", 2))
  expect_equal(output, lapply(expected, function(x) {toString(x)}))
  
  unlink(fileName1)
  unlink(fileName2)
})
