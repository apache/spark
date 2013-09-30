# run in REPL with 'test_dir(path/to/inst/tests)'

context("parallelize() and collect()")

# Mock data
numVector <- c(-10:97)
numList <- list(sqrt(1), sqrt(2), sqrt(3), 4 ** 10)
strVector <- c("Dexter Morgan: I suppose I should be upset, even feel",
               "violated, but I'm not. No, in fact, I think this is a friendly",
               "message, like \"Hey, wanna play?\" and yes, I want to play. ",
               "I really, really do.")
strList <- list("Dexter Morgan: Blood. Sometimes it sets my teeth on edge, ",
                "other times it helps me control the chaos.",
                "Dexter Morgan: Harry and Dorris Morgan did a wonderful job ",
                "raising me. But they're both dead now. I didn't kill them. Honest.")

# JavaSparkContext handle
jsc <- sparkR.init()

test_that("parallelize() on simple vectors and lists returns an RRDD", {
  numVectorRRDD <- parallelize(jsc, numVector, 1)
  numVectorRRDD2 <- parallelize(jsc, numVector, 10)
  numListRRDD <- parallelize(jsc, numList, 1)
  numListRRDD2 <- parallelize(jsc, numList, 4)
  strVectorRRDD <- parallelize(jsc, strVector, 2)
  strVectorRRDD2 <- parallelize(jsc, strVector, 3)
  strListRRDD <- parallelize(jsc, strList, 4)
  strListRRDD2 <- parallelize(jsc, strList, 1)

  rrdds <- c(numVectorRRDD,
             numVectorRRDD2,
             numListRRDD,
             numListRRDD2,
             strVectorRRDD,
             strVectorRRDD2,
             strListRRDD,
             strListRRDD2)

  for (rrdd in rrdds) {
    expect_that(class(rrdd), is_equivalent_to("RRDD"))
    expect_true(.hasSlot(rrdd, "jrdd")
                && class(rrdd@jrdd) == "jobjRef"
                && .jinstanceof(rrdd@jrdd, "org/apache/spark/api/java/JavaRDD"))
  }
})

test_that("collect(), following a parallelize(), gives back the original collections", {
  numVectorRRDD <- parallelize(jsc, numVector, 10)
  expect_equal(collect(numVectorRRDD), as.list(numVector))

  numListRRDD <- parallelize(jsc, numList, 1)
  numListRRDD2 <- parallelize(jsc, numList, 4)
  expect_equal(collect(numListRRDD), as.list(numList))
  expect_equal(collect(numListRRDD2), as.list(numList))

  strVectorRRDD <- parallelize(jsc, strVector, 2)
  strVectorRRDD2 <- parallelize(jsc, strVector, 3)
  expect_equal(collect(strVectorRRDD), as.list(strVector))
  expect_equal(collect(strVectorRRDD2), as.list(strVector))

  strListRRDD <- parallelize(jsc, strList, 4)
  strListRRDD2 <- parallelize(jsc, strList, 1)
  expect_equal(collect(strListRRDD), as.list(strList))
  expect_equal(collect(strListRRDD2), as.list(strList))
})

