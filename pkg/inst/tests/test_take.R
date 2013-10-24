context("tests RRDD function take()")

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

test_that("take() gives back the original elements in correct count and order", {
  numVectorRRDD <- parallelize(jsc, numVector, 10)
  expect_equal(take(numVectorRRDD, 1), as.list(head(numVector, n = 1)))
  expect_equal(take(numVectorRRDD, 3), as.list(head(numVector, n = 3)))
  expect_equal(take(numVectorRRDD, length(numVector)), as.list(numVector))
  expect_equal(take(numVectorRRDD, length(numVector) + 1), as.list(numVector))

  numListRRDD <- parallelize(jsc, numList, 1)
  numListRRDD2 <- parallelize(jsc, numList, 4)
  expect_equal(take(numListRRDD, 3), take(numListRRDD2, 3))
  expect_equal(take(numListRRDD, 5), take(numListRRDD2, 5))
  expect_equal(take(numListRRDD, 1), as.list(head(numList, n = 1)))
  expect_equal(take(numListRRDD2, 999), numList)

  strVectorRRDD <- parallelize(jsc, strVector, 2)
  strVectorRRDD2 <- parallelize(jsc, strVector, 3)
  expect_equal(take(strVectorRRDD, 4), as.list(strVector))
  expect_equal(take(strVectorRRDD2, 2), as.list(head(strVector, n = 2)))

  strListRRDD <- parallelize(jsc, strList, 4)
  strListRRDD2 <- parallelize(jsc, strList, 1)
  expect_equal(take(strListRRDD, 3), as.list(head(strList, n = 3)))
  expect_equal(take(strListRRDD2, 1), as.list(head(strList, n = 1)))

  expect_true(length(take(strListRRDD, 0)) == 0)
  expect_true(length(take(strVectorRRDD, 0)) == 0)
  expect_true(length(take(numListRRDD, 0)) == 0)
  expect_true(length(take(numVectorRRDD, 0)) == 0)
})

