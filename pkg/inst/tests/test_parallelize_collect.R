context("parallelize() and collect()")

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
jsc <- sparkR.init()

test_that("parallelize() on simple vectors and lists returns an RRDD", {
  numVectorRRDD <- parallelize(jsc, numVector, 1)
  numVectorRRDD2 <- parallelize(jsc, numVector, 10)
  expect_that(class(numVectorRRDD), is_equivalent_to("RRDD"))
  expect_that(class(numVectorRRDD2), is_equivalent_to("RRDD"))

  numListRRDD <- parallelize(jsc, numList, 1)
  numListRRDD2 <- parallelize(jsc, numList, 4)
  expect_that(class(numListRRDD), is_equivalent_to("RRDD"))
  expect_that(class(numListRRDD2), is_equivalent_to("RRDD"))

  strVectorRRDD <- parallelize(jsc, strVector, 2)
  strVectorRRDD2 <- parallelize(jsc, strVector, 3)
  expect_that(class(strVectorRRDD), is_equivalent_to("RRDD"))
  expect_that(class(strVectorRRDD2), is_equivalent_to("RRDD"))

  strListRRDD <- parallelize(jsc, strList, 4)
  strListRRDD2 <- parallelize(jsc, strList, 1)
  expect_that(class(strListRRDD), is_equivalent_to("RRDD"))
  expect_that(class(strListRRDD2), is_equivalent_to("RRDD"))
})

test_that("collect(), following a parallelize(), gives back the original collections", {
  # numVectorRRDD <- parallelize(jsc, numVector, 10)
  # expect_that(collect(numVectorRRDD), equals(numVector))
  # expect_that(collect(numVectorRRDD), is_identical_to(numVector))

  # numListRRDD <- parallelize(jsc, numList, 1)
  # numListRRDD2 <- parallelize(jsc, numList, 4)
  # expect_that(class(numListRRDD), is_equivalent_to("RRDD"))
  # expect_that(class(numListRRDD2), is_equivalent_to("RRDD"))

  # strVectorRRDD <- parallelize(jsc, strVector, 2)
  # strVectorRRDD2 <- parallelize(jsc, strVector, 3)
  # expect_that(class(strVectorRRDD), is_equivalent_to("RRDD"))
  # expect_that(class(strVectorRRDD2), is_equivalent_to("RRDD"))

  # strListRRDD <- parallelize(jsc, strList, 4)
  # strListRRDD2 <- parallelize(jsc, strList, 1)
  # expect_that(class(strListRRDD), is_equivalent_to("RRDD"))
  # expect_that(class(strListRRDD2), is_equivalent_to("RRDD"))
})
