context("functions in sparkR.R")

test_that("start and stop SparkContext multiple times", {
    for (i in 1:3) {
        sc  <- sparkR.init()
        rdd <- parallelize(sc, 1:10)
        expect_equal(count(rdd), 10)
        sparkR.stop()
    }
})
