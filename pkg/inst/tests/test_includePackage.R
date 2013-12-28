context("include R packages")

# JavaSparkContext handle
sc <- sparkR.init()

# Partitioned data
nums <- 1:3
rdd <- parallelize(sc, nums, 3L)

test_that("include inside function", {
  # Only run the test if Matrix is installed.
  if ("Matrix" %in% rownames(installed.packages())) {
    suppressPackageStartupMessages(library(Matrix))
    generateSparse <- function(x) {
      suppressPackageStartupMessages(library(Matrix))
      sparseMatrix(i=c(1, 2, 3), j=c(1, 2, 3), x=c(1, 2, 3))
    }

    sparseMat <- lapplyPartition(rdd, generateSparse)
    actual <- collect(sparseMat)
  }
})

test_that("use include package", {
  # Only run the test if Matrix is installed.
  if ("Matrix" %in% rownames(installed.packages())) {
    suppressPackageStartupMessages(library(Matrix))
    generateSparse <- function(x) {
      sparseMatrix(i=c(1, 2, 3), j=c(1, 2, 3), x=c(1, 2, 3))
    }
    includePackage(sc, Matrix)
    sparseMat <- lapplyPartition(rdd, generateSparse)
    actual <- collect(sparseMat)
  }
})
