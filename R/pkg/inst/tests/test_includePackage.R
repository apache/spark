context("include R packages")

# JavaSparkContext handle
sc <- sparkR.init()

# Partitioned data
nums <- 1:2
rdd <- parallelize(sc, nums, 2L)

test_that("include inside function", {
  # Only run the test if plyr is installed.
  if ("plyr" %in% rownames(installed.packages())) {
    suppressPackageStartupMessages(library(plyr))
    generateData <- function(x) {
      suppressPackageStartupMessages(library(plyr))
      attach(airquality)
      result <- transform(Ozone, logOzone = log(Ozone))
      result
    }

    data <- lapplyPartition(rdd, generateData)
    actual <- collect(data)
  }
})

test_that("use include package", {
  # Only run the test if plyr is installed.
  if ("plyr" %in% rownames(installed.packages())) {
    suppressPackageStartupMessages(library(plyr))
    generateData <- function(x) {
      attach(airquality)
      result <- transform(Ozone, logOzone = log(Ozone))
      result
    }

    includePackage(sc, plyr)
    data <- lapplyPartition(rdd, generateData)
    actual <- collect(data)
  }
})
