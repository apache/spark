#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

context("include R packages")

# JavaSparkContext handle
sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)
sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)

# Partitioned data
nums <- 1:2
rdd <- parallelize(sc, nums, 2L)

test_that("include inside function", {
  # Only run the test if plyr is installed.
  if ("plyr" %in% rownames(installed.packages()) &&
      suppressPackageStartupMessages(suppressWarnings(library(plyr, logical.return = TRUE)))) {
    generateData <- function(x) {
      suppressPackageStartupMessages(library(plyr))
      attach(airquality)
      result <- transform(Ozone, logOzone = log(Ozone))
      result
    }

    data <- lapplyPartition(rdd, generateData)
    actual <- collectRDD(data)
  }
  expect_true(TRUE)
})

test_that("use include package", {
  # Only run the test if plyr is installed.
  if ("plyr" %in% rownames(installed.packages()) &&
      suppressPackageStartupMessages(suppressWarnings(library(plyr, logical.return = TRUE)))) {
    generateData <- function(x) {
      attach(airquality)
      result <- transform(Ozone, logOzone = log(Ozone))
      result
    }

    includePackage(sc, plyr)
    data <- lapplyPartition(rdd, generateData)
    actual <- collectRDD(data)
  }
  expect_true(TRUE)
})

sparkR.session.stop()
