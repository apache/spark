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

library(testthat)

context("MLlib statistics algorithms")

# Tests for MLlib statistics algorithms in SparkR
sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

test_that("spark.kstest", {
  data <- data.frame(test = c(0.1, 0.15, 0.2, 0.3, 0.25, -1, -0.5))
  df <- createDataFrame(data)
  testResult <- spark.kstest(df, "test", "norm")
  stats <- summary(testResult)

  rStats <- ks.test(data$test, "pnorm", alternative = "two.sided")

  expect_equal(stats$p.value, rStats$p.value, tolerance = 1e-4)
  expect_equal(stats$statistic, unname(rStats$statistic), tolerance = 1e-4)
  expect_match(capture.output(stats)[1], "Kolmogorov-Smirnov test summary:")

  testResult <- spark.kstest(df, "test", "norm", -0.5)
  stats <- summary(testResult)

  rStats <- ks.test(data$test, "pnorm", -0.5, 1, alternative = "two.sided")

  expect_equal(stats$p.value, rStats$p.value, tolerance = 1e-4)
  expect_equal(stats$statistic, unname(rStats$statistic), tolerance = 1e-4)
  expect_match(capture.output(stats)[1], "Kolmogorov-Smirnov test summary:")

  # Test print.summary.KSTest
  printStats <- capture.output(print.summary.KSTest(stats))
  expect_match(printStats[1], "Kolmogorov-Smirnov test summary:")
  expect_match(printStats[5],
               "Low presumption against null hypothesis: Sample follows theoretical distribution. ")
})

sparkR.session.stop()
