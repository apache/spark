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

# With testthat, tests are run in alphabetic order on the test filenames.
# Name this test file starting with an `a` to make sure it is run first.
context("a first set of tests for the package")

filesBefore <- list.files(path = file.path(Sys.getenv("SPARK_HOME"), "R"), all.files = TRUE)

sparkSession <- sparkR.session(enableHiveSupport = FALSE)

test_that("No extra files are created in SPARK_HOME by starting the session", {
  # Check that it is not creating any extra file.
  # Does not check the tempdir which would be cleaned up after.
  filesAfter <- list.files(path = file.path(Sys.getenv("SPARK_HOME"), "R"), all.files = TRUE)

  # get testthat to show the diff by first making the 2 lists equal in length
  expect_equal(length(filesBefore), length(filesAfter))
  l <- max(length(filesBefore), length(filesAfter))
  length(filesBefore) <- l
  length(filesAfter) <- l
  expect_equal(sort(filesBefore, na.last = TRUE), sort(filesAfter, na.last = TRUE))
})

test_that("Check masked functions", {
  # Check that we are not masking any new function from base, stats, testthat unexpectedly
  # NOTE: We should avoid adding entries to *namesOfMaskedCompletely* as masked functions make it
  # hard for users to use base R functions. Please check when in doubt.
  namesOfMaskedCompletely <- c("cov", "filter", "sample")
  namesOfMasked <- c("describe", "cov", "filter", "lag", "na.omit", "predict", "sd", "var",
                     "colnames", "colnames<-", "intersect", "rank", "rbind", "sample", "subset",
                     "summary", "transform", "drop", "window", "as.data.frame", "union")
  if (as.numeric(R.version$major) >= 3 && as.numeric(R.version$minor) >= 3) {
    namesOfMasked <- c("endsWith", "startsWith", namesOfMasked)
  }
  masked <- conflicts(detail = TRUE)$`package:SparkR`
  expect_true("describe" %in% masked)  # only when with testthat..
  func <- lapply(masked, function(x) { capture.output(showMethods(x))[[1]] })
  funcSparkROrEmpty <- grepl("\\(package SparkR\\)$|^$", func)
  maskedBySparkR <- masked[funcSparkROrEmpty]
  expect_equal(length(maskedBySparkR), length(namesOfMasked))
  # make the 2 lists the same length so expect_equal will print their content
  l <- max(length(maskedBySparkR), length(namesOfMasked))
  length(maskedBySparkR) <- l
  length(namesOfMasked) <- l
  expect_equal(sort(maskedBySparkR, na.last = TRUE), sort(namesOfMasked, na.last = TRUE))
  # above are those reported as masked when `library(SparkR)`
  # note that many of these methods are still callable without base:: or stats:: prefix
  # there should be a test for each of these, except followings, which are currently "broken"
  funcHasAny <- unlist(lapply(masked, function(x) {
                                        any(grepl("=\"ANY\"", capture.output(showMethods(x)[-1])))
                                      }))
  maskedCompletely <- masked[!funcHasAny]
  expect_equal(length(maskedCompletely), length(namesOfMaskedCompletely))
  l <- max(length(maskedCompletely), length(namesOfMaskedCompletely))
  length(maskedCompletely) <- l
  length(namesOfMaskedCompletely) <- l
  expect_equal(sort(maskedCompletely, na.last = TRUE),
               sort(namesOfMaskedCompletely, na.last = TRUE))
})

sparkR.session.stop()
