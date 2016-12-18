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
# Name this test starting with an `a` to make sure it is run first
context("a first test for package")

filesBefore <- list.files(path = file.path(Sys.getenv("SPARK_HOME"), "R"), all.files = TRUE)

sparkSession <- sparkR.session(enableHiveSupport = FALSE)

test_that("No extra files are created in SPARK_HOME", {
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

sparkR.session.stop()
