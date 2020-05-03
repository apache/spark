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

context("Windows-specific tests")

test_that("sparkJars tag in SparkContext", {
  if (!is_windows()) {
    skip("This test is only for Windows, skipped")
  }

  testOutput <- launchScript("ECHO", "a/b/c", wait = TRUE)
  abcPath <- testOutput[1]
  expect_equal(abcPath, "a\\b\\c")
})
