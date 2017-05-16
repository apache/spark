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

context("JVM API")

sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

test_that("Create and call methods on object", {
  jarr <- sparkR.newJObject("java.util.ArrayList")
  # Add an element to the array
  sparkR.callJMethod(jarr, "add", 1L)
  # Check if get returns the same element
  expect_equal(sparkR.callJMethod(jarr, "get", 0L), 1L)
})

test_that("Call static methods", {
  # Convert a boolean to a string
  strTrue <- sparkR.callJStatic("java.lang.String", "valueOf", TRUE)
  expect_equal(strTrue, "true")
})

sparkR.session.stop()
