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

context("functions in client.R")

test_that("adding spark-testing-base as a package works", {
  args <- generateSparkSubmitArgs("", "", "", "",
                                  "holdenk:spark-testing-base:1.3.0_0.0.5")
  expect_equal(gsub("[[:space:]]", "", args),
               gsub("[[:space:]]", "",
                    "--packages holdenk:spark-testing-base:1.3.0_0.0.5"))
})

test_that("no package specified doesn't add packages flag", {
  args <- generateSparkSubmitArgs("", "", "", "", "")
  expect_equal(gsub("[[:space:]]", "", args),
               "")
})

test_that("multiple packages don't produce a warning", {
  expect_that(generateSparkSubmitArgs("", "", "", "", c("A", "B")), not(gives_warning()))
})
