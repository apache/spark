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
context("include an external JAR in SparkContext")

filePath <- file.path(Sys.getenv("SPARK_HOME"),
              "R/lib/SparkR/test_support/sparktestjar_2.10-1.0.jar")
sparkR.stop()
sc <- sparkR.init(master = "local", sparkJars = filePath)

test_that("sparkJars tag in SparkContext", {
  helloTest <- SparkR:::callJStatic("sparkR.test.hello",
                                    "helloWorld",
                                    "Dave")
  expect_true(helloTest == "Hello, Dave")

  basicFunction <- SparkR:::callJStatic("sparkR.test.basicFunction",
                                        "addStuff",
                                        2L,
                                        2L)
  expect_true(basicFunction == 4L)
})
