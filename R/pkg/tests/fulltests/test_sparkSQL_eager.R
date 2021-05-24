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

context("test show SparkDataFrame when eager execution is enabled.")

test_that("eager execution is not enabled", {
  # Start Spark session without eager execution enabled
  sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

  df <- createDataFrame(faithful)
  expect_is(df, "SparkDataFrame")
  expected <- "eruptions:double, waiting:double"
  expect_output(show(df), expected)

  # Stop Spark session
  sparkR.session.stop()
})

test_that("eager execution is enabled", {
  # Start Spark session with eager execution enabled
  sparkConfig <- list(spark.sql.repl.eagerEval.enabled = "true")

  sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE, sparkConfig = sparkConfig)

  df <- createDataFrame(faithful)
  expect_is(df, "SparkDataFrame")
  expected <- paste0("(+---------+-------+\n",
                     "|eruptions|waiting|\n",
                     "+---------+-------+\n)*",
                     "(only showing top 20 rows)")
  expect_output(show(df), expected)

  # Stop Spark session
  sparkR.session.stop()
})

test_that("eager execution is enabled with maxNumRows and truncate set", {
  # Start Spark session with eager execution enabled
  sparkConfig <- list(spark.sql.repl.eagerEval.enabled = "true",
                      spark.sql.repl.eagerEval.maxNumRows = as.integer(5),
                      spark.sql.repl.eagerEval.truncate = as.integer(2))

  sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE, sparkConfig = sparkConfig)

  df <- arrange(createDataFrame(faithful), "waiting")
  expect_is(df, "SparkDataFrame")
  expected <- paste0("(+---------+-------+\n",
                     "|eruptions|waiting|\n",
                     "+---------+-------+\n",
                     "|       1.|     43|\n)*",
                     "(only showing top 5 rows)")
  expect_output(show(df), expected)

  # Stop Spark session
  sparkR.session.stop()
})
