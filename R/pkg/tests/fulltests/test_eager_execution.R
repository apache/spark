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

context("Show SparkDataFrame when eager execution is enabled.")

test_that("eager execution is not enabled", {
  # Start Spark session without eager execution enabled
  sparkSession <- if (windows_with_hadoop()) {
    sparkR.session(master = sparkRTestMaster)
  } else {
    sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)
  }
  
  df <- suppressWarnings(createDataFrame(iris))
  expect_is(df, "SparkDataFrame")
  expected <- "Sepal_Length:double, Sepal_Width:double, Petal_Length:double, Petal_Width:double, Species:string"
  expect_output(show(df), expected)
  
  # Stop Spark session
  sparkR.session.stop()
})

test_that("eager execution is enabled", {
  # Start Spark session without eager execution enabled
  sparkSession <- if (windows_with_hadoop()) {
    sparkR.session(master = sparkRTestMaster,
                   sparkConfig = list(spark.sql.repl.eagerEval.enabled = "true"))
  } else {
    sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE, 
                   sparkConfig = list(spark.sql.repl.eagerEval.enabled = "true"))
  }
  
  df <- suppressWarnings(createDataFrame(iris))
  expect_is(df, "SparkDataFrame")
  expected <- paste0("+------------+-----------+------------+-----------+-------+\n",
                     "|Sepal_Length|Sepal_Width|Petal_Length|Petal_Width|Species|\n",
                     "+------------+-----------+------------+-----------+-------+\n")
  expect_output(show(df), expected)
  
  # Stop Spark session
  sparkR.session.stop()
})
