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

context("test functions in sparkR.R")

test_that("repeatedly starting and stopping SparkR", {
  for (i in 1:4) {
    sc <- sparkR.init()
    rdd <- parallelize(sc, 1:20, 2L)
    expect_equal(count(rdd), 20)
    sparkR.stop()
  }
})

test_that("repeatedly starting and stopping SparkR SQL", {
  for (i in 1:4) {
    sc <- sparkR.init()
    sqlContext <- sparkRSQL.init(sc)
    df <- createDataFrame(sqlContext, data.frame(a = 1:20))
    expect_equal(count(df), 20)
    sparkR.stop()
  }
})

test_that("rdd GC across sparkR.stop", {
  sparkR.stop()
  sc <- sparkR.init() # sc should get id 0
  rdd1 <- parallelize(sc, 1:20, 2L) # rdd1 should get id 1
  rdd2 <- parallelize(sc, 1:10, 2L) # rdd2 should get id 2
  sparkR.stop()

  sc <- sparkR.init() # sc should get id 0 again

  # GC rdd1 before creating rdd3 and rdd2 after
  rm(rdd1)
  gc()

  rdd3 <- parallelize(sc, 1:20, 2L) # rdd3 should get id 1 now
  rdd4 <- parallelize(sc, 1:10, 2L) # rdd4 should get id 2 now

  rm(rdd2)
  gc()

  count(rdd3)
  count(rdd4)
})

test_that("job group functions can be called", {
  sc <- sparkR.init()
  setJobGroup(sc, "groupId", "job description", TRUE)
  cancelJobGroup(sc, "groupId")
  clearJobGroup(sc)
})

test_that("getClientModeSparkSubmitOpts() returns spark-submit args from whitelist", {
  e <- new.env()
  e[["spark.driver.memory"]] <- "512m"
  ops <- getClientModeSparkSubmitOpts("sparkrmain", e)
  expect_equal("--driver-memory \"512m\" sparkrmain", ops)

  e[["spark.driver.memory"]] <- "5g"
  e[["spark.driver.extraClassPath"]] <- "/opt/class_path" # nolint
  e[["spark.driver.extraJavaOptions"]] <- "-XX:+UseCompressedOops -XX:+UseCompressedStrings"
  e[["spark.driver.extraLibraryPath"]] <- "/usr/local/hadoop/lib" # nolint
  e[["random"]] <- "skipthis"
  ops2 <- getClientModeSparkSubmitOpts("sparkr-shell", e)
  # nolint start
  expect_equal(ops2, paste0("--driver-class-path \"/opt/class_path\" --driver-java-options \"",
                      "-XX:+UseCompressedOops -XX:+UseCompressedStrings\" --driver-library-path \"",
                      "/usr/local/hadoop/lib\" --driver-memory \"5g\" sparkr-shell"))
  # nolint end

  e[["spark.driver.extraClassPath"]] <- "/" # too short
  ops3 <- getClientModeSparkSubmitOpts("--driver-memory 4g sparkr-shell2", e)
  # nolint start
  expect_equal(ops3, paste0("--driver-java-options \"-XX:+UseCompressedOops ",
                      "-XX:+UseCompressedStrings\" --driver-library-path \"/usr/local/hadoop/lib\"",
                      " --driver-memory 4g sparkr-shell2"))
  # nolint end
})

test_that("sparkJars sparkPackages as comma-separated strings", {
  expect_warning(processSparkJars(" a, b "))
  jars <- suppressWarnings(processSparkJars(" a, b "))
  expect_equal(jars, c("a", "b"))

  jars <- suppressWarnings(processSparkJars(" abc ,, def "))
  expect_equal(jars, c("abc", "def"))

  jars <- suppressWarnings(processSparkJars(c(" abc ,, def ", "", "xyz", " ", "a,b")))
  expect_equal(jars, c("abc", "def", "xyz", "a", "b"))

  p <- processSparkPackages(c("ghi", "lmn"))
  expect_equal(p, c("ghi", "lmn"))

  # check normalizePath
  f <- dir()[[1]]
  expect_that(processSparkJars(f), not(gives_warning()))
  expect_match(processSparkJars(f), f)
})
