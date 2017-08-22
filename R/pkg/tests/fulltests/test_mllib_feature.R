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

context("MLlib feature processing algorithms.")

# Tests for MLlib feature processing algorithms in SparkR
sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

test_that("spark.binarizer", {
  sdfIris <- createDataFrame(iris)
  binarySL <- spark.binarizer(sdfIris, "Sepal_Length", "Sepal_Length_binary", 5.800)
  binarySW <- spark.binarizer(sdfIris, "Sepal_Width", "Sepal_Width_binary", 3.000)
  binaryPL <- spark.binarizer(sdfIris, "Petal_Length", "Petal_Length_binary", 4.350)
  binaryPW <- spark.binarizer(sdfIris, "Petal_Width", "Petal_Width_binary", 1.300)

  expect_equal(as.data.frame(binarySL)$Sepal_Length_binary,
               ifelse(iris$Sepal.Length > 5.800, 1, 0))
  expect_equal(as.data.frame(binarySW)$Sepal_Width_binary,
               ifelse(iris$Sepal.Width > 3.000, 1, 0))
  expect_equal(as.data.frame(binaryPL)$Petal_Length_binary,
               ifelse(iris$Petal.Length > 4.350, 1, 0))
  expect_equal(as.data.frame(binaryPW)$Petal_Width_binary,
               ifelse(iris$Petal.Width > 1.300, 1, 0))
})

sparkR.session.stop()
