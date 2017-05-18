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

context("Serialization implementation")

test_that("writeRaw can write multiple batches", {
  vec <- 1:100
  batch <- serialize(vec, connection = NULL)

  fileName1 <- tempfile()
  conn1 <- file(fileName1, "wb")
  writeRaw(conn1, batch, maxBatchSize = 20)
  close(conn1)

  fileName2 <- tempfile()
  conn2 <- file(fileName2, "wb")
  writeRaw(conn2, batch)
  close(conn2)

  expect_equal(file.info(fileName1)$size, file.info(fileName2)$size)
})
