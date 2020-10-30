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

context("SparkSQL Avro functions")

sparkR.session(
  master = sparkRTestMaster
)

test_that("avro column functions", {
  skip_if_not(
    grepl("spark-avro", sparkR.conf("spark.jars", "")),
    "spark-avro jar not present"
  )

  schema <- '{"namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "favorite_color", "type": ["string", "null"]}
    ]
  }'

  c0 <- column("foo")
  c1 <- from_avro(c0, schema)
  expect_s4_class(c1, "Column")
  c2 <- from_avro("foo", schema)
  expect_s4_class(c2, "Column")
  c3 <- to_avro(c1)
  expect_s4_class(c3, "Column")
  c4 <- to_avro(c1, schema)
  expect_s4_class(c4, "Column")
})
