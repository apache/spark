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

context("Daemon Initialization")

sparkR.stop()
sparkR.session(
  master = "local[*]",
  spark.r.daemonInit = 'message("Initting the Daemon ..."); testInit <- "wow"'
)

df <- createDataFrame (list(list(1L, 1, "1", 0.1), list(1L, 2, "1", 0.2), list(3L, 3, "3", 0.3)),
                       c("a", "b", "c", "d"))

eek <- function(...) {
  warning(...)
  quit(status = 1, save = "no")
}

schema <-
  structType(
    structField("a", "integer"),
    structField("c", "string"),
    structField("avg", "double"))

result <- gapply(df,
                 c("a", "c"),
                 function(key, x) {
                   if(!exists("testInit"))
                     eek("daemon did not initialize")
                   data.frame(key, mean(x$b), stringsAsFactors = FALSE)
                 }, schema)

test_that("Daemon Initialization",
          {
            result <- collect(result)
            expect_equal(max(result$avg), 3)
          })

sparkR.stop()
