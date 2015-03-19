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

library(SparkR)

args <- commandArgs(trailing = TRUE)

sc <- sparkR.init(appName = "PiR")

slices <- ifelse(length(args) > 1, as.integer(args[[2]]), 2)

n <- 100000 * slices

piFunc <- function(elem) {
  rands <- runif(n = 2, min = -1, max = 1)
  val <- ifelse((rands[1]^2 + rands[2]^2) < 1, 1.0, 0.0)
  val
}


piFuncVec <- function(elems) {
  message(length(elems))
  rands1 <- runif(n = length(elems), min = -1, max = 1)
  rands2 <- runif(n = length(elems), min = -1, max = 1)
  val <- ifelse((rands1^2 + rands2^2) < 1, 1.0, 0.0)
  sum(val)
}

rdd <- parallelize(sc, 1:n, slices)
count <- reduce(lapplyPartition(rdd, piFuncVec), sum)
cat("Pi is roughly", 4.0 * count / n, "\n")
cat("Num elements in RDD ", count(rdd), "\n")
