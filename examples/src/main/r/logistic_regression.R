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

if (length(args) != 3) {
  print("Usage: logistic_regression <file> <iters> <dimension>")
  q("no")
}

# Initialize Spark context
sc <- sparkR.init(appName = "LogisticRegressionR")
iterations <- as.integer(args[[2]])
D <- as.integer(args[[3]])

readPartition <- function(part){
  part = strsplit(part, " ", fixed = T)
  list(matrix(as.numeric(unlist(part)), ncol = length(part[[1]])))
}

# Read data points and convert each partition to a matrix
points <- cache(lapplyPartition(textFile(sc, args[[1]]), readPartition))

# Initialize w to a random value
w <- runif(n=D, min = -1, max = 1)
cat("Initial w: ", w, "\n")

# Compute logistic regression gradient for a matrix of data points
gradient <- function(partition) {
  partition = partition[[1]]
  Y <- partition[, 1]  # point labels (first column of input file)
  X <- partition[, -1] # point coordinates

  # For each point (x, y), compute gradient function
  dot <- X %*% w
  logit <- 1 / (1 + exp(-Y * dot))
  grad <- t(X) %*% ((logit - 1) * Y)
  list(grad)
}

for (i in 1:iterations) {
  cat("On iteration ", i, "\n")
  w <- w - reduce(lapplyPartition(points, gradient), "+")
}

cat("Final w: ", w, "\n")
