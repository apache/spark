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

# Instructions: https://github.com/amplab-extras/SparkR-pkg/wiki/SparkR-Example:-Digit-Recognition-on-EC2

library(SparkR)
library(Matrix)

args <- commandArgs(trailing = TRUE)

# number of random features; default to 1100
D <- ifelse(length(args) > 0, as.integer(args[[1]]), 1100)
# number of partitions for training dataset
trainParts <- 12
# dimension of digits
d <- 784
# number of test examples
NTrain <- 60000
# number of training examples
NTest <- 10000
# scale of features
gamma <- 4e-4

sc <- sparkR.init(appName = "SparkR-LinearSolver")

# You can also use HDFS path to speed things up:
# hdfs://<master>/train-mnist-dense-with-labels.data
file <- textFile(sc, "/data/train-mnist-dense-with-labels.data", trainParts)

W <- gamma * matrix(nrow=D, ncol=d, data=rnorm(D*d))
b <- 2 * pi * matrix(nrow=D, ncol=1, data=runif(D))
broadcastW <- broadcast(sc, W)
broadcastB <- broadcast(sc, b)

includePackage(sc, Matrix)
numericLines <- lapplyPartitionsWithIndex(file,
                       function(split, part) {
                         matList <- sapply(part, function(line) {
                           as.numeric(strsplit(line, ",", fixed=TRUE)[[1]])
                         }, simplify=FALSE)
                         mat <- Matrix(ncol=d+1, data=unlist(matList, F, F),
                                       sparse=T, byrow=T)
                         mat
                       })

featureLabels <- cache(lapplyPartition(
    numericLines,
    function(part) {
      label <- part[,1]
      mat <- part[,-1]
      ones <- rep(1, nrow(mat))
      features <- cos(
        mat %*% t(value(broadcastW)) + (matrix(ncol=1, data=ones) %*% t(value(broadcastB))))
      onesMat <- Matrix(ones)
      featuresPlus <- cBind(features, onesMat)
      labels <- matrix(nrow=nrow(mat), ncol=10, data=-1)
      for (i in 1:nrow(mat)) {
        labels[i, label[i]] <- 1
      }
      list(label=labels, features=featuresPlus)
  }))

FTF <- Reduce("+", collect(lapplyPartition(featureLabels,
    function(part) {
      t(part$features) %*% part$features
    }), flatten=F))

FTY <- Reduce("+", collect(lapplyPartition(featureLabels,
    function(part) {
      t(part$features) %*% part$label
    }), flatten=F))

# solve for the coefficient matrix
C <- solve(FTF, FTY)

test <- Matrix(as.matrix(read.csv("/data/test-mnist-dense-with-labels.data",
                         header=F), sparse=T))
testData <- test[,-1]
testLabels <- matrix(ncol=1, test[,1])

err <- 0

# contstruct the feature maps for all examples from this digit
featuresTest <- cos(testData %*% t(value(broadcastW)) +
    (matrix(ncol=1, data=rep(1, NTest)) %*% t(value(broadcastB))))
featuresTest <- cBind(featuresTest, Matrix(rep(1, NTest)))

# extract the one vs. all assignment
results <- featuresTest %*% C
labelsGot <- apply(results, 1, which.max)
err <- sum(testLabels != labelsGot) / nrow(testLabels)

cat("\nFinished running. The error rate is: ", err, ".\n")
