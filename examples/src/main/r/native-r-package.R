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

# This example illustrates how to use third-party R packages in your task
# which is distributed by Spark. We support two scenarios:
#  - Install packages from CRAN to executors directly.
#  - Install packages from local file system to executors.
#
# To run this example use
# ./bin/spark-submit examples/src/main/r/native-r-package.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-native-r-package-example")

# Get the location of the default library
libDir <- .libPaths()[1]

# Install third-party R packages from CRAN to executors directly if it does not exist,
# then the packages can be used by the corresponding task.

# Perform distributed training of multiple models with spark.lapply
costs <- exp(seq(from = log(1), to = log(1000), length.out = 5))
train <- function(cost) {
    if("e1071" %in% rownames(installed.packages(libDir)) == FALSE) {
        install.packages("e1071", repos = "https://cran.r-project.org")
    }
    library(e1071)
    model <- svm(Species ~ ., data = iris, cost = cost)
    summary(model)
}
model.summaries <- spark.lapply(costs, train)

# Print the summary of each model
print(model.summaries)

# Install third-party R packages from local file system to executors if it does not exist,
# then the packages can be used by the corresponding task.

# Downloaded e1071 package source code to a directory
packagesDir <- paste0(tempdir(), "/", "packages")
dir.create(packagesDir)
download.packages("e1071", packagesDir, repos = "https://cran.r-project.org")
filename <- list.files(packagesDir, "^e1071")
packagesPath <- file.path(packagesDir, filename)
# Add the third-party R package to be downloaded with this Spark job on every node.
spark.addFile(packagesPath)

path <- spark.getSparkFiles(filename)
costs <- exp(seq(from = log(1), to = log(1000), length.out = 5))
train <- function(cost) {
    if("e1071" %in% rownames(installed.packages(libDir)) == FALSE) {
        install.packages(path, repos=NULL, type="source")
    }
    library(e1071)
    model <- svm(Species ~ ., data = iris, cost = cost)
    summary(model)
}
model.summaries <- spark.lapply(costs, train)

# Print the summary of each model
print(model.summaries)

unlink(packagesDir, recursive = TRUE)
