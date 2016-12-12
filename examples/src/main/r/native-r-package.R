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

# This example illustrates how to install third-party R packages to executors
# in your SparkR jobs distributed by "spark.lapply".
#
# Note: This example will install packages to a temporary directory on your machine.
#       The directory will be removed automatically when the example exit.
#       You environment should be connected to internet to run this example,
#       otherwise, you should change "repos" to your private repository url.
#       And the environment need to have necessary tools such as gcc to compile
#       and install R package "e1071".
#
# To run this example use
# ./bin/spark-submit examples/src/main/r/native-r-package.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-native-r-package-example")

# $example on$
# The directory where the third-party R packages are installed.
libDir <- paste0(tempdir(), "/", "Rlib")
dir.create(libDir)

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
    if("e1071" %in% rownames(installed.packages(lib = libDir)) == FALSE) {
        install.packages(path, lib = libDir, repos = NULL, type = "source")
    }
    library(e1071)
    model <- svm(Species ~ ., data = iris, cost = cost)
    summary(model)
}
model.summaries <- spark.lapply(costs, train)

# Print the summary of each model
print(model.summaries)

unlink(libDir, recursive = TRUE)
unlink(packagesDir, recursive = TRUE)
# $example off$
