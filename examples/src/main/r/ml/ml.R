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

# To run this example use
# ./bin/spark-submit examples/src/main/r/ml/ml.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-ML-example")

############################ model read/write ##############################################
# $example on:read_write$
irisDF <- suppressWarnings(createDataFrame(iris))
# Fit a generalized linear model of family "gaussian" with spark.glm
gaussianDF <- irisDF
gaussianTestDF <- irisDF
gaussianGLM <- spark.glm(gaussianDF, Sepal_Length ~ Sepal_Width + Species, family = "gaussian")

# Save and then load a fitted MLlib model
modelPath <- tempfile(pattern = "ml", fileext = ".tmp")
write.ml(gaussianGLM, modelPath)
gaussianGLM2 <- read.ml(modelPath)

# Check model summary
summary(gaussianGLM2)

# Check model prediction
gaussianPredictions <- predict(gaussianGLM2, gaussianTestDF)
showDF(gaussianPredictions)

unlink(modelPath)
# $example off:read_write$

############################ fit models with spark.lapply #####################################
# Perform distributed training of multiple models with spark.lapply
algorithms <- c("Hartigan-Wong", "Lloyd", "MacQueen")
train <- function(algorithm) {
  model <- kmeans(x = iris[1:4], centers = 3, algorithm = algorithm)
  model$withinss
}

model.withinss <- spark.lapply(algorithms, train)

# Print the within-cluster sum of squares for each model
print(model.withinss)

# Stop the SparkSession now
sparkR.session.stop()
