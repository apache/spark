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
# ./bin/spark-submit examples/src/main/r/ml/kmeans.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-ML-kmeans-example")

# $example on$
# Fit a k-means model with spark.kmeans
irisDF <- suppressWarnings(createDataFrame(iris))
kmeansDF <- irisDF
kmeansTestDF <- irisDF
kmeansModel <- spark.kmeans(kmeansDF, ~ Sepal_Length + Sepal_Width + Petal_Length + Petal_Width,
                            k = 3)

# Model summary
summary(kmeansModel)

# Get fitted result from the k-means model
showDF(fitted(kmeansModel))

# Prediction
kmeansPredictions <- predict(kmeansModel, kmeansTestDF)
showDF(kmeansPredictions)
# $example off$
