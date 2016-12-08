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
# ./bin/spark-submit examples/src/main/r/ml/als.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-ML-als-example")

# $example on$
# Load training data
data <- list(list(0, 0, 4.0), list(0, 1, 2.0), list(1, 1, 3.0),
             list(1, 2, 4.0), list(2, 1, 1.0), list(2, 2, 5.0))
df <- createDataFrame(data, c("userId", "movieId", "rating"))
training <- df
test <- df

# Fit a recommendation model using ALS with spark.als
model <- spark.als(training, maxIter = 5, regParam = 0.01, userCol = "userId",
                   itemCol = "movieId", ratingCol = "rating")

# Model summary
summary(model)

# Prediction
predictions <- predict(model, test)
showDF(predictions)
# $example off$
