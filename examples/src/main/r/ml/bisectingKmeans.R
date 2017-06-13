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
# ./bin/spark-submit examples/src/main/r/ml/bisectingKmeans.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-ML-bisectingKmeans-example")

# $example on$
t <- as.data.frame(Titanic)
training <- createDataFrame(t)

# Fit bisecting k-means model with four centers
model <- spark.bisectingKmeans(training, Class ~ Survived, k = 4)

# get fitted result from a bisecting k-means model
fitted.model <- fitted(model, "centers")

# Model summary
head(summary(fitted.model))

# fitted values on training data
fitted <- predict(model, training)
head(select(fitted, "Class", "prediction"))
# $example off$

sparkR.session.stop()
