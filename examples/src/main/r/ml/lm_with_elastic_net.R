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
# ./bin/spark-submit examples/src/main/r/ml/lm_with_elastic_net.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-ML-lm-example")

# $example on$
# Load training data
df <- read.df("data/mllib/sample_linear_regression_data.txt", source = "libsvm")
training <- df
test <- df

# Fit a linear regression model
model <- spark.lm(training, label ~ features, regParam = 0.3, elasticNetParam = 0.8)

# Prediction
predictions <- predict(model, test)
head(predictions)

# Summarize
summary(model)

# $example off$

sparkR.session.stop()
