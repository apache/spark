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
# ./bin/spark-submit examples/src/main/r/ml/kstest.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-ML-kstest-example")

# $example on$
# Load training data
data <- data.frame(test = c(0.1, 0.15, 0.2, 0.3, 0.25, -1, -0.5))
df <- createDataFrame(data)
training <- df
test <- df

# Conduct the two-sided Kolmogorov-Smirnov (KS) test with spark.kstest
model <- spark.kstest(df, "test", "norm")

# Model summary
summary(model)
# $example off$

sparkR.session.stop()
