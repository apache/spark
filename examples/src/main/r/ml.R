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
# ./bin/sparkR examples/src/main/r/ml.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkContext and SQLContext
sc <- sparkR.init(appName="SparkR-ML-example")
sqlContext <- sparkRSQL.init(sc)

# Train GLM of family 'gaussian'
training1 <- suppressWarnings(createDataFrame(sqlContext, iris))
test1 <- training1
model1 <- glm(Sepal_Length ~ Sepal_Width + Species, training1, family = "gaussian")

# Model summary
summary(model1)

# Prediction
predictions1 <- predict(model1, test1)
head(select(predictions1, "Sepal_Length", "prediction"))

# Train GLM of family 'binomial'
training2 <- filter(training1, training1$Species != "setosa")
test2 <- training2
model2 <- glm(Species ~ Sepal_Length + Sepal_Width, data = training2, family = "binomial")

# Model summary
summary(model2)

# Prediction (Currently the output of prediction for binomial GLM is the indexed label,
# we need to transform back to the original string label later)
predictions2 <- predict(model2, test2)
head(select(predictions2, "Species", "prediction"))

# Stop the SparkContext now
sparkR.stop()
