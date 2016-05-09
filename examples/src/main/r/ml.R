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
# ./bin/spark-submit examples/src/main/r/ml.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkContext and SQLContext
sc <- sparkR.init(appName="SparkR-ML-example")
sqlContext <- sparkRSQL.init(sc)

############################ spark.glm and glm ##############################################

# Fit a generalized linear model with spark.glm
training1 <- suppressWarnings(createDataFrame(sqlContext, iris))
test1 <- training1
model1 <- spark.glm(training1, Sepal_Length ~ Sepal_Width + Species, family = "gaussian")

# Model summary
summary(model1)

# Prediction
predictions1 <- predict(model1, test1)
showDF(predictions1)

# Fit a generalized linear model with glm (R-compliant)
sameModel <- glm(Sepal_Length ~ Sepal_Width + Species, training1, family = "gaussian")
summary(sameModel)

############################ spark.survreg ##############################################

# Use the ovarian dataset available in R survival package
library(survival)

# Fit an accelerated failure time (AFT) survival regression model with spark.survreg
training2 <- suppressWarnings(createDataFrame(sqlContext, ovarian))
test2 <- training2
model2 <- spark.survreg(training2, Surv(futime, fustat) ~ ecog_ps + rx)

# Model summary
summary(model2)

# Prediction
predictions2 <- predict(model2, test2)
showDF(predictions2)

############################ spark.naiveBayes ##############################################

# Fit a Bernoulli naive Bayes model with spark.naiveBayes
titanic <- as.data.frame(Titanic)
training3 <- suppressWarnings(createDataFrame(sqlContext, titanic[titanic$Freq > 0, -5]))
test3 <- training3
model3 <- spark.naiveBayes(training3, Survived ~ Class + Sex + Age)

# Model summary
summary(model3)

# Prediction
predictions3 <- predict(model3, test3)
showDF(predictions3)

############################ spark.kmeans ##############################################

# Fit a k-means model with spark.kmeans
training4 <- suppressWarnings(createDataFrame(sqlContext, iris))
test4 <- training4
model4 <- spark.kmeans(training4, ~ Sepal_Length + Sepal_Width + Petal_Length + Petal_Width, k = 3)

# Model summary
summary(model4)

# Get fitted result from the k-means model
showDF(fitted(model4))

# Prediction
predictions4 <- predict(model4, test4)
showDF(predictions4)

############################ model read/write ##############################################

# Save and then load a fitted MLlib model
modelPath <- tempfile(pattern = "ml", fileext = ".tmp")
write.ml(model1, modelPath)
sameModel <- read.ml(modelPath)

# Check model summary
summary(sameModel)

# Check model prediction
samePredictions <- predict(sameModel, test1)
showDF(samePredictions)

unlink(modelPath)

# Stop the SparkContext now
sparkR.stop()
