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

# Initialize SparkSession
sparkR.session(appName = "SparkR-ML-example")

############################ spark.glm and glm ##############################################
# $example on:glm$
irisDF <- suppressWarnings(createDataFrame(iris))
# Fit a generalized linear model of family "gaussian" with spark.glm
gaussianDF <- irisDF
gaussianTestDF <- irisDF
gaussianGLM <- spark.glm(gaussianDF, Sepal_Length ~ Sepal_Width + Species, family = "gaussian")

# Model summary
summary(gaussianGLM)

# Prediction
gaussianPredictions <- predict(gaussianGLM, gaussianTestDF)
showDF(gaussianPredictions)

# Fit a generalized linear model with glm (R-compliant)
gaussianGLM2 <- glm(Sepal_Length ~ Sepal_Width + Species, gaussianDF, family = "gaussian")
summary(gaussianGLM2)

# Fit a generalized linear model of family "binomial" with spark.glm
binomialDF <- filter(irisDF, irisDF$Species != "setosa")
binomialTestDF <- binomialDF
binomialGLM <- spark.glm(binomialDF, Species ~ Sepal_Length + Sepal_Width, family = "binomial")

# Model summary
summary(binomialGLM)

# Prediction
binomialPredictions <- predict(binomialGLM, binomialTestDF)
showDF(binomialPredictions)
# $example off:glm$
############################ spark.survreg ##############################################
# $example on:survreg$
# Use the ovarian dataset available in R survival package
library(survival)

# Fit an accelerated failure time (AFT) survival regression model with spark.survreg
ovarianDF <- suppressWarnings(createDataFrame(ovarian))
aftDF <- ovarianDF
aftTestDF <- ovarianDF
aftModel <- spark.survreg(aftDF, Surv(futime, fustat) ~ ecog_ps + rx)

# Model summary
summary(aftModel)

# Prediction
aftPredictions <- predict(aftModel, aftTestDF)
showDF(aftPredictions)
# $example off:survreg$
############################ spark.naiveBayes ##############################################
# $example on:naiveBayes$
# Fit a Bernoulli naive Bayes model with spark.naiveBayes
titanic <- as.data.frame(Titanic)
titanicDF <- createDataFrame(titanic[titanic$Freq > 0, -5])
nbDF <- titanicDF
nbTestDF <- titanicDF
nbModel <- spark.naiveBayes(nbDF, Survived ~ Class + Sex + Age)

# Model summary
summary(nbModel)

# Prediction
nbPredictions <- predict(nbModel, nbTestDF)
showDF(nbPredictions)
# $example off:naiveBayes$
############################ spark.kmeans ##############################################
# $example on:kmeans$
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
# $example off:kmeans$
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
families <- c("gaussian", "poisson")
train <- function(family) {
  model <- glm(Sepal.Length ~ Sepal.Width + Species, iris, family = family)
  summary(model)
}
model.summaries <- spark.lapply(families, train)

# Print the summary of each model
print(model.summaries)


# Stop the SparkSession now
sparkR.session.stop()
