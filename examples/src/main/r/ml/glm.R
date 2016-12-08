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
# ./bin/spark-submit examples/src/main/r/ml/glm.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-ML-glm-example")

# $example on$
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
# Note: Filter out "setosa" from label column (two labels left) to match "binomial" family.
binomialDF <- filter(irisDF, irisDF$Species != "setosa")
binomialTestDF <- binomialDF
binomialGLM <- spark.glm(binomialDF, Species ~ Sepal_Length + Sepal_Width, family = "binomial")

# Model summary
summary(binomialGLM)

# Prediction
binomialPredictions <- predict(binomialGLM, binomialTestDF)
showDF(binomialPredictions)
# $example off$
