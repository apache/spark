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
training <- read.df("data/mllib/sample_multiclass_classification_data.txt", source = "libsvm")
# Fit a generalized linear model of family "gaussian" with spark.glm
df_list <- randomSplit(training, c(7, 3), 2)
gaussianDF <- df_list[[1]]
gaussianTestDF <- df_list[[2]]
gaussianGLM <- spark.glm(gaussianDF, label ~ features, family = "gaussian")

# Model summary
summary(gaussianGLM)

# Prediction
gaussianPredictions <- predict(gaussianGLM, gaussianTestDF)
head(gaussianPredictions)

# Fit a generalized linear model with glm (R-compliant)
gaussianGLM2 <- glm(label ~ features, gaussianDF, family = "gaussian")
summary(gaussianGLM2)

# Fit a generalized linear model of family "binomial" with spark.glm
training2 <- read.df("data/mllib/sample_multiclass_classification_data.txt", source = "libsvm")
training2 <- transform(training2, label = cast(training2$label > 1, "integer"))
df_list2 <- randomSplit(training2, c(7, 3), 2)
binomialDF <- df_list2[[1]]
binomialTestDF <- df_list2[[2]]
binomialGLM <- spark.glm(binomialDF, label ~ features, family = "binomial")

# Model summary
summary(binomialGLM)

# Prediction
binomialPredictions <- predict(binomialGLM, binomialTestDF)
head(binomialPredictions)

# Fit a generalized linear model of family "tweedie" with spark.glm
training3 <- read.df("data/mllib/sample_multiclass_classification_data.txt", source = "libsvm")
tweedieDF <- transform(training3, label = training3$label * exp(randn(10)))
tweedieGLM <- spark.glm(tweedieDF, label ~ features, family = "tweedie",
                        var.power = 1.2, link.power = 0)

# Model summary
summary(tweedieGLM)
# $example off$

sparkR.session.stop()
