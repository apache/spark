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
# ./bin/spark-submit examples/src/main/r/ml/lda.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-ML-lda-example")

# $example on$
# Load training data
df <- read.df("data/mllib/sample_lda_libsvm_data.txt", source = "libsvm")
training <- df
test <- df

# Fit a latent dirichlet allocation model with spark.lda
model <- spark.lda(training, k = 10, maxIter = 10)

# Model summary
summary(model)

# Posterior probabilities
posterior <- spark.posterior(model, test)
showDF(posterior)

# The log perplexity of the LDA model
logPerplexity <- spark.perplexity(model, test)
print(paste0("The upper bound bound on perplexity: ", logPerplexity))
# $example off$
