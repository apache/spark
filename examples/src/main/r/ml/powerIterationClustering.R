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
# ./bin/spark-submit examples/src/main/r/ml/powerIterationClustering.R

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-ML-powerIterationCLustering-example")

# $example on$
df <- createDataFrame(list(list(0L, 1L, 1.0), list(0L, 2L, 1.0),
                           list(1L, 2L, 1.0), list(3L, 4L, 1.0),
                           list(4L, 0L, 0.1)),
                      schema = c("src", "dst", "weight"))
# assign clusters
clusters <- spark.assignClusters(df, k = 2L, maxIter = 20L,
                                 initMode = "degree", weightCol = "weight")

showDF(arrange(clusters, clusters$id))
# $example off$

sparkR.session.stop()
