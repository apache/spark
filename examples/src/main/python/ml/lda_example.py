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


from __future__ import print_function

from pyspark.sql import SparkSession

# $example on$
from pyspark.ml.clustering import LDA
from pyspark.mllib.linalg import Vectors
from pyspark.sql import Row
# $example off$


"""
A simple example demonstrating LDA.
"""


if __name__ == "__main__":
    spark = SparkSession.builder.appName("PythonLDAExample").getOrCreate()

    # $example on$
    # Loads data
    data = spark.read.text("data/mllib/sample_lda_data.txt").rdd
    parsed = data \
        .map(lambda row: Row(features=Vectors.dense([float(x) for x in row.value.split(' ')])))
    dataset = spark.createDataFrame(parsed)

    # Trains a LDA model
    lda = LDA(k=10, maxIter=10)

    model = lda.fit(dataset)

    ll = model.logLikelihood(dataset)
    lp = model.logPerplexity(dataset)
    print(ll)
    print(lp)

    # describeTopics
    topics = model.describeTopics(3)

    # Shows the result
    transformed = model.transform(dataset)
    topics.show(truncate=False)
    transformed.show(truncate=False)
    # $example off$

    spark.stop()
