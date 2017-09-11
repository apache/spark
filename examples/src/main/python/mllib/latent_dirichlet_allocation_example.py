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

from pyspark import SparkContext
# $example on$
from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.linalg import Vectors
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="LatentDirichletAllocationExample")  # SparkContext

    # $example on$
    # Load and parse the data
    data = sc.textFile("data/mllib/sample_lda_data.txt")
    parsedData = data.map(lambda line: Vectors.dense([float(x) for x in line.strip().split(' ')]))
    # Index documents with unique IDs
    corpus = parsedData.zipWithIndex().map(lambda x: [x[1], x[0]]).cache()

    # Cluster the documents into three topics using LDA
    ldaModel = LDA.train(corpus, k=3)

    # Output topics. Each is a distribution over words (matching word count vectors)
    print("Learned topics (as distributions over vocab of " + str(ldaModel.vocabSize())
          + " words):")
    topics = ldaModel.topicsMatrix()
    for topic in range(3):
        print("Topic " + str(topic) + ":")
        for word in range(0, ldaModel.vocabSize()):
            print(" " + str(topics[word][topic]))

    # Save and load model
    ldaModel.save(sc, "target/org/apache/spark/PythonLatentDirichletAllocationExample/LDAModel")
    sameModel = LDAModel\
        .load(sc, "target/org/apache/spark/PythonLatentDirichletAllocationExample/LDAModel")
    # $example off$

    sc.stop()
