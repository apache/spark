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

"""
Pipeline Example.
"""

# $example on$
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import Word2Vec
from pyspark.ml.FakeTransformer import Int2Str
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PurePipelineExample") \
        .getOrCreate()

    # $example on$
    # Prepare training documents from a list of (id, text, label) tuples.
    training = spark.createDataFrame([
        (1,),
        (2,),
        (3,),
        (4,)], ["text"])

    # Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    int2str = Int2Str(suffix="xxx", inputCol="text", outputCol="sentence")
    word2vec = Word2Vec(vectorSize=5, seed=42, inputCol="sentence", outputCol="model")
    pipeline = Pipeline(stages=[int2str, word2vec])

    # Fit the pipeline to training documents.
    model = pipeline.fit(training)

    print(model.stages)

    model.transform(training).show()
    # $example off$

    spark.stop()