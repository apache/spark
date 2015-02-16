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

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.classification import LogisticRegression


"""
A simple text classification pipeline that recognizes "spark" from
input text. This is to show how to create and configure a Spark ML
pipeline in Python. Run with:

  bin/spark-submit examples/src/main/python/ml/simple_text_classification_pipeline.py
"""


if __name__ == "__main__":
    sc = SparkContext(appName="SimpleTextClassificationPipeline")
    sqlCtx = SQLContext(sc)

    # Prepare training documents, which are labeled.
    LabeledDocument = Row("id", "text", "label")
    training = sc.parallelize([(0L, "a b c d e spark", 1.0),
                               (1L, "b d", 0.0),
                               (2L, "spark f g h", 1.0),
                               (3L, "hadoop mapreduce", 0.0)]) \
        .map(lambda x: LabeledDocument(*x)).toDF()

    # Configure an ML pipeline, which consists of tree stages: tokenizer, hashingTF, and lr.
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
    lr = LogisticRegression(maxIter=10, regParam=0.01)
    pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

    # Fit the pipeline to training documents.
    model = pipeline.fit(training)

    # Prepare test documents, which are unlabeled.
    Document = Row("id", "text")
    test = sc.parallelize([(4L, "spark i j k"),
                           (5L, "l m n"),
                           (6L, "mapreduce spark"),
                           (7L, "apache hadoop")]) \
        .map(lambda x: Document(*x)).toDF()

    # Make predictions on test documents and print columns of interest.
    prediction = model.transform(test)
    selected = prediction.select("id", "text", "prediction")
    for row in selected.collect():
        print row

    sc.stop()
