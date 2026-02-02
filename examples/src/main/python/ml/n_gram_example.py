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

# $example on$
from pyspark.ml.feature import NGram
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("NGramExample")\
        .getOrCreate()

    # $example on$
    wordDataFrame = spark.createDataFrame([
        (0, ["Hi", "I", "heard", "about", "Spark"]),
        (1, ["I", "wish", "Java", "could", "use", "case", "classes"]),
        (2, ["Logistic", "regression", "models", "are", "neat"])
    ], ["id", "words"])

    ngram = NGram(n=2, inputCol="words", outputCol="ngrams")

    ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.select("ngrams").show(truncate=False)
    # $example off$

    spark.stop()
