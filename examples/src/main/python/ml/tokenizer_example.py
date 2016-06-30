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

# $example on$
from pyspark.ml.feature import Tokenizer, RegexTokenizer
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("TokenizerExample")\
        .getOrCreate()

    # $example on$
    sentenceDataFrame = spark.createDataFrame([
        (0, "Hi I heard about Spark"),
        (1, "I wish Java could use case classes"),
        (2, "Logistic,regression,models,are,neat")
    ], ["label", "sentence"])
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    wordsDataFrame = tokenizer.transform(sentenceDataFrame)
    for words_label in wordsDataFrame.select("words", "label").take(3):
        print(words_label)
    regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")
    # alternatively, pattern="\\w+", gaps(False)
    # $example off$

    spark.stop()
