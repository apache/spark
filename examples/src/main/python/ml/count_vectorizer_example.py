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

from pyspark.sql import SparkSession
# $example on$
from pyspark.ml.feature import CountVectorizer
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("CountVectorizerExample")\
        .getOrCreate()

    # $example on$
    # Input data: Each row is a bag of words with a ID.
    df = spark.createDataFrame([
        (0, "a b c".split(" ")),
        (1, "a b b c a".split(" "))
    ], ["id", "words"])

    # fit a CountVectorizerModel from the corpus.
    cv = CountVectorizer(inputCol="words", outputCol="features", vocabSize=3, minDF=2.0)

    model = cv.fit(df)

    result = model.transform(df)
    result.show(truncate=False)
    # $example off$

    spark.stop()
