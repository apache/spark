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
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("OneHotEncoderEstimatorExample")\
        .getOrCreate()

    # $example on$
    df = spark.createDataFrame([
        (0, "a", "x"),
        (1, "b", "y"),
        (2, "c", "y"),
        (3, "a", "z"),
        (4, "a", "y"),
        (5, "c", "z")
    ], ["id", "category1", "category2"])

    # TODO: Replace this with multi-column API of StringIndexer once SPARK-11215 is merged.
    stringIndexer1 = StringIndexer(inputCol="category1", outputCol="categoryIndex1")
    stringIndexer2 = StringIndexer(inputCol="category2", outputCol="categoryIndex2")

    stringIndexerModel1 = stringIndexer1.fit(df)
    indexed1 = stringIndexerModel1.transform(df)
    stringIndexerModel2 = stringIndexer2.fit(indexed1)
    indexed2 = stringIndexerModel2.transform(indexed1)

    encoder = OneHotEncoderEstimator(inputCols=["categoryIndex1", "categoryIndex2"],
                                     outputCols=["categoryVec1", "categoryVec2"])
    model = encoder.fit(indexed2)
    encoded = model.transform(indexed2)
    encoded.show()
    # $example off$

    spark.stop()
