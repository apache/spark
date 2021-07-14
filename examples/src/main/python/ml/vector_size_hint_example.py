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
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import (VectorSizeHint, VectorAssembler)
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("VectorSizeHintExample")\
        .getOrCreate()

    # $example on$
    dataset = spark.createDataFrame(
        [(0, 18, 1.0, Vectors.dense([0.0, 10.0, 0.5]), 1.0),
         (0, 18, 1.0, Vectors.dense([0.0, 10.0]), 0.0)],
        ["id", "hour", "mobile", "userFeatures", "clicked"])

    sizeHint = VectorSizeHint(
        inputCol="userFeatures",
        handleInvalid="skip",
        size=3)

    datasetWithSize = sizeHint.transform(dataset)
    print("Rows where 'userFeatures' is not the right size are filtered out")
    datasetWithSize.show(truncate=False)

    assembler = VectorAssembler(
        inputCols=["hour", "mobile", "userFeatures"],
        outputCol="features")

    # This dataframe can be used by downstream transformers as before
    output = assembler.transform(datasetWithSize)
    print("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(truncate=False)
    # $example off$

    spark.stop()
