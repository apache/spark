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
from pyspark.ml.feature import FeatureHasher
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("FeatureHasherExample")\
        .getOrCreate()

    # $example on$
    dataset = spark.createDataFrame([
        (2.2, True, "1", "foo"),
        (3.3, False, "2", "bar"),
        (4.4, False, "3", "baz"),
        (5.5, False, "4", "foo")
    ], ["real", "bool", "stringNum", "string"])

    hasher = FeatureHasher(inputCols=["real", "bool", "stringNum", "string"],
                           outputCol="features")

    featurized = hasher.transform(dataset)
    featurized.show(truncate=False)
    # $example off$

    spark.stop()
