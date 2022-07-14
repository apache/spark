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
An example for VarianceThresholdSelector.
Run with:
  bin/spark-submit examples/src/main/python/ml/variance_threshold_selector_example.py
"""
from pyspark.sql import SparkSession
# $example on$
from pyspark.ml.feature import VarianceThresholdSelector
from pyspark.ml.linalg import Vectors
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("VarianceThresholdSelectorExample")\
        .getOrCreate()

    # $example on$
    df = spark.createDataFrame([
        (1, Vectors.dense([6.0, 7.0, 0.0, 7.0, 6.0, 0.0])),
        (2, Vectors.dense([0.0, 9.0, 6.0, 0.0, 5.0, 9.0])),
        (3, Vectors.dense([0.0, 9.0, 3.0, 0.0, 5.0, 5.0])),
        (4, Vectors.dense([0.0, 9.0, 8.0, 5.0, 6.0, 4.0])),
        (5, Vectors.dense([8.0, 9.0, 6.0, 5.0, 4.0, 4.0])),
        (6, Vectors.dense([8.0, 9.0, 6.0, 0.0, 0.0, 0.0]))], ["id", "features"])

    selector = VarianceThresholdSelector(varianceThreshold=8.0, outputCol="selectedFeatures")

    result = selector.fit(df).transform(df)

    print("Output: Features with variance lower than %f are removed." %
          selector.getVarianceThreshold())
    result.show()
    # $example off$

    spark.stop()
