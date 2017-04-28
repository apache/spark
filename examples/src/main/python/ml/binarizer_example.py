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

from pyspark.sql import SparkSession
# $example on$
from pyspark.ml.feature import Binarizer
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("BinarizerExample")\
        .getOrCreate()

    # $example on$
    continuousDataFrame = spark.createDataFrame([
        (0, 0.1),
        (1, 0.8),
        (2, 0.2)
    ], ["id", "feature"])

    binarizer = Binarizer(threshold=0.5, inputCol="feature", outputCol="binarized_feature")

    binarizedDataFrame = binarizer.transform(continuousDataFrame)

    print("Binarizer output with Threshold = %f" % binarizer.getThreshold())
    binarizedDataFrame.show()
    # $example off$

    spark.stop()
