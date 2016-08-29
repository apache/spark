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
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import Row
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("VectorSlicerExample")\
        .getOrCreate()

    # $example on$
    df = spark.createDataFrame([
        Row(userFeatures=Vectors.sparse(3, {0: -2.0, 1: 2.3}),),
        Row(userFeatures=Vectors.dense([-2.0, 2.3, 0.0]),)])

    slicer = VectorSlicer(inputCol="userFeatures", outputCol="features", indices=[1])

    output = slicer.transform(df)

    output.select("userFeatures", "features").show()
    # $example off$

    spark.stop()
