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
from pyspark.ml.feature import PolynomialExpansion
from pyspark.ml.linalg import Vectors
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PolynomialExpansionExample")\
        .getOrCreate()

    # $example on$
    df = spark.createDataFrame([
        (Vectors.dense([2.0, 1.0]),),
        (Vectors.dense([0.0, 0.0]),),
        (Vectors.dense([3.0, -1.0]),)
    ], ["features"])

    polyExpansion = PolynomialExpansion(degree=3, inputCol="features", outputCol="polyFeatures")
    polyDF = polyExpansion.transform(df)

    polyDF.show(truncate=False)
    # $example off$

    spark.stop()
