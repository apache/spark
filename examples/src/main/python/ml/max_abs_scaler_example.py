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
from pyspark.ml.feature import MaxAbsScaler
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MaxAbsScalerExample")\
        .getOrCreate()

    # $example on$
    dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    scaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")

    # Compute summary statistics and generate MaxAbsScalerModel
    scalerModel = scaler.fit(dataFrame)

    # rescale each feature to range [-1, 1].
    scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
    # $example off$

    spark.stop()
