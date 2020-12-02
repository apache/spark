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
from pyspark.ml.feature import Bucketizer
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("BucketizerExample")\
        .getOrCreate()

    # $example on$
    splits = [-float("inf"), -0.5, 0.0, 0.5, float("inf")]

    data = [(-999.9,), (-0.5,), (-0.3,), (0.0,), (0.2,), (999.9,)]
    dataFrame = spark.createDataFrame(data, ["features"])

    bucketizer = Bucketizer(splits=splits, inputCol="features", outputCol="bucketedFeatures")

    # Transform original data into its bucket index.
    bucketedData = bucketizer.transform(dataFrame)

    print("Bucketizer output with %d buckets" % (len(bucketizer.getSplits())-1))
    bucketedData.show()
    # $example off$

    spark.stop()
