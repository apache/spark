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
from pyspark.ml.feature import QuantileDiscretizer
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("QuantileDiscretizerExample")\
        .getOrCreate()

    # $example on$
    data = [(0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2)]
    df = spark.createDataFrame(data, ["id", "hour"])
    # $example off$

    # Output of QuantileDiscretizer for such small datasets can depend on the number of
    # partitions. Here we force a single partition to ensure consistent results.
    # Note this is not necessary for normal use cases
    df = df.repartition(1)

    # $example on$
    discretizer = QuantileDiscretizer(numBuckets=3, inputCol="hour", outputCol="result")

    result = discretizer.fit(df).transform(df)
    result.show()
    # $example off$

    spark.stop()
