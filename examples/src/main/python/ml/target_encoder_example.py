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
from pyspark.ml.feature import TargetEncoder

# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TargetEncoderExample").getOrCreate()

    # Note: categorical features are usually first encoded with StringIndexer
    # $example on$
    df = spark.createDataFrame(
        [
            (0.0, 1.0, 0, 10.0),
            (1.0, 0.0, 1, 20.0),
            (2.0, 1.0, 0, 30.0),
            (0.0, 2.0, 1, 40.0),
            (0.0, 1.0, 0, 50.0),
            (2.0, 0.0, 1, 60.0),
        ],
        ["categoryIndex1", "categoryIndex2", "binaryLabel", "continuousLabel"],
    )

    # binary target
    encoder = TargetEncoder(
        inputCols=["categoryIndex1", "categoryIndex2"],
        outputCols=["categoryIndex1Target", "categoryIndex2Target"],
        labelCol="binaryLabel",
        targetType="binary"
    )
    model = encoder.fit(df)
    encoded = model.transform(df)
    encoded.show()

    # continuous target
    encoder = TargetEncoder(
        inputCols=["categoryIndex1", "categoryIndex2"],
        outputCols=["categoryIndex1Target", "categoryIndex2Target"],
        labelCol="continuousLabel",
        targetType="continuous"
    )

    model = encoder.fit(df)
    encoded = model.transform(df)
    encoded.show()
    # $example off$

    spark.stop()
