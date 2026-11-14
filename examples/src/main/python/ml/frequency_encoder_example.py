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
from pyspark.ml.feature import FrequencyEncoder

# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("FrequencyEncoderExample").getOrCreate()

    # Note: categorical features are usually first encoded with StringIndexer
    # $example on$
    df = spark.createDataFrame(
        [
            (0.0, 1.0),
            (1.0, 0.0),
            (2.0, 1.0),
            (0.0, 2.0),
            (0.0, 1.0),
            (2.0, 0.0),
        ],
        ["categoryIndex1", "categoryIndex2"],
    )

    # proportions of the training rows, the default
    encoder = FrequencyEncoder(
        inputCols=["categoryIndex1", "categoryIndex2"],
        outputCols=["categoryIndex1Freq", "categoryIndex2Freq"],
    )
    model = encoder.fit(df)
    encoded = model.transform(df)
    encoded.show()

    # raw counts instead of proportions
    count_encoder = FrequencyEncoder(
        inputCols=["categoryIndex1", "categoryIndex2"],
        outputCols=["categoryIndex1Count", "categoryIndex2Count"],
        normalize=False,
    )
    count_model = count_encoder.fit(df)
    count_encoded = count_model.transform(df)
    count_encoded.show()
    # $example off$

    spark.stop()
