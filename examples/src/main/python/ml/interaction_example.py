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
from pyspark.ml.feature import Interaction
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("InteractionExample")\
        .getOrCreate()

    # $example on$
    df = spark.createDataFrame([
        (0, 1, 2),
        (1, 4, 3),
        (2, 6, 1),
        (3, 10, 8),
        (4, 9, 2),
        (5, 1, 1)
    ], ["id1", "id2", "id3"])

    interaction = StringIndexer(inputCol="category", outputCol="categoryIndex")
    model = stringIndexer.fit(df)
    indexed = model.transform(df)

    encoder = OneHotEncoder(inputCol="categoryIndex", outputCol="categoryVec")
    encoded = encoder.transform(indexed)
    encoded.show()
    # $example off$

    spark.stop()
