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

from pyspark import SparkContext
# $example on$
from pyspark.ml.feature import IndexToString, StringIndexer
# $example off$
from pyspark.sql import SQLContext

if __name__ == "__main__":
    sc = SparkContext(appName="IndexToStringExample")
    sqlContext = SQLContext(sc)

    # $example on$
    df = sqlContext.createDataFrame(
        [(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")],
        ["id", "category"])

    stringIndexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
    model = stringIndexer.fit(df)
    indexed = model.transform(df)

    converter = IndexToString(inputCol="categoryIndex", outputCol="originalCategory")
    converted = converter.transform(indexed)

    converted.select("id", "originalCategory").show()
    # $example off$

    sc.stop()
