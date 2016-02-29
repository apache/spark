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
from pyspark.sql import SQLContext
# $example on$
from pyspark.ml.feature import StopWordsRemover
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="StopWordsRemoverExample")
    sqlContext = SQLContext(sc)

    # $example on$
    sentenceData = sqlContext.createDataFrame([
        (0, ["I", "saw", "the", "red", "baloon"]),
        (1, ["Mary", "had", "a", "little", "lamb"])
    ], ["label", "raw"])

    remover = StopWordsRemover(inputCol="raw", outputCol="filtered")
    remover.transform(sentenceData).show(truncate=False)
    # $example off$

    sc.stop()
