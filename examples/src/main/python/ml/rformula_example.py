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
from pyspark.ml.feature import RFormula
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="RFormulaExample")
    sqlContext = SQLContext(sc)

    # $example on$
    dataset = sqlContext.createDataFrame(
        [(7, "US", 18, 1.0),
         (8, "CA", 12, 0.0),
         (9, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    formula = RFormula(
        formula="clicked ~ country + hour",
        featuresCol="features",
        labelCol="label")
    output = formula.fit(dataset).transform(dataset)
    output.select("features", "label").show()
    # $example off$

    sc.stop()
