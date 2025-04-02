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
from pyspark.ml.feature import RFormula
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("RFormulaExample")\
        .getOrCreate()

    # $example on$
    dataset = spark.createDataFrame(
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

    spark.stop()
