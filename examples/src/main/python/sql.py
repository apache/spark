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

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonSQL")\
        .getOrCreate()

    # A list of Rows. Infer schema from the first row, create a DataFrame and print the schema
    rows = [Row(name="John", age=19), Row(name="Smith", age=23), Row(name="Sarah", age=18)]
    some_df = spark.createDataFrame(rows)
    some_df.printSchema()

    # A list of tuples
    tuples = [("John", 19), ("Smith", 23), ("Sarah", 18)]
    # Schema with two fields - person_name and person_age
    schema = StructType([StructField("person_name", StringType(), False),
                        StructField("person_age", IntegerType(), False)])
    # Create a DataFrame by applying the schema to the RDD and print the schema
    another_df = spark.createDataFrame(tuples, schema)
    another_df.printSchema()
    # root
    #  |-- age: long (nullable = true)
    #  |-- name: string (nullable = true)

    # A JSON dataset is pointed to by path.
    # The path can be either a single text file or a directory storing text files.
    if len(sys.argv) < 2:
        path = "file://" + \
            os.path.join(os.environ['SPARK_HOME'], "examples/src/main/resources/people.json")
    else:
        path = sys.argv[1]
    # Create a DataFrame from the file(s) pointed to by path
    people = spark.read.json(path)
    # root
    #  |-- person_name: string (nullable = false)
    #  |-- person_age: integer (nullable = false)

    # The inferred schema can be visualized using the printSchema() method.
    people.printSchema()
    # root
    #  |-- age: long (nullable = true)
    #  |-- name: string (nullable = true)

    # Creates a temporary view using the DataFrame.
    people.createOrReplaceTempView("people")

    # SQL statements can be run by using the sql methods provided by `spark`
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    for each in teenagers.collect():
        print(each[0])

    spark.stop()
