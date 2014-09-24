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

import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: sql <file>"
        exit(-1)
    sc = SparkContext(appName="PythonSQL")
    sqlContext = SQLContext(sc)

    # A JSON dataset is pointed to by path.
    # The path can be either a single text file or a directory storing text files.
    path = "examples/src/main/resources/people.json"
    # Create a SchemaRDD from the file(s) pointed to by path
    people = sqlContext.jsonFile(path)

    # The inferred schema can be visualized using the printSchema() method.
    people.printSchema()
    # root
    #  |-- age: IntegerType
    #  |-- name: StringType

    # Register this SchemaRDD as a table.
    people.registerAsTable("people")

    # SQL statements can be run by using the sql methods provided by sqlContext
    teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    for each in teenagers.collect():
        print each[0]

    sc.stop()
