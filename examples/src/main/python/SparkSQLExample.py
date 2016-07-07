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

from pyspark.sql import SparkSession

"""
A simple example demonstrating Spark SQL.
Run with:
  ./bin/spark-submit examples/src/main/python/SparkSQLExample.py
"""

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonSQL")\
        .config("spark.some.config.option", "some-value")\
        .getOrCreate()

    # spark is an existing SparkSession
    df = spark.read.json("examples/src/main/resources/people.json")

    # Displays the content of the DataFrame to stdout
    df.show()
    ## age  name
    ## null Michael
    ## 30   Andy
    ## 19   Justin

    # Print the schema in a tree format
    df.printSchema()
    ## root
    ## |-- age: long (nullable = true)
    ## |-- name: string (nullable = true)

    # Select only the "name" column
    df.select("name").show()
    ## name
    ## Michael
    ## Andy
    ## Justin

    # Select everybody, but increment the age by 1
    df.select(df['name'], df['age'] + 1).show()
    ## name    (age + 1)
    ## Michael null
    ## Andy    31
    ## Justin  20

    # Select people older than 21
    df.filter(df['age'] > 21).show()
    ## age name
    ## 30  Andy

    # Count people by age
    df.groupBy("age").count().show()
    ## age  count
    ## null 1
    ## 19   1
    ## 30   1

    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    # SQL statements can be run by using the sql methods provided by `spark`.
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    teenagers.show()

    sc = spark.sparkContext


    spark.stop()

