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
A simple example demonstrating Spark SQL Data Source.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/SqlDataSourceExample.py
"""


def runBasicDataSourceExample(spark):
    # $example on:generic_load_save_functions$
    df = spark.read.load("examples/src/main/resources/users.parquet")
    df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    # $example off:generic_load_save_functions$

    # $example on:manual_load_options$
    df = spark.read.load("examples/src/main/resources/people.json", format="json")
    df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")
    # $example off:manual_load_options$

    # $example on:direct_sql$
    df = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
    # $example off:direct_sql$


def runBasicParquetExample(spark):
    # $example on:basic_parquet_example$
    peopleDF = spark.read.json("examples/src/main/resources/people.json")

    # DataFrames can be saved as Parquet files, maintaining the schema information.
    peopleDF.write.parquet("people.parquet")

    # Read in the Parquet file created above.
    # Parquet files are self-describing so the schema is preserved.
    # The result of loading a parquet file is also a DataFrame.
    parquetFile = spark.read.parquet("people.parquet")

    # Parquet files can also be used to create a temporary view and then used in SQL statements.
    parquetFile.createOrReplaceTempView("parquetFile")
    teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    teenagers.show()
    # +------+
    # |  name|
    # +------+
    # |Justin|
    # +------+
    # $example off:basic_parquet_example$

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PythonSQL") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    runBasicDataSourceExample(spark)
    runBasicParquetExample(spark)

    spark.stop()
