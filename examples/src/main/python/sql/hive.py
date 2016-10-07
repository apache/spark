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

# $example on:spark_hive$
from os.path import expanduser, join

from pyspark.sql import SparkSession
from pyspark.sql import Row
# $example off:spark_hive$

"""
A simple example demonstrating Spark SQL Hive integration.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/hive.py
"""


if __name__ == "__main__":
    # $example on:spark_hive$
    # warehouse_location points to the default location for managed databases and tables
    warehouse_location = 'file:${system:user.dir}/spark-warehouse'

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Hive integration example") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    # spark is an existing SparkSession
    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    # Queries are expressed in HiveQL
    spark.sql("SELECT * FROM src").show()
    # +---+-------+
    # |key|  value|
    # +---+-------+
    # |238|val_238|
    # | 86| val_86|
    # |311|val_311|
    # ...

    # Aggregation queries are also supported.
    spark.sql("SELECT COUNT(*) FROM src").show()
    # +--------+
    # |count(1)|
    # +--------+
    # |    500 |
    # +--------+

    # The results of SQL queries are themselves DataFrames and support all normal functions.
    sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    # The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
    stringsDS = sqlDF.rdd.map(lambda row: "Key: %d, Value: %s" % (row.key, row.value))
    for record in stringsDS.collect():
        print(record)
    # Key: 0, Value: val_0
    # Key: 0, Value: val_0
    # Key: 0, Value: val_0
    # ...

    # You can also use DataFrames to create temporary views within a SparkSession.
    Record = Row("key", "value")
    recordsDF = spark.createDataFrame([Record(i, "val_" + str(i)) for i in range(1, 101)])
    recordsDF.createOrReplaceTempView("records")

    # Queries can then join DataFrame data with data stored in Hive.
    spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    # +---+------+---+------+
    # |key| value|key| value|
    # +---+------+---+------+
    # |  2| val_2|  2| val_2|
    # |  4| val_4|  4| val_4|
    # |  5| val_5|  5| val_5|
    # ...
    # $example off:spark_hive$

    spark.stop()
