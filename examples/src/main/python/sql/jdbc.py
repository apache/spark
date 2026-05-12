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

"""
A simple example demonstrating Spark SQL JDBC integration.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/jdbc.py [jdbc_url]
"""
import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: jdbc.py <jdbc_url>", file=sys.stderr)
        sys.exit(-1)
    url = sys.argv[1]

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL JDBC integration example") \
        .getOrCreate()

    # 1. Create a DataFrame
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "name"])

    # 2. Write data to a JDBC source
    df.write.jdbc(url, "test_table", mode="overwrite", properties={})

    # 3. Read data from a JDBC source
    jdbcDF = spark.read.jdbc(url, "test_table", properties={})
    jdbcDF.show()

    spark.stop()
