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
Read data file users.parquet in local Spark distro:

$ cd $SPARK_HOME
$ export AVRO_PARQUET_JARS=/path/to/parquet-avro-1.5.0.jar
$ ./bin/spark-submit
        ./examples/src/main/python/parquet_inputformat.py \\
        examples/src/main/resources/users.parquet
<...lots of log output...>
Row(name='Alyssa', favorite_color=None, favorite_numbers=[3, 9, 15, 20])
Row(name='Ben', favorite_color='red', favorite_numbers=[])
<...more log output...>
"""
import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: parquet_inputformat.py <data_file>

        Run with example jar:
        ./bin/spark-submit /path/to/examples/parquet_inputformat.py <data_file>
        Assumes you have Parquet data stored in <data_file>.
        """, file=sys.stderr)
        sys.exit(-1)

    path = sys.argv[1]

    spark = SparkSession\
        .builder\
        .appName("ParquetInputFormat")\
        .getOrCreate()

    sc = spark.sparkContext

    parquet_rdd = spark.read.parquet(path).rdd
    output = parquet_rdd.map(lambda x: x[1]).collect()
    for k in output:
        print(k)

    spark.stop()
