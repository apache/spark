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

import sys

from functools import reduce
from pyspark.sql import SparkSession

"""
Read data file users.avro in local Spark distro:

$ cd $SPARK_HOME
$ ./bin/spark-submit --driver-class-path /path/to/example/jar \
> ./examples/src/main/python/avro_inputformat.py \
> examples/src/main/resources/users.avro
{u'favorite_color': None, u'name': u'Alyssa', u'favorite_numbers': [3, 9, 15, 20]}
{u'favorite_color': u'red', u'name': u'Ben', u'favorite_numbers': []}

To read name and favorite_color fields only, specify the following reader schema:

$ cat examples/src/main/resources/user.avsc
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}

$ ./bin/spark-submit --driver-class-path /path/to/example/jar \
> ./examples/src/main/python/avro_inputformat.py \
> examples/src/main/resources/users.avro examples/src/main/resources/user.avsc
{u'favorite_color': None, u'name': u'Alyssa'}
{u'favorite_color': u'red', u'name': u'Ben'}
"""
if __name__ == "__main__":
    if len(sys.argv) != 2 and len(sys.argv) != 3:
        print("""
        Usage: avro_inputformat <data_file> [reader_schema_file]

        Run with example jar:
        ./bin/spark-submit --driver-class-path /path/to/example/jar \
        /path/to/examples/avro_inputformat.py <data_file> [reader_schema_file]
        Assumes you have Avro data stored in <data_file>. Reader schema can be optionally specified
        in [reader_schema_file].
        """, file=sys.stderr)
        exit(-1)

    path = sys.argv[1]

    spark = SparkSession\
        .builder\
        .appName("AvroKeyInputFormat")\
        .getOrCreate()

    sc = spark.sparkContext

    conf = None
    if len(sys.argv) == 3:
        schema_rdd = sc.textFile(sys.argv[2], 1).collect()
        conf = {"avro.schema.input.key": reduce(lambda x, y: x + y, schema_rdd)}

    avro_rdd = sc.newAPIHadoopFile(
        path,
        "org.apache.avro.mapreduce.AvroKeyInputFormat",
        "org.apache.avro.mapred.AvroKey",
        "org.apache.hadoop.io.NullWritable",
        keyConverter="org.apache.spark.examples.pythonconverters.AvroWrapperToJavaConverter",
        conf=conf)
    output = avro_rdd.map(lambda x: x[0]).collect()
    for k in output:
        print(k)

    spark.stop()
