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

"""
Create test data in HBase first:

hbase(main):016:0> create 'test', 'f1'
0 row(s) in 1.0430 seconds

hbase(main):017:0> put 'test', 'row1', 'f1', 'value1'
0 row(s) in 0.0130 seconds

hbase(main):018:0> put 'test', 'row2', 'f1', 'value2'
0 row(s) in 0.0030 seconds

hbase(main):019:0> put 'test', 'row3', 'f1', 'value3'
0 row(s) in 0.0050 seconds

hbase(main):020:0> put 'test', 'row4', 'f1', 'value4'
0 row(s) in 0.0110 seconds

hbase(main):021:0> scan 'test'
ROW                           COLUMN+CELL
 row1                         column=f1:, timestamp=1401883411986, value=value1
 row2                         column=f1:, timestamp=1401883415212, value=value2
 row3                         column=f1:, timestamp=1401883417858, value=value3
 row4                         column=f1:, timestamp=1401883420805, value=value4
4 row(s) in 0.0240 seconds
"""
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, """
        Usage: hbase_inputformat <host> <table>

        Run with example jar:
        ./bin/spark-submit --driver-class-path /path/to/example/jar /path/to/examples/hbase_inputformat.py <host> <table>
        Assumes you have some data in HBase already, running on <host>, in <table>
        """
        exit(-1)

    host = sys.argv[1]
    table = sys.argv[2]
    sc = SparkContext(appName="HBaseInputFormat")

    conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
    hbase_rdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter="org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter",
        valueConverter="org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter",
        conf=conf)
    output = hbase_rdd.collect()
    for (k, v) in output:
        print (k, v)
