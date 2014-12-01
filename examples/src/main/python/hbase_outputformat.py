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
Create test table in HBase first:

hbase(main):001:0> create 'test', 'f1'
0 row(s) in 0.7840 seconds

> hbase_outputformat <host> test row1 f1 q1 value1
> hbase_outputformat <host> test row2 f1 q1 value2
> hbase_outputformat <host> test row3 f1 q1 value3
> hbase_outputformat <host> test row4 f1 q1 value4

hbase(main):002:0> scan 'test'
ROW                   COLUMN+CELL
 row1                 column=f1:q1, timestamp=1405659615726, value=value1
 row2                 column=f1:q1, timestamp=1405659626803, value=value2
 row3                 column=f1:q1, timestamp=1405659640106, value=value3
 row4                 column=f1:q1, timestamp=1405659650292, value=value4
4 row(s) in 0.0780 seconds
"""
if __name__ == "__main__":
    if len(sys.argv) != 7:
        print >> sys.stderr, """
        Usage: hbase_outputformat <host> <table> <row> <family> <qualifier> <value>

        Run with example jar:
        ./bin/spark-submit --driver-class-path /path/to/example/jar \
        /path/to/examples/hbase_outputformat.py <args>
        Assumes you have created <table> with column family <family> in HBase
        running on <host> already
        """
        exit(-1)

    host = sys.argv[1]
    table = sys.argv[2]
    sc = SparkContext(appName="HBaseOutputFormat")

    conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    sc.parallelize([sys.argv[3:]]).map(lambda x: (x[0], x)).saveAsNewAPIHadoopDataset(
        conf=conf,
        keyConverter=keyConv,
        valueConverter=valueConv)

    sc.stop()
