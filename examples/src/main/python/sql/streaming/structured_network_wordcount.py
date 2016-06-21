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
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: structured_network_wordcount.py <hostname> <port> <checkpoint dir>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py
    localhost 9999 <checkpoint dir>`
"""
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: network_wordcount.py <hostname> <port> <checkpoint dir>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()


    df = spark.readStream.format('socket').option('host', sys.argv[1])\
        .option('port', sys.argv[2]).load()

    words = df.select(explode(split(df.value, ' ')).alias('word'))
    wordCounts = words.groupBy('word').count()

    wordCounts.writeStream.outputMode('complete').format('console')\
        .option('checkpointLocation', sys.argv[3]).start().awaitTermination()

    spark.stop()