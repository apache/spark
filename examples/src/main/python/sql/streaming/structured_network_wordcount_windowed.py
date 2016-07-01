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
 Counts words in UTF8 encoded, '\n' delimited text received from the network over a
 sliding window of configurable duration. Each line from the network is tagged
 with a timestamp that is used to determine the windows into which it falls.

 Usage: structured_network_wordcount_windowed.py <hostname> <port> <window duration>
   <optional slide duration>
 <hostname> and <port> describe the TCP server that Structured Streaming
 would connect to receive data.
 <window duration> gives the size of window, specified as integer number of seconds
 <slide duration> gives the amount of time successive windows are offset from one another,
 given in the same units as above. <slide duration> should be less than or equal to
 <window duration>. If the two are equal, successive windows have no overlap. If
 <slide duration> is not provided, it defaults to <window duration>.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit
    examples/src/main/python/sql/streaming/structured_network_wordcount_windowed.py
    localhost 9999 <window duration> <optional slide duration>`

 One recommended <window duration>, <slide duration> pair is 60, 30
"""
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

if __name__ == "__main__":
    if len(sys.argv) != 5 and len(sys.argv) != 4:
        msg = ("Usage: structured_network_wordcount_windowed.py <hostname> <port> "
               "<window duration in seconds> <optional slide duration in seconds>")
        print(msg, file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    windowSize = int(sys.argv[3])
    slideSize = int(sys.argv[4]) if (len(sys.argv) == 5) else windowSize
    if slideSize > windowSize:
        print("<slide duration> must be less than or equal to <window duration>", file=sys.stderr)
    windowArg = '{} seconds'.format(windowSize)
    slideArg = '{} seconds'.format(slideSize)


    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCountWindowed")\
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .option('includeTimestamp', 'true')\
        .load()

    # Split the lines into words, retaining timestamps
    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(split(lines.value, ' ')).alias('word'),
        lines.timestamp
    )

    # Group the data by window and word and compute the count of each group
    windowedCounts = words.groupBy(
        window(words.timestamp, windowArg, slideArg),
        words.word
    ).count().orderBy('window')

    # Start running the query that prints the windowed word counts to the console
    query = windowedCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .option('truncate', 'false')\
        .start()

    query.awaitTermination()
