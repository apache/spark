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

r"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network over a
 sliding window of configurable duration.

 Usage: structured_sessionization.py <hostname> <port>
 <hostname> and <port> describe the TCP server that Structured Streaming
 would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit
    examples/src/main/python/sql/streaming/structured_sessionization.py
    localhost 9999`
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import count, session_window

if __name__ == "__main__":
    if len(sys.argv) != 3 and len(sys.argv) != 2:
        msg = "Usage: structured_sessionization.py <hostname> <port> "
        print(msg, file=sys.stderr)
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession\
        .builder\
        .appName("StructuredSessionization")\
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
    # split() splits each line into an array, and explode() turns the array into multiple rows
    # treat words as sessionId of events
    events = lines.select(
        explode(split(lines.value, ' ')).alias('sessionId'),
        lines.timestamp.alias('eventTime')
    )

    # Group the data by window and word and compute the count of each group
    windowedCounts = events \
        .groupBy(session_window(events.eventTime, "10 seconds").alias('session'),
                 events.sessionId) \
        .agg(count("*").alias("numEvents")) \
        .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
                    "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
                    "numEvents")

    # Start running the query that prints the session updates to the console
    query = windowedCounts\
        .writeStream\
        .outputMode('update')\
        .format('console')\
        .start()

    query.awaitTermination()
