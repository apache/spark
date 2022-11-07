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
 Split lines into words, group by words and use the state per key to track session of each key.
 Each session window sets a 10 seconds processing time timeout.
 After 10 seconds of idle period, the session summary will be finalized and output to sink.
 Usage: structured_network_wordcount_windowed.py <hostname> <port>
 <hostname> and <port> describe the TCP server that Structured Streaming
 would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit
    examples/src/main/python/sql/streaming/structured_network_wordcount_session_window.py
    localhost 9999`
"""
import sys
from typing import Iterable, Any

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import (
    LongType,
    StringType,
    TimestampType,
    StructType,
    StructField,
)
from pyspark.sql.streaming.state import GroupStateTimeout, GroupState

if __name__ == "__main__":
    if len(sys.argv) != 3:
        msg = "Usage: structured_network_wordcount_session_window.py <hostname> <port>"
        print(msg, file=sys.stderr)
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession.builder.appName(
        "StructuredNetworkWordCountSessionWindow"
    ).getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = (
        spark.readStream.format("socket")
        .option("host", host)
        .option("port", port)
        .option("includeTimestamp", "true")
        .load()
    )

    # Split the lines into words, retaining timestamps, each word become a sessionId
    events = lines.select(
        explode(split(lines.value, " ")).alias("sessionId"),
        lines.timestamp,
    )

    # Type of output records.
    session_schema = StructType(
        [
            StructField("sessionId", StringType()),
            StructField("count", LongType()),
            StructField("start", TimestampType()),
            StructField("end", TimestampType()),
        ]
    )
    # Type of group state.
    # Omit the session id in the state since it is available as group key
    session_state_schema = StructType(
        [
            StructField("count", LongType()),
            StructField("start", TimestampType()),
            StructField("end", TimestampType()),
        ]
    )

    def func(
        key: Any, pdfs: Iterable[pd.DataFrame], state: GroupState
    ) -> Iterable[pd.DataFrame]:
        if state.hasTimedOut:
            count, start, end = state.get
            state.remove()
            (session_id,) = key
            yield pd.DataFrame(
                {
                    "sessionId": [session_id],
                    "count": [count],
                    "start": [start],
                    "end": [end],
                }
            )
        else:
            pdf_iter = iter(pdfs)
            first_pdf = next(pdf_iter)
            start = first_pdf["timestamp"].min()
            end = first_pdf["timestamp"].max()
            count = len(first_pdf)
            for pdf in pdf_iter:
                start = min(start, pdf["timestamp"].min())
                end = max(end, pdf["timestamp"].max())
                count = count + len(pdf)
            if state.exists:
                (old_count, start, old_end) = state.get
                count = count + old_count
                end = max(end, old_end)
            state.update((count, start, end))
            state.setTimeoutDuration(10000)
            yield pd.DataFrame()

    # Group the data by window and word and compute the count of each group
    sessions = events.groupBy(events["sessionId"]).applyInPandasWithState(
        func,
        session_schema,
        session_state_schema,
        "Update",
        GroupStateTimeout.ProcessingTimeTimeout,
    )

    # Start running the query that prints the windowed word counts to the console
    query = sessions.writeStream.outputMode("update").format("console").start()

    query.awaitTermination()
