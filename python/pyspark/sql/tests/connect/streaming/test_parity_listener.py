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

import unittest
import time

from pyspark.sql.tests.streaming.test_streaming_listener import StreamingListenerTestsMixin
from pyspark.sql.streaming.listener import (
    StreamingQueryListener,
    QueryStartedEvent,
    QueryProgressEvent,
    QueryIdleEvent,
    QueryTerminatedEvent,
)
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    MapType,
)
from pyspark.sql.functions import count, lit
from pyspark.testing.connectutils import ReusedConnectTestCase


def get_start_event_schema():
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("runId", StringType(), False),
            StructField("name", StringType(), True),
            StructField("timestamp", StringType(), False),
        ]
    )


def get_idle_event_schema():
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("runId", StringType(), False),
            StructField("timestamp", StringType(), False),
        ]
    )


def get_terminated_event_schema():
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("runId", StringType(), False),
            StructField("exception", StringType(), True),
            StructField("errorClassOnException", StringType(), True),
        ]
    )


def get_state_operators_progress_schema():
    return StructType(
        [
            StructField("operatorName", StringType(), False),
            StructField("numRowsTotal", IntegerType(), False),
            StructField("numRowsUpdated", IntegerType(), False),
            StructField("numRowsRemoved", IntegerType(), False),
            StructField("allUpdatesTimeMs", IntegerType(), False),
            StructField("allRemovalsTimeMs", IntegerType(), False),
            StructField("commitTimeMs", IntegerType(), False),
            StructField("memoryUsedBytes", IntegerType(), False),
            StructField("numRowsDroppedByWatermark", IntegerType(), False),
            StructField("numShufflePartitions", IntegerType(), False),
            StructField("numStateStoreInstances", IntegerType(), False),
            StructField("customMetrics", MapType(StringType(), IntegerType(), True), True),
        ]
    )


def get_source_progress_schema():
    return StructType(
        [
            StructField("description", StringType(), False),
            StructField("startOffset", StringType(), False),
            StructField("endOffset", StringType(), False),
            StructField("latestOffset", StringType(), False),
            StructField("numInputRows", IntegerType(), False),
            StructField("inputRowsPerSecond", FloatType(), False),
            StructField("processedRowsPerSecond", FloatType(), False),
            StructField("metrics", MapType(StringType(), StringType(), True), True),
        ]
    )


def get_sink_progress_schema():
    return StructType(
        [
            StructField("description", StringType(), False),
            StructField("numOutputRows", IntegerType(), False),
            StructField("metrics", MapType(StringType(), StringType(), True), True),
        ]
    )


def get_streaming_query_progress_schema():
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("runId", StringType(), False),
            StructField("name", StringType(), True),
            StructField("timestamp", StringType(), False),
            StructField("batchId", IntegerType(), False),
            StructField("batchDuration", IntegerType(), False),
            StructField("durationMs", MapType(StringType(), IntegerType(), True), True),
            StructField("eventTime", MapType(StringType(), StringType(), True), True),
            StructField("stateOperators", ArrayType(get_state_operators_progress_schema()), True),
            StructField("sources", ArrayType(get_source_progress_schema()), True),
            StructField("sink", get_sink_progress_schema(), True),  # TODO: false?
            StructField("numInputRows", IntegerType(), False),
            StructField("inputRowsPerSecond", FloatType(), False),
            StructField("processedRowsPerSecond", FloatType(), False),
            StructField("observedMetrics", MapType(StringType(), StringType()), False),
        ]
    )


def get_progress_event_schema():
    return StructType([StructField("progress", get_streaming_query_progress_schema(), False)])


class TestListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        df = self.spark.createDataFrame(
            data=[(event.asDict())],
            schema=get_start_event_schema(),
        )
        df.write.saveAsTable("listener_start_events")

    def onQueryProgress(self, event):
        print(event.asDict())
        df = self.spark.createDataFrame(
            data=[event.asDict()],
            schema=get_progress_event_schema(),
        )
        df.write.mode("append").saveAsTable("listener_progress_events")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        df = self.spark.createDataFrame(
            data=[event.asDict()],
            schema=get_terminated_event_schema(),
        )
        df.write.saveAsTable("listener_terminated_events")


class StreamingListenerParityTests(StreamingListenerTestsMixin, ReusedConnectTestCase):
    def test_listener_events(self):
        test_listener = TestListener()

        try:
            self.spark.streams.addListener(test_listener)

            # This ensures the read socket on the server won't crash (i.e. because of timeout)
            # when there hasn't been a new event for a long time
            time.sleep(30)

            df = self.spark.readStream.format("rate").option("rowsPerSecond", 10).load()
            df_observe = df.observe("my_event", count(lit(1)).alias("rc"))
            df_stateful = df_observe.groupBy().count()  # make query stateful
            q = (
                df_stateful.writeStream.format("noop")
                .queryName("test")
                .outputMode("complete")
                .start()
            )

            self.assertTrue(q.isActive)
            time.sleep(10)
            self.assertTrue(q.lastProgress["batchId"] > 0)  # ensure at least one batch is ran
            q.stop()
            self.assertFalse(q.isActive)

            start_event = QueryStartedEvent.fromJson(
                self.spark.read.table("listener_start_events").collect()[0].asDict()
            )

            progress_event = QueryProgressEvent.fromJson(
                self.spark.read.table("listener_progress_events").collect()[0].asDict()
            )

            terminated_event = QueryTerminatedEvent.fromJson(
                self.spark.read.table("listener_terminated_events").collect()[0].asDict()
            )

            self.check_start_event(start_event)
            self.check_progress_event(progress_event)
            self.check_terminated_event(terminated_event)

        finally:
            self.spark.streams.removeListener(test_listener)

            # Remove again to verify this won't throw any error
            self.spark.streams.removeListener(test_listener)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.streaming.test_parity_listener import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
