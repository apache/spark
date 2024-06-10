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

import pyspark.cloudpickle
from pyspark.sql.tests.streaming.test_streaming_listener import StreamingListenerTestsMixin
from pyspark.sql.streaming.listener import StreamingQueryListener
from pyspark.sql.functions import count, lit
from pyspark.testing.connectutils import ReusedConnectTestCase


# Listeners that has spark commands in callback handler functions
class TestListenerSpark(StreamingQueryListener):
    def onQueryStarted(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_start_events")

    def onQueryProgress(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_progress_events")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        e = pyspark.cloudpickle.dumps(event)
        df = self.spark.createDataFrame(data=[(e,)])
        df.write.mode("append").saveAsTable("listener_terminated_events")


# V1: Initial interface of StreamingQueryListener containing methods `onQueryStarted`,
# `onQueryProgress`, `onQueryTerminated`. It is prior to Spark 3.5.
class TestListenerLocalV1(StreamingQueryListener):
    def __init__(self):
        self.start = []
        self.progress = []
        self.terminated = []

    def onQueryStarted(self, event):
        self.start.append(event)

    def onQueryProgress(self, event):
        self.progress.append(event)

    def onQueryTerminated(self, event):
        self.terminated.append(event)


class TestListenerLocalV2(StreamingQueryListener):
    def __init__(self):
        self.start = []
        self.progress = []
        self.terminated = []

    def onQueryStarted(self, event):
        self.start.append(event)

    def onQueryProgress(self, event):
        self.progress.append(event)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        self.terminated.append(event)


class StreamingListenerParityTests(StreamingListenerTestsMixin, ReusedConnectTestCase):
    def test_listener_management(self):
        listener1 = TestListenerLocalV1()
        listener2 = TestListenerLocalV2()

        try:
            self.spark.streams.addListener(listener1)
            self.spark.streams.addListener(listener2)
            q = (
                self.spark.readStream.format("rate")
                .load()
                .writeStream.format("noop")
                .queryName("test_local")
                .start()
            )

            # Both listeners should have listener events already because onQueryStarted
            # is always called before DataStreamWriter.start() returns
            self.assertEqual(len(listener1.start), 1)
            self.assertEqual(len(listener2.start), 1)
            self.check_start_event(listener1.start[0])
            self.check_start_event(listener2.start[0])

            while q.lastProgress is None:
                q.awaitTermination(0.5)
            # removeListener is a blocking call, resources are cleaned up by the time it returns
            self.spark.streams.removeListener(listener1)
            self.spark.streams.removeListener(listener2)

            # Add back the listener and stop the query, now should see a terminated event
            self.spark.streams.addListener(listener1)
            q.stop()

            # need to wait a while before QueryTerminatedEvent reaches client
            while len(listener1.terminated) == 0:
                time.sleep(1)

            self.assertEqual(len(listener1.terminated), 1)

            for event in listener1.progress:
                self.check_progress_event(event, is_stateful=False)
            self.check_terminated_event(listener1.terminated[0])

        finally:
            for listener in self.spark.streams._sqlb._listener_bus:
                self.spark.streams.removeListener(listener)
            for q in self.spark.streams.active:
                q.stop()

    def test_slow_query(self):
        try:
            listener = TestListenerLocalV2()
            self.spark.streams.addListener(listener)

            slow_query = (
                self.spark.readStream.format("rate")
                .load()
                .writeStream.format("noop")
                .trigger(processingTime="20 seconds")
                .start()
            )
            fast_query = (
                self.spark.readStream.format("rate").load().writeStream.format("noop").start()
            )

            while slow_query.lastProgress is None:
                slow_query.awaitTermination(20)

            slow_query.stop()
            fast_query.stop()

            self.assertTrue(slow_query.id in [str(e.id) for e in listener.start])
            self.assertTrue(fast_query.id in [str(e.id) for e in listener.start])

            self.assertTrue(slow_query.id in [str(e.progress.id) for e in listener.progress])
            self.assertTrue(fast_query.id in [str(e.progress.id) for e in listener.progress])

            self.assertTrue(slow_query.id in [str(e.id) for e in listener.terminated])
            self.assertTrue(fast_query.id in [str(e.id) for e in listener.terminated])

        finally:
            for listener in self.spark.streams._sqlb._listener_bus:
                self.spark.streams.removeListener(listener)
            for q in self.spark.streams.active:
                q.stop()

    def test_listener_throw(self):
        """
        Following Vanilla Spark's behavior, when the callback of user-defined listener throws,
        other listeners should still proceed.
        """

        class UselessListener(StreamingQueryListener):
            def onQueryStarted(self, e):
                raise Exception("My bad!")

            def onQueryProgress(self, e):
                raise Exception("My bad again!")

            def onQueryTerminated(self, e):
                raise Exception("I'm so sorry!")

        try:
            listener_good = TestListenerLocalV2()
            listener_bad = UselessListener()
            self.spark.streams.addListener(listener_good)
            self.spark.streams.addListener(listener_bad)

            q = self.spark.readStream.format("rate").load().writeStream.format("noop").start()

            while q.lastProgress is None:
                q.awaitTermination(0.5)

            q.stop()
            # need to wait a while before QueryTerminatedEvent reaches client
            time.sleep(5)
            self.assertTrue(len(listener_good.start) > 0)
            self.assertTrue(len(listener_good.progress) > 0)
            self.assertTrue(len(listener_good.terminated) > 0)
        finally:
            for listener in self.spark.streams._sqlb._listener_bus:
                self.spark.streams.removeListener(listener)
            for q in self.spark.streams.active:
                q.stop()

    def test_listener_events_spark_command(self):
        test_listener = TestListenerSpark()

        try:
            with self.table(
                "listener_start_events",
                "listener_progress_events",
                "listener_terminated_events",
            ):
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
                    .outputMode("update")
                    .trigger(processingTime="5 seconds")
                    .start()
                )

                self.assertTrue(q.isActive)
                # ensure at least one batch is ran
                while q.lastProgress is None or q.lastProgress["batchId"] == 0:
                    q.awaitTermination(0.5)
                q.stop()
                self.assertFalse(q.isActive)

                time.sleep(
                    60
                )  # Sleep to make sure listener_terminated_events is written successfully

                start_event = pyspark.cloudpickle.loads(
                    self.spark.read.table("listener_start_events").collect()[0][0]
                )

                progress_event = pyspark.cloudpickle.loads(
                    self.spark.read.table("listener_progress_events").collect()[0][0]
                )

                terminated_event = pyspark.cloudpickle.loads(
                    self.spark.read.table("listener_terminated_events").collect()[0][0]
                )

                self.check_start_event(start_event)
                self.check_progress_event(progress_event, is_stateful=True)
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
