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

import threading
import time
import unittest
import uuid
from unittest.mock import MagicMock

import pyspark.cloudpickle
from pyspark.errors import AnalysisException
from pyspark.sql.connect.streaming.query import StreamingQueryListenerBus
from pyspark.sql.functions import count, lit
from pyspark.sql.streaming.listener import QueryStartedEvent, StreamingQueryListener
from pyspark.sql.tests.streaming.test_streaming_listener import StreamingListenerTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.utils import eventually


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


class StreamingQueryListenerBusTests(unittest.TestCase):
    def test_remove_last_listener_with_pending_event(self):
        listener = TestListenerLocalV2()
        next_listener = TestListenerLocalV2()

        client = MagicMock()
        sqm = MagicMock()
        sqm._session.client = client
        listener_bus = StreamingQueryListenerBus(sqm)
        listener_bus._listener_bus.append(listener)

        # Reproduce the ordering that used to deadlock: removal requests server-side
        # shutdown while the event thread dispatches a pending event. The request must
        # not hold the listener state lock, which the event thread also needs. After
        # dispatch, keep the event thread alive long enough to verify that append waits
        # for shutdown to finish.
        remove_command_started = threading.Event()
        event_dispatched = threading.Event()
        event_thread_can_exit = threading.Event()
        removal_finished = threading.Event()
        append_finished = threading.Event()
        lifecycle_wait_started = threading.Event()
        thread_errors = []
        threads = []

        class TrackingLifecycleLock:
            def __init__(self):
                self._lock = threading.Lock()

            def __enter__(self):
                if self._lock.locked():
                    lifecycle_wait_started.set()
                self._lock.acquire()

            def __exit__(self, exc_type, exc_value, traceback):
                self._lock.release()

        listener_bus._lifecycle_lock = TrackingLifecycleLock()

        def execute_command(_):
            remove_command_started.set()
            if not event_dispatched.wait(5):
                raise TimeoutError("pending event was not dispatched")

        client.execute_command.side_effect = execute_command

        event = QueryStartedEvent(
            id=uuid.uuid4(),
            runId=uuid.uuid4(),
            name="pending-event",
            timestamp="2026-09-17T00:00:00.000Z",
            jobTags=set(),
        )

        def dispatch_pending_event():
            try:
                if not remove_command_started.wait(5):
                    raise TimeoutError("listener removal did not start")
                listener_bus.post_to_all(event)
                event_dispatched.set()
                event_thread_can_exit.wait()
            except BaseException as error:
                thread_errors.append(error)

        def remove_listener():
            try:
                listener_bus.remove(listener)
            except BaseException as error:
                thread_errors.append(error)
            finally:
                removal_finished.set()

        register_server_side_listener = MagicMock(return_value=iter(()))
        listener_bus._register_server_side_listener = register_server_side_listener

        def append_listener():
            try:
                listener_bus.append(next_listener)
            except BaseException as error:
                thread_errors.append(error)
            finally:
                append_finished.set()

        event_thread = threading.Thread(target=dispatch_pending_event, daemon=True)
        listener_bus._execution_thread = event_thread
        removal_thread = threading.Thread(target=remove_listener, daemon=True)
        threads.extend([event_thread, removal_thread])

        try:
            event_thread.start()
            removal_thread.start()

            self.assertTrue(
                event_dispatched.wait(5),
                "removeListener blocked the event thread while waiting for it to exit",
            )

            append_thread = threading.Thread(target=append_listener, daemon=True)
            threads.append(append_thread)
            append_thread.start()
            self.assertTrue(
                lifecycle_wait_started.wait(5),
                "addListener did not wait for the listener shutdown",
            )
            self.assertFalse(append_finished.is_set())

            event_thread_can_exit.set()
            self.assertTrue(removal_finished.wait(5))
            self.assertTrue(append_finished.wait(5))

            self.assertEqual(thread_errors, [])
            self.assertEqual(listener.start, [event])
            self.assertEqual(listener_bus._listener_bus, [next_listener])
            register_server_side_listener.assert_called_once_with()
        finally:
            event_thread_can_exit.set()
            for thread in threads:
                thread.join(timeout=1)


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

            eventually(timeout=20, catch_assertions=True)(
                lambda: self.assertTrue(slow_query.id in [str(e.id) for e in listener.terminated])
            )()
            eventually(timeout=20, catch_assertions=True)(
                lambda: self.assertTrue(fast_query.id in [str(e.id) for e in listener.terminated])
            )()

        finally:
            for listener in self.spark.streams._sqlb._listener_bus:
                self.spark.streams.removeListener(listener)
            for q in self.spark.streams.active:
                q.stop()

    def test_listener_throw(self):
        """
        Following classic Spark's behavior, when the callback of user-defined listener throws,
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

            @eventually(timeout=5, catch_assertions=True)
            def check_listner():
                self.assertTrue(len(listener_good.start) > 0)
                self.assertTrue(len(listener_good.progress) > 0)
                self.assertTrue(len(listener_good.terminated) > 0)

            check_listner()
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

                events = {
                    "start_event": None,
                    "progress_event": None,
                    "terminated_event": None,
                }

                @eventually(timeout=60, catch_assertions=True)
                def load_event(event_name, table_name):
                    try:
                        table = self.spark.read.table(table_name).collect()
                    except AnalysisException as e:
                        # It's possible that the table has not been created yet
                        if e.getCondition() == "TABLE_OR_VIEW_NOT_FOUND":
                            return False
                        raise e
                    if len(table) == 0:
                        return False
                    events[event_name] = pyspark.cloudpickle.loads(table[0][0])
                    return True

                load_event("start_event", "listener_start_events")
                load_event("progress_event", "listener_progress_events")
                load_event("terminated_event", "listener_terminated_events")

                self.check_start_event(events["start_event"])
                self.check_progress_event(events["progress_event"], is_stateful=True)
                self.check_terminated_event(events["terminated_event"])
        finally:
            self.spark.streams.removeListener(test_listener)
            # Remove again to verify this won't throw any error
            self.spark.streams.removeListener(test_listener)

    def test_server_listener_uninterruptible(self):
        listener = TestListenerLocalV1()

        try:
            self.spark.streams.addListener(listener)
            q = (
                self.spark.readStream.format("rate")
                .load()
                .writeStream.format("noop")
                .queryName("test_listener_uninterruptible")
                .start()
            )

            self.assertEqual(len(listener.start), 1)
            self.assertEqual(str(listener.start[0].id), q.id)

            while q.lastProgress is None:
                q.awaitTermination(0.5)

            # Interrupt should stop the query but should not impact the listener,
            # therefore there should be a QueryTerminatedEvent sent from the server.
            self.spark.interruptAll()

            # Need to wait a while before the query really stops
            while q.isActive:
                q.awaitTermination(0.5)

            # Need to wait a while before QueryTerminatedEvent reaches client
            while len(listener.terminated) == 0:
                time.sleep(1)

            self.assertEqual(len(listener.terminated), 1)
            self.assertEqual(str(listener.terminated[0].id), q.id)

        finally:
            for listener in self.spark.streams._sqlb._listener_bus:
                self.spark.streams.removeListener(listener)
            for q in self.spark.streams.active:
                q.stop()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
