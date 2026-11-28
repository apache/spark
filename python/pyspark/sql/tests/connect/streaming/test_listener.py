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

import functools
import json
import queue
import threading
import unittest
import uuid
import warnings
from unittest import mock

from pyspark.errors import PySparkValueError
from pyspark.sql.streaming.listener import StreamingQueryListener
from pyspark.testing.connectutils import connect_requirement_message, should_test_connect
from pyspark.testing.utils import eventually

if should_test_connect:
    from google.protobuf import any_pb2

    import pyspark.sql.connect.proto as pb2
    from pyspark.sql.connect import proto
    from pyspark.sql.connect.client.core import PlanObservedMetrics
    from pyspark.sql.connect.streaming.query import StreamingQueryListenerBus
    from pyspark.sql.metrics import PlanMetrics


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class StreamingQueryListenerBusResponseTests(unittest.TestCase):
    def test_ignores_execution_metadata_before_and_after_registration(self):
        listener_added = pb2.StreamingQueryListenerEventsResult(listener_bus_listener_added=True)
        listener_events = pb2.StreamingQueryListenerEventsResult()
        event = listener_events.events.add(event_json="{}")
        results = iter(
            [
                PlanMetrics("metric", 1, 0, []),
                {"streaming_query_listener_events_result": listener_added},
                PlanObservedMetrics("observation", [], []),
                {"streaming_query_listener_events_result": listener_events},
            ]
        )
        manager = mock.Mock()
        manager._session.client.execute_command_as_iterator.return_value = results
        listener_bus = StreamingQueryListenerBus(manager)

        remaining_results = listener_bus._register_server_side_listener()
        with (
            mock.patch.object(
                listener_bus, "deserialize", return_value=mock.sentinel.deserialized_event
            ) as deserialize,
            mock.patch.object(listener_bus, "post_to_all") as post_to_all,
        ):
            listener_bus._query_event_handler(remaining_results)

        deserialize.assert_called_once_with(event)
        post_to_all.assert_called_once_with(mock.sentinel.deserialized_event)

    def test_rejects_unexpected_response(self):
        responses = [
            any_pb2.Any(),
            {"unexpected": object()},
            {"streaming_query_listener_events_result": object()},
        ]
        for response in responses:
            with self.subTest(response=response):
                with self.assertRaises(PySparkValueError) as error:
                    list(StreamingQueryListenerBus._iter_listener_events(iter([response])))

                self.assertEqual(error.exception.getCondition(), "UNKNOWN_RESPONSE")


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class StreamingQueryListenerBusCloseTests(unittest.TestCase):
    def test_close_removes_every_listener_and_server_listener_once(self):
        listener_added = pb2.StreamingQueryListenerEventsResult(listener_bus_listener_added=True)
        manager = mock.Mock()
        manager._session.client.is_closed = False
        manager._session.client.execute_command_as_iterator.return_value = iter(
            [{"streaming_query_listener_events_result": listener_added}]
        )
        listener_bus = StreamingQueryListenerBus(manager)
        listeners = [object(), object(), object()]
        for listener in listeners:
            listener_bus.append(listener)

        listener_bus.close()

        self.assertEqual(listener_bus._listener_bus, [])
        self.assertIsNone(listener_bus._execution_thread)
        manager._session.client.execute_command_as_iterator.assert_called_once()
        manager._session.client.execute_command.assert_called_once()
        remove_cmd = manager._session.client.execute_command.call_args[0][0]
        bus_cmd = remove_cmd.streaming_query_listener_bus_command
        self.assertTrue(bus_cmd.remove_listener_bus_listener)

    def test_close_with_a_closed_client_clears_the_listeners_without_a_removal(self):
        # After SparkSession.stop(), the release of the session has removed the server side
        # listener and the client is closed, so close() sends no removal and warns nothing.
        listener_added = pb2.StreamingQueryListenerEventsResult(listener_bus_listener_added=True)
        manager = mock.Mock()
        manager._session.client.is_closed = False
        manager._session.client.execute_command_as_iterator.return_value = iter(
            [{"streaming_query_listener_events_result": listener_added}]
        )
        manager._session.client.execute_command.side_effect = RuntimeError("closed client")
        listener_bus = StreamingQueryListenerBus(manager)
        for listener in [object(), object()]:
            listener_bus.append(listener)
        # SparkSession.__del__ runs only after the handler thread has ended.
        listener_bus._execution_thread.join(timeout=10)
        manager._session.client.is_closed = True

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            listener_bus.close()

        self.assertEqual([str(w.message) for w in caught], [])
        self.assertEqual(listener_bus._listener_bus, [])
        self.assertIsNone(listener_bus._execution_thread)
        manager._session.client.execute_command.assert_not_called()


class _CallbackListener(StreamingQueryListener):
    """A listener that records idle events and runs an optional action on each of them."""

    def __init__(self, on_idle=None):
        self.events = []
        self.called = threading.Event()
        self._on_idle = on_idle

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        pass

    def onQueryIdle(self, event):
        self.events.append(event)
        if self._on_idle is not None:
            self._on_idle(self)
        self.called.set()

    def onQueryTerminated(self, event):
        pass


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class StreamingQueryListenerBusReentrancyTests(unittest.TestCase):
    """
    A listener callback runs on the event handling thread. Adding or removing a listener from
    it must neither deadlock that thread nor join it from itself.
    """

    TIMEOUT = 10

    def setUp(self):
        self.streams = []
        added = pb2.StreamingQueryListenerEventsResult(listener_bus_listener_added=True)

        def stream(events):
            yield {"streaming_query_listener_events_result": added}
            while True:
                event_json = events.get()
                if event_json is None:
                    return
                result = pb2.StreamingQueryListenerEventsResult()
                result.events.add(
                    event_type=proto.StreamingQueryEventType.QUERY_IDLE_EVENT,
                    event_json=event_json,
                )
                yield {"streaming_query_listener_events_result": result}

        def add_server_listener(_):
            self.streams.append(queue.Queue())
            return stream(self.streams[-1])

        self.server_listener_removed = threading.Event()

        def remove_server_listener(_):
            # Like the server, end the event stream of the server side listener.
            self.streams[-1].put(None)
            self.server_listener_removed.set()

        self.manager = mock.Mock()
        self.client = self.manager._session.client
        self.client.is_closed = False
        self.client.execute_command_as_iterator.side_effect = add_server_listener
        self.client.execute_command.side_effect = remove_server_listener
        # A deadlocked handler thread must fail the test, not keep the test process alive.
        patcher = mock.patch(
            "pyspark.sql.connect.streaming.query.Thread",
            functools.partial(threading.Thread, daemon=True),
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.bus = StreamingQueryListenerBus(self.manager)

    def post_event(self):
        event = {"id": str(uuid.uuid4()), "runId": str(uuid.uuid4()), "timestamp": "0"}
        self.streams[-1].put(json.dumps(event))

    def assert_called(self, listener):
        self.assertTrue(listener.called.wait(self.TIMEOUT), "listener callback did not run")
        listener.called.clear()

    def assert_thread_ends(self, thread):
        thread.join(self.TIMEOUT)
        self.assertFalse(thread.is_alive(), "event handling thread is blocked")

    def assert_no_listener_left(self, handler_thread):
        self.assert_thread_ends(handler_thread)

        @eventually(timeout=self.TIMEOUT, catch_assertions=True)
        def check():
            self.assertIsNone(self.bus._execution_thread)

        check()
        self.assertEqual(self.bus._listener_bus, [])
        self.client.execute_command.assert_called_once()

    def run_with_timeout(self, func):
        thread = threading.Thread(target=func, daemon=True)
        thread.start()
        self.assert_thread_ends(thread)

    def test_remove_the_only_listener_from_its_callback(self):
        listener = _CallbackListener(on_idle=self.bus.remove)
        self.bus.append(listener)
        handler_thread = self.bus._execution_thread

        self.post_event()

        self.assert_called(listener)
        self.assert_no_listener_left(handler_thread)
        self.assertEqual(len(listener.events), 1)

    def test_remove_a_listener_from_its_callback_with_other_listeners(self):
        removed = _CallbackListener(on_idle=self.bus.remove)
        others = [_CallbackListener(), _CallbackListener()]
        for listener in [others[0], removed, others[1]]:
            self.bus.append(listener)
        handler_thread = self.bus._execution_thread

        self.post_event()
        self.assert_called(removed)
        self.post_event()
        for listener in others:
            self.assert_called(listener)

        @eventually(timeout=self.TIMEOUT, catch_assertions=True)
        def check():
            for listener in others:
                self.assertEqual(len(listener.events), 2)

        check()
        self.assertEqual(len(removed.events), 1)
        self.assertEqual(self.bus._listener_bus, others)
        self.assertIs(self.bus._execution_thread, handler_thread)
        self.assertTrue(handler_thread.is_alive())
        self.client.execute_command.assert_not_called()

        self.run_with_timeout(self.bus.close)
        self.assert_no_listener_left(handler_thread)

    def test_add_a_listener_from_a_callback(self):
        added = _CallbackListener()

        def add_once(listener):
            if len(listener.events) == 1:
                self.bus.append(added)

        listener = _CallbackListener(on_idle=add_once)
        self.bus.append(listener)
        handler_thread = self.bus._execution_thread

        self.post_event()
        self.assert_called(listener)
        self.post_event()
        self.assert_called(added)

        self.assertEqual(self.bus._listener_bus, [listener, added])
        self.assertEqual(len(added.events), 1)
        self.assertIs(self.bus._execution_thread, handler_thread)
        self.client.execute_command_as_iterator.assert_called_once()

        self.run_with_timeout(self.bus.close)
        self.assert_no_listener_left(handler_thread)

    def test_replace_the_only_listener_from_its_callback(self):
        added = _CallbackListener()

        def replace(listener):
            self.bus.remove(listener)
            self.bus.append(added)

        listener = _CallbackListener(on_idle=replace)
        self.bus.append(listener)
        handler_thread = self.bus._execution_thread

        self.post_event()
        self.assert_called(listener)
        self.post_event()
        self.assert_called(added)

        self.assertEqual(self.bus._listener_bus, [added])
        self.assertEqual(len(listener.events), 1)
        self.client.execute_command_as_iterator.assert_called_once()
        self.client.execute_command.assert_not_called()

        self.run_with_timeout(self.bus.close)
        self.assert_no_listener_left(handler_thread)

    def test_remove_a_listener_from_its_callback_while_it_is_being_removed(self):
        # remove() of the last listener holds _thread_lock while it joins the handler thread,
        # which is running a callback that removes the same listener.
        def remove_after_server_removal(listener):
            self.assertTrue(self.server_listener_removed.wait(self.TIMEOUT))
            self.bus.remove(listener)

        listener = _CallbackListener(on_idle=remove_after_server_removal)
        self.bus.append(listener)
        handler_thread = self.bus._execution_thread
        self.post_event()

        self.run_with_timeout(lambda: self.bus.remove(listener))

        self.assert_called(listener)
        self.assert_no_listener_left(handler_thread)

    def test_add_a_listener_from_a_callback_while_the_last_listener_is_being_removed(self):
        added = _CallbackListener()

        def add_after_server_removal(listener):
            self.assertTrue(self.server_listener_removed.wait(self.TIMEOUT))
            self.bus.append(added)

        listener = _CallbackListener(on_idle=add_after_server_removal)
        self.bus.append(listener)
        old_handler_thread = self.bus._execution_thread
        self.post_event()

        self.run_with_timeout(lambda: self.bus.remove(listener))
        self.assert_called(listener)
        self.assert_thread_ends(old_handler_thread)

        # The added listener outlives the removed server side listener, so the bus registers
        # a new one and starts a new handler thread for it.
        @eventually(timeout=self.TIMEOUT, catch_assertions=True)
        def check():
            self.assertEqual(self.client.execute_command_as_iterator.call_count, 2)
            self.assertIsNotNone(self.bus._execution_thread)

        check()
        self.assertEqual(self.bus._listener_bus, [added])
        new_handler_thread = self.bus._execution_thread
        self.assertIsNot(new_handler_thread, old_handler_thread)
        self.post_event()
        self.assert_called(added)

        self.client.execute_command.reset_mock()
        self.run_with_timeout(self.bus.close)
        self.assert_no_listener_left(new_handler_thread)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
