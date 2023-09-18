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
import uuid
from collections.abc import Generator
from typing import Optional, Any

import grpc

from pyspark.sql.connect.client import SparkConnectClient, ChannelBuilder
import pyspark.sql.connect.proto as proto
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

from pyspark.sql.connect.client.core import Retrying
from pyspark.sql.connect.client.reattach import (
    RetryException,
    ExecutePlanResponseReattachableIterator,
)

if should_test_connect:
    import pandas as pd
    import pyarrow as pa


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectClientTestCase(unittest.TestCase):
    def test_user_agent_passthrough(self):
        client = SparkConnectClient("sc://foo/;user_agent=bar", use_reattachable_execute=False)
        mock = MockService(client._session_id)
        client._stub = mock

        command = proto.Command()
        client.execute_command(command)

        self.assertIsNotNone(mock.req, "ExecutePlan API was not called when expected")
        self.assertRegex(mock.req.client_type, r"^bar spark/[^ ]+ os/[^ ]+ python/[^ ]+$")

    def test_user_agent_default(self):
        client = SparkConnectClient("sc://foo/", use_reattachable_execute=False)
        mock = MockService(client._session_id)
        client._stub = mock

        command = proto.Command()
        client.execute_command(command)

        self.assertIsNotNone(mock.req, "ExecutePlan API was not called when expected")
        self.assertRegex(
            mock.req.client_type, r"^_SPARK_CONNECT_PYTHON spark/[^ ]+ os/[^ ]+ python/[^ ]+$"
        )

    def test_properties(self):
        client = SparkConnectClient("sc://foo/;token=bar", use_reattachable_execute=False)
        self.assertEqual(client.token, "bar")
        self.assertEqual(client.host, "foo")

        client = SparkConnectClient("sc://foo/", use_reattachable_execute=False)
        self.assertIsNone(client.token)

    def test_channel_builder(self):
        class CustomChannelBuilder(ChannelBuilder):
            @property
            def userId(self) -> Optional[str]:
                return "abc"

        client = SparkConnectClient(
            CustomChannelBuilder("sc://foo/"), use_reattachable_execute=False
        )

        self.assertEqual(client._user_id, "abc")

    def test_interrupt_all(self):
        client = SparkConnectClient("sc://foo/;token=bar", use_reattachable_execute=False)
        mock = MockService(client._session_id)
        client._stub = mock

        client.interrupt_all()
        self.assertIsNotNone(mock.req, "Interrupt API was not called when expected")

    def test_is_closed(self):
        client = SparkConnectClient("sc://foo/;token=bar", use_reattachable_execute=False)

        self.assertFalse(client.is_closed)
        client.close()
        self.assertTrue(client.is_closed)

    def test_retry(self):
        client = SparkConnectClient("sc://foo/;token=bar")

        total_sleep = 0

        def sleep(t):
            nonlocal total_sleep
            total_sleep += t

        try:
            for attempt in Retrying(
                can_retry=SparkConnectClient.retry_exception, sleep=sleep, **client._retry_policy
            ):
                with attempt:
                    raise RetryException()
        except RetryException:
            pass

        # tolerated at least 10 mins of fails
        self.assertGreaterEqual(total_sleep, 600)

    def test_channel_builder_with_session(self):
        dummy = str(uuid.uuid4())
        chan = ChannelBuilder(f"sc://foo/;session_id={dummy}")
        client = SparkConnectClient(chan)
        self.assertEqual(client._session_id, chan.session_id)


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectClientReattachTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.request = proto.ExecutePlanRequest()
        self.policy = {
            "max_retries": 3,
            "backoff_multiplier": 4.0,
            "initial_backoff": 10,
            "max_backoff": 10,
            "jitter": 10,
            "min_jitter_threshold": 10,
        }
        self.response = proto.ExecutePlanResponse(
            response_id="1",
        )
        self.finished = proto.ExecutePlanResponse(
            result_complete=proto.ExecutePlanResponse.ResultComplete(),
            response_id="2",
        )

    def _stub_with(self, execute=None, attach=None):
        return MockSparkConnectStub(
            execute_ops=ResponseGenerator(execute) if execute is not None else None,
            attach_ops=ResponseGenerator(attach) if attach is not None else None,
        )

    def assertEventually(self, callable, timeout_ms=1000):
        """Helper method that will continuously evaluate the callable to not raise an
        exception."""
        import time

        limit = time.monotonic_ns() + timeout_ms * 1000 * 1000
        while time.monotonic_ns() < limit:
            try:
                callable()
                break
            except Exception:
                time.sleep(0.1)
        callable()

    def test_basic_flow(self):
        stub = self._stub_with([self.response, self.finished])
        ite = ExecutePlanResponseReattachableIterator(self.request, stub, self.policy, [])
        for b in ite:
            pass

        def check_all():
            self.assertEqual(0, stub.attach_calls)
            self.assertEqual(1, stub.release_until_calls)
            self.assertEqual(1, stub.release_calls)
            self.assertEqual(1, stub.execute_calls)

        self.assertEventually(check_all, timeout_ms=1000)

    def test_fail_during_execute(self):
        def fatal():
            raise TestException("Fatal")

        stub = self._stub_with([self.response, fatal])
        with self.assertRaises(TestException):
            ite = ExecutePlanResponseReattachableIterator(self.request, stub, self.policy, [])
            for b in ite:
                pass

        def check():
            self.assertEqual(0, stub.attach_calls)
            self.assertEqual(1, stub.release_calls)
            self.assertEqual(1, stub.release_until_calls)
            self.assertEqual(1, stub.execute_calls)

        self.assertEventually(check, timeout_ms=1000)

    def test_fail_and_retry_during_execute(self):
        def non_fatal():
            raise TestException("Non Fatal", grpc.StatusCode.UNAVAILABLE)

        stub = self._stub_with(
            [self.response, non_fatal], [self.response, self.response, self.finished]
        )
        ite = ExecutePlanResponseReattachableIterator(self.request, stub, self.policy, [])
        for b in ite:
            pass

        def check():
            self.assertEqual(1, stub.attach_calls)
            self.assertEqual(1, stub.release_calls)
            self.assertEqual(3, stub.release_until_calls)
            self.assertEqual(1, stub.execute_calls)

        self.assertEventually(check, timeout_ms=1000)

    def test_fail_and_retry_during_reattach(self):
        count = 0

        def non_fatal():
            nonlocal count
            if count < 2:
                count += 1
                raise TestException("Non Fatal", grpc.StatusCode.UNAVAILABLE)
            else:
                return proto.ExecutePlanResponse()

        stub = self._stub_with(
            [self.response, non_fatal], [self.response, non_fatal, self.response, self.finished]
        )
        ite = ExecutePlanResponseReattachableIterator(self.request, stub, self.policy, [])
        for b in ite:
            pass

        def check():
            self.assertEqual(2, stub.attach_calls)
            self.assertEqual(3, stub.release_until_calls)
            self.assertEqual(1, stub.release_calls)
            self.assertEqual(1, stub.execute_calls)

        self.assertEventually(check, timeout_ms=1000)


class TestException(grpc.RpcError, grpc.Call):
    """Exception mock to test retryable exceptions."""

    def __init__(self, msg, code=grpc.StatusCode.INTERNAL):
        self.msg = msg
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return self.msg

    def trailing_metadata(self):
        return ()


class ResponseGenerator(Generator):
    """This class is used to generate values that are returned by the streaming
    iterator of the GRPC stub."""

    def __init__(self, funs):
        self._funs = funs
        self._iterator = iter(self._funs)

    def send(self, value: Any) -> proto.ExecutePlanResponse:
        val = next(self._iterator)
        if callable(val):
            return val()
        else:
            return val

    def throw(self, type: Any = None, value: Any = None, traceback: Any = None) -> Any:
        super().throw(type, value, traceback)

    def close(self) -> None:
        return super().close()


class MockSparkConnectStub:
    """Simple mock class for the GRPC stub used by the re-attachable execution."""

    def __init__(self, execute_ops=None, attach_ops=None):
        self._execute_ops = execute_ops
        self._attach_ops = attach_ops
        # Call counters
        self.execute_calls = 0
        self.release_calls = 0
        self.release_until_calls = 0
        self.attach_calls = 0

    def ExecutePlan(self, *args, **kwargs):
        self.execute_calls += 1
        return self._execute_ops

    def ReattachExecute(self, *args, **kwargs):
        self.attach_calls += 1
        return self._attach_ops

    def ReleaseExecute(self, req: proto.ReleaseExecuteRequest, *args, **kwargs):
        if req.HasField("release_all"):
            self.release_calls += 1
        elif req.HasField("release_until"):
            print("increment")
            self.release_until_calls += 1


class MockService:
    # Simplest mock of the SparkConnectService.
    # If this needs more complex logic, it needs to be replaced with Python mocking.

    req: Optional[proto.ExecutePlanRequest]

    def __init__(self, session_id: str):
        self._session_id = session_id
        self.req = None

    def ExecutePlan(self, req: proto.ExecutePlanRequest, metadata):
        self.req = req
        resp = proto.ExecutePlanResponse()
        resp.session_id = self._session_id

        pdf = pd.DataFrame(data={"col1": [1, 2]})
        schema = pa.Schema.from_pandas(pdf)
        table = pa.Table.from_pandas(pdf)
        sink = pa.BufferOutputStream()

        writer = pa.ipc.new_stream(sink, schema=schema)
        writer.write(table)
        writer.close()

        buf = sink.getvalue()
        resp.arrow_batch.data = buf.to_pybytes()
        return [resp]

    def Interrupt(self, req: proto.InterruptRequest, metadata):
        self.req = req
        resp = proto.InterruptResponse()
        resp.session_id = self._session_id
        return resp


if __name__ == "__main__":
    from pyspark.sql.tests.connect.client.test_client import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
