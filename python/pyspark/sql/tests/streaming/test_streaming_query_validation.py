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

from pyspark.sql.streaming.query import StreamingQuery, StreamingQueryManager
from pyspark.testing.utils import connect_requirement_message, should_test_connect

if should_test_connect:
    from pyspark.sql.connect.streaming.query import StreamingQuery as ConnectStreamingQuery
    from pyspark.sql.connect.streaming.query import (
        StreamingQueryManager as ConnectStreamingQueryManager,
    )


class StreamingQueryTimeoutRoundingTests(unittest.TestCase):
    """A tiny but positive timeout like 0.0001 seconds used to silently round down to 0ms
    (int() truncates), which some backends then reject outright as "has to be positive".
    Using math.ceil() instead rounds it up to 1ms. Exercised directly, without a running
    query, by substituting a fake JVM/Connect handle and capturing the millisecond value
    it receives."""

    def test_classic_await_termination_small_timeout_does_not_round_down_to_zero(self):
        calls = []

        class FakeJsq:
            def awaitTermination(self, timeout_ms):
                calls.append(timeout_ms)
                return True

        StreamingQuery(jsq=FakeJsq()).awaitTermination(0.0001)
        self.assertEqual(calls, [1])

    def test_classic_await_any_termination_small_timeout_does_not_round_down_to_zero(self):
        calls = []

        class FakeJsqm:
            def awaitAnyTermination(self, timeout_ms):
                calls.append(timeout_ms)
                return True

        StreamingQueryManager(jsqm=FakeJsqm()).awaitAnyTermination(0.0001)
        self.assertEqual(calls, [1])

    @unittest.skipIf(not should_test_connect, connect_requirement_message)
    def test_connect_await_termination_small_timeout_does_not_round_down_to_zero(self):
        import pyspark.sql.connect.proto as pb2

        query = ConnectStreamingQuery(session=None, queryId="q", runId="r")
        captured = []

        def fake_execute(cmd):
            captured.append(cmd.await_termination.timeout_ms)
            result = pb2.StreamingQueryCommandResult()
            result.await_termination.terminated = True
            return result

        query._execute_streaming_query_cmd = fake_execute
        query.awaitTermination(0.0001)
        self.assertEqual(captured, [1])

    @unittest.skipIf(not should_test_connect, connect_requirement_message)
    def test_connect_await_any_termination_small_timeout_does_not_round_down_to_zero(self):
        import pyspark.sql.connect.proto as pb2

        manager = ConnectStreamingQueryManager(session=None)
        captured = []

        def fake_execute(cmd):
            captured.append(cmd.await_any_termination.timeout_ms)
            result = pb2.StreamingQueryManagerCommandResult()
            result.await_any_termination.terminated = True
            return result

        manager._execute_streaming_query_manager_cmd = fake_execute
        manager.awaitAnyTermination(0.0001)
        self.assertEqual(captured, [1])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
