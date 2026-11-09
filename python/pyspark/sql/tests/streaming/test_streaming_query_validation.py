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

from pyspark.sql.streaming.query import StreamingQuery
from pyspark.testing.utils import connect_requirement_message, should_test_connect

if should_test_connect:
    from pyspark.sql.connect.streaming.query import StreamingQuery as ConnectStreamingQuery


class StreamingQueryTimeoutRoundingTests(unittest.TestCase):
    """awaitTermination's timeout in seconds is converted to milliseconds. int() truncates,
    so a tiny but positive timeout like 0.0001 seconds used to round down to 0ms, which the
    engine then rejects outright as "has to be positive". Clamping only sub-1ms positive
    values up to 1ms (rather than e.g. math.ceil() on everything) fixes that without
    introducing floating-point overshoot for ordinary values: 2.007 * 1000 is
    2007.0000000000002, so ceil() would turn 2.007s into 2008ms instead of 2007ms.

    awaitAnyTermination does not need this: unlike awaitTermination, it already treats a
    sub-1ms/0ms timeout as "check once and return immediately" on both classic and Connect
    (Connect's handler there just delegates to the classic implementation), so rounding up
    would only make it block for a full 10ms polling interval it doesn't block for today.

    Exercised directly, without a running query, by substituting a fake JVM/Connect handle
    and capturing the millisecond value it receives."""

    def test_classic_await_termination_small_timeout_does_not_round_down_to_zero(self):
        calls = []

        class FakeJsq:
            def awaitTermination(self, timeout_ms):
                calls.append(timeout_ms)
                return True

        StreamingQuery(jsq=FakeJsq()).awaitTermination(0.0001)
        self.assertEqual(calls, [1])

    def test_classic_await_termination_does_not_overshoot_on_floating_point_noise(self):
        calls = []

        class FakeJsq:
            def awaitTermination(self, timeout_ms):
                calls.append(timeout_ms)
                return True

        # 2.007 * 1000 == 2007.0000000000002 in floating point.
        StreamingQuery(jsq=FakeJsq()).awaitTermination(2.007)
        self.assertEqual(calls, [2007])

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
    def test_connect_await_termination_does_not_overshoot_on_floating_point_noise(self):
        import pyspark.sql.connect.proto as pb2

        query = ConnectStreamingQuery(session=None, queryId="q", runId="r")
        captured = []

        def fake_execute(cmd):
            captured.append(cmd.await_termination.timeout_ms)
            result = pb2.StreamingQueryCommandResult()
            result.await_termination.terminated = True
            return result

        query._execute_streaming_query_cmd = fake_execute
        query.awaitTermination(2.007)
        self.assertEqual(captured, [2007])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
