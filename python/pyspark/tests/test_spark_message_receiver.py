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
import io
import unittest
from typing import BinaryIO

from pyspark.messages.spark_message_receiver import SparkMessageReceiver
from pyspark.messages.zero_copy_byte_stream import ZeroCopyByteStream


class StubMessageReceiver(SparkMessageReceiver):
    """Concrete stub for testing the state machine in SparkMessageReceiver."""

    def __init__(self) -> None:
        super().__init__()

    def _do_get_init_message(self) -> ZeroCopyByteStream:
        return ZeroCopyByteStream(memoryview(b"init"))

    def _do_get_data_stream(self) -> BinaryIO:
        return io.BytesIO(b"data")

    def _do_get_finish_signal_from_stream(self) -> None:
        pass


class SparkMessageReceiverTests(unittest.TestCase):
    """Tests for SparkMessageReceiver state transitions."""

    def test_happy_path(self):
        """Calling init -> data -> finish in order succeeds."""
        receiver = StubMessageReceiver()
        init_msg = receiver.get_init_message()
        self.assertIsInstance(init_msg, ZeroCopyByteStream)
        data = receiver.get_data_stream()
        self.assertEqual(data.read(), b"data")
        # Should not raise
        receiver.get_finish_signal_from_stream()

    def test_invalid_transitions_fail(self):
        """Calling methods out of order raises AssertionError."""
        # Each entry: (setup_calls, invalid_call)
        cases = [
            ([], "get_data_stream"),
            ([], "get_finish_signal_from_stream"),
            (["get_init_message"], "get_init_message"),
            (["get_init_message", "get_data_stream"], "get_data_stream"),
            (
                ["get_init_message", "get_data_stream", "get_finish_signal_from_stream"],
                "get_finish_signal_from_stream",
            ),
        ]
        for setup_calls, invalid_call in cases:
            with self.subTest(setup=setup_calls, invalid=invalid_call):
                receiver = StubMessageReceiver()
                for call in setup_calls:
                    getattr(receiver, call)()
                with self.assertRaises(AssertionError):
                    getattr(receiver, invalid_call)()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
