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

from pyspark.messages.socket.spark_socket_message_receiver import SparkSocketMessageReceiver
from pyspark.messages.spark_message_receiver import SparkMessageReceiver
from pyspark.messages.zero_copy_byte_stream import ZeroCopyByteStream
from pyspark.serializers import SpecialLengths


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


class SparkSocketMessageReceiverTests(unittest.TestCase):
    """Tests for the socket-framed receiver."""

    def _framed_init(self, content: bytes) -> io.BytesIO:
        frame = io.BytesIO()
        frame.write(SpecialLengths.START_OF_INIT_MESSAGE.to_bytes(4, "big", signed=True))
        frame.write(len(content).to_bytes(4, "big", signed=True))
        frame.write(content)
        frame.seek(0)
        return frame

    def test_init_message_reads_declared_content(self):
        receiver = SparkSocketMessageReceiver(self._framed_init(b"abcdef"))
        stream = receiver.get_init_message()
        self.assertEqual(bytes(stream.read(6)), b"abcdef")

    def test_init_message_over_read_raises_eof_instead_of_blocking(self):
        # The init message is fully materialized, so the stream must be marked finished:
        # a parse that runs past the declared length (e.g. a writer/reader protocol
        # mismatch, see SPARK-58241) must fail fast with EOFError -- with an unfinished
        # stream it would block forever waiting for a chunk that never arrives.
        receiver = SparkSocketMessageReceiver(self._framed_init(b"abcdef"))
        stream = receiver.get_init_message()
        stream.read(4)
        with self.assertRaises(EOFError):
            stream.read(4)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
