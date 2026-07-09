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

from typing import BinaryIO

from pyspark.serializers import read_int, SpecialLengths
from pyspark.messages.zero_copy_byte_stream import ZeroCopyByteStream
from pyspark.messages.spark_message_receiver import (
    SparkMessageReceiver,
)


def _assert_message_id(message_id: int, expected: int) -> None:
    assert message_id == expected, (
        f"Expected message with id {expected} but got message with id {message_id} instead."
    )


class SparkSocketMessageReceiver(SparkMessageReceiver):
    def __init__(self, infile: BinaryIO):
        super().__init__()
        self._infile = infile

    def _do_get_init_message(self) -> ZeroCopyByteStream:
        message_id = read_int(self._infile)
        _assert_message_id(message_id, SpecialLengths.START_OF_INIT_MESSAGE)

        # Read the length and init content
        message_length = read_int(self._infile)
        message_content = self._infile.read(message_length)

        return ZeroCopyByteStream(memoryview(message_content))

    def _do_get_data_stream(self) -> BinaryIO:
        # For socket communication, we just pass along the underlying socket
        # for the data channel. We already stripped the initialization data
        # at this state. Therefore, any bytes following this are data bytes.
        #
        # Note: We deliberately did not introduce a message header for
        # data messages to reduce the overhead, especially for small
        # batch sizes and real-time-mode (RTM).
        return self._infile

    def _do_get_finish_signal_from_stream(self) -> None:
        # If everything finished properly, we should read END_OF_STREAM.
        # Anything else means something went wrong during processing.
        message_id = read_int(self._infile)
        _assert_message_id(message_id, SpecialLengths.END_OF_STREAM)
