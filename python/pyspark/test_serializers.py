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
import math
import struct
import sys
import unittest

try:
    import xmlrunner
except ImportError:
    xmlrunner = None

from pyspark import serializers


def read_int(b):
    return struct.unpack("!i", b)[0]


def write_int(i):
    return struct.pack("!i", i)


class SerializersTest(unittest.TestCase):

    def test_chunked_stream(self):
        original_bytes = bytearray(range(100))
        for data_length in [1, 10, 100]:
            for buffer_length in [1, 2, 3, 5, 20, 99, 100, 101, 500]:
                dest = ByteArrayOutput()
                stream_out = serializers.ChunkedStream(dest, buffer_length)
                stream_out.write(original_bytes[:data_length])
                stream_out.close()
                num_chunks = int(math.ceil(float(data_length) / buffer_length))
                # length for each chunk, and a final -1 at the very end
                exp_size = (num_chunks + 1) * 4 + data_length
                self.assertEqual(len(dest.buffer), exp_size)
                dest_pos = 0
                data_pos = 0
                for chunk_idx in range(num_chunks):
                    chunk_length = read_int(dest.buffer[dest_pos:(dest_pos + 4)])
                    if chunk_idx == num_chunks - 1:
                        exp_length = data_length % buffer_length
                        if exp_length == 0:
                            exp_length = buffer_length
                    else:
                        exp_length = buffer_length
                    self.assertEqual(chunk_length, exp_length)
                    dest_pos += 4
                    dest_chunk = dest.buffer[dest_pos:dest_pos + chunk_length]
                    orig_chunk = original_bytes[data_pos:data_pos + chunk_length]
                    self.assertEqual(dest_chunk, orig_chunk)
                    dest_pos += chunk_length
                    data_pos += chunk_length
                # ends with a -1
                self.assertEqual(dest.buffer[-4:], write_int(-1))


class ByteArrayOutput(object):
    def __init__(self):
        self.buffer = bytearray()

    def write(self, b):
        self.buffer += b

    def close(self):
        pass

if __name__ == '__main__':
    from pyspark.test_serializers import *
    if xmlrunner:
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'), verbosity=2)
    else:
        unittest.main(verbosity=2)
