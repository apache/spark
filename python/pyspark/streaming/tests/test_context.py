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
import os
import struct
import tempfile
import time

from pyspark.streaming import StreamingContext
from pyspark.testing.streamingutils import PySparkStreamingTestCase


class StreamingContextTests(PySparkStreamingTestCase):

    duration = 0.1
    setupCalled = False

    def _add_input_stream(self):
        inputs = [range(1, x) for x in range(101)]
        stream = self.ssc.queueStream(inputs)
        self._collect(stream, 1, block=False)

    def test_stop_only_streaming_context(self):
        self._add_input_stream()
        self.ssc.start()
        self.ssc.stop(False)
        self.assertEqual(len(self.sc.parallelize(range(5), 5).glom().collect()), 5)

    def test_stop_multiple_times(self):
        self._add_input_stream()
        self.ssc.start()
        self.ssc.stop(False)
        self.ssc.stop(False)

    def test_queue_stream(self):
        input = [list(range(i + 1)) for i in range(3)]
        dstream = self.ssc.queueStream(input)
        result = self._collect(dstream, 3)
        self.assertEqual(input, result)

    def test_text_file_stream(self):
        d = tempfile.mkdtemp()
        self.ssc = StreamingContext(self.sc, self.duration)
        dstream2 = self.ssc.textFileStream(d).map(int)
        result = self._collect(dstream2, 2, block=False)
        self.ssc.start()
        for name in ('a', 'b'):
            time.sleep(1)
            with open(os.path.join(d, name), "w") as f:
                f.writelines(["%d\n" % i for i in range(10)])
        self.wait_for(result, 2)
        self.assertEqual([list(range(10)), list(range(10))], result)

    def test_binary_records_stream(self):
        d = tempfile.mkdtemp()
        self.ssc = StreamingContext(self.sc, self.duration)
        dstream = self.ssc.binaryRecordsStream(d, 10).map(
            lambda v: struct.unpack("10b", bytes(v)))
        result = self._collect(dstream, 2, block=False)
        self.ssc.start()
        for name in ('a', 'b'):
            time.sleep(1)
            with open(os.path.join(d, name), "wb") as f:
                f.write(bytearray(range(10)))
        self.wait_for(result, 2)
        self.assertEqual([list(range(10)), list(range(10))], [list(v[0]) for v in result])

    def test_union(self):
        input = [list(range(i + 1)) for i in range(3)]
        dstream = self.ssc.queueStream(input)
        dstream2 = self.ssc.queueStream(input)
        dstream3 = self.ssc.union(dstream, dstream2)
        result = self._collect(dstream3, 3)
        expected = [i * 2 for i in input]
        self.assertEqual(expected, result)

    def test_transform(self):
        dstream1 = self.ssc.queueStream([[1]])
        dstream2 = self.ssc.queueStream([[2]])
        dstream3 = self.ssc.queueStream([[3]])

        def func(rdds):
            rdd1, rdd2, rdd3 = rdds
            return rdd2.union(rdd3).union(rdd1)

        dstream = self.ssc.transform([dstream1, dstream2, dstream3], func)

        self.assertEqual([2, 3, 1], self._take(dstream, 3))

    def test_transform_pairrdd(self):
        # This regression test case is for SPARK-17756.
        dstream = self.ssc.queueStream(
            [[1], [2], [3]]).transform(lambda rdd: rdd.cartesian(rdd))
        self.assertEqual([(1, 1), (2, 2), (3, 3)], self._take(dstream, 3))

    def test_get_active(self):
        self.assertEqual(StreamingContext.getActive(), None)

        # Verify that getActive() returns the active context
        self.ssc.queueStream([[1]]).foreachRDD(lambda rdd: rdd.count())
        self.ssc.start()
        self.assertEqual(StreamingContext.getActive(), self.ssc)

        # Verify that getActive() returns None
        self.ssc.stop(False)
        self.assertEqual(StreamingContext.getActive(), None)

        # Verify that if the Java context is stopped, then getActive() returns None
        self.ssc = StreamingContext(self.sc, self.duration)
        self.ssc.queueStream([[1]]).foreachRDD(lambda rdd: rdd.count())
        self.ssc.start()
        self.assertEqual(StreamingContext.getActive(), self.ssc)
        self.ssc._jssc.stop(False)
        self.assertEqual(StreamingContext.getActive(), None)

    def test_get_active_or_create(self):
        # Test StreamingContext.getActiveOrCreate() without checkpoint data
        # See CheckpointTests for tests with checkpoint data
        self.ssc = None
        self.assertEqual(StreamingContext.getActive(), None)

        def setupFunc():
            ssc = StreamingContext(self.sc, self.duration)
            ssc.queueStream([[1]]).foreachRDD(lambda rdd: rdd.count())
            self.setupCalled = True
            return ssc

        # Verify that getActiveOrCreate() (w/o checkpoint) calls setupFunc when no context is active
        self.setupCalled = False
        self.ssc = StreamingContext.getActiveOrCreate(None, setupFunc)
        self.assertTrue(self.setupCalled)

        # Verify that getActiveOrCreate() returns active context and does not call the setupFunc
        self.ssc.start()
        self.setupCalled = False
        self.assertEqual(StreamingContext.getActiveOrCreate(None, setupFunc), self.ssc)
        self.assertFalse(self.setupCalled)

        # Verify that getActiveOrCreate() calls setupFunc after active context is stopped
        self.ssc.stop(False)
        self.setupCalled = False
        self.ssc = StreamingContext.getActiveOrCreate(None, setupFunc)
        self.assertTrue(self.setupCalled)

        # Verify that if the Java context is stopped, then getActive() returns None
        self.ssc = StreamingContext(self.sc, self.duration)
        self.ssc.queueStream([[1]]).foreachRDD(lambda rdd: rdd.count())
        self.ssc.start()
        self.assertEqual(StreamingContext.getActive(), self.ssc)
        self.ssc._jssc.stop(False)
        self.setupCalled = False
        self.ssc = StreamingContext.getActiveOrCreate(None, setupFunc)
        self.assertTrue(self.setupCalled)

    def test_await_termination_or_timeout(self):
        self._add_input_stream()
        self.ssc.start()
        self.assertFalse(self.ssc.awaitTerminationOrTimeout(0.001))
        self.ssc.stop(False)
        self.assertTrue(self.ssc.awaitTerminationOrTimeout(0.001))


if __name__ == "__main__":
    import unittest
    from pyspark.streaming.tests.test_context import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
