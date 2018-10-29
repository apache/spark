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

import glob
import os
import sys
from itertools import chain
import time
import operator
import tempfile
import random
import struct
import shutil
from functools import reduce

try:
    import xmlrunner
except ImportError:
    xmlrunner = None

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

if sys.version >= "3":
    long = int

from pyspark.context import SparkConf, SparkContext, RDD
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.streaming.listener import StreamingListener


class PySparkStreamingTestCase(unittest.TestCase):

    timeout = 30  # seconds
    duration = .5

    @classmethod
    def setUpClass(cls):
        class_name = cls.__name__
        conf = SparkConf().set("spark.default.parallelism", 1)
        cls.sc = SparkContext(appName=class_name, conf=conf)
        cls.sc.setCheckpointDir(tempfile.mkdtemp())

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
        # Clean up in the JVM just in case there has been some issues in Python API
        try:
            jSparkContextOption = SparkContext._jvm.SparkContext.get()
            if jSparkContextOption.nonEmpty():
                jSparkContextOption.get().stop()
        except:
            pass

    def setUp(self):
        self.ssc = StreamingContext(self.sc, self.duration)

    def tearDown(self):
        if self.ssc is not None:
            self.ssc.stop(False)
        # Clean up in the JVM just in case there has been some issues in Python API
        try:
            jStreamingContextOption = StreamingContext._jvm.SparkContext.getActive()
            if jStreamingContextOption.nonEmpty():
                jStreamingContextOption.get().stop(False)
        except:
            pass

    def wait_for(self, result, n):
        start_time = time.time()
        while len(result) < n and time.time() - start_time < self.timeout:
            time.sleep(0.01)
        if len(result) < n:
            print("timeout after", self.timeout)

    def _take(self, dstream, n):
        """
        Return the first `n` elements in the stream (will start and stop).
        """
        results = []

        def take(_, rdd):
            if rdd and len(results) < n:
                results.extend(rdd.take(n - len(results)))

        dstream.foreachRDD(take)

        self.ssc.start()
        self.wait_for(results, n)
        return results

    def _collect(self, dstream, n, block=True):
        """
        Collect each RDDs into the returned list.

        :return: list, which will have the collected items.
        """
        result = []

        def get_output(_, rdd):
            if rdd and len(result) < n:
                r = rdd.collect()
                if r:
                    result.append(r)

        dstream.foreachRDD(get_output)

        if not block:
            return result

        self.ssc.start()
        self.wait_for(result, n)
        return result

    def _test_func(self, input, func, expected, sort=False, input2=None):
        """
        @param input: dataset for the test. This should be list of lists.
        @param func: wrapped function. This function should return PythonDStream object.
        @param expected: expected output for this testcase.
        """
        if not isinstance(input[0], RDD):
            input = [self.sc.parallelize(d, 1) for d in input]
        input_stream = self.ssc.queueStream(input)
        if input2 and not isinstance(input2[0], RDD):
            input2 = [self.sc.parallelize(d, 1) for d in input2]
        input_stream2 = self.ssc.queueStream(input2) if input2 is not None else None

        # Apply test function to stream.
        if input2:
            stream = func(input_stream, input_stream2)
        else:
            stream = func(input_stream)

        result = self._collect(stream, len(expected))
        if sort:
            self._sort_result_based_on_key(result)
            self._sort_result_based_on_key(expected)
        self.assertEqual(expected, result)

    def _sort_result_based_on_key(self, outputs):
        """Sort the list based on first value."""
        for output in outputs:
            output.sort(key=lambda x: x[0])


class BasicOperationTests(PySparkStreamingTestCase):

    def test_map(self):
        """Basic operation test for DStream.map."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.map(str)
        expected = [list(map(str, x)) for x in input]
        self._test_func(input, func, expected)

    def test_flatMap(self):
        """Basic operation test for DStream.flatMap."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.flatMap(lambda x: (x, x * 2))
        expected = [list(chain.from_iterable((map(lambda y: [y, y * 2], x))))
                    for x in input]
        self._test_func(input, func, expected)

    def test_filter(self):
        """Basic operation test for DStream.filter."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.filter(lambda x: x % 2 == 0)
        expected = [[y for y in x if y % 2 == 0] for x in input]
        self._test_func(input, func, expected)

    def test_count(self):
        """Basic operation test for DStream.count."""
        input = [range(5), range(10), range(20)]

        def func(dstream):
            return dstream.count()
        expected = [[len(x)] for x in input]
        self._test_func(input, func, expected)

    def test_slice(self):
        """Basic operation test for DStream.slice."""
        import datetime as dt
        self.ssc = StreamingContext(self.sc, 1.0)
        self.ssc.remember(4.0)
        input = [[1], [2], [3], [4]]
        stream = self.ssc.queueStream([self.sc.parallelize(d, 1) for d in input])

        time_vals = []

        def get_times(t, rdd):
            if rdd and len(time_vals) < len(input):
                time_vals.append(t)

        stream.foreachRDD(get_times)

        self.ssc.start()
        self.wait_for(time_vals, 4)
        begin_time = time_vals[0]

        def get_sliced(begin_delta, end_delta):
            begin = begin_time + dt.timedelta(seconds=begin_delta)
            end = begin_time + dt.timedelta(seconds=end_delta)
            rdds = stream.slice(begin, end)
            result_list = [rdd.collect() for rdd in rdds]
            return [r for result in result_list for r in result]

        self.assertEqual(set([1]), set(get_sliced(0, 0)))
        self.assertEqual(set([2, 3]), set(get_sliced(1, 2)))
        self.assertEqual(set([2, 3, 4]), set(get_sliced(1, 4)))
        self.assertEqual(set([1, 2, 3, 4]), set(get_sliced(0, 4)))

    def test_reduce(self):
        """Basic operation test for DStream.reduce."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.reduce(operator.add)
        expected = [[reduce(operator.add, x)] for x in input]
        self._test_func(input, func, expected)

    def test_reduceByKey(self):
        """Basic operation test for DStream.reduceByKey."""
        input = [[("a", 1), ("a", 1), ("b", 1), ("b", 1)],
                 [("", 1), ("", 1), ("", 1), ("", 1)],
                 [(1, 1), (1, 1), (2, 1), (2, 1), (3, 1)]]

        def func(dstream):
            return dstream.reduceByKey(operator.add)
        expected = [[("a", 2), ("b", 2)], [("", 4)], [(1, 2), (2, 2), (3, 1)]]
        self._test_func(input, func, expected, sort=True)

    def test_mapValues(self):
        """Basic operation test for DStream.mapValues."""
        input = [[("a", 2), ("b", 2), ("c", 1), ("d", 1)],
                 [(0, 4), (1, 1), (2, 2), (3, 3)],
                 [(1, 1), (2, 1), (3, 1), (4, 1)]]

        def func(dstream):
            return dstream.mapValues(lambda x: x + 10)
        expected = [[("a", 12), ("b", 12), ("c", 11), ("d", 11)],
                    [(0, 14), (1, 11), (2, 12), (3, 13)],
                    [(1, 11), (2, 11), (3, 11), (4, 11)]]
        self._test_func(input, func, expected, sort=True)

    def test_flatMapValues(self):
        """Basic operation test for DStream.flatMapValues."""
        input = [[("a", 2), ("b", 2), ("c", 1), ("d", 1)],
                 [(0, 4), (1, 1), (2, 1), (3, 1)],
                 [(1, 1), (2, 1), (3, 1), (4, 1)]]

        def func(dstream):
            return dstream.flatMapValues(lambda x: (x, x + 10))
        expected = [[("a", 2), ("a", 12), ("b", 2), ("b", 12),
                     ("c", 1), ("c", 11), ("d", 1), ("d", 11)],
                    [(0, 4), (0, 14), (1, 1), (1, 11), (2, 1), (2, 11), (3, 1), (3, 11)],
                    [(1, 1), (1, 11), (2, 1), (2, 11), (3, 1), (3, 11), (4, 1), (4, 11)]]
        self._test_func(input, func, expected)

    def test_glom(self):
        """Basic operation test for DStream.glom."""
        input = [range(1, 5), range(5, 9), range(9, 13)]
        rdds = [self.sc.parallelize(r, 2) for r in input]

        def func(dstream):
            return dstream.glom()
        expected = [[[1, 2], [3, 4]], [[5, 6], [7, 8]], [[9, 10], [11, 12]]]
        self._test_func(rdds, func, expected)

    def test_mapPartitions(self):
        """Basic operation test for DStream.mapPartitions."""
        input = [range(1, 5), range(5, 9), range(9, 13)]
        rdds = [self.sc.parallelize(r, 2) for r in input]

        def func(dstream):
            def f(iterator):
                yield sum(iterator)
            return dstream.mapPartitions(f)
        expected = [[3, 7], [11, 15], [19, 23]]
        self._test_func(rdds, func, expected)

    def test_countByValue(self):
        """Basic operation test for DStream.countByValue."""
        input = [list(range(1, 5)) * 2, list(range(5, 7)) + list(range(5, 9)), ["a", "a", "b", ""]]

        def func(dstream):
            return dstream.countByValue()
        expected = [[(1, 2), (2, 2), (3, 2), (4, 2)],
                    [(5, 2), (6, 2), (7, 1), (8, 1)],
                    [("a", 2), ("b", 1), ("", 1)]]
        self._test_func(input, func, expected, sort=True)

    def test_groupByKey(self):
        """Basic operation test for DStream.groupByKey."""
        input = [[(1, 1), (2, 1), (3, 1), (4, 1)],
                 [(1, 1), (1, 1), (1, 1), (2, 1), (2, 1), (3, 1)],
                 [("a", 1), ("a", 1), ("b", 1), ("", 1), ("", 1), ("", 1)]]

        def func(dstream):
            return dstream.groupByKey().mapValues(list)

        expected = [[(1, [1]), (2, [1]), (3, [1]), (4, [1])],
                    [(1, [1, 1, 1]), (2, [1, 1]), (3, [1])],
                    [("a", [1, 1]), ("b", [1]), ("", [1, 1, 1])]]
        self._test_func(input, func, expected, sort=True)

    def test_combineByKey(self):
        """Basic operation test for DStream.combineByKey."""
        input = [[(1, 1), (2, 1), (3, 1), (4, 1)],
                 [(1, 1), (1, 1), (1, 1), (2, 1), (2, 1), (3, 1)],
                 [("a", 1), ("a", 1), ("b", 1), ("", 1), ("", 1), ("", 1)]]

        def func(dstream):
            def add(a, b):
                return a + str(b)
            return dstream.combineByKey(str, add, add)
        expected = [[(1, "1"), (2, "1"), (3, "1"), (4, "1")],
                    [(1, "111"), (2, "11"), (3, "1")],
                    [("a", "11"), ("b", "1"), ("", "111")]]
        self._test_func(input, func, expected, sort=True)

    def test_repartition(self):
        input = [range(1, 5), range(5, 9)]
        rdds = [self.sc.parallelize(r, 2) for r in input]

        def func(dstream):
            return dstream.repartition(1).glom()
        expected = [[[1, 2, 3, 4]], [[5, 6, 7, 8]]]
        self._test_func(rdds, func, expected)

    def test_union(self):
        input1 = [range(3), range(5), range(6)]
        input2 = [range(3, 6), range(5, 6)]

        def func(d1, d2):
            return d1.union(d2)

        expected = [list(range(6)), list(range(6)), list(range(6))]
        self._test_func(input1, func, expected, input2=input2)

    def test_cogroup(self):
        input = [[(1, 1), (2, 1), (3, 1)],
                 [(1, 1), (1, 1), (1, 1), (2, 1)],
                 [("a", 1), ("a", 1), ("b", 1), ("", 1), ("", 1)]]
        input2 = [[(1, 2)],
                  [(4, 1)],
                  [("a", 1), ("a", 1), ("b", 1), ("", 1), ("", 2)]]

        def func(d1, d2):
            return d1.cogroup(d2).mapValues(lambda vs: tuple(map(list, vs)))

        expected = [[(1, ([1], [2])), (2, ([1], [])), (3, ([1], []))],
                    [(1, ([1, 1, 1], [])), (2, ([1], [])), (4, ([], [1]))],
                    [("a", ([1, 1], [1, 1])), ("b", ([1], [1])), ("", ([1, 1], [1, 2]))]]
        self._test_func(input, func, expected, sort=True, input2=input2)

    def test_join(self):
        input = [[('a', 1), ('b', 2)]]
        input2 = [[('b', 3), ('c', 4)]]

        def func(a, b):
            return a.join(b)

        expected = [[('b', (2, 3))]]
        self._test_func(input, func, expected, True, input2)

    def test_left_outer_join(self):
        input = [[('a', 1), ('b', 2)]]
        input2 = [[('b', 3), ('c', 4)]]

        def func(a, b):
            return a.leftOuterJoin(b)

        expected = [[('a', (1, None)), ('b', (2, 3))]]
        self._test_func(input, func, expected, True, input2)

    def test_right_outer_join(self):
        input = [[('a', 1), ('b', 2)]]
        input2 = [[('b', 3), ('c', 4)]]

        def func(a, b):
            return a.rightOuterJoin(b)

        expected = [[('b', (2, 3)), ('c', (None, 4))]]
        self._test_func(input, func, expected, True, input2)

    def test_full_outer_join(self):
        input = [[('a', 1), ('b', 2)]]
        input2 = [[('b', 3), ('c', 4)]]

        def func(a, b):
            return a.fullOuterJoin(b)

        expected = [[('a', (1, None)), ('b', (2, 3)), ('c', (None, 4))]]
        self._test_func(input, func, expected, True, input2)

    def test_update_state_by_key(self):

        def updater(vs, s):
            if not s:
                s = []
            s.extend(vs)
            return s

        input = [[('k', i)] for i in range(5)]

        def func(dstream):
            return dstream.updateStateByKey(updater)

        expected = [[0], [0, 1], [0, 1, 2], [0, 1, 2, 3], [0, 1, 2, 3, 4]]
        expected = [[('k', v)] for v in expected]
        self._test_func(input, func, expected)

    def test_update_state_by_key_initial_rdd(self):

        def updater(vs, s):
            if not s:
                s = []
            s.extend(vs)
            return s

        initial = [('k', [0, 1])]
        initial = self.sc.parallelize(initial, 1)

        input = [[('k', i)] for i in range(2, 5)]

        def func(dstream):
            return dstream.updateStateByKey(updater, initialRDD=initial)

        expected = [[0, 1, 2], [0, 1, 2, 3], [0, 1, 2, 3, 4]]
        expected = [[('k', v)] for v in expected]
        self._test_func(input, func, expected)

    def test_failed_func(self):
        # Test failure in
        # TransformFunction.apply(rdd: Option[RDD[_]], time: Time)
        input = [self.sc.parallelize([d], 1) for d in range(4)]
        input_stream = self.ssc.queueStream(input)

        def failed_func(i):
            raise ValueError("This is a special error")

        input_stream.map(failed_func).pprint()
        self.ssc.start()
        try:
            self.ssc.awaitTerminationOrTimeout(10)
        except:
            import traceback
            failure = traceback.format_exc()
            self.assertTrue("This is a special error" in failure)
            return

        self.fail("a failed func should throw an error")

    def test_failed_func2(self):
        # Test failure in
        # TransformFunction.apply(rdd: Option[RDD[_]], rdd2: Option[RDD[_]], time: Time)
        input = [self.sc.parallelize([d], 1) for d in range(4)]
        input_stream1 = self.ssc.queueStream(input)
        input_stream2 = self.ssc.queueStream(input)

        def failed_func(rdd1, rdd2):
            raise ValueError("This is a special error")

        input_stream1.transformWith(failed_func, input_stream2, True).pprint()
        self.ssc.start()
        try:
            self.ssc.awaitTerminationOrTimeout(10)
        except:
            import traceback
            failure = traceback.format_exc()
            self.assertTrue("This is a special error" in failure)
            return

        self.fail("a failed func should throw an error")

    def test_failed_func_with_reseting_failure(self):
        input = [self.sc.parallelize([d], 1) for d in range(4)]
        input_stream = self.ssc.queueStream(input)

        def failed_func(i):
            if i == 1:
                # Make it fail in the second batch
                raise ValueError("This is a special error")
            else:
                return i

        # We should be able to see the results of the 3rd and 4th batches even if the second batch
        # fails
        expected = [[0], [2], [3]]
        self.assertEqual(expected, self._collect(input_stream.map(failed_func), 3))
        try:
            self.ssc.awaitTerminationOrTimeout(10)
        except:
            import traceback
            failure = traceback.format_exc()
            self.assertTrue("This is a special error" in failure)
            return

        self.fail("a failed func should throw an error")


class StreamingListenerTests(PySparkStreamingTestCase):

    duration = .5

    class BatchInfoCollector(StreamingListener):

        def __init__(self):
            super(StreamingListener, self).__init__()
            self.batchInfosCompleted = []
            self.batchInfosStarted = []
            self.batchInfosSubmitted = []
            self.streamingStartedTime = []

        def onStreamingStarted(self, streamingStarted):
            self.streamingStartedTime.append(streamingStarted.time)

        def onBatchSubmitted(self, batchSubmitted):
            self.batchInfosSubmitted.append(batchSubmitted.batchInfo())

        def onBatchStarted(self, batchStarted):
            self.batchInfosStarted.append(batchStarted.batchInfo())

        def onBatchCompleted(self, batchCompleted):
            self.batchInfosCompleted.append(batchCompleted.batchInfo())

    def test_batch_info_reports(self):
        batch_collector = self.BatchInfoCollector()
        self.ssc.addStreamingListener(batch_collector)
        input = [[1], [2], [3], [4]]

        def func(dstream):
            return dstream.map(int)
        expected = [[1], [2], [3], [4]]
        self._test_func(input, func, expected)

        batchInfosSubmitted = batch_collector.batchInfosSubmitted
        batchInfosStarted = batch_collector.batchInfosStarted
        batchInfosCompleted = batch_collector.batchInfosCompleted
        streamingStartedTime = batch_collector.streamingStartedTime

        self.wait_for(batchInfosCompleted, 4)

        self.assertEqual(len(streamingStartedTime), 1)

        self.assertGreaterEqual(len(batchInfosSubmitted), 4)
        for info in batchInfosSubmitted:
            self.assertGreaterEqual(info.batchTime().milliseconds(), 0)
            self.assertGreaterEqual(info.submissionTime(), 0)

            for streamId in info.streamIdToInputInfo():
                streamInputInfo = info.streamIdToInputInfo()[streamId]
                self.assertGreaterEqual(streamInputInfo.inputStreamId(), 0)
                self.assertGreaterEqual(streamInputInfo.numRecords, 0)
                for key in streamInputInfo.metadata():
                    self.assertIsNotNone(streamInputInfo.metadata()[key])
                self.assertIsNotNone(streamInputInfo.metadataDescription())

            for outputOpId in info.outputOperationInfos():
                outputInfo = info.outputOperationInfos()[outputOpId]
                self.assertGreaterEqual(outputInfo.batchTime().milliseconds(), 0)
                self.assertGreaterEqual(outputInfo.id(), 0)
                self.assertIsNotNone(outputInfo.name())
                self.assertIsNotNone(outputInfo.description())
                self.assertGreaterEqual(outputInfo.startTime(), -1)
                self.assertGreaterEqual(outputInfo.endTime(), -1)
                self.assertIsNone(outputInfo.failureReason())

            self.assertEqual(info.schedulingDelay(), -1)
            self.assertEqual(info.processingDelay(), -1)
            self.assertEqual(info.totalDelay(), -1)
            self.assertEqual(info.numRecords(), 0)

        self.assertGreaterEqual(len(batchInfosStarted), 4)
        for info in batchInfosStarted:
            self.assertGreaterEqual(info.batchTime().milliseconds(), 0)
            self.assertGreaterEqual(info.submissionTime(), 0)

            for streamId in info.streamIdToInputInfo():
                streamInputInfo = info.streamIdToInputInfo()[streamId]
                self.assertGreaterEqual(streamInputInfo.inputStreamId(), 0)
                self.assertGreaterEqual(streamInputInfo.numRecords, 0)
                for key in streamInputInfo.metadata():
                    self.assertIsNotNone(streamInputInfo.metadata()[key])
                self.assertIsNotNone(streamInputInfo.metadataDescription())

            for outputOpId in info.outputOperationInfos():
                outputInfo = info.outputOperationInfos()[outputOpId]
                self.assertGreaterEqual(outputInfo.batchTime().milliseconds(), 0)
                self.assertGreaterEqual(outputInfo.id(), 0)
                self.assertIsNotNone(outputInfo.name())
                self.assertIsNotNone(outputInfo.description())
                self.assertGreaterEqual(outputInfo.startTime(), -1)
                self.assertGreaterEqual(outputInfo.endTime(), -1)
                self.assertIsNone(outputInfo.failureReason())

            self.assertGreaterEqual(info.schedulingDelay(), 0)
            self.assertEqual(info.processingDelay(), -1)
            self.assertEqual(info.totalDelay(), -1)
            self.assertEqual(info.numRecords(), 0)

        self.assertGreaterEqual(len(batchInfosCompleted), 4)
        for info in batchInfosCompleted:
            self.assertGreaterEqual(info.batchTime().milliseconds(), 0)
            self.assertGreaterEqual(info.submissionTime(), 0)

            for streamId in info.streamIdToInputInfo():
                streamInputInfo = info.streamIdToInputInfo()[streamId]
                self.assertGreaterEqual(streamInputInfo.inputStreamId(), 0)
                self.assertGreaterEqual(streamInputInfo.numRecords, 0)
                for key in streamInputInfo.metadata():
                    self.assertIsNotNone(streamInputInfo.metadata()[key])
                self.assertIsNotNone(streamInputInfo.metadataDescription())

            for outputOpId in info.outputOperationInfos():
                outputInfo = info.outputOperationInfos()[outputOpId]
                self.assertGreaterEqual(outputInfo.batchTime().milliseconds(), 0)
                self.assertGreaterEqual(outputInfo.id(), 0)
                self.assertIsNotNone(outputInfo.name())
                self.assertIsNotNone(outputInfo.description())
                self.assertGreaterEqual(outputInfo.startTime(), 0)
                self.assertGreaterEqual(outputInfo.endTime(), 0)
                self.assertIsNone(outputInfo.failureReason())

            self.assertGreaterEqual(info.schedulingDelay(), 0)
            self.assertGreaterEqual(info.processingDelay(), 0)
            self.assertGreaterEqual(info.totalDelay(), 0)
            self.assertEqual(info.numRecords(), 0)


class WindowFunctionTests(PySparkStreamingTestCase):

    timeout = 15

    def test_window(self):
        input = [range(1), range(2), range(3), range(4), range(5)]

        def func(dstream):
            return dstream.window(1.5, .5).count()

        expected = [[1], [3], [6], [9], [12], [9], [5]]
        self._test_func(input, func, expected)

    def test_count_by_window(self):
        input = [range(1), range(2), range(3), range(4), range(5)]

        def func(dstream):
            return dstream.countByWindow(1.5, .5)

        expected = [[1], [3], [6], [9], [12], [9], [5]]
        self._test_func(input, func, expected)

    def test_count_by_window_large(self):
        input = [range(1), range(2), range(3), range(4), range(5), range(6)]

        def func(dstream):
            return dstream.countByWindow(2.5, .5)

        expected = [[1], [3], [6], [10], [15], [20], [18], [15], [11], [6]]
        self._test_func(input, func, expected)

    def test_count_by_value_and_window(self):
        input = [range(1), range(2), range(3), range(4), range(5), range(6)]

        def func(dstream):
            return dstream.countByValueAndWindow(2.5, .5)

        expected = [[(0, 1)],
                    [(0, 2), (1, 1)],
                    [(0, 3), (1, 2), (2, 1)],
                    [(0, 4), (1, 3), (2, 2), (3, 1)],
                    [(0, 5), (1, 4), (2, 3), (3, 2), (4, 1)],
                    [(0, 5), (1, 5), (2, 4), (3, 3), (4, 2), (5, 1)],
                    [(0, 4), (1, 4), (2, 4), (3, 3), (4, 2), (5, 1)],
                    [(0, 3), (1, 3), (2, 3), (3, 3), (4, 2), (5, 1)],
                    [(0, 2), (1, 2), (2, 2), (3, 2), (4, 2), (5, 1)],
                    [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1)]]
        self._test_func(input, func, expected)

    def test_group_by_key_and_window(self):
        input = [[('a', i)] for i in range(5)]

        def func(dstream):
            return dstream.groupByKeyAndWindow(1.5, .5).mapValues(list)

        expected = [[('a', [0])], [('a', [0, 1])], [('a', [0, 1, 2])], [('a', [1, 2, 3])],
                    [('a', [2, 3, 4])], [('a', [3, 4])], [('a', [4])]]
        self._test_func(input, func, expected)

    def test_reduce_by_invalid_window(self):
        input1 = [range(3), range(5), range(1), range(6)]
        d1 = self.ssc.queueStream(input1)
        self.assertRaises(ValueError, lambda: d1.reduceByKeyAndWindow(None, None, 0.1, 0.1))
        self.assertRaises(ValueError, lambda: d1.reduceByKeyAndWindow(None, None, 1, 0.1))

    def test_reduce_by_key_and_window_with_none_invFunc(self):
        input = [range(1), range(2), range(3), range(4), range(5), range(6)]

        def func(dstream):
            return dstream.map(lambda x: (x, 1))\
                .reduceByKeyAndWindow(operator.add, None, 5, 1)\
                .filter(lambda kv: kv[1] > 0).count()

        expected = [[2], [4], [6], [6], [6], [6]]
        self._test_func(input, func, expected)


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


class CheckpointTests(unittest.TestCase):

    setupCalled = False

    @staticmethod
    def tearDownClass():
        # Clean up in the JVM just in case there has been some issues in Python API
        if SparkContext._jvm is not None:
            jStreamingContextOption = \
                SparkContext._jvm.org.apache.spark.streaming.StreamingContext.getActive()
            if jStreamingContextOption.nonEmpty():
                jStreamingContextOption.get().stop()

    def setUp(self):
        self.ssc = None
        self.sc = None
        self.cpd = None

    def tearDown(self):
        if self.ssc is not None:
            self.ssc.stop(True)
        if self.sc is not None:
            self.sc.stop()
        if self.cpd is not None:
            shutil.rmtree(self.cpd)

    def test_transform_function_serializer_failure(self):
        inputd = tempfile.mkdtemp()
        self.cpd = tempfile.mkdtemp("test_transform_function_serializer_failure")

        def setup():
            conf = SparkConf().set("spark.default.parallelism", 1)
            sc = SparkContext(conf=conf)
            ssc = StreamingContext(sc, 0.5)

            # A function that cannot be serialized
            def process(time, rdd):
                sc.parallelize(range(1, 10))

            ssc.textFileStream(inputd).foreachRDD(process)
            return ssc

        self.ssc = StreamingContext.getOrCreate(self.cpd, setup)
        try:
            self.ssc.start()
        except:
            import traceback
            failure = traceback.format_exc()
            self.assertTrue(
                "It appears that you are attempting to reference SparkContext" in failure)
            return

        self.fail("using SparkContext in process should fail because it's not Serializable")

    def test_get_or_create_and_get_active_or_create(self):
        inputd = tempfile.mkdtemp()
        outputd = tempfile.mkdtemp() + "/"

        def updater(vs, s):
            return sum(vs, s or 0)

        def setup():
            conf = SparkConf().set("spark.default.parallelism", 1)
            sc = SparkContext(conf=conf)
            ssc = StreamingContext(sc, 2)
            dstream = ssc.textFileStream(inputd).map(lambda x: (x, 1))
            wc = dstream.updateStateByKey(updater)
            wc.map(lambda x: "%s,%d" % x).saveAsTextFiles(outputd + "test")
            wc.checkpoint(2)
            self.setupCalled = True
            return ssc

        # Verify that getOrCreate() calls setup() in absence of checkpoint files
        self.cpd = tempfile.mkdtemp("test_streaming_cps")
        self.setupCalled = False
        self.ssc = StreamingContext.getOrCreate(self.cpd, setup)
        self.assertTrue(self.setupCalled)

        self.ssc.start()

        def check_output(n):
            while not os.listdir(outputd):
                if self.ssc.awaitTerminationOrTimeout(0.5):
                    raise Exception("ssc stopped")
            time.sleep(1)  # make sure mtime is larger than the previous one
            with open(os.path.join(inputd, str(n)), 'w') as f:
                f.writelines(["%d\n" % i for i in range(10)])

            while True:
                if self.ssc.awaitTerminationOrTimeout(0.5):
                    raise Exception("ssc stopped")
                p = os.path.join(outputd, max(os.listdir(outputd)))
                if '_SUCCESS' not in os.listdir(p):
                    # not finished
                    continue
                ordd = self.ssc.sparkContext.textFile(p).map(lambda line: line.split(","))
                d = ordd.values().map(int).collect()
                if not d:
                    continue
                self.assertEqual(10, len(d))
                s = set(d)
                self.assertEqual(1, len(s))
                m = s.pop()
                if n > m:
                    continue
                self.assertEqual(n, m)
                break

        check_output(1)
        check_output(2)

        # Verify the getOrCreate() recovers from checkpoint files
        self.ssc.stop(True, True)
        time.sleep(1)
        self.setupCalled = False
        self.ssc = StreamingContext.getOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.ssc.start()
        check_output(3)

        # Verify that getOrCreate() uses existing SparkContext
        self.ssc.stop(True, True)
        time.sleep(1)
        self.sc = SparkContext(conf=SparkConf())
        self.setupCalled = False
        self.ssc = StreamingContext.getOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.assertTrue(self.ssc.sparkContext == self.sc)

        # Verify the getActiveOrCreate() recovers from checkpoint files
        self.ssc.stop(True, True)
        time.sleep(1)
        self.setupCalled = False
        self.ssc = StreamingContext.getActiveOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.ssc.start()
        check_output(4)

        # Verify that getActiveOrCreate() returns active context
        self.setupCalled = False
        self.assertEqual(StreamingContext.getActiveOrCreate(self.cpd, setup), self.ssc)
        self.assertFalse(self.setupCalled)

        # Verify that getActiveOrCreate() uses existing SparkContext
        self.ssc.stop(True, True)
        time.sleep(1)
        self.sc = SparkContext(conf=SparkConf())
        self.setupCalled = False
        self.ssc = StreamingContext.getActiveOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.assertTrue(self.ssc.sparkContext == self.sc)

        # Verify that getActiveOrCreate() calls setup() in absence of checkpoint files
        self.ssc.stop(True, True)
        shutil.rmtree(self.cpd)  # delete checkpoint directory
        time.sleep(1)
        self.setupCalled = False
        self.ssc = StreamingContext.getActiveOrCreate(self.cpd, setup)
        self.assertTrue(self.setupCalled)

        # Stop everything
        self.ssc.stop(True, True)


class KinesisStreamTests(PySparkStreamingTestCase):

    def test_kinesis_stream_api(self):
        # Don't start the StreamingContext because we cannot test it in Jenkins
        kinesisStream1 = KinesisUtils.createStream(
            self.ssc, "myAppNam", "mySparkStream",
            "https://kinesis.us-west-2.amazonaws.com", "us-west-2",
            InitialPositionInStream.LATEST, 2, StorageLevel.MEMORY_AND_DISK_2)
        kinesisStream2 = KinesisUtils.createStream(
            self.ssc, "myAppNam", "mySparkStream",
            "https://kinesis.us-west-2.amazonaws.com", "us-west-2",
            InitialPositionInStream.LATEST, 2, StorageLevel.MEMORY_AND_DISK_2,
            "awsAccessKey", "awsSecretKey")

    def test_kinesis_stream(self):
        if not are_kinesis_tests_enabled:
            sys.stderr.write(
                "Skipped test_kinesis_stream (enable by setting environment variable %s=1"
                % kinesis_test_environ_var)
            return

        import random
        kinesisAppName = ("KinesisStreamTests-%d" % abs(random.randint(0, 10000000)))
        kinesisTestUtils = self.ssc._jvm.org.apache.spark.streaming.kinesis.KinesisTestUtils(2)
        try:
            kinesisTestUtils.createStream()
            aWSCredentials = kinesisTestUtils.getAWSCredentials()
            stream = KinesisUtils.createStream(
                self.ssc, kinesisAppName, kinesisTestUtils.streamName(),
                kinesisTestUtils.endpointUrl(), kinesisTestUtils.regionName(),
                InitialPositionInStream.LATEST, 10, StorageLevel.MEMORY_ONLY,
                aWSCredentials.getAWSAccessKeyId(), aWSCredentials.getAWSSecretKey())

            outputBuffer = []

            def get_output(_, rdd):
                for e in rdd.collect():
                    outputBuffer.append(e)

            stream.foreachRDD(get_output)
            self.ssc.start()

            testData = [i for i in range(1, 11)]
            expectedOutput = set([str(i) for i in testData])
            start_time = time.time()
            while time.time() - start_time < 120:
                kinesisTestUtils.pushData(testData)
                if expectedOutput == set(outputBuffer):
                    break
                time.sleep(10)
            self.assertEqual(expectedOutput, set(outputBuffer))
        except:
            import traceback
            traceback.print_exc()
            raise
        finally:
            self.ssc.stop(False)
            kinesisTestUtils.deleteStream()
            kinesisTestUtils.deleteDynamoDBTable(kinesisAppName)


# Search jar in the project dir using the jar name_prefix for both sbt build and maven build because
# the artifact jars are in different directories.
def search_jar(dir, name_prefix):
    # We should ignore the following jars
    ignored_jar_suffixes = ("javadoc.jar", "sources.jar", "test-sources.jar", "tests.jar")
    jars = (glob.glob(os.path.join(dir, "target/scala-*/" + name_prefix + "-*.jar")) +  # sbt build
            glob.glob(os.path.join(dir, "target/" + name_prefix + "_*.jar")))  # maven build
    return [jar for jar in jars if not jar.endswith(ignored_jar_suffixes)]


def _kinesis_asl_assembly_dir():
    SPARK_HOME = os.environ["SPARK_HOME"]
    return os.path.join(SPARK_HOME, "external/kinesis-asl-assembly")


def search_kinesis_asl_assembly_jar():
    jars = search_jar(_kinesis_asl_assembly_dir(), "spark-streaming-kinesis-asl-assembly")
    if not jars:
        return None
    elif len(jars) > 1:
        raise Exception(("Found multiple Spark Streaming Kinesis ASL assembly JARs: %s; please "
                         "remove all but one") % (", ".join(jars)))
    else:
        return jars[0]


# Must be same as the variable and condition defined in KinesisTestUtils.scala and modules.py
kinesis_test_environ_var = "ENABLE_KINESIS_TESTS"
are_kinesis_tests_enabled = os.environ.get(kinesis_test_environ_var) == '1'

if __name__ == "__main__":
    from pyspark.streaming.tests import *
    kinesis_asl_assembly_jar = search_kinesis_asl_assembly_jar()

    if kinesis_asl_assembly_jar is None:
        kinesis_jar_present = False
        jars_args = ""
    else:
        kinesis_jar_present = True
        jars_args = "--jars %s" % kinesis_asl_assembly_jar

    existing_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
    os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join([jars_args, existing_args])
    testcases = [BasicOperationTests, WindowFunctionTests, StreamingContextTests, CheckpointTests,
                 StreamingListenerTests]

    if kinesis_jar_present is True:
        testcases.append(KinesisStreamTests)
    elif are_kinesis_tests_enabled is False:
        sys.stderr.write("Skipping all Kinesis Python tests as the optional Kinesis project was "
                         "not compiled into a JAR. To run these tests, "
                         "you need to build Spark with 'build/sbt -Pkinesis-asl assembly/package "
                         "streaming-kinesis-asl-assembly/assembly' or "
                         "'build/mvn -Pkinesis-asl package' before running this test.")
    else:
        raise Exception(
            ("Failed to find Spark Streaming Kinesis assembly jar in %s. "
             % _kinesis_asl_assembly_dir()) +
            "You need to build Spark with 'build/sbt -Pkinesis-asl "
            "assembly/package streaming-kinesis-asl-assembly/assembly'"
            "or 'build/mvn -Pkinesis-asl package' before running this test.")

    sys.stderr.write("Running tests: %s \n" % (str(testcases)))
    failed = False
    for testcase in testcases:
        sys.stderr.write("[Running %s]\n" % (testcase))
        tests = unittest.TestLoader().loadTestsFromTestCase(testcase)
        if xmlrunner:
            result = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2).run(tests)
            if not result.wasSuccessful():
                failed = True
        else:
            result = unittest.TextTestRunner(verbosity=2).run(tests)
            if not result.wasSuccessful():
                failed = True
    sys.exit(failed)
