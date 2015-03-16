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
from itertools import chain
import time
import operator
import unittest
import tempfile
import struct

from pyspark.context import SparkConf, SparkContext, RDD
from pyspark.streaming.context import StreamingContext


class PySparkStreamingTestCase(unittest.TestCase):

    timeout = 10  # seconds
    duration = 1

    def setUp(self):
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.default.parallelism", 1)
        self.sc = SparkContext(appName=class_name, conf=conf)
        self.sc.setCheckpointDir("/tmp")
        # TODO: decrease duration to speed up tests
        self.ssc = StreamingContext(self.sc, self.duration)

    def tearDown(self):
        self.ssc.stop()

    def wait_for(self, result, n):
        start_time = time.time()
        while len(result) < n and time.time() - start_time < self.timeout:
            time.sleep(0.01)
        if len(result) < n:
            print "timeout after", self.timeout

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
        expected = map(lambda x: map(str, x), input)
        self._test_func(input, func, expected)

    def test_flatMap(self):
        """Basic operation test for DStream.faltMap."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.flatMap(lambda x: (x, x * 2))
        expected = map(lambda x: list(chain.from_iterable((map(lambda y: [y, y * 2], x)))),
                       input)
        self._test_func(input, func, expected)

    def test_filter(self):
        """Basic operation test for DStream.filter."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.filter(lambda x: x % 2 == 0)
        expected = map(lambda x: filter(lambda y: y % 2 == 0, x), input)
        self._test_func(input, func, expected)

    def test_count(self):
        """Basic operation test for DStream.count."""
        input = [range(5), range(10), range(20)]

        def func(dstream):
            return dstream.count()
        expected = map(lambda x: [len(x)], input)
        self._test_func(input, func, expected)

    def test_reduce(self):
        """Basic operation test for DStream.reduce."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.reduce(operator.add)
        expected = map(lambda x: [reduce(operator.add, x)], input)
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
                 [("", 4), (1, 1), (2, 2), (3, 3)],
                 [(1, 1), (2, 1), (3, 1), (4, 1)]]

        def func(dstream):
            return dstream.mapValues(lambda x: x + 10)
        expected = [[("a", 12), ("b", 12), ("c", 11), ("d", 11)],
                    [("", 14), (1, 11), (2, 12), (3, 13)],
                    [(1, 11), (2, 11), (3, 11), (4, 11)]]
        self._test_func(input, func, expected, sort=True)

    def test_flatMapValues(self):
        """Basic operation test for DStream.flatMapValues."""
        input = [[("a", 2), ("b", 2), ("c", 1), ("d", 1)],
                 [("", 4), (1, 1), (2, 1), (3, 1)],
                 [(1, 1), (2, 1), (3, 1), (4, 1)]]

        def func(dstream):
            return dstream.flatMapValues(lambda x: (x, x + 10))
        expected = [[("a", 2), ("a", 12), ("b", 2), ("b", 12),
                     ("c", 1), ("c", 11), ("d", 1), ("d", 11)],
                    [("", 4), ("", 14), (1, 1), (1, 11), (2, 1), (2, 11), (3, 1), (3, 11)],
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
        input = [range(1, 5) * 2, range(5, 7) + range(5, 9), ["a", "a", "b", ""]]

        def func(dstream):
            return dstream.countByValue()
        expected = [[4], [4], [3]]
        self._test_func(input, func, expected)

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

        expected = [range(6), range(6), range(6)]
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


class WindowFunctionTests(PySparkStreamingTestCase):

    timeout = 20

    def test_window(self):
        input = [range(1), range(2), range(3), range(4), range(5)]

        def func(dstream):
            return dstream.window(3, 1).count()

        expected = [[1], [3], [6], [9], [12], [9], [5]]
        self._test_func(input, func, expected)

    def test_count_by_window(self):
        input = [range(1), range(2), range(3), range(4), range(5)]

        def func(dstream):
            return dstream.countByWindow(3, 1)

        expected = [[1], [3], [6], [9], [12], [9], [5]]
        self._test_func(input, func, expected)

    def test_count_by_window_large(self):
        input = [range(1), range(2), range(3), range(4), range(5), range(6)]

        def func(dstream):
            return dstream.countByWindow(5, 1)

        expected = [[1], [3], [6], [10], [15], [20], [18], [15], [11], [6]]
        self._test_func(input, func, expected)

    def test_count_by_value_and_window(self):
        input = [range(1), range(2), range(3), range(4), range(5), range(6)]

        def func(dstream):
            return dstream.countByValueAndWindow(5, 1)

        expected = [[1], [2], [3], [4], [5], [6], [6], [6], [6], [6]]
        self._test_func(input, func, expected)

    def test_group_by_key_and_window(self):
        input = [[('a', i)] for i in range(5)]

        def func(dstream):
            return dstream.groupByKeyAndWindow(3, 1).mapValues(list)

        expected = [[('a', [0])], [('a', [0, 1])], [('a', [0, 1, 2])], [('a', [1, 2, 3])],
                    [('a', [2, 3, 4])], [('a', [3, 4])], [('a', [4])]]
        self._test_func(input, func, expected)

    def test_reduce_by_invalid_window(self):
        input1 = [range(3), range(5), range(1), range(6)]
        d1 = self.ssc.queueStream(input1)
        self.assertRaises(ValueError, lambda: d1.reduceByKeyAndWindow(None, None, 0.1, 0.1))
        self.assertRaises(ValueError, lambda: d1.reduceByKeyAndWindow(None, None, 1, 0.1))


class StreamingContextTests(PySparkStreamingTestCase):

    duration = 0.1

    def _add_input_stream(self):
        inputs = map(lambda x: range(1, x), range(101))
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
        self.ssc.stop()
        self.ssc.stop()

    def test_queue_stream(self):
        input = [range(i + 1) for i in range(3)]
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
        self.assertEqual([range(10), range(10)], result)

    def test_binary_records_stream(self):
        d = tempfile.mkdtemp()
        self.ssc = StreamingContext(self.sc, self.duration)
        dstream = self.ssc.binaryRecordsStream(d, 10).map(
            lambda v: struct.unpack("10b", str(v)))
        result = self._collect(dstream, 2, block=False)
        self.ssc.start()
        for name in ('a', 'b'):
            time.sleep(1)
            with open(os.path.join(d, name), "wb") as f:
                f.write(bytearray(range(10)))
        self.wait_for(result, 2)
        self.assertEqual([range(10), range(10)], map(lambda v: list(v[0]), result))

    def test_union(self):
        input = [range(i + 1) for i in range(3)]
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


class CheckpointTests(PySparkStreamingTestCase):

    def setUp(self):
        pass

    def test_get_or_create(self):
        inputd = tempfile.mkdtemp()
        outputd = tempfile.mkdtemp() + "/"

        def updater(vs, s):
            return sum(vs, s or 0)

        def setup():
            conf = SparkConf().set("spark.default.parallelism", 1)
            sc = SparkContext(conf=conf)
            ssc = StreamingContext(sc, 0.5)
            dstream = ssc.textFileStream(inputd).map(lambda x: (x, 1))
            wc = dstream.updateStateByKey(updater)
            wc.map(lambda x: "%s,%d" % x).saveAsTextFiles(outputd + "test")
            wc.checkpoint(.5)
            return ssc

        cpd = tempfile.mkdtemp("test_streaming_cps")
        self.ssc = ssc = StreamingContext.getOrCreate(cpd, setup)
        ssc.start()

        def check_output(n):
            while not os.listdir(outputd):
                time.sleep(0.1)
            time.sleep(1)  # make sure mtime is larger than the previous one
            with open(os.path.join(inputd, str(n)), 'w') as f:
                f.writelines(["%d\n" % i for i in range(10)])

            while True:
                p = os.path.join(outputd, max(os.listdir(outputd)))
                if '_SUCCESS' not in os.listdir(p):
                    # not finished
                    time.sleep(0.01)
                    continue
                ordd = ssc.sparkContext.textFile(p).map(lambda line: line.split(","))
                d = ordd.values().map(int).collect()
                if not d:
                    time.sleep(0.01)
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
        ssc.stop(True, True)

        time.sleep(1)
        self.ssc = ssc = StreamingContext.getOrCreate(cpd, setup)
        ssc.start()
        check_output(3)


if __name__ == "__main__":
    unittest.main()
