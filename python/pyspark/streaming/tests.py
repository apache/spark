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

"""
Unit tests for Python SparkStreaming; additional tests are implemented as doctests in
individual modules.

Callback server is sometimes unstable sometimes, which cause error in test case.
But this is very rare case.
"""
from itertools import chain
import time
import operator
import unittest

from pyspark.context import SparkContext
from pyspark.streaming.context import StreamingContext


class PySparkStreamingTestCase(unittest.TestCase):

    timeout = 10  # seconds

    def setUp(self):
        class_name = self.__class__.__name__
        self.sc = SparkContext(appName=class_name)
        self.sc.setCheckpointDir("/tmp")
        # TODO: decrease duration to speed up tests
        self.ssc = StreamingContext(self.sc, duration=1)

    def tearDown(self):
        self.ssc.stop()

    def _test_func(self, input, func, expected, sort=False):
        """
        @param input: dataset for the test. This should be list of lists.
        @param func: wrapped function. This function should return PythonDStream object.
        @param expected: expected output for this testcase.
        """
        # Generate input stream with user-defined input.
        input_stream = self.ssc.queueStream(input)
        # Apply test function to stream.
        stream = func(input_stream)
        result = stream.collect()
        self.ssc.start()

        start_time = time.time()
        # Loop until get the expected the number of the result from the stream.
        while True:
            current_time = time.time()
            # Check time out.
            if (current_time - start_time) > self.timeout:
                break
            # StreamingContext.awaitTermination is not used to wait because
            # if py4j server is called every 50 milliseconds, it gets an error.
            time.sleep(0.05)
            # Check if the output is the same length of expected output.
            if len(expected) == len(result):
                break
        if sort:
            self._sort_result_based_on_key(result)
            self._sort_result_based_on_key(expected)
        self.assertEqual(expected, result)

    def _sort_result_based_on_key(self, outputs):
        """Sort the list based on first value."""
        for output in outputs:
            output.sort(key=lambda x: x[0])


class TestBasicOperations(PySparkStreamingTestCase):

    def test_take(self):
        input = [range(i) for i in range(3)]
        dstream = self.ssc.queueStream(input)
        self.assertEqual([0, 0, 1], dstream.take(3))

    def test_first(self):
        input = [range(10)]
        dstream = self.ssc.queueStream(input)
        self.assertEqual(0, dstream)

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

    def test_union(self):
        input1 = [range(3), range(5), range(1), range(6)]
        input2 = [range(3, 6), range(5, 6), range(1, 6)]

        d1 = self.ssc.queueStream(input1)
        d2 = self.ssc.queueStream(input2)
        d = d1.union(d2)
        result = d.collect()
        expected = [range(6), range(6), range(6), range(6)]

        self.ssc.start()
        start_time = time.time()
        # Loop until get the expected the number of the result from the stream.
        while True:
            current_time = time.time()
            # Check time out.
            if (current_time - start_time) > self.timeout * 2:
                break
            # StreamingContext.awaitTermination is not used to wait because
            # if py4j server is called every 50 milliseconds, it gets an error.
            time.sleep(0.05)
            # Check if the output is the same length of expected output.
            if len(expected) == len(result):
                break
        self.assertEqual(expected, result)


class TestWindowFunctions(PySparkStreamingTestCase):

    timeout = 15

    def test_count_by_window(self):
        input = [range(1), range(2), range(3), range(4), range(5), range(6)]

        def func(dstream):
            return dstream.countByWindow(4, 1)

        expected = [[1], [3], [6], [9], [12], [15], [11], [6]]
        self._test_func(input, func, expected)

    def test_count_by_window_large(self):
        input = [range(1), range(2), range(3), range(4), range(5), range(6)]

        def func(dstream):
            return dstream.countByWindow(6, 1)

        expected = [[1], [3], [6], [10], [15], [20], [18], [15], [11], [6]]
        self._test_func(input, func, expected)

    def test_group_by_key_and_window(self):
        input = [[('a', i)] for i in range(5)]

        def func(dstream):
            return dstream.groupByKeyAndWindow(4, 1).mapValues(list)

        expected = [[('a', [0])], [('a', [0, 1])], [('a', [0, 1, 2])], [('a', [1, 2, 3])],
                    [('a', [2, 3, 4])], [('a', [3, 4])], [('a', [4])]]
        self._test_func(input, func, expected)

    def test_reduce_by_invalid_window(self):
        input1 = [range(3), range(5), range(1), range(6)]
        d1 = self.ssc.queueStream(input1)
        self.assertRaises(ValueError, lambda: d1.reduceByKeyAndWindow(None, None, 0.1, 0.1))
        self.assertRaises(ValueError, lambda: d1.reduceByKeyAndWindow(None, None, 1, 0.1))

    def update_state_by_key(self):

        def updater(it):
            for k, vs, s in it:
                if not s:
                    s = vs
                else:
                    s.extend(vs)
                yield (k, s)

        input = [[('k', i)] for i in range(5)]

        def func(dstream):
            return dstream.updateStateByKey(updater)

        expected = [[0], [0, 1], [0, 1, 2], [0, 1, 2, 3], [0, 1, 2, 3, 4]]
        expected = [[('k', v)] for v in expected]
        self._test_func(input, func, expected)


class TestStreamingContext(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext(master="local[2]", appName=self.__class__.__name__)
        self.batachDuration = 0.1
        self.ssc = StreamingContext(self.sc, self.batachDuration)

    def tearDown(self):
        self.ssc.stop()
        self.sc.stop()

    def test_stop_only_streaming_context(self):
        self._addInputStream()
        self.ssc.start()
        self.ssc.stop(False)
        self.assertEqual(len(self.sc.parallelize(range(5), 5).glom().collect()), 5)

    def test_stop_multiple_times(self):
        self._addInputStream()
        self.ssc.start()
        self.ssc.stop()
        self.ssc.stop()

    def _addInputStream(self):
        # Make sure each length of input is over 3
        inputs = map(lambda x: range(1, x), range(5, 101))
        stream = self.ssc.queueStream(inputs)
        stream.collect()

    def test_queueStream(self):
        input = [range(i) for i in range(3)]
        dstream = self.ssc.queueStream(input)
        result = dstream.collect()
        self.ssc.start()
        time.sleep(1)
        self.assertEqual(input, result[:3])

    def test_union(self):
        input = [range(i) for i in range(3)]
        dstream = self.ssc.queueStream(input)
        dstream2 = self.ssc.queueStream(input)
        dstream3 = self.ssc.union(dstream, dstream2)
        result = dstream3.collect()
        self.ssc.start()
        time.sleep(1)
        expected = [i * 2 for i in input]
        self.assertEqual(expected, result[:3])

    def test_transform(self):
        dstream1 = self.ssc.queueStream([[1]])
        dstream2 = self.ssc.queueStream([[2]])
        dstream3 = self.ssc.queueStream([[3]])

        def func(rdds):
            rdd1, rdd2, rdd3 = rdds
            return rdd2.union(rdd3).union(rdd1)

        dstream = self.ssc.transform([dstream1, dstream2, dstream3], func)

        self.assertEqual([2, 3, 1], dstream.take(3))


if __name__ == "__main__":
    unittest.main()
