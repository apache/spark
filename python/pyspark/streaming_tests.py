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
Unit tests for PySpark; additional tests are implemented as doctests in
individual modules.

This file would be merged to tests.py after all functions are ready.
But for now, this file is separated due to focusing to streaming test case.

Callback server seems like unstable sometimes, which cause error in test case.

"""
from itertools import chain
import time
import unittest
import operator

from pyspark.context import SparkContext
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *


class PySparkStreamingTestCase(unittest.TestCase):
    def setUp(self):
        class_name = self.__class__.__name__
        self.ssc = StreamingContext(appName=class_name, duration=Seconds(1))

    def tearDown(self):
        # Do not call pyspark.streaming.context.StreamingContext.stop directly because
        # we do not wait to shutdown call back server and py4j client
        self.ssc._jssc.stop()
        self.ssc._sc.stop()
        # Why does it long time to terminate StremaingContext and SparkContext?
        # Should we change the sleep time if this depends on machine spec?
        time.sleep(10)

    @classmethod
    def tearDownClass(cls):
        time.sleep(5)
        SparkContext._gateway._shutdown_callback_server()


class TestBasicOperationsSuite(PySparkStreamingTestCase):
    """
    2 tests for each function for batach deserializer and unbatch deserilizer because
    we cannot change the deserializer after streaming process starts.
    Default numInputPartitions is 2.
    If the number of input element is over 3, that DStream use batach deserializer.
    If not, that DStream use unbatch deserializer.

    Most of the operation uses UTF8 deserializer to get value from Scala.
    I am wondering if these test are enough or not.
    All tests input should have list of lists. This represents stream.
    Every batch interval, the first object of list are chosen to make DStream.
    Please see the BasicTestSuits in Scala which is close to this implementation.
    """
    def setUp(self):
        PySparkStreamingTestCase.setUp(self)
        self.timeout = 10  # seconds
        self.numInputPartitions = 2
        self.result = list()

    def tearDown(self):
        PySparkStreamingTestCase.tearDown(self)

    @classmethod
    def tearDownClass(cls):
        PySparkStreamingTestCase.tearDownClass()

    def test_map_batch(self):
        """Basic operation test for DStream.map with batch deserializer"""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.map(lambda x: str(x))
        expected_output = map(lambda x: map(lambda y: str(y), x), test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_map_unbatach(self):
        """Basic operation test for DStream.map with unbatch deserializer"""
        test_input = [range(1, 4), range(4, 7), range(7, 10)]

        def test_func(dstream):
            return dstream.map(lambda x: str(x))
        expected_output = map(lambda x: map(lambda y: str(y), x), test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_flatMap_batch(self):
        """Basic operation test for DStream.faltMap with batch deserializer"""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.flatMap(lambda x: (x, x * 2))
        expected_output = map(lambda x: list(chain.from_iterable((map(lambda y: [y, y * 2], x)))), 
                              test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_flatMap_unbatch(self):
        """Basic operation test for DStream.faltMap with unbatch deserializer"""
        test_input = [range(1, 4), range(4, 7), range(7, 10)]

        def test_func(dstream):
            return dstream.flatMap(lambda x: (x, x * 2))
        expected_output = map(lambda x: list(chain.from_iterable((map(lambda y: [y, y * 2], x)))),
                              test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_filter_batch(self):
        """Basic operation test for DStream.filter with batch deserializer"""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.filter(lambda x: x % 2 == 0)
        expected_output = map(lambda x: filter(lambda y: y % 2 == 0, x), test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_filter_unbatch(self):
        """Basic operation test for DStream.filter with unbatch deserializer"""
        test_input = [range(1, 4), range(4, 7), range(7, 10)]

        def test_func(dstream):
            return dstream.filter(lambda x: x % 2 == 0)
        expected_output = map(lambda x: filter(lambda y: y % 2 == 0, x), test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_count_batch(self):
        """Basic operation test for DStream.count with batch deserializer"""
        test_input = [range(1, 5), range(1, 10), range(1, 20)]

        def test_func(dstream):
            return dstream.count()
        expected_output = map(lambda x: [len(x)], test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_count_unbatch(self):
        """Basic operation test for DStream.count with unbatch deserializer"""
        test_input = [[], [1], range(1, 3), range(1, 4)]

        def test_func(dstream):
            return dstream.count()
        expected_output = map(lambda x: [len(x)], test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_reduce_batch(self):
        """Basic operation test for DStream.reduce with batch deserializer"""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.reduce(operator.add)
        expected_output = map(lambda x: [reduce(operator.add, x)], test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_reduce_unbatch(self):
        """Basic operation test for DStream.reduce with unbatch deserializer"""
        test_input = [[1], range(1, 3), range(1, 4)]

        def test_func(dstream):
            return dstream.reduce(operator.add)
        expected_output = map(lambda x: [reduce(operator.add, x)], test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_reduceByKey_batch(self):
        """Basic operation test for DStream.reduceByKey with batch deserializer"""
        test_input = [["a", "a", "b", "b"], ["", "", "", ""]]

        def test_func(dstream):
            return dstream.map(lambda x: (x, 1)).reduceByKey(operator.add)
        expected_output = [[("a", 2), ("b", 2)], [("", 4)]]
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_reduceByKey_unbatch(self):
        """Basic operation test for DStream.reduceByKey with unbatch deserilizer"""
        test_input = [["a", "a", "b"], ["", ""], []]

        def test_func(dstream):
            return dstream.map(lambda x: (x, 1)).reduceByKey(operator.add)
        expected_output = [[("a", 2), ("b", 1)], [("", 2)], []]
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_mapValues_batch(self):
        """Basic operation test for DStream.mapValues with batch deserializer"""
        test_input = [["a", "a", "b", "b"], ["", "", "", ""]]

        def test_func(dstream):
            return dstream.map(lambda x: (x, 1))\
                          .reduceByKey(operator.add)\
                          .mapValues(lambda x: x + 10)
        expected_output = [[("a", 12), ("b", 12)], [("", 14)]]
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_mapValues_unbatch(self):
        """Basic operation test for DStream.mapValues with unbatch deserializer"""
        test_input = [["a", "a", "b"], ["", ""], []]

        def test_func(dstream):
            return dstream.map(lambda x: (x, 1))\
                          .reduceByKey(operator.add)\
                          .mapValues(lambda x: x + 10)
        expected_output = [[("a", 12), ("b", 11)], [("", 12)], []]
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_flatMapValues_batch(self):
        """Basic operation test for DStream.flatMapValues with batch deserializer"""
        test_input = [["a", "a", "b", "b"], ["", "", "", ""]]

        def test_func(dstream):
            return dstream.map(lambda x: (x, 1))\
                          .reduceByKey(operator.add)\
                          .flatMapValues(lambda x: (x, x + 10))
        expected_output = [[("a", 2), ("a", 12), ("b", 2), ("b", 12)], [("", 4), ("", 14)]]
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_flatMapValues_unbatch(self):
        """Basic operation test for DStream.flatMapValues with unbatch deserializer"""
        test_input = [["a", "a", "b"], ["", ""], []]

        def test_func(dstream):
            return dstream.map(lambda x: (x, 1))\
                          .reduceByKey(operator.add)\
                          .flatMapValues(lambda x: (x, x + 10))
        expected_output = [[("a", 2), ("a", 12), ("b", 1), ("b", 11)], [("", 2), ("", 12)], []]
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_glom_batch(self):
        """Basic operation test for DStream.glom with batch deserializer"""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]
        numSlices = 2

        def test_func(dstream):
            return dstream.glom()
        expected_output = [[[1, 2], [3, 4]], [[5, 6], [7, 8]], [[9, 10], [11, 12]]]
        output = self._run_stream(test_input, test_func, expected_output, numSlices)
        self.assertEqual(expected_output, output)

    def test_glom_unbatach(self):
        """Basic operation test for DStream.glom with unbatch deserialiser"""
        test_input = [range(1, 4), range(4, 7), range(7, 10)]
        numSlices = 2

        def test_func(dstream):
            return dstream.glom()
        expected_output = [[[1], [2, 3]], [[4], [5, 6]], [[7], [8, 9]]]
        output = self._run_stream(test_input, test_func, expected_output, numSlices)
        self.assertEqual(expected_output, output)

    def test_mapPartitions_batch(self):
        """Basic operation test for DStream.mapPartitions with batch deserializer."""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]
        numSlices = 2

        def test_func(dstream):
            def f(iterator):
                yield sum(iterator)
            return dstream.mapPartitions(f)
        expected_output = [[3, 7], [11, 15], [19, 23]]
        output = self._run_stream(test_input, test_func, expected_output, numSlices)
        self.assertEqual(expected_output, output)

    def test_mapPartitions_unbatch(self):
        """Basic operation test for DStream.mapPartitions with unbatch deserializer."""
        test_input = [range(1, 4), range(4, 7), range(7, 10)]
        numSlices = 2

        def test_func(dstream):
            def f(iterator):
                yield sum(iterator)
            return dstream.mapPartitions(f)
        expected_output = [[1, 5], [4, 11], [7, 17]]
        output = self._run_stream(test_input, test_func, expected_output, numSlices)
        self.assertEqual(expected_output, output)

    def test_countByValue_batch(self):
        """Basic operation test for DStream.countByValue with batch deserializer."""
        test_input = [range(1, 5) + range(1,5), range(5, 7) + range(5, 9), ["a", "a", "b", ""]]

        def test_func(dstream):
            return dstream.countByValue()
        expected_output = [[(1, 2), (2, 2), (3, 2), (4, 2)],
                           [(5, 2), (6, 2), (7, 1), (8, 1)],
                           [("a", 2), ("b", 1), ("", 1)]]
        output = self._run_stream(test_input, test_func, expected_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def test_countByValue_unbatch(self):
        """Basic operation test for DStream.countByValue with unbatch deserializer."""
        test_input = [range(1, 4), [1, 1, ""], ["a", "a", "b"]]

        def test_func(dstream):
            return dstream.countByValue()
        expected_output = [[(1, 1), (2, 1), (3, 1)],
                           [(1, 2), ("", 1)],
                           [("a", 2), ("b", 1)]]
        output = self._run_stream(test_input, test_func, expected_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def test_groupByKey_batch(self):
        """Basic operation test for DStream.groupByKey with batch deserializer."""
        test_input = [range(1, 5), [1, 1, 1, 2, 2, 3], ["a", "a", "b", "", "", ""]]
        def test_func(dstream):
            return dstream.map(lambda x: (x, 1)).groupByKey()
        expected_output = [[(1, [1]), (2, [1]), (3, [1]), (4, [1])],
                           [(1, [1, 1, 1]), (2, [1, 1]), (3, [1])],
                           [("a", [1, 1]), ("b", [1]), ("", [1, 1, 1])]]
        scattered_output = self._run_stream(test_input, test_func, expected_output)
        output = self._convert_iter_value_to_list(scattered_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def test_groupByKey_unbatch(self):
        """Basic operation test for DStream.groupByKey with unbatch deserializer."""
        test_input = [range(1, 4), [1, 1, ""], ["a", "a", "b"]]

        def test_func(dstream):
            return dstream.map(lambda x: (x, 1)).groupByKey()
        expected_output = [[(1, [1]), (2, [1]), (3, [1])],
                           [(1, [1, 1]), ("", [1])],
                           [("a", [1, 1]), ("b", [1])]]
        scattered_output = self._run_stream(test_input, test_func, expected_output)
        output = self._convert_iter_value_to_list(scattered_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def test_combineByKey_batch(self):
        """Basic operation test for DStream.combineByKey with batch deserializer."""
        test_input = [range(1, 5), [1, 1, 1, 2, 2, 3], ["a", "a", "b", "", "", ""]]

        def test_func(dstream):
            def add(a, b): return a + str(b)
            return dstream.map(lambda x: (x, 1)).combineByKey(str, add, add)
        expected_output = [[(1, "1"), (2, "1"), (3, "1"), (4, "1")],
                           [(1, "111"), (2, "11"), (3, "1")],
                           [("a", "11"), ("b", "1"), ("", "111")]]
        output = self._run_stream(test_input, test_func, expected_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def test_combineByKey_unbatch(self):
        """Basic operation test for DStream.combineByKey with unbatch deserializer."""
        test_input = [range(1, 4), [1, 1, ""], ["a", "a", "b"]]

        def test_func(dstream):
            def add(a, b): return a + str(b)
            return dstream.map(lambda x: (x, 1)).combineByKey(str, add, add)
        expected_output = [[(1, "1"), (2, "1"), (3, "1")],
                           [(1, "11"), ("", "1")],
                           [("a", "11"), ("b", "1")]]
        output = self._run_stream(test_input, test_func, expected_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def _convert_iter_value_to_list(self, outputs):
        """Return key value pair list. Value is converted to iterator to list."""
        result = list()
        for output in outputs:
            result.append(map(lambda (x, y): (x, list(y)), output))
        return result

    def _sort_result_based_on_key(self, outputs):
        """Sort the list base onf first value."""
        for output in outputs:
            output.sort(key=lambda x: x[0])

    def _run_stream(self, test_input, test_func, expected_output, numSlices=None):
        """
        Start stream and return the output.
        @param test_input: dataset for the test. This should be list of lists.
        @param test_func: wrapped test_function. This function should return PythonDstream object.
        @param expexted_output: expected output for this testcase.
        @param numSlices: the number of slices in the rdd in the dstream.
        """
        # Generate input stream with user-defined input.
        numSlices = numSlices or self.numInputPartitions
        test_input_stream = self.ssc._testInputStream(test_input, numSlices)
        # Apply test function to stream.
        test_stream = test_func(test_input_stream)
        # Add job to get output from stream.
        test_stream._test_output(self.result)
        self.ssc.start()

        start_time = time.time()
        # Loop until get the expected the number of the result from the stream.
        while True:
            current_time = time.time()
            # Check time out.
            if (current_time - start_time) > self.timeout:
                break
            self.ssc.awaitTermination(50)
            # Check if the output is the same length of expexted output.
            if len(expected_output) == len(self.result):
                break

        return self.result

class TestSaveAsFilesSuite(PySparkStreamingTestCase):
    def setUp(self):
        PySparkStreamingTestCase.setUp(self)
        self.timeout = 10  # seconds
        self.numInputPartitions = 2
        self.result = list()

    def tearDown(self):
        PySparkStreamingTestCase.tearDown(self)

    @classmethod
    def tearDownClass(cls):
        PySparkStreamingTestCase.tearDownClass()

        start_time = time.time()
        while True:
            current_time = time.time()
            # check time out
            if (current_time - start_time) > self.timeout:
                break
            self.ssc.awaitTermination(50)
            if len(expected_output) == len(StreamOutput.result):
                break
        return StreamOutput.result

if __name__ == "__main__":
    unittest.main()
