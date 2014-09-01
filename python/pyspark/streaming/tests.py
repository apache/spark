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
import sys

if sys.version_info[:2] <= (2, 6):
    import unittest2 as unittest
else:
    import unittest

from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *


class PySparkStreamingTestCase(unittest.TestCase):
    def setUp(self):
        class_name = self.__class__.__name__
        self.ssc = StreamingContext(appName=class_name, duration=Seconds(1))

    def tearDown(self):
        # Do not call pyspark.streaming.context.StreamingContext.stop directly because
        # we do not wait to shutdown py4j client.
        self.ssc._jssc.stop()
        self.ssc._sc.stop()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        # Make sure tp shutdown the callback server
        SparkContext._gateway._shutdown_callback_server()


class TestBasicOperationsSuite(PySparkStreamingTestCase):
    """
    2 tests for each function for batach deserializer and unbatch deserilizer because
    the deserializer is not changed dunamically after streaming process starts.
    Default numInputPartitions is 2.
    If the number of input element is over 3, that DStream use batach deserializer.
    If not, that DStream use unbatch deserializer.

    All tests input should have list of lists(3 lists are default). This list represents stream.
    Every batch interval, the first object of list are chosen to make DStream.
    e.g The first list in the list is input of the first batch.
    Please see the BasicTestSuits in Scala which is close to this implementation.
    """
    def setUp(self):
        PySparkStreamingTestCase.setUp(self)
        self.timeout = 10  # seconds
        self.numInputPartitions = 2

    def tearDown(self):
        PySparkStreamingTestCase.tearDown(self)

    @classmethod
    def tearDownClass(cls):
        PySparkStreamingTestCase.tearDownClass()

    def test_map_batch(self):
        """Basic operation test for DStream.map with batch deserializer."""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.map(lambda x: str(x))
        expected_output = map(lambda x: map(lambda y: str(y), x), test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_map_unbatach(self):
        """Basic operation test for DStream.map with unbatch deserializer."""
        test_input = [range(1, 4), range(4, 7), range(7, 10)]

        def test_func(dstream):
            return dstream.map(lambda x: str(x))
        expected_output = map(lambda x: map(lambda y: str(y), x), test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_flatMap_batch(self):
        """Basic operation test for DStream.faltMap with batch deserializer."""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.flatMap(lambda x: (x, x * 2))
        expected_output = map(lambda x: list(chain.from_iterable((map(lambda y: [y, y * 2], x)))),
                              test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_flatMap_unbatch(self):
        """Basic operation test for DStream.faltMap with unbatch deserializer."""
        test_input = [range(1, 4), range(4, 7), range(7, 10)]

        def test_func(dstream):
            return dstream.flatMap(lambda x: (x, x * 2))
        expected_output = map(lambda x: list(chain.from_iterable((map(lambda y: [y, y * 2], x)))),
                              test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_filter_batch(self):
        """Basic operation test for DStream.filter with batch deserializer."""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.filter(lambda x: x % 2 == 0)
        expected_output = map(lambda x: filter(lambda y: y % 2 == 0, x), test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_filter_unbatch(self):
        """Basic operation test for DStream.filter with unbatch deserializer."""
        test_input = [range(1, 4), range(4, 7), range(7, 10)]

        def test_func(dstream):
            return dstream.filter(lambda x: x % 2 == 0)
        expected_output = map(lambda x: filter(lambda y: y % 2 == 0, x), test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_count_batch(self):
        """Basic operation test for DStream.count with batch deserializer."""
        test_input = [range(1, 5), range(1, 10), range(1, 20)]

        def test_func(dstream):
            return dstream.count()
        expected_output = map(lambda x: [len(x)], test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_count_unbatch(self):
        """Basic operation test for DStream.count with unbatch deserializer."""
        test_input = [[], [1], range(1, 3), range(1, 4)]

        def test_func(dstream):
            return dstream.count()
        expected_output = map(lambda x: [len(x)], test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_reduce_batch(self):
        """Basic operation test for DStream.reduce with batch deserializer."""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.reduce(operator.add)
        expected_output = map(lambda x: [reduce(operator.add, x)], test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_reduce_unbatch(self):
        """Basic operation test for DStream.reduce with unbatch deserializer."""
        test_input = [[1], range(1, 3), range(1, 4)]

        def test_func(dstream):
            return dstream.reduce(operator.add)
        expected_output = map(lambda x: [reduce(operator.add, x)], test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_reduceByKey_batch(self):
        """Basic operation test for DStream.reduceByKey with batch deserializer."""
        test_input = [[("a", 1), ("a", 1), ("b", 1), ("b", 1)],
                      [("", 1), ("", 1), ("", 1), ("", 1)],
                      [(1, 1), (1, 1), (2, 1), (2, 1), (3, 1)]]

        def test_func(dstream):
            return dstream.reduceByKey(operator.add)
        expected_output = [[("a", 2), ("b", 2)], [("", 4)], [(1, 2), (2, 2), (3, 1)]]
        output = self._run_stream(test_input, test_func, expected_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def test_reduceByKey_unbatch(self):
        """Basic operation test for DStream.reduceByKey with unbatch deserializer."""
        test_input = [[("a", 1), ("a", 1), ("b", 1)], [("", 1), ("", 1)], []]

        def test_func(dstream):
            return dstream.reduceByKey(operator.add)
        expected_output = [[("a", 2), ("b", 1)], [("", 2)], []]
        output = self._run_stream(test_input, test_func, expected_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def test_mapValues_batch(self):
        """Basic operation test for DStream.mapValues with batch deserializer."""
        test_input = [[("a", 2), ("b", 2), ("c", 1), ("d", 1)],
                      [("", 4), (1, 1), (2, 2), (3, 3)],
                      [(1, 1), (2, 1), (3, 1), (4, 1)]]

        def test_func(dstream):
            return dstream.mapValues(lambda x: x + 10)
        expected_output = [[("a", 12), ("b", 12), ("c", 11), ("d", 11)],
                           [("", 14), (1, 11), (2, 12), (3, 13)],
                           [(1, 11), (2, 11), (3, 11), (4, 11)]]
        output = self._run_stream(test_input, test_func, expected_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def test_mapValues_unbatch(self):
        """Basic operation test for DStream.mapValues with unbatch deserializer."""
        test_input = [[("a", 2), ("b", 1)], [("", 2)], [], [(1, 1), (2, 2)]]

        def test_func(dstream):
            return dstream.mapValues(lambda x: x + 10)
        expected_output = [[("a", 12), ("b", 11)], [("", 12)], [], [(1, 11), (2, 12)]]
        output = self._run_stream(test_input, test_func, expected_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def test_flatMapValues_batch(self):
        """Basic operation test for DStream.flatMapValues with batch deserializer."""
        test_input = [[("a", 2), ("b", 2), ("c", 1), ("d", 1)],
                      [("", 4), (1, 1), (2, 1), (3, 1)],
                      [(1, 1), (2, 1), (3, 1), (4, 1)]]

        def test_func(dstream):
            return dstream.flatMapValues(lambda x: (x, x + 10))
        expected_output = [[("a", 2), ("a", 12), ("b", 2), ("b", 12),
                            ("c", 1), ("c", 11), ("d", 1), ("d", 11)],
                           [("", 4), ("", 14), (1, 1), (1, 11), (2, 1), (2, 11), (3, 1), (3, 11)],
                           [(1, 1), (1, 11), (2, 1), (2, 11), (3, 1), (3, 11), (4, 1), (4, 11)]]
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_flatMapValues_unbatch(self):
        """Basic operation test for DStream.flatMapValues with unbatch deserializer."""
        test_input = [[("a", 2), ("b", 1)], [("", 2)], []]

        def test_func(dstream):
            return dstream.flatMapValues(lambda x: (x, x + 10))
        expected_output = [[("a", 2), ("a", 12), ("b", 1), ("b", 11)], [("", 2), ("", 12)], []]
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_glom_batch(self):
        """Basic operation test for DStream.glom with batch deserializer."""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]
        numSlices = 2

        def test_func(dstream):
            return dstream.glom()
        expected_output = [[[1, 2], [3, 4]], [[5, 6], [7, 8]], [[9, 10], [11, 12]]]
        output = self._run_stream(test_input, test_func, expected_output, numSlices)
        self.assertEqual(expected_output, output)

    def test_glom_unbatach(self):
        """Basic operation test for DStream.glom with unbatch deserializer."""
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
        test_input = [range(1, 5) * 2, range(5, 7) + range(5, 9), ["a", "a", "b", ""]]

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
        test_input = [[(1, 1), (2, 1), (3, 1), (4, 1)],
                      [(1, 1), (1, 1), (1, 1), (2, 1), (2, 1), (3, 1)],
                      [("a", 1), ("a", 1), ("b", 1), ("", 1), ("", 1), ("", 1)]]

        def test_func(dstream):
            return dstream.groupByKey()
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
        test_input = [[(1, 1), (2, 1), (3, 1)],
                      [(1, 1), (1, 1), ("", 1)],
                      [("a", 1), ("a", 1), ("b", 1)]]

        def test_func(dstream):
            return dstream.groupByKey()
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
        test_input = [[(1, 1), (2, 1), (3, 1), (4, 1)],
                      [(1, 1), (1, 1), (1, 1), (2, 1), (2, 1), (3, 1)],
                      [("a", 1), ("a", 1), ("b", 1), ("", 1), ("", 1), ("", 1)]]

        def test_func(dstream):
            def add(a, b):
                return a + str(b)
            return dstream.combineByKey(str, add, add)
        expected_output = [[(1, "1"), (2, "1"), (3, "1"), (4, "1")],
                           [(1, "111"), (2, "11"), (3, "1")],
                           [("a", "11"), ("b", "1"), ("", "111")]]
        output = self._run_stream(test_input, test_func, expected_output)
        for result in (output, expected_output):
            self._sort_result_based_on_key(result)
        self.assertEqual(expected_output, output)

    def test_combineByKey_unbatch(self):
        """Basic operation test for DStream.combineByKey with unbatch deserializer."""
        test_input = [[(1, 1), (2, 1), (3, 1)],
                      [(1, 1), (1, 1), ("", 1)],
                      [("a", 1),  ("a", 1), ("b", 1)]]

        def test_func(dstream):
            def add(a, b):
                return a + str(b)
            return dstream.combineByKey(str, add, add)
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
        Start stream and return the result.
        @param test_input: dataset for the test. This should be list of lists.
        @param test_func: wrapped test_function. This function should return PythonDStream object.
        @param expected_output: expected output for this testcase.
        @param numSlices: the number of slices in the rdd in the dstream.
        """
        # Generate input stream with user-defined input.
        numSlices = numSlices or self.numInputPartitions
        test_input_stream = self.ssc._testInputStream(test_input, numSlices)
        # Apply test function to stream.
        test_stream = test_func(test_input_stream)
        # Add job to get output from stream.
        result = list()
        test_stream._test_output(result)
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
            if len(expected_output) == len(result):
                break

        return result


class TestStreamingContextSuite(unittest.TestCase):
    """
    Should we have conf property in  SparkContext?
    @property
    def conf(self):
        return self._conf

    """
    def setUp(self):
        self.master = "local[2]"
        self.appName = self.__class__.__name__
        self.batachDuration = Milliseconds(500)
        self.sparkHome = "SomeDir"
        self.envPair = {"key": "value"}
        self.ssc = None
        self.sc = None

    def tearDown(self):
        # Do not call pyspark.streaming.context.StreamingContext.stop directly because
        # we do not wait to shutdown py4j client.
        # We need change this simply calll streamingConxt.Stop
        #self.ssc._jssc.stop()
        if self.ssc is not None:
            self.ssc.stop()
        if self.sc is not None:
            self.sc.stop()
        # Why does it long time to terminate StremaingContext and SparkContext?
        # Should we change the sleep time if this depends on machine spec?
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        # Make sure tp shutdown the callback server
        SparkContext._gateway._shutdown_callback_server()

    def test_from_no_conf_constructor(self):
        self.ssc = StreamingContext(master=self.master, appName=self.appName,
                               duration=self.batachDuration)
        # Alternative call master: ssc.sparkContext.master
        # I try to make code close to Scala.
        self.assertEqual(self.ssc.sparkContext._conf.get("spark.master"), self.master)
        self.assertEqual(self.ssc.sparkContext._conf.get("spark.app.name"), self.appName)

    def test_from_no_conf_plus_spark_home(self):
        self.ssc = StreamingContext(master=self.master, appName=self.appName, 
                               sparkHome=self.sparkHome, duration=self.batachDuration)
        self.assertEqual(self.ssc.sparkContext._conf.get("spark.home"), self.sparkHome)

    def test_from_no_conf_plus_spark_home_plus_env(self):
        self.ssc = StreamingContext(master=self.master, appName=self.appName, 
                               sparkHome=self.sparkHome, environment=self.envPair,
                               duration=self.batachDuration)
        self.assertEqual(self.ssc.sparkContext._conf.get("spark.executorEnv.key"), self.envPair["key"])

    def test_from_existing_spark_context(self):
        self.sc = SparkContext(master=self.master, appName=self.appName)
        self.ssc = StreamingContext(sparkContext=self.sc, duration=self.batachDuration)

    def test_existing_spark_context_with_settings(self):
        conf = SparkConf()
        conf.set("spark.cleaner.ttl", "10")
        self.sc = SparkContext(master=self.master, appName=self.appName, conf=conf)
        self.ssc = StreamingContext(sparkContext=self.sc, duration=self.batachDuration)
        self.assertEqual(int(self.ssc.sparkContext._conf.get("spark.cleaner.ttl")), 10)

    def test_from_conf_with_settings(self):
        conf = SparkConf()
        conf.set("spark.cleaner.ttl", "10")
        conf.setMaster(self.master)
        conf.setAppName(self.appName)
        self.ssc = StreamingContext(conf=conf, duration=self.batachDuration)
        self.assertEqual(int(self.ssc.sparkContext._conf.get("spark.cleaner.ttl")), 10)

    def test_stop_only_streaming_context(self):
        self.sc = SparkContext(master=self.master, appName=self.appName)
        self.ssc = StreamingContext(sparkContext=self.sc, duration=self.batachDuration)
        self._addInputStream(self.ssc)
        self.ssc.start()
        self.ssc.stop(False)
        self.assertEqual(len(self.sc.parallelize(range(5), 5).glom().collect()), 5)

    def test_stop_multiple_times(self):
        self.ssc = StreamingContext(master=self.master, appName=self.appName,
                               duration=self.batachDuration)
        self._addInputStream(self.ssc)
        self.ssc.start()
        self.ssc.stop()
        self.ssc.stop()

    def _addInputStream(self, s):
        # Make sure each length of input is over 3 and 
        # numSlice is 2 due to deserializer problem in pyspark.streaming
        test_inputs = map(lambda x: range(1, x), range(5, 101))
        test_stream = s._testInputStream(test_inputs, 2)
        # Register fake output operation
        result = list()
        test_stream._test_output(result)

if __name__ == "__main__":
    unittest.main()
