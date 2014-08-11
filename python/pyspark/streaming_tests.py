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

This file will merged to tests.py. But for now, this file is separated due
to focusing to streaming test case

"""
from itertools import chain
import os
import time
import unittest
import operator

from pyspark.context import SparkContext
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *


SPARK_HOME = os.environ["SPARK_HOME"]


class StreamOutput:
    """
    a class to store the output from stream
    """
    result = list()


class PySparkStreamingTestCase(unittest.TestCase):
    def setUp(self):
        class_name = self.__class__.__name__
        self.ssc = StreamingContext(appName=class_name, duration=Seconds(1))

    def tearDown(self):
        # Do not call StreamingContext.stop directly because we do not wait to shutdown
        # call back server and py4j client
        self.ssc._jssc.stop()
        self.ssc._sc.stop()
        # Why does it long time to terminaete StremaingContext and SparkContext?
        # Should we change the sleep time if this depends on machine spec?
        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        time.sleep(5)
        SparkContext._gateway._shutdown_callback_server()


class TestBasicOperationsSuite(PySparkStreamingTestCase):
    """
    Input and output of this TestBasicOperationsSuite is the equivalent to 
    Scala TestBasicOperationsSuite.
    """
    def setUp(self):
        PySparkStreamingTestCase.setUp(self)
        StreamOutput.result = list()
        self.timeout = 10  # seconds

    def tearDown(self):
        PySparkStreamingTestCase.tearDown(self)

    @classmethod
    def tearDownClass(cls):
        PySparkStreamingTestCase.tearDownClass()

    def test_map(self):
        """Basic operation test for DStream.map"""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.map(lambda x: str(x))
        expected_output = map(lambda x: map(lambda y: str(y), x), test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_flatMap(self):
        """Basic operation test for DStream.faltMap"""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.flatMap(lambda x: (x, x * 2))
        expected_output = map(lambda x: list(chain.from_iterable((map(lambda y: [y, y * 2], x)))), 
                              test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_filter(self):
        """Basic operation test for DStream.filter"""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.filter(lambda x: x % 2 == 0)
        expected_output = map(lambda x: filter(lambda y: y % 2 == 0, x), test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_count(self):
        """Basic operation test for DStream.count"""
        test_input = [[], [1], range(1, 3), range(1, 4), range(1, 5)]

        def test_func(dstream):
            return dstream.count()
        expected_output = map(lambda x: [len(x)], test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)
        
    def test_reduce(self):
        """Basic operation test for DStream.reduce"""
        test_input = [range(1, 5), range(5, 9), range(9, 13)]

        def test_func(dstream):
            return dstream.reduce(operator.add)
        expected_output = map(lambda x: [reduce(operator.add, x)], test_input)
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def test_reduceByKey(self):
        """Basic operation test for DStream.reduceByKey"""
        test_input = [["a", "a", "b"], ["", ""], []]

        def test_func(dstream):
            return dstream.map(lambda x: (x, 1)).reduceByKey(operator.add)
        expected_output = [[("a", 2), ("b", 1)], [("", 2)], []]
        output = self._run_stream(test_input, test_func, expected_output)
        self.assertEqual(expected_output, output)

    def _run_stream(self, test_input, test_func, expected_output):
        """Start stream and return the output"""
        # Generate input stream with user-defined input
        test_input_stream = self.ssc._testInputStream(test_input)
        # Applied test function to stream
        test_stream = test_func(test_input_stream)
        # Add job to get output from stream
        test_stream._test_output(StreamOutput.result)
        self.ssc.start()

        start_time = time.time()
        # loop until get the result from stream
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
