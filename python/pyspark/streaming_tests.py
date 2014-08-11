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
from fileinput import input
from glob import glob
from itertools import chain
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile

from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *


SPARK_HOME = os.environ["SPARK_HOME"]

class buff:
    """
    Buffer for store the output from stream
    """
    result = None

class PySparkStreamingTestCase(unittest.TestCase):
    def setUp(self):
        print "set up"
        class_name = self.__class__.__name__
        self.ssc = StreamingContext(appName=class_name, duration=Seconds(1))

    def tearDown(self):
        print "tear donw"
        self.ssc.stop()
        time.sleep(10)

class TestBasicOperationsSuite(PySparkStreamingTestCase):
    def setUp(self):
        PySparkStreamingTestCase.setUp(self)
        buff.result = None
        self.timeout = 10 # seconds

    def tearDown(self):
        PySparkStreamingTestCase.tearDown(self)

    def test_map(self):
        test_input = [range(1,5), range(5,9), range(9, 13)]
        def test_func(dstream):
            return dstream.map(lambda x: str(x))
        expected = map(str, test_input)
        output = self.run_stream(test_input, test_func)
        self.assertEqual(output, expected)

    def test_flatMap(self):
        test_input = [range(1,5), range(5,9), range(9, 13)]
        def test_func(dstream):
            return dstream.flatMap(lambda x: (x, x * 2))
        # Maybe there be good way to create flatmap
        excepted = map(lambda x: list(chain.from_iterable((map(lambda y:[y, y*2], x)))), 
                       test_input)
        output = self.run_stream(test_input, test_func)

    def run_stream(self, test_input, test_func):
        # Generate input stream with user-defined input
        test_input_stream = self.ssc._testInputStream(test_input)
        # Applyed test function to stream
        test_stream = test_func(test_input_stream)
        # Add job to get outpuf from stream
        test_stream._test_output(buff)
        self.ssc.start()

        start_time = time.time()
        while True:
            current_time = time.time()
            # check time out
            if (current_time - start_time) > self.timeout:
                self.ssc.stop()
                break
            self.ssc.awaitTermination(50)
            if buff.result is not None:
                break
        return buff.result

if __name__ == "__main__":
    unittest.main()
