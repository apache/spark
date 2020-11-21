#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import tempfile
import time
import unittest

from pyspark import SparkConf, SparkContext, RDD
from pyspark.streaming import StreamingContext
from pyspark.testing.utils import search_jar


# Must be same as the variable and condition defined in KinesisTestUtils.scala and modules.py
kinesis_test_environ_var = "ENABLE_KINESIS_TESTS"
should_skip_kinesis_tests = not os.environ.get(kinesis_test_environ_var) == '1'

if should_skip_kinesis_tests:
    kinesis_requirement_message = (
        "Skipping all Kinesis Python tests as environmental variable 'ENABLE_KINESIS_TESTS' "
        "was not set.")
else:
    kinesis_asl_assembly_jar = search_jar("external/kinesis-asl-assembly",
                                          "spark-streaming-kinesis-asl-assembly-",
                                          "spark-streaming-kinesis-asl-assembly_")
    if kinesis_asl_assembly_jar is None:
        kinesis_requirement_message = (  # type: ignore
            "Skipping all Kinesis Python tests as the optional Kinesis project was "
            "not compiled into a JAR. To run these tests, "
            "you need to build Spark with 'build/sbt -Pkinesis-asl assembly/package "
            "streaming-kinesis-asl-assembly/assembly' or "
            "'build/mvn -Pkinesis-asl package' before running this test.")
    else:
        existing_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        jars_args = "--jars %s" % kinesis_asl_assembly_jar
        os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join([jars_args, existing_args])
        kinesis_requirement_message = None  # type: ignore

should_test_kinesis = kinesis_requirement_message is None


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

        Returns
        -------
        list
            which will have the collected items.
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
        Parameters
        ----------
        input : list
            dataset for the test. This should be list of lists.
        func : function
            wrapped function. This function should return PythonDStream object.
        expected
            expected output for this testcase.
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
