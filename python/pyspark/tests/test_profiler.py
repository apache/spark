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
import sys
import tempfile
import unittest
from io import StringIO

from pyspark import SparkConf, SparkContext, BasicProfiler
from pyspark.profiler import has_memory_profiler
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.errors import PythonException
from pyspark.testing.utils import PySparkTestCase


class ProfilerTests(PySparkTestCase):
    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.python.profile", "true")
        self.sc = SparkContext("local[4]", class_name, conf=conf)

    def test_profiler(self):
        self.do_computation()

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(1, len(profilers))
        id, profiler, _ = profilers[0]
        stats = profiler.stats()
        self.assertTrue(stats is not None)
        width, stat_list = stats.get_print_list([])
        func_names = [func_name for fname, n, func_name in stat_list]
        self.assertTrue("heavy_foo" in func_names)

        old_stdout = sys.stdout
        sys.stdout = io = StringIO()
        self.sc.show_profiles()
        self.assertTrue("heavy_foo" in io.getvalue())
        sys.stdout = old_stdout

        d = tempfile.gettempdir()
        self.sc.dump_profiles(d)
        self.assertTrue("rdd_%d.pstats" % id in os.listdir(d))

    def test_custom_profiler(self):
        class TestCustomProfiler(BasicProfiler):
            def show(self, id):
                self.result = "Custom formatting"

        self.sc.profiler_collector.profiler_cls = TestCustomProfiler

        self.do_computation()

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(1, len(profilers))
        _, profiler, _ = profilers[0]
        self.assertTrue(isinstance(profiler, TestCustomProfiler))

        self.sc.show_profiles()
        self.assertEqual("Custom formatting", profiler.result)

    def do_computation(self):
        def heavy_foo(x):
            for i in range(1 << 18):
                x = 1  # noqa: F841

        rdd = self.sc.parallelize(range(100))
        rdd.foreach(heavy_foo)


class ProfilerTests2(unittest.TestCase):
    def test_profiler_disabled(self):
        sc = SparkContext(
            conf=SparkConf()
            .set("spark.python.profile", "false")
            .set("spark.python.profile.memory", "false")
        )
        try:
            self.assertRaisesRegex(
                RuntimeError,
                "'spark.python.profile' or 'spark.python.profile.memory' configuration must be set",
                lambda: sc.show_profiles(),
            )
            self.assertRaisesRegex(
                RuntimeError,
                "'spark.python.profile' or 'spark.python.profile.memory' configuration must be set",
                lambda: sc.dump_profiles("/tmp/abc"),
            )
        finally:
            sc.stop()

    def test_profiler_all_enabled(self):
        sc = SparkContext(
            conf=SparkConf()
            .set("spark.python.profile", "true")
            .set("spark.python.profile.memory", "true")
        )
        spark = SparkSession(sparkContext=sc)

        @udf("int")
        def plus_one(v):
            return v + 1

        try:
            self.assertRaisesRegex(
                RuntimeError,
                "'spark.python.profile' and 'spark.python.profile.memory' configuration"
                " cannot be enabled together",
                lambda: spark.range(10).select(plus_one("id")).collect(),
            )
        finally:
            sc.stop()

    @unittest.skipIf(has_memory_profiler, "Test when memory-profiler is not installed.")
    def test_no_memory_profile_installed(self):
        sc = SparkContext(
            conf=SparkConf()
            .set("spark.python.profile", "false")
            .set("spark.python.profile.memory", "true")
        )
        spark = SparkSession(sparkContext=sc)

        @udf("int")
        def plus_one(v):
            return v + 1

        try:
            self.assertRaisesRegex(
                PythonException,
                "Install the 'memory_profiler' library in the cluster to enable memory "
                "profiling",
                lambda: spark.range(10).select(plus_one("id")).collect(),
            )
        finally:
            sc.stop()


if __name__ == "__main__":
    from pyspark.tests.test_profiler import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
