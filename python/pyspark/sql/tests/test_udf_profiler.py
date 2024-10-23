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

import tempfile
import unittest
import os
import sys
import warnings
from io import StringIO
from typing import Iterator

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf
from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.profiler import UDFBasicProfiler


class UDFProfilerTests(unittest.TestCase):
    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.python.profile", "true")
        self.spark = (
            SparkSession.builder.master("local[4]")
            .config(conf=conf)
            .appName(class_name)
            .getOrCreate()
        )
        self.sc = self.spark.sparkContext

    def tearDown(self):
        self.spark.stop()
        sys.path = self._old_sys_path

    def test_udf_profiler(self):
        self.do_computation()

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(3, len(profilers))

        old_stdout = sys.stdout
        try:
            sys.stdout = io = StringIO()
            self.sc.show_profiles()
        finally:
            sys.stdout = old_stdout

        d = tempfile.gettempdir()
        self.sc.dump_profiles(d)

        for i, udf_name in enumerate(["add1", "add2", "add1"]):
            id, profiler, _ = profilers[i]
            with self.subTest(id=id, udf_name=udf_name):
                stats = profiler.stats()
                self.assertTrue(stats is not None)
                width, stat_list = stats.get_print_list([])
                func_names = [func_name for fname, n, func_name in stat_list]
                self.assertTrue(udf_name in func_names)

                self.assertTrue(udf_name in io.getvalue())
                self.assertTrue("udf_%d.pstats" % id in os.listdir(d))

    def test_custom_udf_profiler(self):
        class TestCustomProfiler(UDFBasicProfiler):
            def show(self, id):
                self.result = "Custom formatting"

        self.sc.profiler_collector.udf_profiler_cls = TestCustomProfiler

        self.do_computation()

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(3, len(profilers))
        _, profiler, _ = profilers[0]
        self.assertTrue(isinstance(profiler, TestCustomProfiler))

        self.sc.show_profiles()
        self.assertEqual("Custom formatting", profiler.result)

    def do_computation(self):
        @udf
        def add1(x):
            return x + 1

        @udf
        def add2(x):
            return x + 2

        df = self.spark.range(10)
        df.select(add1("id"), add2("id"), add1("id")).collect()

    # Unsupported
    def exec_pandas_udf_iter_to_iter(self):
        import pandas as pd

        @pandas_udf("int")
        def iter_to_iter(batch_ser: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for ser in batch_ser:
                yield ser + 1

        self.spark.range(10).select(iter_to_iter("id")).collect()

    # Unsupported
    def exec_map(self):
        import pandas as pd

        def map(pdfs: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            for pdf in pdfs:
                yield pdf[pdf.id == 1]

        df = self.spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0)], ("id", "v"))
        df.mapInPandas(map, schema=df.schema).collect()

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)  # type: ignore
    def test_unsupported(self):
        with warnings.catch_warnings(record=True) as warns:
            warnings.simplefilter("always")
            self.exec_pandas_udf_iter_to_iter()
            user_warns = [warn.message for warn in warns if isinstance(warn.message, UserWarning)]
            self.assertTrue(len(user_warns) > 0)
            self.assertTrue(
                "Profiling UDFs with iterators input/output is not supported" in str(user_warns[0])
            )

        with warnings.catch_warnings(record=True) as warns:
            warnings.simplefilter("always")
            self.exec_map()
            user_warns = [warn.message for warn in warns if isinstance(warn.message, UserWarning)]
            self.assertTrue(len(user_warns) > 0)
            self.assertTrue(
                "Profiling UDFs with iterators input/output is not supported" in str(user_warns[0])
            )


if __name__ == "__main__":
    from pyspark.sql.tests.test_udf_profiler import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
