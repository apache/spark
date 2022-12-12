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
import warnings
from io import StringIO
from typing import Iterator
from unittest import mock

from pyspark import SparkConf, SparkContext
from pyspark.profiler import has_memory_profiler
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, udf
from pyspark.testing.sqlutils import have_pandas, pandas_requirement_message
from pyspark.testing.utils import PySparkTestCase


@unittest.skipIf(not has_memory_profiler, "Must have memory-profiler installed.")
@unittest.skipIf(not have_pandas, pandas_requirement_message)
class MemoryProfilerTests(PySparkTestCase):
    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.python.profile.memory", "true")
        self.sc = SparkContext("local[4]", class_name, conf=conf)
        self.spark = SparkSession(sparkContext=self.sc)

    def test_memory_profiler(self):
        self.exec_python_udf()

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(1, len(profilers))
        id, profiler, _ = profilers[0]
        stats = profiler.stats()
        self.assertTrue(stats is not None)

        with mock.patch("sys.stdout", new=StringIO()) as fake_out:
            self.sc.show_profiles()
        self.assertTrue("plus_one" in fake_out.getvalue())

        d = tempfile.gettempdir()
        self.sc.dump_profiles(d)
        self.assertTrue("udf_%d_memory.txt" % id in os.listdir(d))

    def test_profile_pandas_udf(self):
        udfs = [self.exec_pandas_udf_ser_to_ser, self.exec_pandas_udf_ser_to_scalar]
        udf_names = ["ser_to_ser", "ser_to_scalar"]
        for f, f_name in zip(udfs, udf_names):
            f()
            with mock.patch("sys.stdout", new=StringIO()) as fake_out:
                self.sc.show_profiles()
            self.assertTrue(f_name in fake_out.getvalue())

        with warnings.catch_warnings(record=True) as warns:
            warnings.simplefilter("always")
            self.exec_pandas_udf_iter_to_iter()
            user_warns = [warn.message for warn in warns if isinstance(warn.message, UserWarning)]
            self.assertTrue(len(user_warns) > 0)
            self.assertTrue(
                "Profiling UDFs with iterators input/output is not supported" in str(user_warns[0])
            )

    def test_profile_pandas_function_api(self):
        apis = [self.exec_grouped_map]
        f_names = ["grouped_map"]
        for api, f_name in zip(apis, f_names):
            api()
            with mock.patch("sys.stdout", new=StringIO()) as fake_out:
                self.sc.show_profiles()
            self.assertTrue(f_name in fake_out.getvalue())

        with warnings.catch_warnings(record=True) as warns:
            warnings.simplefilter("always")
            self.exec_map()
            user_warns = [warn.message for warn in warns if isinstance(warn.message, UserWarning)]
            self.assertTrue(len(user_warns) > 0)
            self.assertTrue(
                "Profiling UDFs with iterators input/output is not supported" in str(user_warns[0])
            )

    def exec_python_udf(self):
        @udf("int")
        def plus_one(v):
            return v + 1

        self.spark.range(10).select(plus_one("id")).collect()

    def exec_pandas_udf_ser_to_ser(self):
        import pandas as pd

        @pandas_udf("int")
        def ser_to_ser(ser: pd.Series) -> pd.Series:
            return ser + 1

        self.spark.range(10).select(ser_to_ser("id")).collect()

    def exec_pandas_udf_ser_to_scalar(self):
        import pandas as pd

        @pandas_udf("int")
        def ser_to_scalar(ser: pd.Series) -> float:
            return ser.median()

        self.spark.range(10).select(ser_to_scalar("id")).collect()

    # Unsupported
    def exec_pandas_udf_iter_to_iter(self):
        import pandas as pd

        @pandas_udf("int")
        def iter_to_iter(batch_ser: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for ser in batch_ser:
                yield ser + 1

        self.spark.range(10).select(iter_to_iter("id")).collect()

    def exec_grouped_map(self):
        import pandas as pd

        def grouped_map(pdf: pd.DataFrame) -> pd.DataFrame:
            return pdf.assign(v=pdf.v - pdf.v.mean())

        df = self.spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0)], ("id", "v"))
        df.groupby("id").applyInPandas(grouped_map, schema="id long, v double").collect()

    # Unsupported
    def exec_map(self):
        import pandas as pd

        def map(pdfs: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            for pdf in pdfs:
                yield pdf[pdf.id == 1]

        df = self.spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0)], ("id", "v"))
        df.mapInPandas(map, schema=df.schema).collect()


if __name__ == "__main__":
    from pyspark.tests.test_memory_profiler import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
