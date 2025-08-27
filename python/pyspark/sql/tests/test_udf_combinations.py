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

from typing import Iterator
import itertools
import unittest

from pyspark.sql.functions import udf, arrow_udf, pandas_udf
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class UDFCombinationsTestsMixin:
    @property
    def python_udf_add1(self):
        @udf("long")
        def add_one(v):
            assert isinstance(v, int)
            return v + 1

        return add_one

    @property
    def arrow_opt_python_udf_add1(self):
        @udf("long")
        def add_one(v, useArrow=True):
            assert isinstance(v, int)
            return v + 1

        return add_one

    @property
    def pandas_udf_add1(self):
        import pandas as pd

        @pandas_udf("long")
        def add_one(s):
            assert isinstance(s, pd.Series)
            return s + 1

        return add_one

    @property
    def pandas_iter_udf_add1(self):
        import pandas as pd

        @pandas_udf("long")
        def add_one(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for s in it:
                assert isinstance(s, pd.Series)
                yield s + 1

        return add_one

    @property
    def arrow_udf_add1(self):
        import pyarrow as pa

        @arrow_udf("long")
        def add_one(a):
            assert isinstance(a, pa.Array)
            return pa.compute.add(a, 1)

        return add_one

    @property
    def arrow_iter_udf_add1(self):
        import pyarrow as pa

        @arrow_udf("long")
        def add_one(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
            for a in it:
                assert isinstance(a, pa.Array)
                yield pa.compute.add(a, 1)

        return add_one

    def all_scalar_functions(self):
        return [
            ("python udf", self.python_udf_add1),
            ("arrow-optimized python udf", self.arrow_opt_python_udf_add1),
            ("scalar pandas udf", self.pandas_udf_add1),
            ("scalar iter pandas udf", self.pandas_iter_udf_add1),
            ("scalar arrow udf", self.arrow_udf_add1),
            ("scalar iter arrow udf", self.arrow_iter_udf_add1),
        ]

    def test_combination_2(self):
        df = self.spark.range(10)

        expected = df.selectExpr("id + 2 AS res").collect()

        combs = itertools.combinations(self.all_scalar_functions(), 2)
        for (t1, f1), (t2, f2) in combs:
            with self.subTest(udf1=t1, udf2=t2):
                result = df.select(f1(f2("id")).alias("res"))
                self.assertEqual(expected, result.collect())

    def test_combination_3(self):
        df = self.spark.range(10)

        expected = df.selectExpr("id + 3 AS res").collect()

        combs = itertools.combinations(self.all_scalar_functions(), 3)
        for (t1, f1), (t2, f2), (t3, f3) in combs:
            with self.subTest(udf1=t1, udf2=t2, udf3=t3):
                result = df.select(f1(f2(f3("id"))).alias("res"))
                self.assertEqual(expected, result.collect())

    def test_combination_4(self):
        df = self.spark.range(10)

        expected = df.selectExpr("id + 4 AS res").collect()

        combs = itertools.combinations(self.all_scalar_functions(), 4)
        for (t1, f1), (t2, f2), (t3, f3), (t4, f4) in combs:
            with self.subTest(udf1=t1, udf2=t2, udf3=t3, udf4=t4):
                result = df.select(f1(f2(f3(f4("id")))).alias("res"))
                self.assertEqual(expected, result.collect())

    def test_combination_5(self):
        df = self.spark.range(10)

        expected = df.selectExpr("id + 5 AS res").collect()

        combs = itertools.combinations(self.all_scalar_functions(), 5)
        for (t1, f1), (t2, f2), (t3, f3), (t4, f4), (t5, f5) in combs:
            with self.subTest(udf1=t1, udf2=t2, udf3=t3, udf4=t4, udf5=t5):
                result = df.select(f1(f2(f3(f4(f5("id"))))).alias("res"))
                self.assertEqual(expected, result.collect())

    def test_combination_6(self):
        df = self.spark.range(10)

        expected = df.selectExpr("id + 6 AS res").collect()

        combs = itertools.combinations(self.all_scalar_functions(), 6)
        for (t1, f1), (t2, f2), (t3, f3), (t4, f4), (t5, f5), (t6, f6) in combs:
            with self.subTest(udf1=t1, udf2=t2, udf3=t3, udf4=t4, udf5=t5, udf6=t6):
                result = df.select(f1(f2(f3(f4(f5(f6("id")))))).alias("res"))
                self.assertEqual(expected, result.collect())


class UDFCombinationsTests(UDFCombinationsTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")


if __name__ == "__main__":
    from pyspark.sql.tests.test_udf_combinations import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
