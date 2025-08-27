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
        def py_add1(v):
            assert isinstance(v, int)
            return v + 1

        return py_add1

    @property
    def arrow_opt_python_udf_add1(self):
        @udf("long")
        def py_arrow_opt_add1(v, useArrow=True):
            assert isinstance(v, int)
            return v + 1

        return py_arrow_opt_add1

    @property
    def pandas_udf_add1(self):
        import pandas as pd

        @pandas_udf("long")
        def pandas_add1(s):
            assert isinstance(s, pd.Series)
            return s + 1

        return pandas_add1

    @property
    def pandas_iter_udf_add1(self):
        import pandas as pd

        @pandas_udf("long")
        def pandas_iter_add1(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for s in it:
                assert isinstance(s, pd.Series)
                yield s + 1

        return pandas_iter_add1

    @property
    def arrow_udf_add1(self):
        import pyarrow as pa

        @arrow_udf("long")
        def arrow_add1(a):
            assert isinstance(a, pa.Array)
            return pa.compute.add(a, 1)

        return arrow_add1

    @property
    def arrow_iter_udf_add1(self):
        import pyarrow as pa

        @arrow_udf("long")
        def arrow_iter_add1(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
            for a in it:
                assert isinstance(a, pa.Array)
                yield pa.compute.add(a, 1)

        return arrow_iter_add1

    def all_scalar_functions(self):
        return [
            self.python_udf_add1,
            self.arrow_opt_python_udf_add1,
            self.pandas_udf_add1,
            self.pandas_iter_udf_add1,
            self.arrow_udf_add1,
            self.arrow_iter_udf_add1,
        ]

    def test_combination_2(self):
        df = self.spark.range(10)

        expected = df.selectExpr("id + 2 AS res").collect()

        combs = itertools.combinations(self.all_scalar_functions(), 2)
        for f1, f2 in combs:
            with self.subTest(
                udf1=f1.__name__,
                udf2=f2.__name__,
            ):
                result = df.select(f1(f2("id")).alias("res"))
                self.assertEqual(expected, result.collect())

    def test_combination_3(self):
        df = self.spark.range(10)

        expected = df.selectExpr("id + 3 AS res").collect()

        combs = itertools.combinations(self.all_scalar_functions(), 3)
        for f1, f2, f3 in combs:
            with self.subTest(
                udf1=f1.__name__,
                udf2=f2.__name__,
                udf3=f3.__name__,
            ):
                result = df.select(f1(f2(f3("id"))).alias("res"))
                self.assertEqual(expected, result.collect())

    def test_combination_4(self):
        df = self.spark.range(10)

        expected = df.selectExpr("id + 4 AS res").collect()

        combs = itertools.combinations(self.all_scalar_functions(), 4)
        for f1, f2, f3, f4 in combs:
            with self.subTest(
                udf1=f1.__name__,
                udf2=f2.__name__,
                udf3=f3.__name__,
                udf4=f4.__name__,
            ):
                result = df.select(f1(f2(f3(f4("id")))).alias("res"))
                self.assertEqual(expected, result.collect())

    def test_combination_5(self):
        df = self.spark.range(10)

        expected = df.selectExpr("id + 5 AS res").collect()

        combs = itertools.combinations(self.all_scalar_functions(), 5)
        for f1, f2, f3, f4, f5 in combs:
            with self.subTest(
                udf1=f1.__name__,
                udf2=f2.__name__,
                udf3=f3.__name__,
                udf4=f4.__name__,
                udf5=f5.__name__,
            ):
                result = df.select(f1(f2(f3(f4(f5("id"))))).alias("res"))
                self.assertEqual(expected, result.collect())

    def test_combination_6(self):
        df = self.spark.range(10)

        expected = df.selectExpr("id + 6 AS res").collect()

        combs = itertools.combinations(self.all_scalar_functions(), 6)
        for f1, f2, f3, f4, f5, f6 in combs:
            with self.subTest(
                udf1=f1.__name__,
                udf2=f2.__name__,
                udf3=f3.__name__,
                udf4=f4.__name__,
                udf5=f5.__name__,
                udf6=f6.__name__,
            ):
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
