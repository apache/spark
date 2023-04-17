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

import unittest

from typing import Any, Iterator

from py4j.protocol import Py4JJavaError

from pyspark.errors import PythonException
from pyspark.sql.functions import lit, udtf
from pyspark.sql.types import Row
from pyspark.testing.sqlutils import ReusedSQLTestCase


class UDTFTestsMixin(ReusedSQLTestCase):

    def test_udtf_single_col(self):
        class TestUDTF:
            def eval(self, a: int):
                yield a,

        func = udtf(TestUDTF, returnType="a: int")
        rows = func(lit(1)).collect()
        self.assertEqual(rows, [Row(a=1)])

    def test_udtf_multi_col(self):
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

        func = udtf(TestUDTF, returnType="a: int, b: int")
        rows = func(lit(1)).collect()
        self.assertEqual(rows, [Row(a=1, b=2)])

    def test_udtf_multi_row(self):
        class TestUDTF:
            def eval(self, a: int, b: int):
                yield a, b, a + b
                yield a, b, a - b
                yield a, b, b - a

        func = udtf(TestUDTF, returnType="a: int, b: int, c: int")
        rows = func(lit(1), lit(2)).collect()
        self.assertEqual(rows, [Row(a=1, b=2, c=3), Row(a=1, b=2, c=-1), Row(a=1, b=2, c=1)])

    def test_udtf_zero_row(self):
        class TestUDTF:
            def eval(self, a: int):
                yield

        func = udtf(TestUDTF, returnType="c: int")
        with self.assertRaisesRegex(Py4JJavaError, "java.lang.NullPointerException"):
            func(lit(1)).collect()

    def test_udtf_decorator(self):
        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

        rows = TestUDTF(lit(1)).collect()
        self.assertEqual(rows, [Row(a=1, b=2)])

    def test_udtf_registration(self):
        class TestUDTF:
            def eval(self, a: int, b: int):
                yield a, b, a + b
                yield a, b, a - b
                yield a, b, b - a

        func = udtf(TestUDTF, returnType="a: int, b: int, c: int")
        self.spark.udtf.register("testUDTF", func)
        df = self.spark.sql("SELECT * FROM testUDTF(1, 2)")
        rows = df.collect()
        self.assertEqual(rows, [Row(a=1, b=2, c=3), Row(a=1, b=2, c=-1), Row(a=1, b=2, c=1)])

    def test_udtf_with_lateral_join(self):
        class TestUDTF:
            def eval(self, a: int, b: int) -> Iterator:
                yield a, b, a + b
                yield a, b, a - b

        func = udtf(TestUDTF, returnType="a: int, b: int, c: int")
        self.spark.udtf.register("testUDTF", func)
        df = self.spark.sql(
            "SELECT f.* FROM values (0, 1), (1, 2) t(a, b), LATERAL testUDTF(a, b) f"
        )
        expected = self.spark.createDataFrame(
            [(0, 1, 1), (0, 1, -1), (1, 2, 3), (1, 2, -1)], schema=["a", "b", "c"]
        )
        self.assertEqual(df.collect(), expected.collect())

    def test_udtf_with_none_output(self):
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a,
                yield None,

        self.assertEqual(TestUDTF(lit(1)).collect(), [Row(a=1), Row(a=None)])
        df = self.spark.createDataFrame([(0, 1), (1, 2)], schema=["a", "b"])
        self.assertEqual(TestUDTF(lit(1)).join(df, "a", "inner").collect(), [Row(a=1, b=2)])
        self.assertEqual(
            TestUDTF(lit(1)).join(df, "a", "left").collect(), [Row(a=None, b=None), Row(a=1, b=2)]
        )

    def test_udtf_with_none_input(self):
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a,

        self.assertEqual(TestUDTF(lit(None)).collect(), [Row(a=None)])
        self.spark.udtf.register("testUDTF", TestUDTF)
        df = self.spark.sql("SELECT * FROM testUDTF(null)")
        self.assertEqual(df.collect(), [Row(a=None)])

    def test_udtf_init(self):
        @udtf(returnType="a: int, b: int, c: string")
        class TestUDTF:
            def __init__(self):
                self.key = "test"

            def eval(self, a: int):
                yield a, a + 1, self.key

        rows = TestUDTF(lit(1)).collect()
        self.assertEqual(rows, [Row(a=1, b=2, c="test")])

    def test_udtf_terminate(self):
        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

            def terminate(self):
                raise ValueError("terminate")

        with self.assertRaisesRegex(PythonException, "Failed to terminate UDTF: terminate"):
            TestUDTF(lit(1)).collect()

    # Invalid cases

    def test_udtf_no_eval(self):
        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def run(self, a: int):
                yield a, a + 1

        with self.assertRaisesRegex(PythonException, "Python UDTF must implement the eval method"):
            TestUDTF(lit(1)).collect()

    def test_udtf_eval_returning_non_tuple(self):
        class TestUDTF:
            def eval(self, a: int):
                yield a

        func = udtf(TestUDTF, returnType="a: int")
        with self.assertRaisesRegex(PythonException, "Unexpected tuple 1 with StructType"):
            func(lit(1)).collect()

    def test_udtf_eval_with_return_stmt(self):
        class TestUDTF:
            def eval(self, a: int):
                return a,

        func = udtf(TestUDTF, returnType="a: int")
        with self.assertRaisesRegex(PythonException, "Unexpected tuple 1 with StructType"):
            func(lit(1)).collect()

    def test_udtf_no_handler(self):
        with self.assertRaisesRegex(TypeError, "the function handler must be a class"):
            @udtf(returnType="a: int")
            def eval(a: int):
                yield a,


class UDTFTests(UDTFTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super(UDTFTests, cls).setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")


if __name__ == "__main__":
    from pyspark.sql.tests.test_udtf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
