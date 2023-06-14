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

from typing import Iterator

from py4j.protocol import Py4JJavaError

from pyspark.errors import PythonException, AnalysisException
from pyspark.sql.functions import lit, udtf
from pyspark.sql.types import Row
from pyspark.testing.sqlutils import ReusedSQLTestCase


class UDTFTestsMixin(ReusedSQLTestCase):
    def test_simple_udtf(self):
        class TestUDTF:
            def eval(self):
                yield "hello", "world"

        func = udtf(TestUDTF, returnType="c1: string, c2: string")
        rows = func().collect()
        self.assertEqual(rows, [Row(c1="hello", c2="world")])

    def test_udtf_yield_single_row_col(self):
        class TestUDTF:
            def eval(self, a: int):
                yield a,

        func = udtf(TestUDTF, returnType="a: int")
        rows = func(lit(1)).collect()
        self.assertEqual(rows, [Row(a=1)])

    def test_udtf_yield_multi_cols(self):
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

        func = udtf(TestUDTF, returnType="a: int, b: int")
        rows = func(lit(1)).collect()
        self.assertEqual(rows, [Row(a=1, b=2)])

    def test_udtf_yield_multi_rows(self):
        class TestUDTF:
            def eval(self, a: int):
                yield a,
                yield a + 1,

        func = udtf(TestUDTF, returnType="a: int")
        rows = func(lit(1)).collect()
        self.assertEqual(rows, [Row(a=1), Row(a=2)])

    def test_udtf_yield_multi_row_col(self):
        class TestUDTF:
            def eval(self, a: int, b: int):
                yield a, b, a + b
                yield a, b, a - b
                yield a, b, b - a

        func = udtf(TestUDTF, returnType="a: int, b: int, c: int")
        rows = func(lit(1), lit(2)).collect()
        self.assertEqual(rows, [Row(a=1, b=2, c=3), Row(a=1, b=2, c=-1), Row(a=1, b=2, c=1)])

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
        self.assertEqual(
            df.collect(), [Row(a=1, b=2, c=3), Row(a=1, b=2, c=-1), Row(a=1, b=2, c=1)]
        )

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

    def test_udtf_eval_with_return_stmt(self):
        class TestUDTF:
            def eval(self, a: int, b: int):
                return [(a, a + 1), (b, b + 1)]

        func = udtf(TestUDTF, returnType="a: int, b: int")
        rows = func(lit(1), lit(2)).collect()
        self.assertEqual(rows, [Row(a=1, b=2), Row(a=2, b=3)])

    def test_udtf_eval_returning_non_tuple(self):
        class TestUDTF:
            def eval(self, a: int):
                yield a

        func = udtf(TestUDTF, returnType="a: int")
        # TODO(SPARK-44005): improve this error message
        with self.assertRaisesRegex(PythonException, "Unexpected tuple 1 with StructType"):
            func(lit(1)).collect()

    def test_udtf_eval_returning_non_generator(self):
        class TestUDTF:
            def eval(self, a: int):
                return (a,)

        func = udtf(TestUDTF, returnType="a: int")
        # TODO(SPARK-44005): improve this error message
        with self.assertRaisesRegex(PythonException, "Unexpected tuple 1 with StructType"):
            func(lit(1)).collect()

    def test_udtf_eval_with_no_return(self):
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                ...

        # TODO(SPARK-43967): Support Python UDTFs with empty return values
        with self.assertRaisesRegex(PythonException, "TypeError"):
            TestUDTF(lit(1)).collect()

        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                return

        with self.assertRaisesRegex(PythonException, "TypeError"):
            TestUDTF(lit(1)).collect()

    def test_udtf_with_conditional_return(self):
        class TestUDTF:
            def eval(self, a: int):
                if a > 5:
                    yield a,

        func = udtf(TestUDTF, returnType="a: int")
        self.spark.udtf.register("test_udtf", func)
        self.assertEqual(
            self.spark.sql("SELECT * FROM range(0, 8) JOIN LATERAL test_udtf(id)").collect(),
            [Row(id=6, a=6), Row(id=7, a=7)],
        )

    def test_udtf_with_empty_yield(self):
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                yield

        # TODO(SPARK-43967): Support Python UDTFs with empty return values
        with self.assertRaisesRegex(Py4JJavaError, "java.lang.NullPointerException"):
            TestUDTF(lit(1)).collect()

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

    def test_udtf_with_wrong_num_input(self):
        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

        with self.assertRaisesRegex(
            PythonException, r"eval\(\) missing 1 required positional argument: 'a'"
        ):
            TestUDTF().collect()

        with self.assertRaisesRegex(
            PythonException, r"eval\(\) takes 2 positional arguments but 3 were given"
        ):
            TestUDTF(lit(1), lit(2)).collect()

    def test_udtf_with_wrong_num_output(self):
        # TODO(SPARK-43968): check this during compile time instead of runtime
        err_msg = (
            "java.lang.IllegalStateException: Input row doesn't have expected number of "
            + "values required by the schema."
        )

        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a,

        with self.assertRaisesRegex(Py4JJavaError, err_msg):
            TestUDTF(lit(1)).collect()

        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

        with self.assertRaisesRegex(Py4JJavaError, err_msg):
            TestUDTF(lit(1)).collect()

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
        @udtf(returnType="key: string, value: float")
        class TestUDTF:
            def __init__(self):
                self._count = 0
                self._sum = 0

            def eval(self, x: int):
                self._count += 1
                self._sum += x
                yield "input", float(x)

            def terminate(self):
                yield "count", float(self._count)
                yield "avg", self._sum / self._count

        self.assertEqual(
            TestUDTF(lit(1)).collect(),
            [Row(key="input", value=1), Row(key="count", value=1.0), Row(key="avg", value=1.0)],
        )

        self.spark.udtf.register("test_udtf", TestUDTF)
        df = self.spark.sql(
            "SELECT id, key, value FROM range(0, 10, 1, 2), "
            "LATERAL test_udtf(id) WHERE key != 'input'"
        )
        self.assertEqual(
            df.collect(),
            [
                Row(id=4, key="count", value=5.0),
                Row(id=4, key="avg", value=2.0),
                Row(id=9, key="count", value=5.0),
                Row(id=9, key="avg", value=7.0),
            ],
        )

    def test_terminate_with_exceptions(self):
        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

            def terminate(self):
                raise ValueError("terminate error")

        with self.assertRaisesRegex(
            PythonException,
            "User defined table function encountered an error in the 'terminate' "
            "method: terminate error",
        ):
            TestUDTF(lit(1)).collect()

    def test_udtf_terminate_with_wrong_num_output(self):
        err_msg = (
            "java.lang.IllegalStateException: Input row doesn't have expected number of "
            "values required by the schema."
        )

        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

            def terminate(self):
                yield 1, 2, 3

        with self.assertRaisesRegex(Py4JJavaError, err_msg):
            TestUDTF(lit(1)).show()

        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

            def terminate(self):
                yield 1,

        with self.assertRaisesRegex(Py4JJavaError, err_msg):
            TestUDTF(lit(1)).show()

    def test_nondeterministic_udtf(self):
        import random

        class RandomUDTF:
            def eval(self, a: int):
                yield a * int(random.random() * 100),

        random_udtf = udtf(RandomUDTF, returnType="x: int").asNondeterministic()
        # TODO(SPARK-43966): support non-deterministic UDTFs
        with self.assertRaisesRegex(AnalysisException, "nondeterministic expressions"):
            random_udtf(lit(1)).collect()

    def test_udtf_with_nondeterministic_input(self):
        from pyspark.sql.functions import rand

        @udtf(returnType="x: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a + 1,

        # TODO(SPARK-43966): support non-deterministic UDTFs
        with self.assertRaisesRegex(AnalysisException, "nondeterministic expressions"):
            TestUDTF(rand(0) * 100).collect()

    def test_udtf_no_eval(self):
        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def run(self, a: int):
                yield a, a + 1

        with self.assertRaisesRegex(
            PythonException,
            "Failed to execute the user defined table function because it has not "
            "implemented the 'eval' method. Please add the 'eval' method and try the "
            "query again.",
        ):
            TestUDTF(lit(1)).collect()

    def test_udtf_with_no_handler_class(self):
        err_msg = "the function handler must be a class"
        with self.assertRaisesRegex(TypeError, err_msg):

            @udtf(returnType="a: int")
            def test_udtf(a: int):
                yield a,

        def test_udtf(a: int):
            yield a

        with self.assertRaisesRegex(TypeError, err_msg):
            udtf(test_udtf, returnType="a: int")


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
