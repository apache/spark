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
import tempfile
import unittest
from typing import Iterator

from py4j.protocol import Py4JJavaError

from pyspark.errors import (
    PySparkAttributeError,
    PythonException,
    PySparkTypeError,
    AnalysisException,
    PySparkRuntimeError,
)
from pyspark.rdd import PythonEvalType
from pyspark.sql.functions import lit, udf, udtf
from pyspark.sql.types import IntegerType, MapType, Row, StringType, StructType
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
    ReusedSQLTestCase,
)


class BaseUDTFTestsMixin:
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
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a

        with self.assertRaisesRegex(PythonException, "UDTF_INVALID_OUTPUT_ROW_TYPE"):
            TestUDTF(lit(1)).collect()

        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                return (a,)

        with self.assertRaisesRegex(PythonException, "UDTF_INVALID_OUTPUT_ROW_TYPE"):
            TestUDTF(lit(1)).collect()

    def test_udtf_with_invalid_return_value(self):
        @udtf(returnType="x: int")
        class TestUDTF:
            def eval(self, a):
                return a

        with self.assertRaisesRegex(PythonException, "UDTF_RETURN_NOT_ITERABLE"):
            TestUDTF(lit(1)).collect()

    def test_udtf_with_zero_arg_and_invalid_return_value(self):
        @udtf(returnType="x: int")
        class TestUDTF:
            def eval(self):
                return 1

        with self.assertRaisesRegex(PythonException, "UDTF_RETURN_NOT_ITERABLE"):
            TestUDTF().collect()

    def test_udtf_with_invalid_return_value_in_terminate(self):
        @udtf(returnType="x: int")
        class TestUDTF:
            def eval(self, a):
                ...

            def terminate(self):
                return 1

        with self.assertRaisesRegex(PythonException, "UDTF_RETURN_NOT_ITERABLE"):
            TestUDTF(lit(1)).collect()

    def test_udtf_eval_with_no_return(self):
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                ...

        self.assertEqual(TestUDTF(lit(1)).collect(), [])

        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                return

        self.assertEqual(TestUDTF(lit(1)).collect(), [])

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

        assertDataFrameEqual(TestUDTF(lit(1)), [Row(a=None)])

    def test_udtf_with_none_output(self):
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a,
                yield None,

        self.assertEqual(TestUDTF(lit(1)).collect(), [Row(a=1), Row(a=None)])
        df = self.spark.createDataFrame([(0, 1), (1, 2)], schema=["a", "b"])
        self.assertEqual(TestUDTF(lit(1)).join(df, "a", "inner").collect(), [Row(a=1, b=2)])
        assertDataFrameEqual(
            TestUDTF(lit(1)).join(df, "a", "left"), [Row(a=None, b=None), Row(a=1, b=2)]
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

    def test_udtf_init_with_additional_args(self):
        @udtf(returnType="x int")
        class TestUDTF:
            def __init__(self, a: int):
                ...

            def eval(self, a: int):
                yield a,

        with self.assertRaisesRegex(
            PythonException, r"__init__\(\) missing 1 required positional argument: 'a'"
        ):
            TestUDTF(lit(1)).show()

    def test_udtf_terminate_with_additional_args(self):
        @udtf(returnType="x int")
        class TestUDTF:
            def eval(self, a: int):
                yield a,

            def terminate(self, a: int):
                ...

        with self.assertRaisesRegex(
            PythonException, r"terminate\(\) missing 1 required positional argument: 'a'"
        ):
            TestUDTF(lit(1)).show()

    def test_udtf_with_wrong_num_output(self):
        err_msg = (
            r"\[UDTF_RETURN_SCHEMA_MISMATCH\] The number of columns in the "
            "result does not match the specified schema."
        )

        # Output less columns than specified return schema
        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a,

        with self.assertRaisesRegex(PythonException, err_msg):
            TestUDTF(lit(1)).collect()

        # Output more columns than specified return schema
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

        with self.assertRaisesRegex(PythonException, err_msg):
            TestUDTF(lit(1)).collect()

    def test_udtf_with_empty_output_schema_and_non_empty_output(self):
        @udtf(returnType=StructType())
        class TestUDTF:
            def eval(self):
                yield 1,

        with self.assertRaisesRegex(PythonException, "UDTF_RETURN_SCHEMA_MISMATCH"):
            TestUDTF().collect()

    def test_udtf_with_non_empty_output_schema_and_empty_output(self):
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self):
                yield tuple()

        with self.assertRaisesRegex(PythonException, "UDTF_RETURN_SCHEMA_MISMATCH"):
            TestUDTF().collect()

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

    def test_init_with_exception(self):
        @udtf(returnType="x: int")
        class TestUDTF:
            def __init__(self):
                raise Exception("error")

            def eval(self):
                yield 1,

        with self.assertRaisesRegex(
            PythonException,
            r"\[UDTF_EXEC_ERROR\] User defined table function encountered an error "
            r"in the '__init__' method: error",
        ):
            TestUDTF().show()

    def test_eval_with_exception(self):
        @udtf(returnType="x: int")
        class TestUDTF:
            def eval(self):
                raise Exception("error")

        with self.assertRaisesRegex(
            PythonException,
            r"\[UDTF_EXEC_ERROR\] User defined table function encountered an error "
            r"in the 'eval' method: error",
        ):
            TestUDTF().show()

    def test_terminate_with_exceptions(self):
        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

            def terminate(self):
                raise ValueError("terminate error")

        with self.assertRaisesRegex(
            PythonException,
            r"\[UDTF_EXEC_ERROR\] User defined table function encountered an error "
            r"in the 'terminate' method: terminate error",
        ):
            TestUDTF(lit(1)).collect()

    def test_udtf_terminate_with_wrong_num_output(self):
        err_msg = (
            r"\[UDTF_RETURN_SCHEMA_MISMATCH\] The number of columns in the result "
            "does not match the specified schema."
        )

        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

            def terminate(self):
                yield 1, 2, 3

        with self.assertRaisesRegex(PythonException, err_msg):
            TestUDTF(lit(1)).show()

        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

            def terminate(self):
                yield 1,

        with self.assertRaisesRegex(PythonException, err_msg):
            TestUDTF(lit(1)).show()

    def test_udtf_determinism(self):
        class TestUDTF:
            def eval(self, a: int):
                yield a,

        func = udtf(TestUDTF, returnType="x: int")
        # The UDTF is marked as non-deterministic by default.
        self.assertFalse(func.deterministic)
        func = func.asDeterministic()
        self.assertTrue(func.deterministic)

    def test_nondeterministic_udtf(self):
        import random

        class RandomUDTF:
            def eval(self, a: int):
                yield a + int(random.random()),

        random_udtf = udtf(RandomUDTF, returnType="x: int")
        assertDataFrameEqual(random_udtf(lit(1)), [Row(x=1)])
        self.spark.udtf.register("random_udtf", random_udtf)
        assertDataFrameEqual(self.spark.sql("select * from random_udtf(1)"), [Row(x=1)])

    def test_udtf_with_nondeterministic_input(self):
        from pyspark.sql.functions import rand

        @udtf(returnType="x: int")
        class TestUDTF:
            def eval(self, a: int):
                yield 1 if a > 100 else 0,

        assertDataFrameEqual(TestUDTF(rand(0) * 100), [Row(x=0)])

    def test_udtf_with_invalid_return_type(self):
        @udtf(returnType="int")
        class TestUDTF:
            def eval(self, a: int):
                yield a + 1,

        with self.assertRaises(PySparkTypeError) as e:
            TestUDTF(lit(1)).collect()

        self.check_error(
            exception=e.exception,
            error_class="UDTF_RETURN_TYPE_MISMATCH",
            message_parameters={"name": "TestUDTF", "return_type": "IntegerType()"},
        )

        @udtf(returnType=MapType(StringType(), IntegerType()))
        class TestUDTF:
            def eval(self, a: int):
                yield a + 1,

        with self.assertRaises(PySparkTypeError) as e:
            TestUDTF(lit(1)).collect()

        self.check_error(
            exception=e.exception,
            error_class="UDTF_RETURN_TYPE_MISMATCH",
            message_parameters={
                "name": "TestUDTF",
                "return_type": "MapType(StringType(), IntegerType(), True)",
            },
        )

    def test_udtf_with_struct_input_type(self):
        @udtf(returnType="x: string")
        class TestUDTF:
            def eval(self, person):
                yield f"{person.name}: {person.age}",

        self.spark.udtf.register("test_udtf", TestUDTF)
        self.assertEqual(
            self.spark.sql(
                "select * from test_udtf(named_struct('name', 'Alice', 'age', 1))"
            ).collect(),
            [Row(x="Alice: 1")],
        )

    def test_udtf_with_array_input_type(self):
        @udtf(returnType="x: string")
        class TestUDTF:
            def eval(self, args):
                yield str(args),

        self.spark.udtf.register("test_udtf", TestUDTF)
        self.assertEqual(
            self.spark.sql("select * from test_udtf(array(1, 2, 3))").collect(),
            [Row(x="[1, 2, 3]")],
        )

    def test_udtf_with_map_input_type(self):
        @udtf(returnType="x: string")
        class TestUDTF:
            def eval(self, m):
                yield str(m),

        self.spark.udtf.register("test_udtf", TestUDTF)
        self.assertEqual(
            self.spark.sql("select * from test_udtf(map('key', 'value'))").collect(),
            [Row(x="{'key': 'value'}")],
        )

    def test_udtf_with_struct_output_types(self):
        @udtf(returnType="x: struct<a:int,b:int>")
        class TestUDTF:
            def eval(self, x: int):
                yield {"a": x, "b": x + 1},

        self.assertEqual(TestUDTF(lit(1)).collect(), [Row(x=Row(a=1, b=2))])

    def test_udtf_with_array_output_types(self):
        @udtf(returnType="x: array<int>")
        class TestUDTF:
            def eval(self, x: int):
                yield [x, x + 1, x + 2],

        self.assertEqual(TestUDTF(lit(1)).collect(), [Row(x=[1, 2, 3])])

    def test_udtf_with_map_output_types(self):
        @udtf(returnType="x: map<int,string>")
        class TestUDTF:
            def eval(self, x: int):
                yield {x: str(x)},

        self.assertEqual(TestUDTF(lit(1)).collect(), [Row(x={1: "1"})])

    def test_udtf_with_empty_output_types(self):
        @udtf(returnType=StructType())
        class TestUDTF:
            def eval(self):
                yield tuple()

        assertDataFrameEqual(TestUDTF(), [Row()])

    def _check_result_or_exception(
        self, func_handler, ret_type, expected, *, err_type=PythonException
    ):
        func = udtf(func_handler, returnType=ret_type)
        if not isinstance(expected, str):
            assertDataFrameEqual(func(), expected)
        else:
            with self.assertRaisesRegex(err_type, expected):
                func().collect()

    def test_numeric_output_type_casting(self):
        class TestUDTF:
            def eval(self):
                yield 1,

        for i, (ret_type, expected) in enumerate(
            [
                ("x: boolean", [Row(x=None)]),
                ("x: tinyint", [Row(x=1)]),
                ("x: smallint", [Row(x=1)]),
                ("x: int", [Row(x=1)]),
                ("x: bigint", [Row(x=1)]),
                ("x: string", [Row(x="1")]),  # int to string is ok, but string to int is None
                (
                    "x: date",
                    "AttributeError",
                ),  # AttributeError: 'int' object has no attribute 'toordinal'
                (
                    "x: timestamp",
                    "AttributeError",
                ),  # AttributeError: 'int' object has no attribute 'tzinfo'
                ("x: byte", [Row(x=1)]),
                ("x: binary", [Row(x=None)]),
                ("x: float", [Row(x=None)]),
                ("x: double", [Row(x=None)]),
                ("x: decimal(10, 0)", [Row(x=None)]),
                ("x: array<int>", [Row(x=None)]),
                ("x: map<string,int>", [Row(x=None)]),
                ("x: struct<a:int>", "UNEXPECTED_TUPLE_WITH_STRUCT"),
            ]
        ):
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_numeric_string_output_type_casting(self):
        class TestUDTF:
            def eval(self):
                yield "1",

        for ret_type, expected in [
            ("x: boolean", [Row(x=None)]),
            ("x: tinyint", [Row(x=None)]),
            ("x: smallint", [Row(x=None)]),
            ("x: int", [Row(x=None)]),
            ("x: bigint", [Row(x=None)]),
            ("x: string", [Row(x="1")]),
            ("x: date", "AttributeError"),
            ("x: timestamp", "AttributeError"),
            ("x: byte", [Row(x=None)]),
            ("x: binary", [Row(x=bytearray(b"1"))]),
            ("x: float", [Row(x=None)]),
            ("x: double", [Row(x=None)]),
            ("x: decimal(10, 0)", [Row(x=None)]),
            ("x: array<int>", [Row(x=None)]),
            ("x: map<string,int>", [Row(x=None)]),
            ("x: struct<a:int>", "UNEXPECTED_TUPLE_WITH_STRUCT"),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_string_output_type_casting(self):
        class TestUDTF:
            def eval(self):
                yield "hello",

        for ret_type, expected in [
            ("x: boolean", [Row(x=None)]),
            ("x: tinyint", [Row(x=None)]),
            ("x: smallint", [Row(x=None)]),
            ("x: int", [Row(x=None)]),
            ("x: bigint", [Row(x=None)]),
            ("x: string", [Row(x="hello")]),
            ("x: date", "AttributeError"),
            ("x: timestamp", "AttributeError"),
            ("x: byte", [Row(x=None)]),
            ("x: binary", [Row(x=bytearray(b"hello"))]),
            ("x: float", [Row(x=None)]),
            ("x: double", [Row(x=None)]),
            ("x: decimal(10, 0)", [Row(x=None)]),
            ("x: array<int>", [Row(x=None)]),
            ("x: map<string,int>", [Row(x=None)]),
            ("x: struct<a:int>", "UNEXPECTED_TUPLE_WITH_STRUCT"),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_array_output_type_casting(self):
        class TestUDTF:
            def eval(self):
                yield [0, 1.1, 2],

        for ret_type, expected in [
            ("x: boolean", [Row(x=None)]),
            ("x: tinyint", [Row(x=None)]),
            ("x: smallint", [Row(x=None)]),
            ("x: int", [Row(x=None)]),
            ("x: bigint", [Row(x=None)]),
            ("x: string", [Row(x="[0, 1.1, 2]")]),
            ("x: date", "AttributeError"),
            ("x: timestamp", "AttributeError"),
            ("x: byte", [Row(x=None)]),
            ("x: binary", [Row(x=None)]),
            ("x: float", [Row(x=None)]),
            ("x: double", [Row(x=None)]),
            ("x: decimal(10, 0)", [Row(x=None)]),
            ("x: array<int>", [Row(x=[0, None, 2])]),
            ("x: array<double>", [Row(x=[None, 1.1, None])]),
            ("x: array<string>", [Row(x=["0", "1.1", "2"])]),
            ("x: array<boolean>", [Row(x=[None, None, None])]),
            ("x: array<array<int>>", [Row(x=[None, None, None])]),
            ("x: map<string,int>", [Row(x=None)]),
            ("x: struct<a:int,b:int,c:int>", [Row(x=Row(a=0, b=None, c=2))]),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_map_output_type_casting(self):
        class TestUDTF:
            def eval(self):
                yield {"a": 0, "b": 1.1, "c": 2},

        for ret_type, expected in [
            ("x: boolean", [Row(x=None)]),
            ("x: tinyint", [Row(x=None)]),
            ("x: smallint", [Row(x=None)]),
            ("x: int", [Row(x=None)]),
            ("x: bigint", [Row(x=None)]),
            ("x: string", [Row(x="{a=0, b=1.1, c=2}")]),
            ("x: date", "AttributeError"),
            ("x: timestamp", "AttributeError"),
            ("x: byte", [Row(x=None)]),
            ("x: binary", [Row(x=None)]),
            ("x: float", [Row(x=None)]),
            ("x: double", [Row(x=None)]),
            ("x: decimal(10, 0)", [Row(x=None)]),
            ("x: array<string>", [Row(x=None)]),
            ("x: map<string,string>", [Row(x={"a": "0", "b": "1.1", "c": "2"})]),
            ("x: map<string,boolean>", [Row(x={"a": None, "b": None, "c": None})]),
            ("x: map<string,int>", [Row(x={"a": 0, "b": None, "c": 2})]),
            ("x: map<string,float>", [Row(x={"a": None, "b": 1.1, "c": None})]),
            ("x: map<string,map<string,int>>", [Row(x={"a": None, "b": None, "c": None})]),
            ("x: struct<a:int>", [Row(x=Row(a=0))]),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_struct_output_type_casting_dict(self):
        class TestUDTF:
            def eval(self):
                yield {"a": 0, "b": 1.1, "c": 2},

        for ret_type, expected in [
            ("x: boolean", [Row(x=None)]),
            ("x: tinyint", [Row(x=None)]),
            ("x: smallint", [Row(x=None)]),
            ("x: int", [Row(x=None)]),
            ("x: bigint", [Row(x=None)]),
            ("x: string", [Row(x="{a=0, b=1.1, c=2}")]),
            ("x: date", "AttributeError"),
            ("x: timestamp", "AttributeError"),
            ("x: byte", [Row(x=None)]),
            ("x: binary", [Row(x=None)]),
            ("x: float", [Row(x=None)]),
            ("x: double", [Row(x=None)]),
            ("x: decimal(10, 0)", [Row(x=None)]),
            ("x: array<string>", [Row(x=None)]),
            ("x: map<string,string>", [Row(x={"a": "0", "b": "1.1", "c": "2"})]),
            ("x: struct<a:string,b:string,c:string>", [Row(Row(a="0", b="1.1", c="2"))]),
            ("x: struct<a:int,b:int,c:int>", [Row(Row(a=0, b=None, c=2))]),
            ("x: struct<a:float,b:float,c:float>", [Row(Row(a=None, b=1.1, c=None))]),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_struct_output_type_casting_row(self):
        self.check_struct_output_type_casting_row(Py4JJavaError)

    def check_struct_output_type_casting_row(self, error_type):
        class TestUDTF:
            def eval(self):
                yield Row(a=0, b=1.1, c=2),

        err = ("PickleException", error_type)

        for ret_type, expected in [
            ("x: boolean", err),
            ("x: tinyint", err),
            ("x: smallint", err),
            ("x: int", err),
            ("x: bigint", err),
            ("x: string", err),
            ("x: date", "ValueError"),
            ("x: timestamp", "ValueError"),
            ("x: byte", err),
            ("x: binary", err),
            ("x: float", err),
            ("x: double", err),
            ("x: decimal(10, 0)", err),
            ("x: array<string>", err),
            ("x: map<string,string>", err),
            ("x: struct<a:string,b:string,c:string>", [Row(Row(a="0", b="1.1", c="2"))]),
            ("x: struct<a:int,b:int,c:int>", [Row(Row(a=0, b=None, c=2))]),
            ("x: struct<a:float,b:float,c:float>", [Row(Row(a=None, b=1.1, c=None))]),
        ]:
            with self.subTest(ret_type=ret_type):
                if isinstance(expected, tuple):
                    self._check_result_or_exception(
                        TestUDTF, ret_type, expected[0], err_type=expected[1]
                    )
                else:
                    self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_inconsistent_output_types(self):
        class TestUDTF:
            def eval(self):
                yield 1,
                yield [1, 2],

        for ret_type, expected in [
            ("x: int", [Row(x=1), Row(x=None)]),
            ("x: array<int>", [Row(x=None), Row(x=[1, 2])]),
        ]:
            with self.subTest(ret_type=ret_type):
                assertDataFrameEqual(udtf(TestUDTF, returnType=ret_type)(), expected)

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_udtf_with_pandas_input_type(self):
        import pandas as pd

        @udtf(returnType="corr: double")
        class TestUDTF:
            def eval(self, s1: pd.Series, s2: pd.Series):
                yield s1.corr(s2)

        self.spark.udtf.register("test_udtf", TestUDTF)
        # TODO(SPARK-43968): check during compile time instead of runtime
        with self.assertRaisesRegex(
            PythonException, "AttributeError: 'int' object has no attribute 'corr'"
        ):
            self.spark.sql(
                "select * from values (1, 2), (2, 3) t(a, b), lateral test_udtf(a, b)"
            ).collect()

    def test_udtf_register_error(self):
        @udf
        def upper(s: str):
            return s.upper()

        with self.assertRaises(PySparkTypeError) as e:
            self.spark.udtf.register("test_udf", upper)

        self.check_error(
            exception=e.exception,
            error_class="INVALID_UDTF_EVAL_TYPE",
            message_parameters={
                "name": "test_udf",
                "eval_type": "SQL_TABLE_UDF, SQL_ARROW_TABLE_UDF",
            },
        )

    def test_udtf_pickle_error(self):
        with tempfile.TemporaryDirectory() as d:
            file = os.path.join(d, "file.txt")
            file_obj = open(file, "w")

            @udtf(returnType="x: int")
            class TestUDTF:
                def eval(self):
                    file_obj
                    yield 1,

            with self.assertRaisesRegex(PySparkRuntimeError, "UDTF_SERIALIZATION_ERROR"):
                TestUDTF().collect()

    def test_udtf_access_spark_session(self):
        df = self.spark.range(10)

        @udtf(returnType="x: int")
        class TestUDTF:
            def eval(self):
                df.collect()
                yield 1,

        with self.assertRaisesRegex(PySparkRuntimeError, "UDTF_SERIALIZATION_ERROR"):
            TestUDTF().collect()

    def test_udtf_no_eval(self):
        with self.assertRaises(PySparkAttributeError) as e:

            @udtf(returnType="a: int, b: int")
            class TestUDTF:
                def run(self, a: int):
                    yield a, a + 1

        self.check_error(
            exception=e.exception,
            error_class="INVALID_UDTF_NO_EVAL",
            message_parameters={"name": "TestUDTF"},
        )

    def test_udtf_with_no_handler_class(self):
        with self.assertRaises(PySparkTypeError) as e:

            @udtf(returnType="a: int")
            def test_udtf(a: int):
                yield a,

        self.check_error(
            exception=e.exception,
            error_class="INVALID_UDTF_HANDLER_TYPE",
            message_parameters={"type": "function"},
        )

        with self.assertRaises(PySparkTypeError) as e:
            udtf(1, returnType="a: int")

        self.check_error(
            exception=e.exception,
            error_class="INVALID_UDTF_HANDLER_TYPE",
            message_parameters={"type": "int"},
        )

    def test_udtf_with_table_argument_query(self):
        class TestUDTF:
            def eval(self, row: Row):
                if row["id"] > 5:
                    yield row["id"],

        func = udtf(TestUDTF, returnType="a: int")
        self.spark.udtf.register("test_udtf", func)
        self.assertEqual(
            self.spark.sql("SELECT * FROM test_udtf(TABLE (SELECT id FROM range(0, 8)))").collect(),
            [Row(a=6), Row(a=7)],
        )

    def test_udtf_with_int_and_table_argument_query(self):
        class TestUDTF:
            def eval(self, i: int, row: Row):
                if row["id"] > i:
                    yield row["id"],

        func = udtf(TestUDTF, returnType="a: int")
        self.spark.udtf.register("test_udtf", func)
        self.assertEqual(
            self.spark.sql(
                "SELECT * FROM test_udtf(5, TABLE (SELECT id FROM range(0, 8)))"
            ).collect(),
            [Row(a=6), Row(a=7)],
        )

    def test_udtf_with_table_argument_identifier(self):
        class TestUDTF:
            def eval(self, row: Row):
                if row["id"] > 5:
                    yield row["id"],

        func = udtf(TestUDTF, returnType="a: int")
        self.spark.udtf.register("test_udtf", func)

        with self.tempView("v"):
            self.spark.sql("CREATE OR REPLACE TEMPORARY VIEW v as SELECT id FROM range(0, 8)")
            self.assertEqual(
                self.spark.sql("SELECT * FROM test_udtf(TABLE (v))").collect(),
                [Row(a=6), Row(a=7)],
            )

    def test_udtf_with_int_and_table_argument_identifier(self):
        class TestUDTF:
            def eval(self, i: int, row: Row):
                if row["id"] > i:
                    yield row["id"],

        func = udtf(TestUDTF, returnType="a: int")
        self.spark.udtf.register("test_udtf", func)

        with self.tempView("v"):
            self.spark.sql("CREATE OR REPLACE TEMPORARY VIEW v as SELECT id FROM range(0, 8)")
            self.assertEqual(
                self.spark.sql("SELECT * FROM test_udtf(5, TABLE (v))").collect(),
                [Row(a=6), Row(a=7)],
            )

    def test_udtf_with_table_argument_unknown_identifier(self):
        class TestUDTF:
            def eval(self, row: Row):
                if row["id"] > 5:
                    yield row["id"],

        func = udtf(TestUDTF, returnType="a: int")
        self.spark.udtf.register("test_udtf", func)

        with self.assertRaisesRegex(AnalysisException, "TABLE_OR_VIEW_NOT_FOUND"):
            self.spark.sql("SELECT * FROM test_udtf(TABLE (v))").collect()

    def test_udtf_with_table_argument_malformed_query(self):
        class TestUDTF:
            def eval(self, row: Row):
                if row["id"] > 5:
                    yield row["id"],

        func = udtf(TestUDTF, returnType="a: int")
        self.spark.udtf.register("test_udtf", func)

        with self.assertRaisesRegex(AnalysisException, "TABLE_OR_VIEW_NOT_FOUND"):
            self.spark.sql("SELECT * FROM test_udtf(TABLE (SELECT * FROM v))").collect()

    def test_udtf_with_table_argument_cte_inside(self):
        class TestUDTF:
            def eval(self, row: Row):
                if row["id"] > 5:
                    yield row["id"],

        func = udtf(TestUDTF, returnType="a: int")
        self.spark.udtf.register("test_udtf", func)
        self.assertEqual(
            self.spark.sql(
                """
                SELECT * FROM test_udtf(TABLE (
                  WITH t AS (
                    SELECT id FROM range(0, 8)
                  )
                  SELECT * FROM t
                ))
                """
            ).collect(),
            [Row(a=6), Row(a=7)],
        )

    def test_udtf_with_table_argument_cte_outside(self):
        class TestUDTF:
            def eval(self, row: Row):
                if row["id"] > 5:
                    yield row["id"],

        func = udtf(TestUDTF, returnType="a: int")
        self.spark.udtf.register("test_udtf", func)
        self.assertEqual(
            self.spark.sql(
                """
                WITH t AS (
                  SELECT id FROM range(0, 8)
                )
                SELECT * FROM test_udtf(TABLE (SELECT id FROM t))
                """
            ).collect(),
            [Row(a=6), Row(a=7)],
        )

        self.assertEqual(
            self.spark.sql(
                """
                WITH t AS (
                  SELECT id FROM range(0, 8)
                )
                SELECT * FROM test_udtf(TABLE (t))
                """
            ).collect(),
            [Row(a=6), Row(a=7)],
        )

    # TODO(SPARK-44233): Fix the subquery resolution.
    @unittest.skip("Fails to resolve the subquery.")
    def test_udtf_with_table_argument_lateral_join(self):
        class TestUDTF:
            def eval(self, row: Row):
                if row["id"] > 5:
                    yield row["id"],

        func = udtf(TestUDTF, returnType="a: int")
        self.spark.udtf.register("test_udtf", func)
        self.assertEqual(
            self.spark.sql(
                """
                SELECT * FROM
                  range(0, 8) AS t,
                  LATERAL test_udtf(TABLE (t))
                """
            ).collect(),
            [Row(a=6), Row(a=7)],
        )

    def test_udtf_with_table_argument_multiple(self):
        class TestUDTF:
            def eval(self, a: Row, b: Row):
                yield a[0], b[0]

        func = udtf(TestUDTF, returnType="a: int, b: int")
        self.spark.udtf.register("test_udtf", func)

        query = """
          SELECT * FROM test_udtf(
            TABLE (SELECT id FROM range(0, 2)),
            TABLE (SELECT id FROM range(0, 3)))
        """

        with self.sql_conf({"spark.sql.tvf.allowMultipleTableArguments.enabled": False}):
            with self.assertRaisesRegex(
                AnalysisException, "TABLE_VALUED_FUNCTION_TOO_MANY_TABLE_ARGUMENTS"
            ):
                self.spark.sql(query).collect()

        with self.sql_conf({"spark.sql.tvf.allowMultipleTableArguments.enabled": True}):
            self.assertEqual(
                self.spark.sql(query).collect(),
                [
                    Row(a=0, b=0),
                    Row(a=1, b=0),
                    Row(a=0, b=1),
                    Row(a=1, b=1),
                    Row(a=0, b=2),
                    Row(a=1, b=2),
                ],
            )

    def test_docstring(self):
        class TestUDTF:
            """A UDTF for test."""

            def __init__(self):
                """Initialize the UDTF"""
                ...

            def eval(self, x: int):
                """Evaluate the input row."""
                yield x + 1,

            def terminate(self):
                """Terminate the UDTF."""
                ...

        cls = udtf(TestUDTF, returnType="y: int").func
        self.assertIn("A UDTF for test", cls.__doc__)
        self.assertIn("Initialize the UDTF", cls.__init__.__doc__)
        self.assertIn("Evaluate the input row", cls.eval.__doc__)
        self.assertIn("Terminate the UDTF", cls.terminate.__doc__)


class UDTFTests(BaseUDTFTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super(UDTFTests, cls).setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDTF.arrow.enabled", "false")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDTF.arrow.enabled")
        finally:
            super(UDTFTests, cls).tearDownClass()


@unittest.skipIf(
    not have_pandas or not have_pyarrow, pandas_requirement_message or pyarrow_requirement_message
)
class UDTFArrowTestsMixin(BaseUDTFTestsMixin):
    def test_eval_type(self):
        def upper(x: str):
            return upper(x)

        class TestUDTF:
            def eval(self, x: str):
                return upper(x)

        self.assertEqual(
            udtf(TestUDTF, returnType="x: string", useArrow=False).evalType,
            PythonEvalType.SQL_TABLE_UDF,
        )

        self.assertEqual(
            udtf(TestUDTF, returnType="x: string", useArrow=True).evalType,
            PythonEvalType.SQL_ARROW_TABLE_UDF,
        )

    def test_udtf_arrow_sql_conf(self):
        class TestUDTF:
            def eval(self):
                yield 1,

        # We do not use `self.sql_conf` here to test the SQL SET command
        # instead of using PySpark's `spark.conf.set`.
        old_value = self.spark.conf.get("spark.sql.execution.pythonUDTF.arrow.enabled")
        self.spark.sql("SET spark.sql.execution.pythonUDTF.arrow.enabled=False")
        self.assertEqual(udtf(TestUDTF, returnType="x: int").evalType, PythonEvalType.SQL_TABLE_UDF)
        self.spark.sql("SET spark.sql.execution.pythonUDTF.arrow.enabled=True")
        self.assertEqual(
            udtf(TestUDTF, returnType="x: int").evalType, PythonEvalType.SQL_ARROW_TABLE_UDF
        )
        self.spark.conf.set("spark.sql.execution.pythonUDTF.arrow.enabled", old_value)

    def test_udtf_eval_returning_non_tuple(self):
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a

        # When arrow is enabled, it can handle non-tuple return value.
        assertDataFrameEqual(TestUDTF(lit(1)), [Row(a=1)])

        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a: int):
                return [a]

        assertDataFrameEqual(TestUDTF(lit(1)), [Row(a=1)])

    def test_numeric_output_type_casting(self):
        class TestUDTF:
            def eval(self):
                yield 1,

        err = "UDTF_ARROW_TYPE_CAST_ERROR"

        for ret_type, expected in [
            ("x: boolean", [Row(x=True)]),
            ("x: tinyint", [Row(x=1)]),
            ("x: smallint", [Row(x=1)]),
            ("x: int", [Row(x=1)]),
            ("x: bigint", [Row(x=1)]),
            ("x: string", [Row(x="1")]),  # require arrow.cast
            ("x: date", err),
            ("x: byte", [Row(x=1)]),
            ("x: binary", [Row(x=bytearray(b"\x01"))]),
            ("x: float", [Row(x=1.0)]),
            ("x: double", [Row(x=1.0)]),
            ("x: decimal(10, 0)", err),
            ("x: array<int>", err),
            ("x: map<string,int>", err),
            ("x: struct<a:int>", err),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_numeric_string_output_type_casting(self):
        class TestUDTF:
            def eval(self):
                yield "1",

        err = "UDTF_ARROW_TYPE_CAST_ERROR"

        for ret_type, expected in [
            ("x: boolean", [Row(x=True)]),
            ("x: tinyint", [Row(x=1)]),
            ("x: smallint", [Row(x=1)]),
            ("x: int", [Row(x=1)]),
            ("x: bigint", [Row(x=1)]),
            ("x: string", [Row(x="1")]),
            ("x: date", err),
            ("x: timestamp", err),
            ("x: byte", [Row(x=1)]),
            ("x: binary", [Row(x=bytearray(b"1"))]),
            ("x: float", [Row(x=1.0)]),
            ("x: double", [Row(x=1.0)]),
            ("x: decimal(10, 0)", [Row(x=1)]),
            ("x: array<string>", [Row(x=["1"])]),
            ("x: array<int>", [Row(x=[1])]),
            ("x: map<string,int>", err),
            ("x: struct<a:int>", err),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_string_output_type_casting(self):
        class TestUDTF:
            def eval(self):
                yield "hello",

        err = "UDTF_ARROW_TYPE_CAST_ERROR"

        for ret_type, expected in [
            ("x: boolean", err),
            ("x: tinyint", err),
            ("x: smallint", err),
            ("x: int", err),
            ("x: bigint", err),
            ("x: string", [Row(x="hello")]),
            ("x: date", err),
            ("x: timestamp", err),
            ("x: byte", err),
            ("x: binary", [Row(x=bytearray(b"hello"))]),
            ("x: float", err),
            ("x: double", err),
            ("x: decimal(10, 0)", err),
            ("x: array<string>", [Row(x=["h", "e", "l", "l", "o"])]),
            ("x: array<int>", err),
            ("x: map<string,int>", err),
            ("x: struct<a:int>", err),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_array_output_type_casting(self):
        class TestUDTF:
            def eval(self):
                yield [0, 1.1, 2],

        err = "UDTF_ARROW_TYPE_CAST_ERROR"

        for ret_type, expected in [
            ("x: boolean", err),
            ("x: tinyint", err),
            ("x: smallint", err),
            ("x: int", err),
            ("x: bigint", err),
            ("x: string", err),
            ("x: date", err),
            ("x: timestamp", err),
            ("x: byte", err),
            ("x: binary", err),
            ("x: float", err),
            ("x: double", err),
            ("x: decimal(10, 0)", err),
            ("x: array<string>", [Row(x=["0", "1.1", "2"])]),
            ("x: array<boolean>", [Row(x=[False, True, True])]),
            ("x: array<int>", [Row(x=[0, 1, 2])]),
            ("x: array<float>", [Row(x=[0, 1.1, 2])]),
            ("x: array<array<int>>", err),
            ("x: map<string,int>", err),
            ("x: struct<a:int>", err),
            ("x: struct<a:int,b:int,c:int>", err),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_map_output_type_casting(self):
        class TestUDTF:
            def eval(self):
                yield {"a": 0, "b": 1.1, "c": 2},

        err = "UDTF_ARROW_TYPE_CAST_ERROR"

        for ret_type, expected in [
            ("x: boolean", err),
            ("x: tinyint", err),
            ("x: smallint", err),
            ("x: int", err),
            ("x: bigint", err),
            ("x: string", err),
            ("x: date", err),
            ("x: timestamp", err),
            ("x: byte", err),
            ("x: binary", err),
            ("x: float", err),
            ("x: double", err),
            ("x: decimal(10, 0)", err),
            ("x: array<string>", [Row(x=["a", "b", "c"])]),
            ("x: map<string,string>", err),
            ("x: map<string,boolean>", err),
            ("x: map<string,int>", [Row(x={"a": 0, "b": 1, "c": 2})]),
            ("x: map<string,float>", [Row(x={"a": 0, "b": 1.1, "c": 2})]),
            ("x: map<string,map<string,int>>", err),
            ("x: struct<a:int>", [Row(x=Row(a=0))]),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_struct_output_type_casting_dict(self):
        class TestUDTF:
            def eval(self):
                yield {"a": 0, "b": 1.1, "c": 2},

        err = "UDTF_ARROW_TYPE_CAST_ERROR"

        for ret_type, expected in [
            ("x: boolean", err),
            ("x: tinyint", err),
            ("x: smallint", err),
            ("x: int", err),
            ("x: bigint", err),
            ("x: string", err),
            ("x: date", err),
            ("x: timestamp", err),
            ("x: byte", err),
            ("x: binary", err),
            ("x: float", err),
            ("x: double", err),
            ("x: decimal(10, 0)", err),
            ("x: array<string>", [Row(x=["a", "b", "c"])]),
            ("x: map<string,string>", err),
            ("x: struct<a:string,b:string,c:string>", [Row(Row(a="0", b="1.1", c="2"))]),
            ("x: struct<a:int,b:int,c:int>", [Row(Row(a=0, b=1, c=2))]),
            ("x: struct<a:float,b:float,c:float>", [Row(Row(a=0, b=1.1, c=2))]),
            ("x: struct<a:struct<>,b:struct<>,c:struct<>>", err),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_struct_output_type_casting_row(self):
        class TestUDTF:
            def eval(self):
                yield Row(a=0, b=1.1, c=2),

        err = "UDTF_ARROW_TYPE_CAST_ERROR"

        for ret_type, expected in [
            ("x: boolean", err),
            ("x: tinyint", err),
            ("x: smallint", err),
            ("x: int", err),
            ("x: bigint", err),
            ("x: string", err),
            ("x: date", err),
            ("x: timestamp", err),
            ("x: byte", err),
            ("x: binary", err),
            ("x: float", err),
            ("x: double", err),
            ("x: decimal(10, 0)", err),
            ("x: array<string>", [Row(x=["0", "1.1", "2"])]),
            ("x: map<string,string>", err),
            ("x: struct<a:string,b:string,c:string>", [Row(Row(a="0", b="1.1", c="2"))]),
            ("x: struct<a:int,b:int,c:int>", [Row(Row(a=0, b=1, c=2))]),
            ("x: struct<a:float,b:float,c:float>", [Row(Row(a=0, b=1.1, c=2))]),
            ("x: struct<a:struct<>,b:struct<>,c:struct<>>", err),
        ]:
            with self.subTest(ret_type=ret_type):
                self._check_result_or_exception(TestUDTF, ret_type, expected)

    def test_inconsistent_output_types(self):
        class TestUDTF:
            def eval(self):
                yield 1,
                yield [1, 2],

        for ret_type in [
            "x: int",
            "x: array<int>",
        ]:
            with self.subTest(ret_type=ret_type):
                with self.assertRaisesRegex(PythonException, "UDTF_ARROW_TYPE_CAST_ERROR"):
                    udtf(TestUDTF, returnType=ret_type)().collect()


class UDTFArrowTests(UDTFArrowTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super(UDTFArrowTests, cls).setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDTF.arrow.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDTF.arrow.enabled")
        finally:
            super(UDTFArrowTests, cls).tearDownClass()


if __name__ == "__main__":
    from pyspark.sql.tests.test_udtf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
