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
import shutil
import tempfile
import unittest
from dataclasses import dataclass
from typing import Iterator, Optional

from py4j.protocol import Py4JJavaError

from pyspark.errors import (
    PySparkAttributeError,
    PythonException,
    PySparkTypeError,
    AnalysisException,
    PySparkPicklingError,
)
from pyspark.files import SparkFiles
from pyspark.rdd import PythonEvalType
from pyspark.sql.functions import (
    array,
    create_map,
    array,
    lit,
    named_struct,
    udf,
    udtf,
    AnalyzeArgument,
    AnalyzeResult,
    OrderingColumn,
    PartitioningColumn,
    SkipRestOfInputTableException,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    Row,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual
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

    # These are expected error message substrings to be used in test cases below.
    tooManyPositionalArguments = "too many positional arguments"
    missingARequiredArgument = "missing a required argument"
    multipleValuesForArgument = "multiple values for argument"

    def test_udtf_with_wrong_num_input(self):
        @udtf(returnType="a: int, b: int")
        class TestUDTF:
            def eval(self, a: int):
                yield a, a + 1

        with self.assertRaisesRegex(PythonException, BaseUDTFTestsMixin.missingARequiredArgument):
            TestUDTF().collect()

        with self.assertRaisesRegex(PythonException, BaseUDTFTestsMixin.tooManyPositionalArguments):
            TestUDTF(lit(1), lit(2)).collect()

    def test_udtf_init_with_additional_args(self):
        @udtf(returnType="x int")
        class TestUDTF:
            def __init__(self, a: int):
                ...

            def eval(self, a: int):
                yield a,

        with self.assertRaisesRegex(PythonException, r".*constructor has more than one argument.*"):
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

    def test_udtf_cleanup_with_exception_in_eval(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "file.txt")

            @udtf(returnType="x: int")
            class TestUDTF:
                def __init__(self):
                    self.path = path

                def eval(self, x: int):
                    raise Exception("eval error")

                def terminate(self):
                    with open(self.path, "a") as f:
                        f.write("terminate")

                def cleanup(self):
                    with open(self.path, "a") as f:
                        f.write("cleanup")

            with self.assertRaisesRegex(PythonException, "eval error"):
                TestUDTF(lit(1)).show()

            with open(path, "r") as f:
                data = f.read()

            # Only cleanup method should be called.
            self.assertEqual(data, "cleanup")

    def test_udtf_cleanup_with_exception_in_terminate(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "file.txt")

            @udtf(returnType="x: int")
            class TestUDTF:
                def __init__(self):
                    self.path = path

                def eval(self, x: int):
                    yield (x,)

                def terminate(self):
                    raise Exception("terminate error")

                def cleanup(self):
                    with open(self.path, "a") as f:
                        f.write("cleanup")

            with self.assertRaisesRegex(PythonException, "terminate error"):
                TestUDTF(lit(1)).show()

            with open(path, "r") as f:
                data = f.read()

            self.assertEqual(data, "cleanup")

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

            with self.assertRaisesRegex(PySparkPicklingError, "UDTF_SERIALIZATION_ERROR"):
                TestUDTF().collect()

    def test_udtf_access_spark_session(self):
        df = self.spark.range(10)

        @udtf(returnType="x: int")
        class TestUDTF:
            def eval(self):
                df.collect()
                yield 1,

        with self.assertRaisesRegex(PySparkPicklingError, "UDTF_SERIALIZATION_ERROR"):
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

            @staticmethod
            def analyze(x: AnalyzeArgument) -> AnalyzeResult:
                """Analyze the argument."""
                ...

            def eval(self, x: int):
                """Evaluate the input row."""
                yield x + 1,

            def terminate(self):
                """Terminate the UDTF."""
                ...

        cls = udtf(TestUDTF).func
        self.assertIn("A UDTF for test", cls.__doc__)
        self.assertIn("Initialize the UDTF", cls.__init__.__doc__)
        self.assertIn("Analyze the argument", cls.analyze.__doc__)
        self.assertIn("Evaluate the input row", cls.eval.__doc__)
        self.assertIn("Terminate the UDTF", cls.terminate.__doc__)

    def test_simple_udtf_with_analyze(self):
        class TestUDTF:
            @staticmethod
            def analyze() -> AnalyzeResult:
                return AnalyzeResult(StructType().add("c1", StringType()).add("c2", StringType()))

            def eval(self):
                yield "hello", "world"

        func = udtf(TestUDTF)
        self.spark.udtf.register("test_udtf", func)

        expected = [Row(c1="hello", c2="world")]
        assertDataFrameEqual(func(), expected)
        assertDataFrameEqual(self.spark.sql("SELECT * FROM test_udtf()"), expected)

    def test_udtf_with_analyze(self):
        class TestUDTF:
            @staticmethod
            def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                assert isinstance(a, AnalyzeArgument)
                assert isinstance(a.dataType, DataType)
                assert a.value is not None
                assert a.isTable is False
                return AnalyzeResult(StructType().add("a", a.dataType))

            def eval(self, a):
                yield a,

        func = udtf(TestUDTF)
        self.spark.udtf.register("test_udtf", func)

        for i, (df, expected_schema, expected_results) in enumerate(
            [
                (func(lit(1)), StructType().add("a", IntegerType()), [Row(a=1)]),
                # another data type
                (func(lit("x")), StructType().add("a", StringType()), [Row(a="x")]),
                # array type
                (
                    func(array(lit(1), lit(2), lit(3))),
                    StructType().add("a", ArrayType(IntegerType(), containsNull=False)),
                    [Row(a=[1, 2, 3])],
                ),
                # map type
                (
                    func(create_map(lit("x"), lit(1), lit("y"), lit(2))),
                    StructType().add(
                        "a", MapType(StringType(), IntegerType(), valueContainsNull=False)
                    ),
                    [Row(a={"x": 1, "y": 2})],
                ),
                # struct type
                (
                    func(named_struct(lit("x"), lit(1), lit("y"), lit(2))),
                    StructType().add(
                        "a",
                        StructType()
                        .add("x", IntegerType(), nullable=False)
                        .add("y", IntegerType(), nullable=False),
                    ),
                    [Row(a=Row(x=1, y=2))],
                ),
                # use SQL
                (
                    self.spark.sql("SELECT * from test_udtf(1)"),
                    StructType().add("a", IntegerType()),
                    [Row(a=1)],
                ),
            ]
        ):
            with self.subTest(query_no=i):
                assertSchemaEqual(df.schema, expected_schema)
                assertDataFrameEqual(df, expected_results)

    def test_udtf_with_analyze_decorator(self):
        @udtf
        class TestUDTF:
            @staticmethod
            def analyze() -> AnalyzeResult:
                return AnalyzeResult(StructType().add("c1", StringType()).add("c2", StringType()))

            def eval(self):
                yield "hello", "world"

        self.spark.udtf.register("test_udtf", TestUDTF)

        expected = [Row(c1="hello", c2="world")]
        assertDataFrameEqual(TestUDTF(), expected)
        assertDataFrameEqual(self.spark.sql("SELECT * FROM test_udtf()"), expected)

    def test_udtf_with_analyze_decorator_parens(self):
        @udtf()
        class TestUDTF:
            @staticmethod
            def analyze() -> AnalyzeResult:
                return AnalyzeResult(StructType().add("c1", StringType()).add("c2", StringType()))

            def eval(self):
                yield "hello", "world"

        self.spark.udtf.register("test_udtf", TestUDTF)

        expected = [Row(c1="hello", c2="world")]
        assertDataFrameEqual(TestUDTF(), expected)
        assertDataFrameEqual(self.spark.sql("SELECT * FROM test_udtf()"), expected)

    def test_udtf_with_analyze_multiple_arguments(self):
        class TestUDTF:
            @staticmethod
            def analyze(a: AnalyzeArgument, b: AnalyzeArgument) -> AnalyzeResult:
                return AnalyzeResult(StructType().add("a", a.dataType).add("b", b.dataType))

            def eval(self, a, b):
                yield a, b

        func = udtf(TestUDTF)
        self.spark.udtf.register("test_udtf", func)

        for i, (df, expected_schema, expected_results) in enumerate(
            [
                (
                    func(lit(1), lit("x")),
                    StructType().add("a", IntegerType()).add("b", StringType()),
                    [Row(a=1, b="x")],
                ),
                (
                    self.spark.sql("SELECT * FROM test_udtf(1, 'x')"),
                    StructType().add("a", IntegerType()).add("b", StringType()),
                    [Row(a=1, b="x")],
                ),
            ]
        ):
            with self.subTest(query_no=i):
                assertSchemaEqual(df.schema, expected_schema)
                assertDataFrameEqual(df, expected_results)

    def test_udtf_with_analyze_arbitrary_number_arguments(self):
        class TestUDTF:
            @staticmethod
            def analyze(*args: AnalyzeArgument) -> AnalyzeResult:
                return AnalyzeResult(
                    StructType([StructField(f"col{i}", a.dataType) for i, a in enumerate(args)])
                )

            def eval(self, *args):
                yield args

        func = udtf(TestUDTF)
        self.spark.udtf.register("test_udtf", func)

        for i, (df, expected_schema, expected_results) in enumerate(
            [
                (
                    func(lit(1)),
                    StructType().add("col0", IntegerType()),
                    [Row(a=1)],
                ),
                (
                    self.spark.sql("SELECT * FROM test_udtf(1, 'x')"),
                    StructType().add("col0", IntegerType()).add("col1", StringType()),
                    [Row(a=1, b="x")],
                ),
                (func(), StructType(), [Row()]),
            ]
        ):
            with self.subTest(query_no=i):
                assertSchemaEqual(df.schema, expected_schema)
                assertDataFrameEqual(df, expected_results)

    def test_udtf_with_analyze_table_argument(self):
        class TestUDTF:
            @staticmethod
            def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                assert isinstance(a, AnalyzeArgument)
                assert isinstance(a.dataType, StructType)
                assert a.value is None
                assert a.isTable is True
                return AnalyzeResult(StructType().add("a", a.dataType[0].dataType))

            def eval(self, a: Row):
                if a["id"] > 5:
                    yield a["id"],

        func = udtf(TestUDTF)
        self.spark.udtf.register("test_udtf", func)

        df = self.spark.sql("SELECT * FROM test_udtf(TABLE (SELECT id FROM range(0, 8)))")
        assertSchemaEqual(df.schema, StructType().add("a", LongType()))
        assertDataFrameEqual(df, [Row(a=6), Row(a=7)])

    def test_udtf_with_analyze_table_argument_adding_columns(self):
        class TestUDTF:
            @staticmethod
            def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                assert isinstance(a.dataType, StructType)
                assert a.isTable is True
                return AnalyzeResult(a.dataType.add("is_even", BooleanType()))

            def eval(self, a: Row):
                yield a["id"], a["id"] % 2 == 0

        func = udtf(TestUDTF)
        self.spark.udtf.register("test_udtf", func)

        df = self.spark.sql("SELECT * FROM test_udtf(TABLE (SELECT id FROM range(0, 4)))")
        assertSchemaEqual(
            df.schema,
            StructType().add("id", LongType(), nullable=False).add("is_even", BooleanType()),
        )
        assertDataFrameEqual(
            df,
            [
                Row(a=0, is_even=True),
                Row(a=1, is_even=False),
                Row(a=2, is_even=True),
                Row(a=3, is_even=False),
            ],
        )

    def test_udtf_with_analyze_table_argument_repeating_rows(self):
        class TestUDTF:
            @staticmethod
            def analyze(n, row) -> AnalyzeResult:
                if n.value is None or not isinstance(n.value, int) or (n.value < 1 or n.value > 10):
                    raise Exception("The first argument must be a scalar integer between 1 and 10")

                if row.isTable is False:
                    raise Exception("The second argument must be a table argument")

                assert isinstance(row.dataType, StructType)
                return AnalyzeResult(row.dataType)

            def eval(self, n: int, row: Row):
                for _ in range(n):
                    yield row

        func = udtf(TestUDTF)
        self.spark.udtf.register("test_udtf", func)

        expected_schema = StructType().add("id", LongType(), nullable=False)
        expected_results = [
            Row(a=0),
            Row(a=0),
            Row(a=1),
            Row(a=1),
            Row(a=2),
            Row(a=2),
            Row(a=3),
            Row(a=3),
        ]
        for i, df in enumerate(
            [
                self.spark.sql("SELECT * FROM test_udtf(2, TABLE (SELECT id FROM range(0, 4)))"),
                self.spark.sql(
                    "SELECT * FROM test_udtf(1 + 1, TABLE (SELECT id FROM range(0, 4)))"
                ),
            ]
        ):
            with self.subTest(query_no=i):
                assertSchemaEqual(df.schema, expected_schema)
                assertDataFrameEqual(df, expected_results)

        with self.assertRaisesRegex(
            AnalysisException, "The first argument must be a scalar integer between 1 and 10"
        ):
            self.spark.sql(
                "SELECT * FROM test_udtf(0, TABLE (SELECT id FROM range(0, 4)))"
            ).collect()

        with self.sql_conf(
            {"spark.sql.tvf.allowMultipleTableArguments.enabled": True}
        ), self.assertRaisesRegex(
            AnalysisException, "The first argument must be a scalar integer between 1 and 10"
        ):
            self.spark.sql(
                """
                SELECT * FROM test_udtf(
                  TABLE (SELECT id FROM range(0, 1)),
                  TABLE (SELECT id FROM range(0, 4)))
                """
            ).collect()

        with self.assertRaisesRegex(
            AnalysisException, "The second argument must be a table argument"
        ):
            self.spark.sql("SELECT * FROM test_udtf(1, 'x')").collect()

    def test_udtf_with_both_return_type_and_analyze(self):
        class TestUDTF:
            @staticmethod
            def analyze() -> AnalyzeResult:
                return AnalyzeResult(StructType().add("c1", StringType()).add("c2", StringType()))

            def eval(self):
                yield "hello", "world"

        with self.assertRaises(PySparkAttributeError) as e:
            udtf(TestUDTF, returnType="c1: string, c2: string")

        self.check_error(
            exception=e.exception,
            error_class="INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE",
            message_parameters={"name": "TestUDTF"},
        )

    def test_udtf_with_neither_return_type_nor_analyze(self):
        class TestUDTF:
            def eval(self):
                yield "hello", "world"

        with self.assertRaises(PySparkAttributeError) as e:
            udtf(TestUDTF)

        self.check_error(
            exception=e.exception,
            error_class="INVALID_UDTF_RETURN_TYPE",
            message_parameters={"name": "TestUDTF"},
        )

    def test_udtf_with_analyze_non_staticmethod(self):
        class TestUDTF:
            def analyze(self) -> AnalyzeResult:
                return AnalyzeResult(StructType().add("c1", StringType()).add("c2", StringType()))

            def eval(self):
                yield "hello", "world"

        with self.assertRaises(PySparkAttributeError) as e:
            udtf(TestUDTF)

        self.check_error(
            exception=e.exception,
            error_class="INVALID_UDTF_RETURN_TYPE",
            message_parameters={"name": "TestUDTF"},
        )

        with self.assertRaises(PySparkAttributeError) as e:
            udtf(TestUDTF, returnType="c1: string, c2: string")

        self.check_error(
            exception=e.exception,
            error_class="INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE",
            message_parameters={"name": "TestUDTF"},
        )

    def test_udtf_with_analyze_returning_non_struct(self):
        class TestUDTF:
            @staticmethod
            def analyze():
                return StringType()

            def eval(self):
                yield "hello", "world"

        func = udtf(TestUDTF)

        with self.assertRaisesRegex(
            AnalysisException,
            "'analyze' method expects a result of type pyspark.sql.udtf.AnalyzeResult, "
            "but instead this method returned a value of type: "
            "<class 'pyspark.sql.types.StringType'>",
        ):
            func().collect()

    def test_udtf_with_analyze_raising_an_exception(self):
        class TestUDTF:
            @staticmethod
            def analyze() -> AnalyzeResult:
                raise Exception("Failed to analyze.")

            def eval(self):
                yield "hello", "world"

        func = udtf(TestUDTF)

        with self.assertRaisesRegex(AnalysisException, "Failed to analyze."):
            func().collect()

    def test_udtf_with_analyze_null_literal(self):
        class TestUDTF:
            @staticmethod
            def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                return AnalyzeResult(StructType().add("a", a.dataType))

            def eval(self, a):
                yield a,

        func = udtf(TestUDTF)

        df = func(lit(None))
        assertSchemaEqual(df.schema, StructType().add("a", NullType()))
        assertDataFrameEqual(df, [Row(a=None)])

    def test_udtf_with_analyze_taking_wrong_number_of_arguments(self):
        class TestUDTF:
            @staticmethod
            def analyze(a: AnalyzeArgument, b: AnalyzeArgument) -> AnalyzeResult:
                return AnalyzeResult(StructType().add("a", a.dataType).add("b", b.dataType))

            def eval(self, a, b):
                yield a, a + 1

        func = udtf(TestUDTF)

        with self.assertRaisesRegex(AnalysisException, r"arguments"):
            func(lit(1)).collect()

        with self.assertRaisesRegex(AnalysisException, r"arguments"):
            func(lit(1), lit(2), lit(3)).collect()

    def test_udtf_with_analyze_taking_keyword_arguments(self):
        @udtf
        class TestUDTF:
            @staticmethod
            def analyze(**kwargs) -> AnalyzeResult:
                return AnalyzeResult(StructType().add("a", StringType()).add("b", StringType()))

            def eval(self, **kwargs):
                yield "hello", "world"

        self.spark.udtf.register("test_udtf", TestUDTF)

        expected = [Row(c1="hello", c2="world")]
        assertDataFrameEqual(TestUDTF(), expected)
        assertDataFrameEqual(self.spark.sql("SELECT * FROM test_udtf()"), expected)
        assertDataFrameEqual(self.spark.sql("SELECT * FROM test_udtf(a=>1)"), expected)

        with self.assertRaisesRegex(
            AnalysisException, BaseUDTFTestsMixin.tooManyPositionalArguments
        ):
            TestUDTF(lit(1)).collect()

        with self.assertRaisesRegex(
            AnalysisException, BaseUDTFTestsMixin.tooManyPositionalArguments
        ):
            self.spark.sql("SELECT * FROM test_udtf(1, 'x')").collect()

    def test_udtf_with_analyze_using_broadcast(self):
        colname = self.sc.broadcast("col1")

        @udtf
        class TestUDTF:
            @staticmethod
            def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                return AnalyzeResult(StructType().add(colname.value, a.dataType))

            def eval(self, a):
                assert colname.value == "col1"
                yield a,

            def terminate(self):
                assert colname.value == "col1"
                yield 100,

        self.spark.udtf.register("test_udtf", TestUDTF)

        for i, df in enumerate([TestUDTF(lit(10)), self.spark.sql("SELECT * FROM test_udtf(10)")]):
            with self.subTest(query_no=i):
                assertSchemaEqual(df.schema, StructType().add("col1", IntegerType()))
                assertDataFrameEqual(df, [Row(col1=10), Row(col1=100)])

    def test_udtf_with_analyze_using_accumulator(self):
        test_accum = self.sc.accumulator(0)

        @udtf
        class TestUDTF:
            @staticmethod
            def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                test_accum.add(1)
                return AnalyzeResult(StructType().add("col1", a.dataType))

            def eval(self, a):
                test_accum.add(10)
                yield a,

            def terminate(self):
                test_accum.add(100)
                yield 100,

        self.spark.udtf.register("test_udtf", TestUDTF)

        for i, df in enumerate([TestUDTF(lit(10)), self.spark.sql("SELECT * FROM test_udtf(10)")]):
            with self.subTest(query_no=i):
                assertSchemaEqual(df.schema, StructType().add("col1", IntegerType()))
                assertDataFrameEqual(df, [Row(col1=10), Row(col1=100)])

        self.assertEqual(test_accum.value, 222)

    def _add_pyfile(self, path):
        self.sc.addPyFile(path)

    def test_udtf_with_analyze_using_pyfile(self):
        with tempfile.TemporaryDirectory() as d:
            pyfile_path = os.path.join(d, "my_pyfile.py")
            with open(pyfile_path, "w") as f:
                f.write("my_func = lambda: 'col1'")

            self._add_pyfile(pyfile_path)

            class TestUDTF:
                @staticmethod
                def call_my_func() -> str:
                    import my_pyfile

                    return my_pyfile.my_func()

                @staticmethod
                def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                    return AnalyzeResult(StructType().add(TestUDTF.call_my_func(), a.dataType))

                def eval(self, a):
                    assert TestUDTF.call_my_func() == "col1"
                    yield a,

                def terminate(self):
                    assert TestUDTF.call_my_func() == "col1"
                    yield 100,

            test_udtf = udtf(TestUDTF)
            self.spark.udtf.register("test_udtf", test_udtf)

            for i, df in enumerate(
                [test_udtf(lit(10)), self.spark.sql("SELECT * FROM test_udtf(10)")]
            ):
                with self.subTest(query_no=i):
                    assertSchemaEqual(df.schema, StructType().add("col1", IntegerType()))
                    assertDataFrameEqual(df, [Row(col1=10), Row(col1=100)])

    def test_udtf_with_analyze_using_zipped_package(self):
        with tempfile.TemporaryDirectory() as d:
            package_path = os.path.join(d, "my_zipfile")
            os.mkdir(package_path)
            pyfile_path = os.path.join(package_path, "__init__.py")
            with open(pyfile_path, "w") as f:
                f.write("my_func = lambda: 'col1'")
            shutil.make_archive(package_path, "zip", d, "my_zipfile")

            self._add_pyfile(f"{package_path}.zip")

            class TestUDTF:
                @staticmethod
                def call_my_func() -> str:
                    import my_zipfile

                    return my_zipfile.my_func()

                @staticmethod
                def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                    return AnalyzeResult(StructType().add(TestUDTF.call_my_func(), a.dataType))

                def eval(self, a):
                    assert TestUDTF.call_my_func() == "col1"
                    yield a,

                def terminate(self):
                    assert TestUDTF.call_my_func() == "col1"
                    yield 100,

            test_udtf = udtf(TestUDTF)
            self.spark.udtf.register("test_udtf", test_udtf)

            for i, df in enumerate(
                [test_udtf(lit(10)), self.spark.sql("SELECT * FROM test_udtf(10)")]
            ):
                with self.subTest(query_no=i):
                    assertSchemaEqual(df.schema, StructType().add("col1", IntegerType()))
                    assertDataFrameEqual(df, [Row(col1=10), Row(col1=100)])

    def _add_archive(self, path):
        self.sc.addArchive(path)

    def test_udtf_with_analyze_using_archive(self):
        with tempfile.TemporaryDirectory() as d:
            archive_path = os.path.join(d, "my_archive")
            os.mkdir(archive_path)
            pyfile_path = os.path.join(archive_path, "my_file.txt")
            with open(pyfile_path, "w") as f:
                f.write("col1")
            shutil.make_archive(archive_path, "zip", d, "my_archive")

            self._add_archive(f"{archive_path}.zip#my_files")

            class TestUDTF:
                @staticmethod
                def read_my_archive() -> str:
                    with open(
                        os.path.join(
                            SparkFiles.getRootDirectory(), "my_files", "my_archive", "my_file.txt"
                        ),
                        "r",
                    ) as my_file:
                        return my_file.read().strip()

                @staticmethod
                def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                    return AnalyzeResult(StructType().add(TestUDTF.read_my_archive(), a.dataType))

                def eval(self, a):
                    assert TestUDTF.read_my_archive() == "col1"
                    yield a,

                def terminate(self):
                    assert TestUDTF.read_my_archive() == "col1"
                    yield 100,

            test_udtf = udtf(TestUDTF)
            self.spark.udtf.register("test_udtf", test_udtf)

            for i, df in enumerate(
                [test_udtf(lit(10)), self.spark.sql("SELECT * FROM test_udtf(10)")]
            ):
                with self.subTest(query_no=i):
                    assertSchemaEqual(df.schema, StructType().add("col1", IntegerType()))
                    assertDataFrameEqual(df, [Row(col1=10), Row(col1=100)])

    def _add_file(self, path):
        self.sc.addFile(path)

    def test_udtf_with_analyze_using_file(self):
        with tempfile.TemporaryDirectory() as d:
            file_path = os.path.join(d, "my_file.txt")
            with open(file_path, "w") as f:
                f.write("col1")

            self._add_file(file_path)

            class TestUDTF:
                @staticmethod
                def read_my_file() -> str:
                    with open(
                        os.path.join(SparkFiles.getRootDirectory(), "my_file.txt"), "r"
                    ) as my_file:
                        return my_file.read().strip()

                @staticmethod
                def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                    return AnalyzeResult(StructType().add(TestUDTF.read_my_file(), a.dataType))

                def eval(self, a):
                    assert TestUDTF.read_my_file() == "col1"
                    yield a,

                def terminate(self):
                    assert TestUDTF.read_my_file() == "col1"
                    yield 100,

            test_udtf = udtf(TestUDTF)
            self.spark.udtf.register("test_udtf", test_udtf)

            for i, df in enumerate(
                [test_udtf(lit(10)), self.spark.sql("SELECT * FROM test_udtf(10)")]
            ):
                with self.subTest(query_no=i):
                    assertSchemaEqual(df.schema, StructType().add("col1", IntegerType()))
                    assertDataFrameEqual(df, [Row(col1=10), Row(col1=100)])

    def test_udtf_with_named_arguments(self):
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a, b):
                yield a,

        self.spark.udtf.register("test_udtf", TestUDTF)

        for i, df in enumerate(
            [
                self.spark.sql("SELECT * FROM test_udtf(a => 10, b => 'x')"),
                self.spark.sql("SELECT * FROM test_udtf(b => 'x', a => 10)"),
                TestUDTF(a=lit(10), b=lit("x")),
                TestUDTF(b=lit("x"), a=lit(10)),
            ]
        ):
            with self.subTest(query_no=i):
                assertDataFrameEqual(df, [Row(a=10)])

    def test_udtf_with_named_arguments_negative(self):
        @udtf(returnType="a: int")
        class TestUDTF:
            def eval(self, a, b):
                yield a,

        self.spark.udtf.register("test_udtf", TestUDTF)

        with self.assertRaisesRegex(
            AnalysisException,
            "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
        ):
            self.spark.sql("SELECT * FROM test_udtf(a => 10, a => 100)").show()

        with self.assertRaisesRegex(AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"):
            self.spark.sql("SELECT * FROM test_udtf(a => 10, 'x')").show()

        with self.assertRaisesRegex(PythonException, BaseUDTFTestsMixin.missingARequiredArgument):
            self.spark.sql("SELECT * FROM test_udtf(c => 'x')").show()

        with self.assertRaisesRegex(PythonException, BaseUDTFTestsMixin.multipleValuesForArgument):
            self.spark.sql("SELECT * FROM test_udtf(10, a => 100)").show()

    def test_udtf_with_kwargs(self):
        @udtf(returnType="a: int, b: string")
        class TestUDTF:
            def eval(self, **kwargs):
                yield kwargs["a"], kwargs["b"]

        self.spark.udtf.register("test_udtf", TestUDTF)

        for i, df in enumerate(
            [
                self.spark.sql("SELECT * FROM test_udtf(a => 10, b => 'x')"),
                self.spark.sql("SELECT * FROM test_udtf(b => 'x', a => 10)"),
                TestUDTF(a=lit(10), b=lit("x")),
                TestUDTF(b=lit("x"), a=lit(10)),
            ]
        ):
            with self.subTest(query_no=i):
                assertDataFrameEqual(df, [Row(a=10, b="x")])

        # negative
        with self.assertRaisesRegex(
            AnalysisException,
            "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
        ):
            self.spark.sql("SELECT * FROM test_udtf(a => 10, a => 100)").show()

        with self.assertRaisesRegex(AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"):
            self.spark.sql("SELECT * FROM test_udtf(a => 10, 'x')").show()

    def test_udtf_with_analyze_kwargs(self):
        @udtf
        class TestUDTF:
            @staticmethod
            def analyze(**kwargs: AnalyzeArgument) -> AnalyzeResult:
                assert isinstance(kwargs["a"].dataType, IntegerType)
                assert kwargs["a"].value == 10
                assert not kwargs["a"].isTable
                assert isinstance(kwargs["b"].dataType, StringType)
                assert kwargs["b"].value == "x"
                assert not kwargs["b"].isTable
                return AnalyzeResult(
                    StructType(
                        [StructField(key, arg.dataType) for key, arg in sorted(kwargs.items())]
                    )
                )

            def eval(self, **kwargs):
                yield tuple(value for _, value in sorted(kwargs.items()))

        self.spark.udtf.register("test_udtf", TestUDTF)

        for i, df in enumerate(
            [
                self.spark.sql("SELECT * FROM test_udtf(a => 10, b => 'x')"),
                self.spark.sql("SELECT * FROM test_udtf(b => 'x', a => 10)"),
                TestUDTF(a=lit(10), b=lit("x")),
                TestUDTF(b=lit("x"), a=lit(10)),
            ]
        ):
            with self.subTest(query_no=i):
                assertDataFrameEqual(df, [Row(a=10, b="x")])

    def test_udtf_with_named_arguments_lateral_join(self):
        @udtf
        class TestUDTF:
            @staticmethod
            def analyze(a, b):
                return AnalyzeResult(StructType().add("a", a.dataType))

            def eval(self, a, b):
                yield a,

        self.spark.udtf.register("test_udtf", TestUDTF)

        # lateral join
        for i, df in enumerate(
            [
                self.spark.sql(
                    "SELECT f.* FROM "
                    "VALUES (0, 'x'), (1, 'y') t(a, b), LATERAL test_udtf(a => a, b => b) f"
                ),
                self.spark.sql(
                    "SELECT f.* FROM "
                    "VALUES (0, 'x'), (1, 'y') t(a, b), LATERAL test_udtf(b => b, a => a) f"
                ),
            ]
        ):
            with self.subTest(query_no=i):
                assertDataFrameEqual(df, [Row(a=0), Row(a=1)])

    def test_udtf_with_named_arguments_and_defaults(self):
        @udtf
        class TestUDTF:
            @staticmethod
            def analyze(a: AnalyzeArgument, b: Optional[AnalyzeArgument] = None):
                assert isinstance(a.dataType, IntegerType)
                assert a.value == 10
                assert not a.isTable
                if b is not None:
                    assert isinstance(b.dataType, StringType)
                    assert b.value == "z"
                    assert not b.isTable
                schema = StructType().add("a", a.dataType)
                if b is None:
                    return AnalyzeResult(schema.add("b", IntegerType()))
                else:
                    return AnalyzeResult(schema.add("b", b.dataType))

            def eval(self, a, b=100):
                yield a, b

        self.spark.udtf.register("test_udtf", TestUDTF)

        # without "b"
        for i, df in enumerate(
            [
                self.spark.sql("SELECT * FROM test_udtf(10)"),
                self.spark.sql("SELECT * FROM test_udtf(a => 10)"),
                TestUDTF(lit(10)),
                TestUDTF(a=lit(10)),
            ]
        ):
            with self.subTest(with_b=False, query_no=i):
                assertDataFrameEqual(df, [Row(a=10, b=100)])

        # with "b"
        for i, df in enumerate(
            [
                self.spark.sql("SELECT * FROM test_udtf(10, b => 'z')"),
                self.spark.sql("SELECT * FROM test_udtf(a => 10, b => 'z')"),
                self.spark.sql("SELECT * FROM test_udtf(b => 'z', a => 10)"),
                TestUDTF(lit(10), b=lit("z")),
                TestUDTF(a=lit(10), b=lit("z")),
                TestUDTF(b=lit("z"), a=lit(10)),
            ]
        ):
            with self.subTest(with_b=True, query_no=i):
                assertDataFrameEqual(df, [Row(a=10, b="z")])

    def test_udtf_with_table_argument_and_partition_by(self):
        class TestUDTF:
            def __init__(self):
                self._sum = 0
                self._partition_col = None

            def eval(self, row: Row):
                # Make sure that the PARTITION BY expressions were projected out.
                assert len(row.asDict().items()) == 2
                assert "partition_col" in row
                assert "input" in row
                self._sum += row["input"]
                if self._partition_col is not None and self._partition_col != row["partition_col"]:
                    # Make sure that all values of the partitioning column are the same
                    # for each row consumed by this method for this instance of the class.
                    raise Exception(
                        f"self._partition_col was {self._partition_col} but the row "
                        + f"value was {row['partition_col']}"
                    )
                self._partition_col = row["partition_col"]

            def terminate(self):
                yield self._partition_col, self._sum

        # This is a basic example.
        func = udtf(TestUDTF, returnType="partition_col: int, total: int")
        self.spark.udtf.register("test_udtf", func)

        base_query = """
            WITH t AS (
              SELECT id AS partition_col, 1 AS input FROM range(1, 21)
              UNION ALL
              SELECT id AS partition_col, 2 AS input FROM range(1, 21)
            )
            SELECT partition_col, total
            FROM test_udtf({table_arg})
            ORDER BY 1, 2
        """

        for table_arg in [
            "TABLE(t) PARTITION BY partition_col - 1",
            "row => TABLE(t) PARTITION BY partition_col - 1",
        ]:
            with self.subTest(table_arg=table_arg):
                assertDataFrameEqual(
                    self.spark.sql(base_query.format(table_arg=table_arg)),
                    [Row(partition_col=x, total=3) for x in range(1, 21)],
                )

        base_query = """
            WITH t AS (
              SELECT {str_first} AS partition_col, id AS input FROM range(0, 2)
              UNION ALL
              SELECT {str_second} AS partition_col, id AS input FROM range(0, 2)
            )
            SELECT partition_col, total
            FROM test_udtf({table_arg})
            ORDER BY 1, 2
        """

        # These cases partition by constant values.
        for str_first, str_second, result_first, result_second in (
            ("123", "456", 123, 456),
            ("123", "NULL", None, 123),
        ):
            for table_arg in [
                "TABLE(t) PARTITION BY partition_col",
                "row => TABLE(t) PARTITION BY partition_col",
            ]:
                with self.subTest(str_first=str_first, str_second=str_second, table_arg=table_arg):
                    assertDataFrameEqual(
                        self.spark.sql(
                            base_query.format(
                                str_first=str_first, str_second=str_second, table_arg=table_arg
                            )
                        ),
                        [
                            Row(partition_col=result_first, total=1),
                            Row(partition_col=result_second, total=1),
                        ],
                    )

        # Combine a lateral join with a TABLE argument with PARTITION BY .
        func = udtf(TestUDTF, returnType="partition_col: int, total: int")
        self.spark.udtf.register("test_udtf", func)

        base_query = """
            WITH t AS (
              SELECT id AS partition_col, 1 AS input FROM range(1, 3)
              UNION ALL
              SELECT id AS partition_col, 2 AS input FROM range(1, 3)
            )
            SELECT v.a, v.b, f.partition_col, f.total
            FROM VALUES (0, 1) AS v(a, b),
            LATERAL test_udtf({table_arg}) f
            ORDER BY 1, 2, 3, 4
        """

        for table_arg in [
            "TABLE(t) PARTITION BY partition_col - 1",
            "row => TABLE(t) PARTITION BY partition_col - 1",
        ]:
            with self.subTest(func_call=table_arg):
                assertDataFrameEqual(
                    self.spark.sql(base_query.format(table_arg=table_arg)),
                    [
                        Row(a=0, b=1, partition_col=1, total=3),
                        Row(a=0, b=1, partition_col=2, total=3),
                    ],
                )

    def test_udtf_with_table_argument_and_partition_by_and_order_by(self):
        class TestUDTF:
            def __init__(self):
                self._last = None
                self._partition_col = None

            def eval(self, row: Row, partition_col: str):
                # Make sure that the PARTITION BY and ORDER BY expressions were projected out.
                assert len(row.asDict().items()) == 2
                assert "partition_col" in row
                assert "input" in row
                # Make sure that all values of the partitioning column are the same
                # for each row consumed by this method for this instance of the class.
                if self._partition_col is not None and self._partition_col != row[partition_col]:
                    raise Exception(
                        f"self._partition_col was {self._partition_col} but the row "
                        + f"value was {row[partition_col]}"
                    )
                self._last = row["input"]
                self._partition_col = row[partition_col]

            def terminate(self):
                yield self._partition_col, self._last

        func = udtf(TestUDTF, returnType="partition_col: int, last: int")
        self.spark.udtf.register("test_udtf", func)

        base_query = """
            WITH t AS (
              SELECT id AS partition_col, 1 AS input FROM range(1, 21)
              UNION ALL
              SELECT id AS partition_col, 2 AS input FROM range(1, 21)
            )
            SELECT partition_col, last
            FROM test_udtf(
              {table_arg},
              partition_col => 'partition_col')
            ORDER BY 1, 2
        """

        for order_by_str, result_val in (
            ("input ASC", 2),
            ("input + 1 ASC", 2),
            ("input DESC", 1),
            ("input - 1 DESC", 1),
        ):
            for table_arg in [
                f"TABLE(t) PARTITION BY partition_col - 1 ORDER BY {order_by_str}",
                f"row => TABLE(t) PARTITION BY partition_col - 1 ORDER BY {order_by_str}",
            ]:
                with self.subTest(table_arg=table_arg):
                    assertDataFrameEqual(
                        self.spark.sql(base_query.format(table_arg=table_arg)),
                        [Row(partition_col=x, last=result_val) for x in range(1, 21)],
                    )

    def test_udtf_with_table_argument_with_single_partition(self):
        class TestUDTF:
            def __init__(self):
                self._count = 0
                self._sum = 0
                self._last = None

            def eval(self, row: Row):
                # Make sure that the rows arrive in the expected order.
                if self._last is not None and self._last > row["input"]:
                    raise Exception(
                        f"self._last was {self._last} but the row value was {row['input']}"
                    )
                self._count += 1
                self._last = row["input"]
                self._sum += row["input"]

            def terminate(self):
                yield self._count, self._sum, self._last

        func = udtf(TestUDTF, returnType="count: int, total: int, last: int")
        self.spark.udtf.register("test_udtf", func)

        base_query = """
            WITH t AS (
              SELECT id AS partition_col, 1 AS input FROM range(1, 21)
              UNION ALL
              SELECT id AS partition_col, 2 AS input FROM range(1, 21)
            )
            SELECT count, total, last
            FROM test_udtf({table_arg})
            ORDER BY 1, 2
        """

        for table_arg in [
            "TABLE(t) WITH SINGLE PARTITION ORDER BY (input, partition_col)",
            "row => TABLE(t) WITH SINGLE PARTITION ORDER BY (input, partition_col)",
        ]:
            with self.subTest(table_arg=table_arg):
                assertDataFrameEqual(
                    self.spark.sql(base_query.format(table_arg=table_arg)),
                    [Row(count=40, total=60, last=2)],
                )

    def test_udtf_with_table_argument_with_single_partition_from_analyze(self):
        @udtf
        class TestUDTF:
            def __init__(self):
                self._count = 0
                self._sum = 0
                self._last = None

            @staticmethod
            def analyze(*args, **kwargs):
                return AnalyzeResult(
                    schema=StructType()
                    .add("count", IntegerType())
                    .add("total", IntegerType())
                    .add("last", IntegerType()),
                    withSinglePartition=True,
                    orderBy=[OrderingColumn("input"), OrderingColumn("partition_col")],
                )

            def eval(self, row: Row):
                # Make sure that the rows arrive in the expected order.
                if self._last is not None and self._last > row["input"]:
                    raise Exception(
                        f"self._last was {self._last} but the row value was {row['input']}"
                    )
                self._count += 1
                self._last = row["input"]
                self._sum += row["input"]

            def terminate(self):
                yield self._count, self._sum, self._last

        self.spark.udtf.register("test_udtf", TestUDTF)

        base_query = """
            WITH t AS (
              SELECT id AS partition_col, 1 AS input FROM range(1, 21)
              UNION ALL
              SELECT id AS partition_col, 2 AS input FROM range(1, 21)
            )
            SELECT count, total, last
            FROM test_udtf({table_arg})
            ORDER BY 1, 2
        """

        for table_arg in ["TABLE(t)", "row => TABLE(t)"]:
            with self.subTest(table_arg):
                assertDataFrameEqual(
                    self.spark.sql(base_query.format(table_arg=table_arg)),
                    [Row(count=40, total=60, last=2)],
                )

    def test_udtf_with_table_argument_with_partition_by_and_order_by_from_analyze(self):
        @udtf
        class TestUDTF:
            def __init__(self):
                self._partition_col = None
                self._count = 0
                self._sum = 0
                self._last = None

            @staticmethod
            def analyze(*args, **kwargs):
                return AnalyzeResult(
                    schema=StructType()
                    .add("partition_col", IntegerType())
                    .add("count", IntegerType())
                    .add("total", IntegerType())
                    .add("last", IntegerType()),
                    partitionBy=[PartitioningColumn("partition_col")],
                    orderBy=[
                        OrderingColumn(name="input", ascending=True, overrideNullsFirst=False)
                    ],
                )

            def eval(self, row: Row):
                # Make sure that the PARTITION BY and ORDER BY expressions were projected out.
                assert len(row.asDict().items()) == 2
                assert "partition_col" in row
                assert "input" in row
                # Make sure that all values of the partitioning column are the same
                # for each row consumed by this method for this instance of the class.
                if self._partition_col is not None and self._partition_col != row["partition_col"]:
                    raise Exception(
                        f"self._partition_col was {self._partition_col} but the row "
                        + f"value was {row['partition_col']}"
                    )
                # Make sure that the rows arrive in the expected order.
                if (
                    self._last is not None
                    and row["input"] is not None
                    and self._last > row["input"]
                ):
                    raise Exception(
                        f"self._last was {self._last} but the row value was {row['input']}"
                    )
                self._partition_col = row["partition_col"]
                self._count += 1
                self._last = row["input"]
                if row["input"] is not None:
                    self._sum += row["input"]

            def terminate(self):
                yield self._partition_col, self._count, self._sum, self._last

        self.spark.udtf.register("test_udtf", TestUDTF)

        base_query = """
            WITH t AS (
              SELECT id AS partition_col, 1 AS input FROM range(1, 21)
              UNION ALL
              SELECT id AS partition_col, 2 AS input FROM range(1, 21)
              UNION ALL
              SELECT 42 AS partition_col, NULL AS input
              UNION ALL
              SELECT 42 AS partition_col, 1 AS input
              UNION ALL
              SELECT 42 AS partition_col, 2 AS input
            )
            SELECT partition_col, count, total, last
            FROM test_udtf({table_arg})
            ORDER BY 1, 2
        """

        for table_arg in ["TABLE(t)", "row => TABLE(t)"]:
            with self.subTest(table_arg=table_arg):
                assertDataFrameEqual(
                    self.spark.sql(base_query.format(table_arg=table_arg)),
                    [Row(partition_col=x, count=2, total=3, last=2) for x in range(1, 21)]
                    + [Row(partition_col=42, count=3, total=3, last=None)],
                )

    def test_udtf_with_prepare_string_from_analyze(self):
        @dataclass
        class AnalyzeResultWithBuffer(AnalyzeResult):
            buffer: str = ""

        @udtf
        class TestUDTF:
            def __init__(self, analyze_result=None):
                self._total = 0
                if analyze_result is not None:
                    self._buffer = analyze_result.buffer
                else:
                    self._buffer = ""

            @staticmethod
            def analyze(argument, _):
                if (
                    argument.value is None
                    or argument.isTable
                    or not isinstance(argument.value, str)
                    or len(argument.value) == 0
                ):
                    raise Exception("The first argument must be non-empty string")
                assert argument.dataType == StringType()
                assert not argument.isTable
                return AnalyzeResultWithBuffer(
                    schema=StructType().add("total", IntegerType()).add("buffer", StringType()),
                    withSinglePartition=True,
                    buffer=argument.value,
                )

            def eval(self, argument, row: Row):
                self._total += 1

            def terminate(self):
                yield self._total, self._buffer

        self.spark.udtf.register("test_udtf", TestUDTF)

        assertDataFrameEqual(
            self.spark.sql(
                """
                WITH t AS (
                  SELECT id FROM range(1, 21)
                )
                SELECT total, buffer
                FROM test_udtf("abc", TABLE(t))
                """
            ),
            [Row(count=20, buffer="abc")],
        )

    def test_udtf_with_skip_rest_of_input_table_exception(self):
        @udtf(returnType="current: int, total: int")
        class TestUDTF:
            def __init__(self):
                self._current = 0
                self._total = 0

            def eval(self, input: Row):
                self._current = input["id"]
                self._total += 1
                if self._total >= 4:
                    raise SkipRestOfInputTableException("Stop at self._total >= 4")

            def terminate(self):
                yield self._current, self._total

        self.spark.udtf.register("test_udtf", TestUDTF)

        # Run a test case including WITH SINGLE PARTITION on the UDTF call. The
        # SkipRestOfInputTableException stops scanning rows after the fourth input row is consumed.
        assertDataFrameEqual(
            self.spark.sql(
                """
                WITH t AS (
                  SELECT id FROM range(1, 21)
                )
                SELECT current, total
                FROM test_udtf(TABLE(t) WITH SINGLE PARTITION ORDER BY id)
                """
            ),
            [Row(current=4, total=4)],
        )

        # Run a test case including WITH SINGLE PARTITION on the UDTF call. The
        # SkipRestOfInputTableException stops scanning rows for each of the two partitions
        # separately.
        assertDataFrameEqual(
            self.spark.sql(
                """
                WITH t AS (
                  SELECT id FROM range(1, 21)
                )
                SELECT current, total
                FROM test_udtf(TABLE(t) PARTITION BY floor(id / 10) ORDER BY id)
                ORDER BY ALL
                """
            ),
            [Row(current=4, total=4), Row(current=13, total=4), Row(current=20, total=1)],
        )


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
