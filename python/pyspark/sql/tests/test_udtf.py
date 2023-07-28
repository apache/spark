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

from typing import Iterator

from pyspark.errors import (
    PySparkAttributeError,
    PythonException,
    PySparkTypeError,
    AnalysisException,
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

    def test_nondeterministic_udtf(self):
        import random

        class RandomUDTF:
            def eval(self, a: int):
                yield a + int(random.random()),

        random_udtf = udtf(RandomUDTF, returnType="x: int").asNondeterministic()
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
                assert isinstance(a.data_type, DataType)
                assert a.value is not None
                assert a.is_table is False
                return AnalyzeResult(StructType().add("a", a.data_type))

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
                return AnalyzeResult(StructType().add("a", a.data_type).add("b", b.data_type))

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

    def test_udtf_with_analyze_arbitary_number_arguments(self):
        class TestUDTF:
            @staticmethod
            def analyze(*args: AnalyzeArgument) -> AnalyzeResult:
                return AnalyzeResult(
                    StructType([StructField(f"col{i}", a.data_type) for i, a in enumerate(args)])
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
                assert isinstance(a.data_type, StructType)
                assert a.value is None
                assert a.is_table is True
                return AnalyzeResult(StructType().add("a", a.data_type[0].dataType))

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
                assert isinstance(a.data_type, StructType)
                assert a.is_table is True
                return AnalyzeResult(a.data_type.add("is_even", BooleanType()))

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

                if row.is_table is False:
                    raise Exception("The second argument must be a table argument")

                assert isinstance(row.data_type, StructType)
                return AnalyzeResult(row.data_type)

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
            "Output of `analyze` static method of Python UDTFs expects "
            "a pyspark.sql.udtf.AnalyzeResult but got: <class 'pyspark.sql.types.StringType'>",
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
                return AnalyzeResult(StructType().add("a", a.data_type))

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
                return AnalyzeResult(StructType().add("a", a.data_type).add("b", b.data_type))

            def eval(self, a):
                yield a, a + 1

        func = udtf(TestUDTF)

        with self.assertRaisesRegex(
            AnalysisException, r"analyze\(\) missing 1 required positional argument: 'b'"
        ):
            func(lit(1)).collect()

        with self.assertRaisesRegex(
            AnalysisException, r"analyze\(\) takes 2 positional arguments but 3 were given"
        ):
            func(lit(1), lit(2), lit(3)).collect()

        with self.assertRaisesRegex(
            PythonException, r"eval\(\) takes 2 positional arguments but 3 were given"
        ):
            func(lit(1), lit(2)).collect()

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

        with self.assertRaisesRegex(
            AnalysisException, r"analyze\(\) takes 0 positional arguments but 1 was given"
        ):
            TestUDTF(lit(1)).collect()

        with self.assertRaisesRegex(
            AnalysisException, r"analyze\(\) takes 0 positional arguments but 2 were given"
        ):
            self.spark.sql("SELECT * FROM test_udtf(1, 'x')").collect()

    def test_udtf_with_analyze_using_broadcast(self):
        colname = self.sc.broadcast("col1")

        @udtf
        class TestUDTF:
            @staticmethod
            def analyze(a: AnalyzeArgument) -> AnalyzeResult:
                return AnalyzeResult(StructType().add(colname.value, a.data_type))

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
                return AnalyzeResult(StructType().add("col1", a.data_type))

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
                    return AnalyzeResult(StructType().add(TestUDTF.call_my_func(), a.data_type))

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
                    return AnalyzeResult(StructType().add(TestUDTF.call_my_func(), a.data_type))

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
                    return AnalyzeResult(StructType().add(TestUDTF.read_my_archive(), a.data_type))

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
                    return AnalyzeResult(StructType().add(TestUDTF.read_my_file(), a.data_type))

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

    def test_udtf_eval_returning_non_tuple(self):
        class TestUDTF:
            def eval(self, a: int):
                yield a

        func = udtf(TestUDTF, returnType="a: int")
        # When arrow is enabled, it can handle non-tuple return value.
        self.assertEqual(func(lit(1)).collect(), [Row(a=1)])

    def test_udtf_eval_returning_non_generator(self):
        class TestUDTF:
            def eval(self, a: int):
                return (a,)

        func = udtf(TestUDTF, returnType="a: int")
        self.assertEqual(func(lit(1)).collect(), [Row(a=1)])


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
