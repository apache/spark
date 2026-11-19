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
"""
Tests for the ``java`` UDF transpilation target (``pyspark.sql.transpile_java``).

The lowering tests assert on generated Java without a JVM; the end-to-end tests run queries
and compare them with the Python function itself.
"""

import ast
import itertools
import textwrap
import unittest

from pyspark.errors import UnsupportedOperationException
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType
from pyspark.sql.udf import UserDefinedFunction
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.util import is_remote_only

_ON = {
    "spark.sql.experimental.optimizer.transpilePyUDFs": True,
    "spark.sql.ansi.enabled": True,
}
_JAVA_ONLY = dict(_ON, **{"spark.sql.experimental.optimizer.pyTranspilers": "java"})
_CATALYST_THEN_JAVA = dict(
    _ON, **{"spark.sql.experimental.optimizer.pyTranspilers": "catalyst,java"}
)


def _lower(source: str, categories=None, result_category="integer") -> str:
    from pyspark.sql.transpile_java import JavaTranspiler

    function_ast = ast.parse(textwrap.dedent(source)).body[0]
    assert isinstance(function_ast, ast.FunctionDef)
    params = [arg.arg for arg in function_ast.args.args]
    categories = categories or {i: "integer" for i in range(len(params))}
    return JavaTranspiler()._method_body(function_ast, params, categories, result_category)


# Module level so that `inspect.getsource` can read them.


def floor_ops(a, b):
    if b == 0:
        return None
    q = a // b
    r = a % b
    if r == 0:
        return q
    return q * 10 + r


def magnitude(a):
    if a is None:
        return None
    return -a if a < 0 else a


def null_aware_eq(a, b):
    same = a == b
    if a is None:
        return not same
    return same


class JavaTranspileLoweringTests(unittest.TestCase):
    def test_locals_and_early_returns(self):
        java = _lower(
            """
            def clamp(x):
                doubled = x * 2
                if doubled > 10:
                    return 10
                return doubled + 1
            """
        )
        self.assertIn("Long _udf_local_0 = ", java)
        self.assertIn("TranspiledJavaUDFHelpers.multiply(_udf_arg_0, Long.valueOf(2L))", java)
        # Every path returns, so no trailing `return null;`, which Java would reject.
        self.assertNotIn("return null;", java)

    def test_falling_off_the_end_returns_null(self):
        java = _lower(
            """
            def f(x):
                if x is not None:
                    return x
            """
        )
        self.assertTrue(java.endswith("return null;"))

    def test_python_shapes_that_decline(self):
        cases = {
            "and": "def f(x):\n    return x > 0 and x < 9",
            "int truthiness": "def f(x):\n    if x:\n        return 1\n    return 0",
            "float literal": "def f(x):\n    return x + 1.5",
            "true division": "def f(x):\n    return x / 2",
            "string": "def f(x):\n    return 'a'",
            "loop": "def f(x):\n    for i in x:\n        return i",
            "chained comparison": "def f(x):\n    return 0 < x < 9",
            "bool arithmetic": "def f(x):\n    return True + x",
            "parameter assignment": "def f(x):\n    x = 1\n    return x",
            "None local": "def f(x):\n    y = None\n    return x",
            "local changing type": "def f(x):\n    y = 1\n    y = x > 0\n    return 1",
            "local out of scope": (
                "def f(x):\n    if x > 0:\n        y = 1\n    else:\n        y = 2\n    return y"
            ),
            "free variable": "def f(x):\n    return x + OFFSET",
        }
        for name, source in cases.items():
            with self.subTest(name):
                with self.assertRaises(UnsupportedOperationException):
                    _lower(source)

    def test_unsupported_parameter_category_declines(self):
        from pyspark.sql.transpile_java import JavaTranspiler

        tree = ast.parse("def f(x):\n    return x").body[0]
        with self.assertRaises(UnsupportedOperationException):
            JavaTranspiler()._transpile_from_ast("src", tree, tree, ["x"], LongType(), {0: "float"})


@unittest.skipIf(is_remote_only(), "UDF transpilation is only supported in classic Spark.")
class JavaTranspileEndToEndTests(ReusedSQLTestCase):
    def _plan(self, df) -> str:
        return df._jdf.queryExecution().optimizedPlan().toString()

    def test_off_by_default_and_catalyst_first(self):
        def plus_one(x):
            return x + 1

        def clamp(x):
            doubled = x * 2
            if doubled > 10:
                return 10
            return doubled + 1

        df = self.spark.range(3)
        with self.sql_conf(_ON):
            self.assertEqual([], UserDefinedFunction(clamp, LongType()).transpiled)
        with self.sql_conf(_CATALYST_THEN_JAVA):
            # Catalyst can lower `plus_one`, so it keeps the option the optimizer can see into.
            plan = self._plan(df.select(UserDefinedFunction(plus_one, LongType())("id")))
            self.assertNotIn("TranspiledJavaUDF", plan)
            result = df.select(UserDefinedFunction(clamp, LongType())("id"))
            self.assertIn("TranspiledJavaUDF", self._plan(result))
            self.assertNotIn("BatchEvalPython", self._plan(result))
            self.assertEqual([1, 3, 5], [r[0] for r in result.collect()])

    def test_matches_python_on_both_eval_paths(self):
        values = [None, -7, -3, -1, 0, 1, 3, 7]
        cases = [
            (floor_ops, LongType(), list(itertools.product(values, values))),
            (null_aware_eq, BooleanType(), list(itertools.product(values, values))),
            (magnitude, LongType(), [(v,) for v in values]),
        ]
        for func, return_type, rows in cases:
            expected_rows = []
            for row in rows:
                try:
                    expected_rows.append((row, func(*row)))
                except TypeError:
                    pass  # Raising inputs are covered by test_python_errors_raise.
            inputs = [row for row, _ in expected_rows]
            schema = ", ".join(f"c{i} bigint" for i in range(len(inputs[0])))
            df = self.spark.createDataFrame(inputs, schema)
            for factory_mode, whole_stage in (("CODEGEN_ONLY", True), ("NO_CODEGEN", False)):
                conf = dict(
                    _JAVA_ONLY,
                    **{
                        "spark.sql.codegen.factoryMode": factory_mode,
                        "spark.sql.codegen.wholeStage": whole_stage,
                    },
                )
                with self.subTest(func=func.__name__, mode=factory_mode), self.sql_conf(conf):
                    udf = UserDefinedFunction(func, return_type)
                    result = df.select(*df.columns, udf(*df.columns))
                    self.assertIn("TranspiledJavaUDF", self._plan(result))
                    actual = {tuple(r[:-1]): r[-1] for r in result.collect()}
                    for row, expected in expected_rows:
                        self.assertEqual(actual[row], expected, f"{func.__name__}{row}")

    def test_python_errors_raise(self):
        def floor_div(a, b):
            return a // b

        def increment(a):
            return a + 1

        def positive(a):
            return a > 0

        cases = [
            (floor_div, LongType(), "c0 bigint, c1 bigint", (1, 0), "DIVIDE_BY_ZERO"),
            (floor_div, LongType(), "c0 bigint, c1 bigint", (1, None), "cannot use NULL"),
            (increment, LongType(), "c0 bigint", (2**63 - 1,), "ARITHMETIC_OVERFLOW"),
            (positive, BooleanType(), "c0 bigint", (None,), "cannot use NULL"),
        ]
        with self.sql_conf(_JAVA_ONLY):
            for func, return_type, schema, row, message in cases:
                with self.subTest(func=func.__name__, row=row):
                    df = self.spark.createDataFrame([row], schema)
                    result = df.select(UserDefinedFunction(func, return_type)(*df.columns))
                    self.assertIn("TranspiledJavaUDF", self._plan(result))
                    with self.assertRaisesRegex(Exception, message):
                        result.collect()

    def test_column_and_return_types(self):
        def above(a: int, flag: bool):
            if flag:
                return a + 1
            return a

        df = self.spark.createDataFrame([(1, True), (5, False)], "a int, flag boolean")
        with self.sql_conf(_JAVA_ONLY):
            result = df.select(UserDefinedFunction(above, IntegerType())("a", "flag"))
            self.assertIn("TranspiledJavaUDF", self._plan(result))
            self.assertEqual([2, 5], [r[0] for r in result.collect()])

            # A double column matches no option, so it runs as Python.
            doubles = self.spark.createDataFrame([(1.5, True)], "a double, flag boolean")
            result = doubles.select(UserDefinedFunction(above, DoubleType())("a", "flag"))
            self.assertNotIn("TranspiledJavaUDF", self._plan(result))
            self.assertEqual([2.5], [r[0] for r in result.collect()])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
