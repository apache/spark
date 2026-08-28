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
Unit tests for UDF transpilation.

These were previously interleaved with the broader UDF mixin in
``test_udf.py``. They are split out because UDF transpilation is currently
only supported in regular (non-Connect) Spark, so they should not be
inherited into the Spark Connect parity test class. The companion
property-based suite lives in ``test_udf_transpile_hypothesis.py``.
"""

import unittest

from pyspark.sql import Row
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
)
from pyspark.sql.udf import UserDefinedFunction
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.util import is_remote_only

# Both flags must be on for the transpiler to attempt a rewrite (at UDF
# construction time and again in the optimizer); ANSI is required because
# transpilation targets ANSI semantics.
_TRANSPILE_ON = {
    "spark.sql.experimental.optimizer.transpilePyUDFs": True,
    "spark.sql.ansi.enabled": True,
}

# The interpreted path to compare against where ANSI is what makes the two differ.
_TRANSPILE_OFF_ANSI_ON = {
    "spark.sql.experimental.optimizer.transpilePyUDFs": False,
    "spark.sql.ansi.enabled": True,
}


@unittest.skipIf(
    is_remote_only(),
    "UDF transpilation is only supported in regular (non-Connect) Spark.",
)
class UDFTranspileUnitTests(ReusedSQLTestCase):
    def test_udf_transpile_basic(self):
        # Test callable object
        class PlusFour:
            def __call__(self, col):
                return col + 4

        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            # Make sure we can transpile the object
            call = PlusFour()
            pudf = UserDefinedFunction(call, LongType())
            self.assertTrue(pudf.transpiled)
            # Now make sure we can run the transpiled UDF*
            input_df = self.spark.createDataFrame([Row(a=1)])
            transformed_df = input_df.select(pudf("a"))
            [row] = transformed_df.collect()
            self.assertEqual(row[0], 5)

        with self.sql_conf({"spark.sql.experimental.optimizer.transpilePyUDFs": False}):
            call = PlusFour()
            pudf = UserDefinedFunction(call, LongType())
            self.assertEqual([], pudf.transpiled)
            # Now make sure we can run the UDF
            input_df = self.spark.createDataFrame([Row(a=1)])
            transformed_df = input_df.select(pudf("a"))
            [row] = transformed_df.collect()
            self.assertEqual(row[0], 5)

    def test_udf_transpile_with_nones(self):
        # Test callable object
        class PlusFour:
            def __call__(self, col):
                if col is not None:
                    return col + 4

        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            # Make sure we can transpile the object
            call = PlusFour()
            pudf = UserDefinedFunction(call, LongType())
            self.assertTrue(pudf.transpiled)
            # Now make sure we can run the transpiled UDF*
            input_df = self.spark.createDataFrame([Row(a=1)])
            transformed_df = input_df.select(pudf("a").alias("result"))
            [row] = transformed_df.collect()
            self.assertEqual(row[0], 5)
            physical_plan = transformed_df._jdf.queryExecution().executedPlan().toString()
            self.assertNotIn("UDF", physical_plan)

        with self.sql_conf({"spark.sql.experimental.optimizer.transpilePyUDFs": False}):
            call = PlusFour()
            pudf = UserDefinedFunction(call, LongType())
            self.assertEqual([], pudf.transpiled)
            # Now make sure we can run the UDF
            input_df = self.spark.createDataFrame([Row(a=1)])
            transformed_df = input_df.select(pudf("a").alias("result"))
            [row] = transformed_df.collect()
            self.assertEqual(row[0], 5)
            physical_plan = transformed_df._jdf.queryExecution().executedPlan().toString()
            self.assertIn("UDF", physical_plan)

    def test_udf_not_transpilable(self):
        class UnsupportedEx:
            def __call__(self, col):
                if col is not None:
                    return col in "4"

        with self.sql_conf({"spark.sql.experimental.optimizer.transpilePyUDFs": True}):
            call = UnsupportedEx()
            pudf = UserDefinedFunction(call, BooleanType())
            self.assertEqual([], pudf.transpiled)

    def test_udf_transpile_requires_ansi(self):
        # Transpilation targets ANSI semantics. With ANSI off the transpiler
        # must skip rewriting (and warn the user) so we don't silently
        # diverge from the Python interpretation; with ANSI on it should
        # produce a Catalyst expression.
        import warnings

        def plus_four(x):
            if x is not None:
                return x + 4

        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": False,
            }
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                pudf = UserDefinedFunction(plus_four, LongType())
            self.assertEqual([], pudf.transpiled)
            ansi_warnings = [w for w in caught if "ANSI mode" in str(w.message)]
            self.assertTrue(
                ansi_warnings,
                "expected an 'ANSI mode' warning when transpilation is "
                "requested but ANSI is disabled",
            )

        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            pudf = UserDefinedFunction(plus_four, LongType())
            self.assertTrue(
                pudf.transpiled,
                "expected transpilation to produce a Catalyst expression "
                "when both transpilePyUDFs and ANSI mode are enabled",
            )

    def test_udf_transpile_does_not_mask_char_varchar_ddl(self):
        # DDL CHAR/VARCHAR is parsed via ``returnType`` inside the optional
        # transpilation try. That structured error must not become a
        # transpilation UserWarning (which warnings-as-errors would raise).
        import warnings

        from pyspark.errors import PySparkNotImplementedError

        def identity(x):
            return x

        def _assert_char_varchar(return_type, data_type):
            with self.assertRaises(PySparkNotImplementedError) as pe:
                UserDefinedFunction(identity, return_type)
            self.check_error(
                exception=pe.exception,
                errorClass="CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON",
                messageParameters={
                    "feature": "Python UDF return types",
                    "data_type": data_type,
                },
            )

        with self.sql_conf(_TRANSPILE_ON):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                _assert_char_varchar("char(3)", "char(3)")
            self.assertFalse(
                any("Exception transpiling" in str(w.message) for w in caught),
                "CHAR/VARCHAR DDL must not be reported as a transpilation failure",
            )

            with warnings.catch_warnings():
                warnings.simplefilter("error")
                _assert_char_varchar("varchar(8)", "varchar(8)")

    def test_udf_transpile_falls_back_for_unsupported_patterns(self):
        # The transpiler intentionally only handles a small subset of
        # Python AST today. Everything outside that subset must
        # gracefully fall back to interpreted Python (with an empty
        # `transpiled` list and a UserWarning) rather than break the
        # UDF -- the "don't break people's Spark code" promise. This test
        # walks the most common unsupported shapes, registers each as a
        # UDF with transpilation on, and asserts (a) construction does
        # not raise, (b) `transpiled == []`, (c) the UDF still produces
        # the correct interpreted result.

        def divide_by_two(x):  # `/` -- ast.Div, not handled.
            if x is not None:
                return x / 2

        def floor_divide_by_two(x):  # `//` -- ast.FloorDiv, not handled.
            if x is not None:
                return x // 2

        def bit_and_one(x):  # `&` -- ast.BitAnd, not handled.
            if x is not None:
                return x & 1

        def bit_or_one(x):  # `|` -- ast.BitOr, not handled.
            if x is not None:
                return x | 1

        def left_shift(x):  # `<<` -- ast.LShift, not handled.
            if x is not None:
                return x << 1

        def multi_statement(x):  # > 1 top-level statement, not handled.
            y = 1
            return x + y if x is not None else 0

        def func_closure_capture(x):
            offset = 7
            if x is not None:
                return x + offset

        cases = [
            ("divide_by_two", divide_by_two, DoubleType(), Row(a=4.0), 2.0),
            ("floor_divide_by_two", floor_divide_by_two, LongType(), Row(a=5), 2),
            ("bit_and_one", bit_and_one, LongType(), Row(a=5), 1),
            ("bit_or_one", bit_or_one, LongType(), Row(a=4), 5),
            ("left_shift", left_shift, LongType(), Row(a=3), 6),
            ("multi_statement", multi_statement, LongType(), Row(a=5), 6),
            ("func_closure_capture", func_closure_capture, LongType(), Row(a=10), 17),
        ]

        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            for label, func, return_type, row, expected in cases:
                with self.subTest(case=label):
                    import warnings as _warnings

                    with _warnings.catch_warnings(record=True) as caught_warnings:
                        _warnings.simplefilter("always")
                        pudf = UserDefinedFunction(func, return_type)
                    self.assertEqual(
                        [],
                        pudf.transpiled,
                        f"{label}: transpiler should not produce a Catalyst "
                        "expression for this AST shape",
                    )
                    fallback = [
                        w
                        for w in caught_warnings
                        if "Unable to transpile" in str(w.message)
                        or "Errors encountered" in str(w.message)
                        or "Exception transpiling" in str(w.message)
                    ]
                    self.assertTrue(
                        fallback,
                        f"{label}: expected a fallback warning when the "
                        "transpiler can't lower the function",
                    )
                    df = self.spark.createDataFrame([row])
                    [result] = df.select(pudf("a")).collect()
                    self.assertEqual(
                        result[0],
                        expected,
                        f"{label}: interpreted UDF result diverged from expected",
                    )

    def test_udf_transpile_boolean_and_or_lowered(self):
        # When `and`/`or` operands are syntactically boolean (Compare
        # results in this case), the transpiler should lower to bitwise
        # `&`/`|` and produce results matching the interpreted UDF.
        # Each UDF is a single top-level statement (the transpiler
        # doesn't support multi-statement bodies yet).
        from pyspark.sql.types import StructField, StructType

        def both_positive(x, y):
            return x > 0 and y > 0

        def either_positive(x, y):
            return x > 0 or y > 0

        schema = StructType(
            [
                StructField("a", LongType(), nullable=True),
                StructField("b", LongType(), nullable=True),
            ]
        )

        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            # NULL inputs propagate through `>` to NULL, which then
            # passes through `&` / `|` per SQL three-valued logic. We
            # only assert on non-NULL inputs here since Python's
            # interpreted `x > 0 and y > 0` would raise on None; the
            # NULL handling itself is covered by the hypothesis suite.
            for func, x, y, expected in [
                (both_positive, 1, 2, True),
                (both_positive, 1, -1, False),
                (both_positive, -1, -1, False),
                (either_positive, -1, 2, True),
                (either_positive, -1, -1, False),
                (either_positive, 1, 1, True),
            ]:
                with self.subTest(func=func.__name__, x=x, y=y):
                    pudf = UserDefinedFunction(func, BooleanType())
                    self.assertTrue(
                        pudf.transpiled,
                        f"{func.__name__}: bool-typed and/or should transpile",
                    )
                    df = self.spark.createDataFrame([Row(a=x, b=y)], schema=schema)
                    [row] = df.select(pudf("a", "b")).collect()
                    self.assertEqual(row[0], expected)

    def test_udf_transpile_less_than_zero(self):
        # Restored from the unsupported-patterns matrix: now that the
        # transpiler handles ast.Lt, `x < 0` should lower to a Catalyst
        # expression and match interpreted Python. The ``is not None``
        # guard short-circuits None inputs through the else branch, so
        # the comparison itself never sees a NULL in this UDF -- and since
        # SPARK-58628 that also means no NULL check is emitted for it (see
        # test_udf_transpile_drops_null_checks_a_guard_already_made).
        from pyspark.sql.types import StructField, StructType

        def less_than_zero(x):
            if x is not None:
                return x < 0

        schema = StructType([StructField("a", LongType(), nullable=True)])
        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            pudf = UserDefinedFunction(less_than_zero, BooleanType())
            self.assertTrue(pudf.transpiled, "less_than_zero should now transpile")
            for value, expected in [(-1, True), (0, False), (5, False), (None, None)]:
                with self.subTest(value=value):
                    df = self.spark.createDataFrame([Row(a=value)], schema=schema)
                    [row] = df.select(pudf("a")).collect()
                    self.assertEqual(row[0], expected)

    def test_udf_transpile_compare_with_none_raises(self):
        # When a comparison's operand is NULL in Spark, Python would have
        # raised TypeError ('>' not supported between NoneType and int).
        # The transpiler wraps Compare ops with a raise_error guard so
        # the rewritten plan fails loudly instead of silently producing
        # NULL three-valued-logic results. Nothing here proves ``x``
        # non-NULL, so the guard is emitted -- contrast the guarded shapes in
        # test_udf_transpile_drops_null_checks_a_guard_already_made.
        from pyspark.sql.types import StructField, StructType

        def gt_zero(x):
            return x > 0

        schema = StructType([StructField("a", LongType(), nullable=True)])
        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            pudf = UserDefinedFunction(gt_zero, BooleanType())
            self.assertTrue(pudf.transpiled, "gt_zero should transpile")
            df = self.spark.createDataFrame([Row(a=None)], schema=schema)
            with self.assertRaises(Exception) as ctx:
                df.select(pudf("a")).collect()
            self.assertIn("cannot compare NULL", str(ctx.exception))

    def test_udf_transpile_eq_none_semantics(self):
        # Python ``==``/``!=`` differ from Spark's three-valued NULL equality:
        # in Python ``None == None`` is ``True`` and ``None == 0`` is ``False``,
        # whereas SQL ``NULL = NULL`` and ``NULL = 0`` both yield ``NULL``. The
        # transpiler's ``_lower_eq`` reproduces Python's semantics; this test
        # exercises every arm of that logic.
        from pyspark.sql.types import StructField, StructType

        def x_eq_zero(x):
            if x is not None:
                return x == 0
            else:
                return None

        def x_neq_zero(x):
            if x is not None:
                return x != 0
            else:
                return None

        def x_eq_y(x, y):
            return x == y

        def x_neq_y(x, y):
            return x != y

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        two_col_schema = StructType(
            [
                StructField("a", LongType(), nullable=True),
                StructField("b", LongType(), nullable=True),
            ]
        )
        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            # Single-arg ``x == 0`` / ``x != 0`` with a None guard.
            pudf_eq = UserDefinedFunction(x_eq_zero, BooleanType())
            pudf_neq = UserDefinedFunction(x_neq_zero, BooleanType())
            self.assertTrue(pudf_eq.transpiled, "x == 0 should transpile")
            self.assertTrue(pudf_neq.transpiled, "x != 0 should transpile")
            for value, eq_expected, neq_expected in [
                (0, True, False),
                (1, False, True),
                (-3, False, True),
                (None, None, None),
            ]:
                with self.subTest(value=value):
                    df = self.spark.createDataFrame([Row(a=value)], schema=long_schema)
                    [row_eq] = df.select(pudf_eq("a")).collect()
                    [row_neq] = df.select(pudf_neq("a")).collect()
                    self.assertEqual(row_eq[0], eq_expected)
                    self.assertEqual(row_neq[0], neq_expected)

            # Two-arg ``x == y`` / ``x != y`` exercising every NULL combination.
            pudf_eq_xy = UserDefinedFunction(x_eq_y, BooleanType())
            pudf_neq_xy = UserDefinedFunction(x_neq_y, BooleanType())
            self.assertTrue(pudf_eq_xy.transpiled, "x == y should transpile")
            self.assertTrue(pudf_neq_xy.transpiled, "x != y should transpile")
            # Python semantics:
            #   None == None -> True;     None != None -> False
            #   None == 0    -> False;    None != 0    -> True
            #   0    == None -> False;    0    != None -> True
            #   1    == 1    -> True;     1    != 1    -> False
            #   1    == 2    -> False;    1    != 2    -> True
            for x, y, eq_expected, neq_expected in [
                (None, None, True, False),
                (None, 0, False, True),
                (0, None, False, True),
                (1, 1, True, False),
                (1, 2, False, True),
            ]:
                with self.subTest(x=x, y=y):
                    df = self.spark.createDataFrame([Row(a=x, b=y)], schema=two_col_schema)
                    [row_eq] = df.select(pudf_eq_xy("a", "b")).collect()
                    [row_neq] = df.select(pudf_neq_xy("a", "b")).collect()
                    self.assertEqual(row_eq[0], eq_expected, f"({x} == {y})")
                    self.assertEqual(row_neq[0], neq_expected, f"({x} != {y})")

    def test_udf_transpile_lte_gte(self):
        # ``<=`` and ``>=`` go through the same ``_lower_value_compare`` path
        # as ``<`` / ``>`` (and so share the NULL-raises-TypeError guard), but
        # the entry points are not exercised elsewhere. Cover both with a None
        # guard, which since SPARK-58628 also means the guard proves the operands
        # non-NULL and no check is emitted at all.
        from pyspark.sql.types import StructField, StructType

        def lte_zero(x):
            if x is not None:
                return x <= 0

        def gte_zero(x):
            if x is not None:
                return x >= 0

        schema = StructType([StructField("a", LongType(), nullable=True)])
        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            pudf_lte = UserDefinedFunction(lte_zero, BooleanType())
            pudf_gte = UserDefinedFunction(gte_zero, BooleanType())
            self.assertTrue(pudf_lte.transpiled, "x <= 0 should transpile")
            self.assertTrue(pudf_gte.transpiled, "x >= 0 should transpile")
            for value, lte_expected, gte_expected in [
                (-1, True, False),
                (0, True, True),
                (1, False, True),
                (None, None, None),
            ]:
                with self.subTest(value=value):
                    df = self.spark.createDataFrame([Row(a=value)], schema=schema)
                    [row_lte] = df.select(pudf_lte("a")).collect()
                    [row_gte] = df.select(pudf_gte("a")).collect()
                    self.assertEqual(row_lte[0], lte_expected)
                    self.assertEqual(row_gte[0], gte_expected)

    def test_udf_transpile_chained_comparison_falls_back(self):
        # ``a < b < c`` is a chained comparison: Python evaluates it as
        # ``(a < b) and (b < c)``. The transpiler refuses chained Compare
        # nodes (``len(ops) != 1``) and must fall back to interpreted Python.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def chained(x):
            return 0 < x < 10

        schema = StructType([StructField("a", LongType(), nullable=False)])
        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            with _warnings.catch_warnings(record=True) as caught:
                _warnings.simplefilter("always")
                pudf = UserDefinedFunction(chained, BooleanType())
            self.assertEqual([], pudf.transpiled, "chained comparison must NOT transpile")
            fallback = [
                w
                for w in caught
                if "Unable to transpile" in str(w.message) or "Errors encountered" in str(w.message)
            ]
            self.assertTrue(fallback, "expected a fallback warning")
            for value, expected in [(5, True), (0, False), (10, False), (-3, False)]:
                with self.subTest(value=value):
                    df = self.spark.createDataFrame([Row(a=value)], schema=schema)
                    [row] = df.select(pudf("a")).collect()
                    self.assertEqual(row[0], expected)

    def test_udf_transpile_multi_row(self):
        # Every other transpile test uses a 1-row DataFrame; this one runs
        # the same arithmetic transpile on a multi-row input to catch any
        # column-reference / batch-boundary bug that single-row tests can't.
        from pyspark.sql.types import StructField, StructType

        def plus_four(x):
            if x is not None:
                return x + 4

        schema = StructType([StructField("a", LongType(), nullable=True)])
        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            pudf = UserDefinedFunction(plus_four, LongType())
            self.assertTrue(pudf.transpiled)
            inputs = [Row(a=v) for v in [-3, -1, 0, 1, 7, None, 100]]
            df = self.spark.createDataFrame(inputs, schema=schema)
            transformed_df = df.select(pudf("a").alias("result"))
            rows = transformed_df.collect()
            actual = [row[0] for row in rows]
            expected = [None if v is None else v + 4 for v in [-3, -1, 0, 1, 7, None, 100]]
            self.assertEqual(actual, expected)
            # Plan should also have the UDF stripped under the rewrite.
            physical_plan = transformed_df._jdf.queryExecution().executedPlan().toString()
            self.assertNotIn("UDF", physical_plan)

    def test_udf_transpile_falls_back_for_non_boolean_short_circuit(self):
        # Python's `x or 0` returns x if truthy else 0; Spark's `|` is
        # bitwise, so we'd silently produce wrong results. The transpiler
        # must refuse, fall back to interpreted Python, and still produce
        # the correct result.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def or_zero(x):
            return x or 0

        def and_one(x):
            return x and 1

        def not_int(x):
            return not 0 + x  # operand is BinOp, statically non-boolean

        long_schema = StructType([StructField("a", LongType(), nullable=True)])

        cases = [
            ("or_zero", or_zero, LongType(), long_schema, Row(a=5), 5),
            ("or_zero_none", or_zero, LongType(), long_schema, Row(a=None), 0),
            ("and_one", and_one, LongType(), long_schema, Row(a=5), 1),
            ("and_one_zero", and_one, LongType(), long_schema, Row(a=0), 0),
            ("not_int", not_int, BooleanType(), long_schema, Row(a=0), True),
            ("not_int_nonzero", not_int, BooleanType(), long_schema, Row(a=3), False),
        ]
        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            for label, func, return_type, schema, row, expected in cases:
                with self.subTest(case=label):
                    with _warnings.catch_warnings(record=True) as caught:
                        _warnings.simplefilter("always")
                        pudf = UserDefinedFunction(func, return_type)
                    self.assertEqual(
                        [],
                        pudf.transpiled,
                        f"{label}: non-boolean and/or/not must NOT be lowered",
                    )
                    fallback = [
                        w
                        for w in caught
                        if "Unable to transpile" in str(w.message)
                        or "Errors encountered" in str(w.message)
                    ]
                    self.assertTrue(fallback, f"{label}: expected a fallback warning")
                    df = self.spark.createDataFrame([row], schema=schema)
                    [result] = df.select(pudf("a")).collect()
                    self.assertEqual(result[0], expected, f"{label}: interpreted mismatch")

    def test_udf_transpile_bare_if_truthiness(self):
        # `if x:` / ternary `x if x else y` on numeric and string columns must
        # now transpile (SPARK-56925).  The transpiler lowers to a type-specific
        # truthiness expression:
        #   numeric  -> coalesce(x != 0, False)
        #   string   -> coalesce(length(x) > 0, False)
        #   bool     -> coalesce(x, False)   (same as the existing boolean path)
        # NULL (Python None) is always treated as falsy, matching Python.
        from pyspark.sql.types import DoubleType, StructField, StructType

        def truthy_int(x):
            if x:
                return x
            else:
                return -1

        def truthy_str(x):
            return x if x else "default"

        def truthy_float(x):
            # NaN is truthy in Python (bool(float('nan')) == True).
            return 1 if x else 0

        def truthy_bool(x: bool):
            return "yes" if x else "no"

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        str_schema = StructType([StructField("a", StringType(), nullable=True)])
        dbl_schema = StructType([StructField("a", DoubleType(), nullable=True)])
        bool_schema = StructType([StructField("a", BooleanType(), nullable=True)])

        with self.sql_conf(_TRANSPILE_ON):
            # --- integer ---
            pudf_int = UserDefinedFunction(truthy_int, LongType())
            self.assertTrue(pudf_int.transpiled, "integer bare-if should transpile")
            df_int = self.spark.createDataFrame([(3,), (0,), (None,)], schema=long_schema)
            projected = df_int.select(pudf_int("a").alias("r"))
            self.assertEqual(0, self._eval_python_count(projected))
            self.assertEqual([r[0] for r in projected.collect()], [3, -1, -1])

            # --- string ---
            pudf_str = UserDefinedFunction(truthy_str, StringType())
            self.assertTrue(pudf_str.transpiled, "string bare-if should transpile")
            df_str = self.spark.createDataFrame([("hi",), ("",), (None,)], schema=str_schema)
            projected = df_str.select(pudf_str("a").alias("r"))
            self.assertEqual(0, self._eval_python_count(projected))
            self.assertEqual([r[0] for r in projected.collect()], ["hi", "default", "default"])

            # --- float (NaN is truthy in Python) ---
            pudf_flt = UserDefinedFunction(truthy_float, LongType())
            self.assertTrue(pudf_flt.transpiled, "float bare-if should transpile")
            nan = float("nan")
            df_flt = self.spark.createDataFrame(
                [(1.5,), (0.0,), (nan,), (None,)], schema=dbl_schema
            )
            projected = df_flt.select(pudf_flt("a").alias("r"))
            self.assertEqual(0, self._eval_python_count(projected))
            # 1.5 truthy, 0.0 falsy, NaN truthy (NaN != 0 is True in Spark), NULL falsy
            self.assertEqual([r[0] for r in projected.collect()], [1, 0, 1, 0])

            # --- bool ---
            pudf_bool = UserDefinedFunction(truthy_bool, StringType())
            self.assertTrue(pudf_bool.transpiled, "bool bare-if should transpile")
            df_bool = self.spark.createDataFrame([(True,), (False,), (None,)], schema=bool_schema)
            projected = df_bool.select(pudf_bool("a").alias("r"))
            self.assertEqual(0, self._eval_python_count(projected))
            self.assertEqual([r[0] for r in projected.collect()], ["yes", "no", "no"])

    def test_udf_transpile_falls_back_for_bare_truthiness_test(self):
        # For categories the transpiler cannot lower to a truthiness expression
        # (currently "binary"), bare `if x:` must still fall back to interpreted
        # Python rather than silently emitting a wrong or un-analyzable plan.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        # bytes annotation -> "binary" category -> _truthiness_col returns None
        def truthy_bytes(x: bytes) -> bool:
            if x:
                return True
            return False

        bin_schema = StructType([StructField("a", BinaryType(), nullable=True)])

        with self.sql_conf(_TRANSPILE_ON):
            with _warnings.catch_warnings(record=True) as caught:
                _warnings.simplefilter("always")
                pudf = UserDefinedFunction(truthy_bytes, BooleanType())
            self.assertEqual(
                [],
                pudf.transpiled,
                "binary bare-if must NOT be lowered to Catalyst",
            )
            fallback = [
                w
                for w in caught
                if "Unable to transpile" in str(w.message) or "Errors encountered" in str(w.message)
            ]
            self.assertTrue(fallback, "expected a fallback warning for binary bare-if")
            df = self.spark.createDataFrame([(b"hi",), (b"",), (None,)], schema=bin_schema)
            results = [r[0] for r in df.select(pudf("a")).collect()]
            self.assertEqual(results, [True, False, False])

    def test_udf_transpile_falls_back_for_mismatched_branch_types(self):
        # An if/ternary whose two branches produce different Spark categories
        # (e.g. numeric vs string) would lower to a CASE WHEN whose branch
        # values share no common type under ANSI. That node is carried as a
        # child of the TranspiledPythonUDF and is type-checked by CheckAnalysis
        # before ConvertToCatalyst can drop it, so without a guard the whole
        # query would fail analysis instead of falling back. The transpiler must
        # refuse and run the UDF as interpreted Python.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def mixed_ternary(x):
            return 1 if x > 0 else "neg"

        def mixed_if(x):
            # Single top-level `if`/`else` so the If-statement lowering path
            # (not the "more than one statement" fallback) exercises the guard.
            if x > 0:
                return "pos"
            else:
                return x

        # Positive control: matching-category branches must still transpile, so
        # the guard does not over-refuse.
        def homogeneous(x):
            return x if x > 0 else 0

        long_schema = StructType([StructField("a", LongType(), nullable=True)])

        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            # Inputs are chosen to take the string-returning branch so the
            # interpreted result is unambiguous.
            mismatch_cases = [
                ("mixed_ternary", mixed_ternary, Row(a=-3), "neg"),
                ("mixed_if", mixed_if, Row(a=10), "pos"),
            ]
            for label, func, row, expected in mismatch_cases:
                with self.subTest(case=label):
                    with _warnings.catch_warnings(record=True) as caught:
                        _warnings.simplefilter("always")
                        pudf = UserDefinedFunction(func, StringType())
                    self.assertEqual(
                        [],
                        pudf.transpiled,
                        f"{label}: mismatched branch types must NOT be lowered to Catalyst",
                    )
                    fallback = [w for w in caught if "Unable to transpile" in str(w.message)]
                    self.assertTrue(fallback, f"{label}: expected a fallback warning")
                    df = self.spark.createDataFrame([row], schema=long_schema)
                    # Must run without an analysis failure and match interpreted Python.
                    [result] = df.select(pudf("a")).collect()
                    self.assertEqual(result[0], expected, f"{label}: interpreted mismatch")

            with self.subTest(case="homogeneous"):
                pudf = UserDefinedFunction(homogeneous, LongType())
                self.assertNotEqual(
                    [],
                    pudf.transpiled,
                    "matching-category branches must still transpile",
                )
                df = self.spark.createDataFrame([Row(a=5), Row(a=-3)], schema=long_schema)
                results = [r[0] for r in df.select(pudf("a")).collect()]
                self.assertEqual(results, [5, 0], "homogeneous branch result mismatch")

    def test_udf_transpile_falls_back_for_cross_category_eq(self):
        # `x == True` on a numeric column would lower to `x = true`, which
        # fails ANSI analysis (BIGINT vs BOOLEAN) while the option is still a
        # child of the TranspiledPythonUDF -- breaking a working UDF. The
        # category gate must refuse so it runs as interpreted Python.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def eq_true(x):
            return x == True  # noqa: E712

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        with self.sql_conf(_TRANSPILE_ON):
            with _warnings.catch_warnings(record=True):
                _warnings.simplefilter("always")
                pudf = UserDefinedFunction(eq_true, BooleanType())
            self.assertEqual([], pudf.transpiled, "cross-category == must not transpile")
            df = self.spark.createDataFrame([Row(a=5), Row(a=1)], schema=long_schema)
            results = [r[0] for r in df.select(pudf("a")).collect()]
            self.assertEqual(results, [5 == True, 1 == True])

    def test_udf_transpile_falls_back_for_nested_ternary_eq(self):
        # A ternary operand used inside `==` must contribute its branches'
        # category, not the old "numeric" catch-all: `("5" if c else "6") == 5`
        # previously passed the equality guard as numeric-vs-numeric and
        # Spark's string-number coercion silently returned True where Python's
        # cross-type == is False. (Reported by Codex review on PR #34.)
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def nested_ternary_eq(x):
            return ("5" if x > 0 else "6") == 5

        def none_branch_ternary_eq(x):
            return ("5" if x > 0 else None) == 5

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        df = self.spark.createDataFrame([Row(a=5), Row(a=-5)], schema=long_schema)
        with self.sql_conf(_TRANSPILE_ON):
            for func in [nested_ternary_eq, none_branch_ternary_eq]:
                with self.subTest(func=func.__name__):
                    with _warnings.catch_warnings(record=True):
                        _warnings.simplefilter("always")
                        pudf = UserDefinedFunction(func, BooleanType())
                    self.assertEqual(
                        [], pudf.transpiled, "string-ternary == int must not transpile"
                    )
                    results = [r[0] for r in df.select(pudf("a")).collect()]
                    self.assertEqual(results, [False, False], "must match Python's ==")

    def test_udf_transpile_str_int_compare_matches_python(self):
        # Comparing a value against a string literal (``x == "5"`` / ``x < "5"``)
        # under the untyped-parameter path produces two candidate options -- a
        # numeric variant and a string variant. The numeric variant mixes
        # categories (numeric column vs string literal): Python compares such
        # values as unequal / raises TypeError, while a lowered ``x = '5'`` would
        # coerce under ANSI and silently diverge -- so ``_lower_eq`` /
        # ``_lower_value_compare`` refuse it. Only the string variant survives.
        # When the UDF is applied to a numeric column, ResolveTranspiledPython-
        # UDFOptions drops the string option (no category match) and the UDF
        # falls back to interpreted Python. We verify the observable guarantee on
        # both sides: ``==`` returns Python's result (no coerced ``True``), and
        # ``<`` raises on the Python side directly and surfaces the same error
        # through Spark rather than returning a coerced answer.
        from pyspark.errors import PythonException
        from pyspark.sql.types import StructField, StructType

        def eq_str(x):
            return x == "5"

        def lt_str(x):
            return x < "5"

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        with self.sql_conf(_TRANSPILE_ON):
            df = self.spark.createDataFrame([Row(a=5), Row(a=1)], schema=long_schema)

            # ``==`` : numeric column -> string option dropped -> interpreted
            # Python, so the result matches ``5 == "5"`` (False), not a coerced
            # ``True`` from ``bigint = '5'``.
            pudf_eq = UserDefinedFunction(eq_str, BooleanType())
            results = [r[0] for r in df.select(pudf_eq("a")).collect()]
            self.assertEqual(results, [5 == "5", 1 == "5"], "must match Python's ==")

            # ``<`` : Python raises TypeError for ``int < str``; the transpiled
            # string option is dropped for a numeric column, so Spark runs the
            # interpreted UDF and surfaces the same error rather than coercing.
            # Asserting PythonException (not a bare Exception) pins this to the
            # interpreted-fallback path: a Catalyst AnalysisException here would
            # instead mean the string option was wrongly kept and broke the
            # query rather than falling back.
            pudf_lt = UserDefinedFunction(lt_str, BooleanType())
            with self.assertRaises(TypeError):
                lt_str(5)
            with self.assertRaises(PythonException) as ctx:
                df.select(pudf_lt("a")).collect()
            self.assertIn("not supported between", str(ctx.exception))

    def test_udf_transpile_falls_back_for_bool_arithmetic(self):
        # `(x > 0) + 1` is valid Python (True + 1 == 2), but the lowered
        # Add(boolean, int) fails ANSI analysis. The category of a
        # boolean-producing operand is now "bool" (not the numeric catch-all),
        # so this refuses and runs as interpreted Python.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def bool_plus_one(x):
            return (x > 0) + 1

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        with self.sql_conf(_TRANSPILE_ON):
            with _warnings.catch_warnings(record=True):
                _warnings.simplefilter("always")
                pudf = UserDefinedFunction(bool_plus_one, LongType())
            self.assertEqual([], pudf.transpiled, "bool arithmetic must not transpile")
            df = self.spark.createDataFrame([Row(a=5), Row(a=-5)], schema=long_schema)
            results = [r[0] for r in df.select(pudf("a")).collect()]
            self.assertEqual(results, [2, 1])

    def test_udf_transpile_falls_back_for_return_wrapped_bool_branch(self):
        # If-statement branches arrive as ast.Return nodes; the branch-category
        # guard must see through the wrapper. A boolean-returning branch vs a
        # numeric one previously slipped past the guard and failed analysis
        # (CASE WHEN [BOOLEAN, INT]) instead of falling back.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def mixed(x):
            if x > 0:
                return x > 5
            else:
                return 1

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        with self.sql_conf(_TRANSPILE_ON):
            with _warnings.catch_warnings(record=True):
                _warnings.simplefilter("always")
                pudf = UserDefinedFunction(mixed, LongType())
            self.assertEqual([], pudf.transpiled, "bool-vs-int branches must not transpile")
            df = self.spark.createDataFrame([Row(a=-3)], schema=long_schema)
            [result] = df.select(pudf("a")).collect()
            self.assertEqual(result[0], 1)

    def test_udf_transpile_plain_self_param_is_an_ordinary_param(self):
        # A plain function whose first parameter is literally named `self` is not a
        # bound receiver -- the call site supplies it. Stripping it emitted
        # `_udf_param_-1` and threw at call construction, so it used to be refused
        # outright; the receiver is decided by dispatch now, so this lowers.
        def weird(self, other):
            return self + other

        self.assertEqual(
            self._vals(weird, LongType(), "a long, b long", [(2, 3)]),
            [5],
            "both parameters are supplied at the call site, so both get placeholders",
        )

    def test_udf_transpile_positional_only_params_lower(self):
        # No placeholder-omission hazard -- always bound by position -- so this
        # lowers now, unlike default/variadic/keyword-only params. Typed str/int
        # and value-dependent on both, so a swapped placeholder or category
        # fails on the result, not just a missing lowering.
        def two_posonly(a: str, b: int, /):
            return a if b > 0 else "-"

        def mixed_posonly(a: str, /, b: int):
            return a if b > 0 else "-"

        posonly_lambda = lambda x, /, y: y - x  # noqa: E731

        self.assertEqual(
            self._vals(two_posonly, StringType(), "a string, b long", [("xy", 5)]), ["xy"]
        )
        self.assertEqual(
            self._vals(mixed_posonly, StringType(), "a string, b long", [("xy", 5)]), ["xy"]
        )
        self.assertEqual(self._vals(posonly_lambda, LongType(), "a long, b long", [(2, 5)]), [3])

    def test_udf_transpile_positional_only_param_with_default_still_falls_back(self):
        # A default's a default whether it's positional-only or not.
        def posonly_with_default(a, b=1, /):
            return a + b

        with self.sql_conf(_TRANSPILE_ON):
            pudf = UserDefinedFunction(posonly_with_default, LongType())
            self.assertEqual([], pudf.transpiled)
            df = self.spark.createDataFrame([(2,)], "a long")
            self.assertEqual(df.select(pudf("a")).first()[0], 3)

    def test_udf_transpile_positional_only_receiver_and_public_params(self):
        # self/a/b all land in posonlyargs here, none in args.args -- the slice
        # _param_category_combos used to miss. Typed str/int so a reversed
        # slice (self swapped in for a public param) fails on the result.
        class Adder:
            def __call__(self, a: str, b: int, /):
                return a if b > 0 else "-"

        self.assertEqual(self._vals(Adder(), StringType(), "a string, b long", [("xy", 3)]), ["xy"])

    def test_udf_transpile_positional_only_kwarg_call_raises_like_python(self):
        # A kwarg naming a positional-only param must still raise -- the
        # transpile candidacy shim resolves kwargs to positions and must not
        # paper over a call Python itself rejects.
        def posonly(a, b, /):
            return a + b

        with self.sql_conf(_TRANSPILE_ON):
            pudf = UserDefinedFunction(posonly, LongType())
            df = self.spark.createDataFrame([(2, 3)], "a long, b long")
            with self.assertRaisesRegex(Exception, "positional-only arguments"):
                df.select(pudf(a=df.a, b=df.b)).collect()

    def test_udf_transpile_falls_back_for_wraps_decorated_function(self):
        # inspect.getsource follows __wrapped__, so a functools.wraps-decorated
        # UDF previously transpiled the WRAPPED function's source while the
        # interpreted path ran the wrapper -- a silent wrong result.
        import functools
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def base(x):
            return x + 1

        @functools.wraps(base)
        def wrapper(x):
            return base(x) * 10

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        with self.sql_conf(_TRANSPILE_ON):
            with _warnings.catch_warnings(record=True):
                _warnings.simplefilter("always")
                pudf = UserDefinedFunction(wrapper, LongType())
            self.assertEqual([], pudf.transpiled, "wraps-decorated UDF must not transpile")
            df = self.spark.createDataFrame([Row(a=5)], schema=long_schema)
            [result] = df.select(pudf("a")).collect()
            self.assertEqual(result[0], 60, "must run the wrapper, not the wrapped source")

    def test_udf_transpile_falls_back_for_none_in_boolop(self):
        # Python's `None and x` short-circuits to None; Spark's three-valued
        # `null AND false` is false. A literal None operand must force a
        # fallback rather than silently diverge.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def none_and(x):
            return None and (x > 0)

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        with self.sql_conf(_TRANSPILE_ON):
            with _warnings.catch_warnings(record=True):
                _warnings.simplefilter("always")
                pudf = UserDefinedFunction(none_and, BooleanType())
            self.assertEqual([], pudf.transpiled, "literal None in and/or must not transpile")
            df = self.spark.createDataFrame([Row(a=-5)], schema=long_schema)
            [result] = df.select(pudf("a")).collect()
            self.assertIsNone(result[0])

    def test_udf_transpile_falls_back_for_uncastable_return_type(self):
        # The lowered expression is cast to the declared return type; a return
        # type no atomic lowering can be cast to (arrays, maps, datetimes, ...)
        # would make that Cast fail CheckAnalysis and break the whole query
        # (the options are children of TranspiledPythonUDF), so such UDFs must
        # fall back at construction instead. Interpreted execution still works
        # (the pickled-UDF converter nulls the type-mismatched results).
        import warnings as _warnings

        from pyspark.sql.types import ArrayType, TimestampType

        plus_one = lambda x: x + 1  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            for rt in (ArrayType(LongType()), TimestampType()):
                with _warnings.catch_warnings(record=True):
                    _warnings.simplefilter("always")
                    pudf = UserDefinedFunction(plus_one, rt)
                self.assertEqual([], pudf.transpiled, f"return type {rt} must not transpile")
            # Interpreted execution keeps working; an int result for an array
            # return type is nulled by the pickled-UDF converter. (Timestamp
            # is not exercised here: its converter accepts ints as micros.)
            with _warnings.catch_warnings(record=True):
                _warnings.simplefilter("always")
                array_udf = UserDefinedFunction(plus_one, ArrayType(LongType()))
            df = self.spark.createDataFrame([Row(a=1)])
            [result] = df.select(array_udf("a")).collect()
            self.assertIsNone(result[0], "interpreted fallback nulls the mismatch")

    def test_udf_transpile_falls_back_for_cross_category_return_cast(self):
        # Per-variant guard: the body category must MATCH the declared return
        # type's category. Un-castable combos (binary body -> numeric return,
        # boolean body -> binary return) would fail analysis outright, and
        # analysis-valid cross-category casts (string -> long, numeric ->
        # boolean, anything -> decimal) diverge from the interpreted path,
        # which nulls type-mismatched results instead of casting -- e.g.
        # `def f(s: str): return s` declared LongType() would return 123 for
        # '123' (or raise CAST_INVALID_INPUT) where interpreted returns NULL.
        import warnings as _warnings

        from pyspark.sql.types import DecimalType

        def bytes_to_long(x: bytes):
            return x

        def bool_to_binary(x):
            return (x > 0) if x is not None else None

        def str_ident(s: str):
            return s

        def plus_one(x):
            return x + 1

        with self.sql_conf(_TRANSPILE_ON):
            for func, rt, label in (
                (bytes_to_long, LongType(), "binary body -> numeric return"),
                (bool_to_binary, BinaryType(), "boolean body -> binary return"),
                (bytes_to_long, StringType(), "binary body -> string return"),
                (str_ident, LongType(), "string body -> numeric return"),
                (plus_one, BooleanType(), "numeric body -> boolean return"),
                (plus_one, DecimalType(10, 2), "numeric body -> decimal return"),
            ):
                with _warnings.catch_warnings(record=True):
                    _warnings.simplefilter("always")
                    pudf = UserDefinedFunction(func, rt)
                self.assertEqual([], pudf.transpiled, f"{label} must not transpile")
            # Interpreted execution of the Codex-flagged example: NULL, not a
            # cast. (The transpiled cast would have returned 123.)
            with _warnings.catch_warnings(record=True):
                _warnings.simplefilter("always")
                str_long = UserDefinedFunction(str_ident, LongType())
            df = self.spark.createDataFrame([("123",)], "s string")
            self.assertIsNone(df.select(str_long("s")).first()[0])

    def test_udf_transpile_falls_back_for_non_numeric_unary(self):
        # Unary +/- only lower for numeric operands: Python raises TypeError
        # on `+s`/`-s` for strings while Spark's ANSI string promotion would
        # silently coerce (`-'5'` -> -5.0), and `-x` on a boolean would fail
        # analysis outright rather than fall back.
        import warnings as _warnings

        def neg_str(s: str):
            return -s

        def pos_str(s: str):
            return +s

        def neg_bool(x: bool):
            return -x

        with self.sql_conf(_TRANSPILE_ON):
            for func in (neg_str, pos_str, neg_bool):
                with _warnings.catch_warnings(record=True):
                    _warnings.simplefilter("always")
                    pudf = UserDefinedFunction(func, LongType())
                self.assertEqual([], pudf.transpiled, f"{func.__name__} must not transpile")
        # Numeric unary still lowers and matches Python.
        neg = lambda x: -x  # noqa: E731
        self.assertEqual(self._vals(neg, LongType(), "a long", [(5,), (-3,)]), [-5, 3])

    def test_udf_transpile_falls_back_for_self_reference(self):
        # A __call__ body that references bare `self` has no column
        # equivalent; the offset scheme previously emitted `_udf_param_-1`,
        # which the JVM builder rejected with an AnalysisException at call
        # construction instead of falling back to interpreted Python.
        import warnings as _warnings

        class PickSelf:
            def __call__(self, x):
                return x if x is not None else self

        with self.sql_conf(_TRANSPILE_ON):
            with _warnings.catch_warnings(record=True):
                _warnings.simplefilter("always")
                pudf = UserDefinedFunction(PickSelf(), LongType())
            self.assertEqual([], pudf.transpiled, "`self` reference must not transpile")
            # Interpreted execution still works (previously the call itself
            # raised). Only non-null rows are exercised: a row that RETURNS
            # `self` would fail JVM-side unpickling of the instance, which is
            # interpreted-UDF behavior unrelated to this guard.
            df = self.spark.createDataFrame([(2,), (7,)], "a long")
            results = [r[0] for r in df.select(pudf("a")).collect()]
            self.assertEqual(results, [2, 7])

    def test_udf_transpile_preserves_auto_column_name(self):
        # The auto-generated column name must stay `f(a)` whether or not the
        # rewrite engages; the TranspiledPythonUDF wrapper (and its option
        # children) must not leak into user-visible schema names.
        from pyspark.sql.types import StructField, StructType

        def plus_four(x):
            return x + 4

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        df = self.spark.createDataFrame([Row(a=1)], schema=long_schema)
        with self.sql_conf(_TRANSPILE_ON):
            pudf = UserDefinedFunction(plus_four, LongType())
            self.assertTrue(pudf.transpiled)
            self.assertEqual(df.select(pudf("a")).columns, ["plus_four(a)"])

    def test_udf_transpile_arity_mismatch_falls_back(self):
        # Calling with the wrong number of arguments is a user error that must
        # surface as the standard Python-side TypeError, not be silently
        # absorbed by a transpiled constant (zero-param case) nor raise a
        # misleading "internal error" AnalysisException (too-few-args case).
        import warnings as _warnings

        from pyspark.errors import PythonException
        from pyspark.sql.types import StructField, StructType

        def zero():
            return 42

        def two(x, y):
            return x + y

        long_schema = StructType([StructField("a", LongType(), nullable=True)])
        df = self.spark.createDataFrame([Row(a=5)], schema=long_schema)
        with self.sql_conf(_TRANSPILE_ON):
            with _warnings.catch_warnings(record=True):
                _warnings.simplefilter("always")
                pudf_zero = UserDefinedFunction(zero, LongType())
                pudf_two = UserDefinedFunction(two, LongType())
            with self.assertRaises(PythonException):
                df.select(pudf_zero("a")).collect()
            with self.assertRaises(PythonException):
                df.select(pudf_two("a")).collect()

    def test_udf_transpile_decimal_input_falls_back(self):
        # Python receives decimal.Decimal objects, which raise TypeError when
        # mixed with float literals; the transpiled numeric lowering would
        # silently succeed. Decimal columns must fall back to interpreted
        # Python (pruned by input category at analysis time).
        from pyspark.errors import PythonException

        def add_half(x):
            return x + 1.5

        with self.sql_conf(_TRANSPILE_ON):
            # DoubleType: the return type must category-match the numeric body
            # for the option to be emitted (a string return type would itself
            # force a fallback before the decimal-input pruning under test).
            pudf = UserDefinedFunction(add_half, DoubleType())
            self.assertTrue(pudf.transpiled, "numeric option should still be produced")
            df = self.spark.sql("SELECT CAST(1.0 AS DECIMAL(10,2)) AS d")
            with self.assertRaises(PythonException):
                df.select(pudf("d")).collect()

    def test_udf_transpile_collated_string_falls_back(self):
        # Under a non-binary collation Spark's `=` follows collation rules
        # ('abc' = 'ABC' is true under UTF8_LCASE) while Python compares
        # codepoints. Collated columns must fall back to interpreted Python.
        def eq_abc(s):
            return s == "ABC"

        with self.sql_conf(_TRANSPILE_ON):
            pudf = UserDefinedFunction(eq_abc, BooleanType())
            self.assertTrue(pudf.transpiled, "string option should still be produced")
            df = self.spark.sql("SELECT 'abc' COLLATE UTF8_LCASE AS s")
            [result] = df.select(pudf("s")).collect()
            self.assertIs(result[0], False, "must match Python, not collation semantics")

    def test_udf_transpile_is_none_semantics(self):
        # `x is None` and `None is x` (and their `is not` variants) should
        # transpile to isNull/isNotNull. Any other identity check (`x is 0`,
        # `x is y`, `x is True`) must NOT transpile -- Python's `is` is an
        # object-identity test with no SQL equivalent outside of None.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        long_schema = StructType([StructField("a", LongType(), nullable=True)])

        def x_is_none(x):
            return x is None

        def x_is_not_none(x):
            if x is not None:
                return x + 1

        def none_is_x(x):
            return None is x

        def none_is_not_x(x):
            if None is not x:
                return x + 1

        def x_is_zero(x):
            return x is 0  # noqa: F632  identity vs equality

        def x_is_true(x):
            return x is True

        def x_is_y(x, y):
            return x is y

        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            # `x is None` and `None is x` should transpile and produce
            # identical results.
            for func, label in [(x_is_none, "x_is_none"), (none_is_x, "none_is_x")]:
                with self.subTest(case=label):
                    pudf = UserDefinedFunction(func, BooleanType())
                    self.assertTrue(
                        pudf.transpiled,
                        f"{label}: expected transpilation to succeed",
                    )
                    df = self.spark.createDataFrame([Row(a=None)], schema=long_schema)
                    [row] = df.select(pudf("a")).collect()
                    self.assertTrue(row[0], f"{label}: None is None should be True")
                    df = self.spark.createDataFrame([Row(a=1)], schema=long_schema)
                    [row] = df.select(pudf("a")).collect()
                    self.assertFalse(row[0], f"{label}: 1 is None should be False")

            # `x is not None` and `None is not x` should transpile.
            for func, label in [
                (x_is_not_none, "x_is_not_none"),
                (none_is_not_x, "none_is_not_x"),
            ]:
                with self.subTest(case=label):
                    pudf = UserDefinedFunction(func, LongType())
                    self.assertTrue(
                        pudf.transpiled,
                        f"{label}: expected transpilation to succeed",
                    )
                    df = self.spark.createDataFrame([Row(a=2)], schema=long_schema)
                    [row] = df.select(pudf("a")).collect()
                    self.assertEqual(row[0], 3, f"{label}: non-None input should return x+1")
                    df = self.spark.createDataFrame([Row(a=None)], schema=long_schema)
                    [row] = df.select(pudf("a")).collect()
                    self.assertIsNone(row[0], f"{label}: None input should return None")

            # Non-None identity checks must NOT transpile and must still
            # return correct results via interpreted Python.
            bool_schema = StructType([StructField("a", BooleanType(), nullable=True)])
            two_col_schema = StructType(
                [
                    StructField("a", LongType(), nullable=True),
                    StructField("b", LongType(), nullable=True),
                ]
            )
            non_none_cases = [
                # CPython interns small ints so `0 is 0` happens to be True in CPython,
                # but that is an implementation detail. The transpiler must still refuse
                # to lower these to isNull/isNotNull. We just verify: (a) no transpile,
                # (b) the interpreted result matches what Python actually produces.
                ("x_is_zero", x_is_zero, BooleanType(), long_schema, Row(a=0), True),
                # `True is True` is True because bool singletons are interned.
                ("x_is_true", x_is_true, BooleanType(), bool_schema, Row(a=True), True),
                ("x_is_y", x_is_y, BooleanType(), two_col_schema, Row(a=1, b=1), True),
            ]
            for label, func, return_type, schema, row, expected in non_none_cases:
                with self.subTest(case=label):
                    with _warnings.catch_warnings(record=True) as caught:
                        _warnings.simplefilter("always")
                        pudf = UserDefinedFunction(func, return_type)
                    self.assertEqual(
                        [],
                        pudf.transpiled,
                        f"{label}: non-None identity check must NOT transpile",
                    )
                    fallback = [
                        w
                        for w in caught
                        if "Unable to transpile" in str(w.message)
                        or "Errors encountered" in str(w.message)
                    ]
                    self.assertTrue(fallback, f"{label}: expected a fallback warning")
                    df = self.spark.createDataFrame([row], schema=schema)
                    args = ["a", "b"] if "b" in schema.fieldNames() else ["a"]
                    [result] = df.select(pudf(*args)).collect()
                    self.assertEqual(result[0], expected, f"{label}: interpreted result mismatch")

    def test_udf_transpile_not_bare_param_falls_back(self):
        # `not x` where x is a bare UDF parameter (unknown type at
        # transpile time) must NOT be lowered: Spark's `~` is bitwise, not
        # Python truthiness, so `not 0` would produce True via Python but
        # Spark's `~0L` is -1 (truthy). The transpiler must refuse and fall
        # back to interpreted Python.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def not_x(x):
            return not x

        long_schema = StructType([StructField("a", LongType(), nullable=True)])

        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            with _warnings.catch_warnings(record=True) as caught:
                _warnings.simplefilter("always")
                pudf = UserDefinedFunction(not_x, BooleanType())
            self.assertEqual([], pudf.transpiled, "not x on bare param must NOT transpile")
            fallback = [
                w
                for w in caught
                if "Unable to transpile" in str(w.message) or "Errors encountered" in str(w.message)
            ]
            self.assertTrue(fallback, "expected a fallback warning for `not x`")
            # Verify interpreted result is still correct.
            for value, expected in [(0, True), (1, False), (None, True)]:
                with self.subTest(value=value):
                    df = self.spark.createDataFrame([Row(a=value)], schema=long_schema)
                    [row] = df.select(pudf("a")).collect()
                    self.assertEqual(row[0], expected)

    def test_udf_transpile_and_or_bare_param_falls_back(self):
        # `x and y` / `x or y` where x/y are bare UDF parameters (unknown
        # type) must NOT be lowered: Python returns one of the operands
        # (truthiness semantics) while Spark's `&`/`|` are bitwise. The
        # transpiler must refuse and fall back.
        import warnings as _warnings

        from pyspark.sql.types import StructField, StructType

        def x_and_y(x, y):
            return x and y

        def x_or_y(x, y):
            return x or y

        schema = StructType(
            [
                StructField("a", LongType(), nullable=True),
                StructField("b", LongType(), nullable=True),
            ]
        )

        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                "spark.sql.ansi.enabled": True,
            }
        ):
            for func, label, row, expected in [
                (x_and_y, "x_and_y_falsy", Row(a=0, b=5), 0),
                (x_and_y, "x_and_y_truthy", Row(a=3, b=5), 5),
                (x_or_y, "x_or_y_falsy_left", Row(a=0, b=5), 5),
                (x_or_y, "x_or_y_truthy_left", Row(a=3, b=0), 3),
            ]:
                with self.subTest(case=label):
                    with _warnings.catch_warnings(record=True) as caught:
                        _warnings.simplefilter("always")
                        pudf = UserDefinedFunction(func, LongType())
                    self.assertEqual(
                        [],
                        pudf.transpiled,
                        f"{label}: and/or on bare params must NOT transpile",
                    )
                    fallback = [
                        w
                        for w in caught
                        if "Unable to transpile" in str(w.message)
                        or "Errors encountered" in str(w.message)
                    ]
                    self.assertTrue(fallback, f"{label}: expected a fallback warning")
                    df = self.spark.createDataFrame([row], schema=schema)
                    [result] = df.select(pudf("a", "b")).collect()
                    self.assertEqual(result[0], expected, f"{label}: interpreted result mismatch")

    def test_cannot_convert_column_into_bool_includes_column_repr(self):
        # The error fired by ``Column.__bool__`` should name the offending
        # column so users can see which expression triggered the fallback.
        from pyspark.errors import PySparkValueError

        df = self.spark.createDataFrame([Row(a=1, b=2)])
        col_a = df["a"]
        with self.assertRaises(PySparkValueError) as ctx:
            bool(col_a)
        message = str(ctx.exception)
        self.assertIn("Cannot convert column into bool", message)
        # Column's stringification is JVM-side and may render the column
        # as ``a`` (unresolved) or with a backtick variant, so we just
        # require the column name appears somewhere in the message.
        self.assertIn("a", message)

    def test_udf_transpile_category_inference_is_linear(self):
        import ast

        from pyspark.sql.transpile import CatalystTranspiler

        class CountingCatalystTranspiler(CatalystTranspiler):
            def __init__(self):
                super().__init__()
                self.category_calls = 0

            def _category_uncached(self, params, node):
                self.category_calls += 1
                return super()._category_uncached(params, node)

        expression = " + ".join(["a"] * 161)
        source = f"def f(a):\n    return {expression}\n"
        function_ast = ast.parse(source).body[0]
        self.assertIsInstance(function_ast, ast.FunctionDef)

        transpiler = CountingCatalystTranspiler()
        converted = transpiler._transpile_from_ast(
            source,
            function_ast,
            function_ast,
            ["a"],
            LongType(),
            {0: "numeric"},
        )

        self.assertIsNotNone(converted)
        self.assertLessEqual(transpiler.category_calls, len(list(ast.walk(function_ast))))

    # ------------------------------------------------------------------
    # Edge cases (SPARK-55206 follow-up). Helpers build a UDF with
    # transpilation on; `_vals` runs it and returns outputs (asserting it
    # transpiled), `_raises` asserts it raises. Arg columns come from the
    # schema. Operator cases are table-driven. Plan-elision checks count
    # `EvalPython` nodes because an ordering compare's `raise_error` message
    # contains "UDF" (so the "UDF" substring is unreliable).
    # ------------------------------------------------------------------

    @staticmethod
    def _udf_and_warnings(func, return_type):
        """Build a UDF, returning it with the text of any warnings it emitted.

        ``udf.py`` reports WHY a UDF fell back only as a warning, so every test that
        cares about a fallback needs them captured. The caller must already be inside
        ``sql_conf(_TRANSPILE_ON)`` -- this does not set the conf.
        """
        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            u = UserDefinedFunction(func, return_type)
        return u, " ".join(str(w.message) for w in caught)

    def _fallback_reason(self, func, return_type=LongType()):
        """Assert ``func`` produced no options, and hand back the reason it reported."""
        u, reasons = self._udf_and_warnings(func, return_type)
        self.assertEqual([], u.transpiled, f"{func} must fall back")
        self.assertTrue(reasons, f"{func} fell back without saying why")
        return u, reasons

    def _transpiled_udf(self, func, return_type):
        """A UDF asserted to have produced options; naming the fallback reason if not.

        Without the captured warning an empty ``transpiled`` asserts as bare "[] is
        not true", and from CI the text is only in a credentialed log artifact. The
        caller must already be inside ``sql_conf(_TRANSPILE_ON)``.
        """
        u, reasons = self._udf_and_warnings(func, return_type)
        self.assertTrue(u.transpiled, f"{func} produced no transpiled options: {reasons}")
        return u

    def _vals(self, func, return_type, schema, rows, require_lowered=True):
        with self.sql_conf(_TRANSPILE_ON):
            u = self._transpiled_udf(func, return_type)
            df = self.spark.createDataFrame(rows, schema)
            projected = df.select(u(*df.columns))
            # ``u.transpiled`` only says options were PRODUCED; the JVM may still
            # discard them and run interpreted Python, returning the right value and
            # hiding a wrong lowering. Pass ``require_lowered=False`` only where the
            # JVM is EXPECTED to discard them.
            if require_lowered:
                self.assertEqual(0, self._eval_python_count(projected), str(func))
            return [r[0] for r in projected.collect()]

    def _raises(self, func, schema, rows, needle="numeric", return_type=None):
        # `is None`, not `or`: a DataType can be falsy (an empty StructType defines
        # __len__), so `return_type or LongType()` would silently swap it out.
        with self.sql_conf(_TRANSPILE_ON):
            u = self._transpiled_udf(func, LongType() if return_type is None else return_type)
            df = self.spark.createDataFrame(rows, schema)
            with self.assertRaises(Exception) as ctx:
                df.select(u(*df.columns)).collect()
            self.assertIn(needle, str(ctx.exception).lower(), str(func))

    @staticmethod
    def _eval_python_count(df):
        return df._jdf.queryExecution().executedPlan().toString().count("EvalPython")

    @staticmethod
    def _optimized_plan(df):
        return df._jdf.queryExecution().optimizedPlan().toString()

    # Internal plan-string details: the alias a pre-evaluated argument gets (ConvertToCatalyst
    # names them ``_udf_param_N``), and the token a plan shows for a draw it still evaluates.
    _SHARED_ARG_ALIAS = "_udf_param_"
    _DRAW = "rand("

    def _shares_an_argument(self, df):
        return self._SHARED_ARG_ALIAS in self._optimized_plan(df)

    def _draw_count(self, df):
        return self._optimized_plan(df).count(self._DRAW)

    def test_udf_transpile_evaluates_inputs_once(self):
        # SPARK-58626: an argument the body uses twice is drawn once per row, so `x == x` over a
        # random input is True on every row and `x if x > 0.5 else 0.0` never returns a value its
        # own condition would have rejected.
        from pyspark.sql.functions import expr, lit, rand

        eq_self = lambda x: x == x  # noqa: E731
        clamp = lambda x: x if x > 0.5 else 0.0  # noqa: E731
        square_fn = lambda x: x * x  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            eq_udf = UserDefinedFunction(eq_self, BooleanType())
            clamp_udf = UserDefinedFunction(clamp, DoubleType())
            self.assertTrue(eq_udf.transpiled and clamp_udf.transpiled)
            rows = self.spark.range(100)

            # `functions.rand()` bakes a literal seed in, but `expr("rand()")` arrives with the seed
            # unresolved and ResolveRandomSeed then gives each spliced copy its OWN seed. Both have
            # to end up sharing one evaluation, or `x == x` compares two independent draws.
            #
            # Sharing leaves `col = col`, which SimplifyBinaryComparison folds to true and column
            # pruning then drops the draw entirely -- so zero draws in the plan is the proof that
            # both sides became the same column. Two independent draws would leave both.
            for arg in (rand(), expr("rand()")):
                eq_df = rows.select(eq_udf(arg).alias("v"))
                self.assertEqual(0, self._draw_count(eq_df))
                self.assertTrue(all(r[0] for r in eq_df.collect()))

            clamped = rows.select(clamp_udf(rand()).alias("v"))
            self.assertEqual(1, self._draw_count(clamped))
            vals = [r[0] for r in clamped.collect()]
            self.assertTrue(all(v == 0.0 or v > 0.5 for v in vals), vals)
            # Both branches have to run for the check above to mean anything.
            self.assertTrue(any(v == 0.0 for v in vals) and any(v > 0.5 for v in vals), vals)

            # A literal and a plain column are as cheap to read twice as to share, so the optimizer
            # puts them back at each use site and neither grows the plan.
            square = UserDefinedFunction(square_fn, LongType())
            self.assertTrue(square.transpiled)
            lit_df = rows.select(square(lit(3)).alias("v"))
            self.assertFalse(self._shares_an_argument(lit_df))
            self.assertEqual([9] * 100, [r[0] for r in lit_df.collect()])
            col_df = self.spark.range(3).select(square("id").alias("v"))
            self.assertFalse(self._shares_an_argument(col_df))
            self.assertEqual([0, 1, 4], [r[0] for r in col_df.collect()])

    def test_udf_transpile_shares_a_python_udf_argument(self):
        # SPARK-58626: an argument that is itself a nondeterministic Python UDF gets a column like
        # any other draw the body reads twice, even though CollapseProject.isCheap counts a
        # PythonUDF as cheap. Cheapness is about wasted work; two calls would be two draws, and
        # `a - a` would come out nonzero.
        #
        # ExtractPythonUDFs would have pointed both copies at one result attribute anyway, so this
        # was the right answer before the column too -- by accident, from an unrelated rule, and at
        # the cost of a second Python round trip (the dedup runs through an ExpressionSet, which
        # holds a nondeterministic expression once per add).
        import random

        draw = lambda x: random.random()  # noqa: E731
        subtract_self = lambda a: a - a  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            rand_udf = UserDefinedFunction(draw, DoubleType()).asNondeterministic()
            diff = UserDefinedFunction(subtract_self, DoubleType())
            self.assertTrue(diff.transpiled)
            df = self.spark.range(20).select(diff(rand_udf("id")))
            self.assertTrue(self._shares_an_argument(df), self._optimized_plan(df))
            self.assertEqual([0.0] * 20, [r[0] for r in df.collect()], "One draw, read twice")

    def test_udf_transpile_repeats_a_deterministic_python_udf_argument(self):
        # SPARK-58626: a deterministic Python UDF argument is cheap to repeat as far as
        # CollapseProject.isCheap is concerned, so it stays at each use site instead of getting a
        # column -- and ExtractPythonUDFs then points both copies at one eval node, so the body
        # still reads one evaluation. What we promise is the evaluation count, not which rule keeps
        # it, and this is the shape where another rule does. A second argument because a call whose
        # arguments are all Python UDFs keeps the batch pipeline instead of lowering.
        def helper(v):
            return v + 1

        opaque = lambda x: helper(x)  # noqa: E731
        first_twice = lambda a, b: a - a + b  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            inner = UserDefinedFunction(opaque, LongType())
            self.assertEqual([], inner.transpiled, "The inner UDF has to stay Python")
            outer = self._transpiled_udf(first_twice, LongType())
            df = self.spark.range(5).select(outer(inner("id"), "id"))
            plan = self._optimized_plan(df)
            self.assertFalse(self._shares_an_argument(df), plan)
            self.assertEqual(
                1,
                self._eval_python_count(df),
                df._jdf.queryExecution().executedPlan().toString(),
            )
            self.assertEqual(list(range(5)), [r[0] for r in df.collect()])

    def test_udf_transpile_declines_inside_a_higher_order_function(self):
        # SPARK-58626: a call inside a lambda is not lowered at all -- Spark applies a Python UDF
        # over the whole array there and we leave that to it -- so the draw stays a single draw per
        # call rather than one per read.
        #
        # `clamp` returns its input or 0.0, so a value in (0.0, 0.5] would be two draws: the
        # condition saw one and the branch another.
        from pyspark.sql.functions import array, col, lit, rand, transform

        clamp = lambda x: x if x > 0.5 else 0.0  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            c = self._transpiled_udf(clamp, DoubleType())
            arr = self.spark.range(500).select(array(lit(1.0), lit(2.0)).alias("a"))
            hof = arr.select(transform(col("a"), lambda e: c(rand())).alias("v"))
            self.assertGreater(self._eval_python_count(hof), 0, self._optimized_plan(hof))
            vals = [v for r in hof.collect() for v in r["v"]]
            self.assertTrue(vals and all(v == 0.0 or v > 0.5 for v in vals), vals)

    def test_udf_transpile_declines_under_an_aggregate(self):
        # SPARK-58626: an Aggregate can't host the column -- a result expression no aggregate
        # function wraps has to be built from the grouping expressions -- so a body reading a
        # nondeterministic argument twice keeps the interpreted UDF, which draws once. `clamp`
        # returns its input or 0.0, so a value in (0.0, 0.5] would be two draws.
        from pyspark.sql.functions import col, rand

        clamp = lambda x: x if x > 0.5 else 0.0  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            c = self._transpiled_udf(clamp, DoubleType())
            agg = self.spark.range(500).groupBy(col("id").alias("g")).agg(c(rand()).alias("v"))
            self.assertGreater(self._eval_python_count(agg), 0, self._optimized_plan(agg))
            vals = [r["v"] for r in agg.collect()]
            self.assertTrue(all(v == 0.0 or v > 0.5 for v in vals), vals)
            self.assertTrue(any(v == 0.0 for v in vals) and any(v > 0.5 for v in vals), vals)

    def test_udf_transpile_gives_each_call_its_own_draw(self):
        # SPARK-58626: one draw per parameter, not one per operator. Two calls each passing their
        # own `rand()` owe two draws, so the doubled values differ on every row. Keying a shared
        # column on the parameter index alone put both bodies on one draw and made this exactly 0.0.
        from pyspark.sql.functions import rand

        add_self = lambda x: x + x  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            u = self._transpiled_udf(add_self, DoubleType())
            df = self.spark.range(500).select((u(rand()) - u(rand())).alias("v"))
            self.assertEqual(0, self._eval_python_count(df))
            self.assertEqual(2, self._optimized_plan(df).count("AS " + self._SHARED_ARG_ALIAS))
            diffs = [r["v"] for r in df.collect()]
            self.assertTrue(all(d != 0.0 for d in diffs), "each call draws for itself")

    def test_udf_transpile_position_can_rule_out_sharing(self):
        # SPARK-58626: inside a lambda nothing is lowered, so nothing is pre-evaluated either -- the
        # empty array below must NOT raise even though the argument divides by zero.
        #
        # A conditional branch, by contrast, DOES get one evaluation, even though a bare Catalyst
        # `when` is lazy. Measured: the interpreted Python UDF this replaces evaluates its inputs
        # in a projection below the conditional and raises there too, so eager is the Python-parity
        # answer as well as the cheaper one. See transpile.py.
        from pyspark.sql.functions import array, col, lit, rand, transform, when

        clamp = lambda x: x if x > 0.5 else 0.0  # noqa: E731
        used_twice = lambda a, b: (a + a) + b  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            c = UserDefinedFunction(clamp, DoubleType())
            self.assertTrue(c.transpiled)
            rows = self.spark.range(200).select((col("id") + 1).cast("double").alias("x"))

            nested = rows.select(when(col("x") > 0, c(rand())).otherwise(lit(-1.0)).alias("v"))
            vals = [r[0] for r in nested.collect()]
            # `x` is `id + 1`, so the otherwise branch never fires and every value came from the
            # body -- which means a value in (0.0, 0.5] could only be the body seeing two draws.
            self.assertTrue(
                all(v == 0.0 or v > 0.5 for v in vals),
                f"a conditional branch must share one draw too: {vals}",
            )
            self.assertEqual(1, self._draw_count(nested))
            self.assertTrue(self._shares_an_argument(nested))

            s = UserDefinedFunction(used_twice, DoubleType())
            self.assertTrue(s.transpiled)
            df = self.spark.createDataFrame([(1.0, 0.0)], "a double, b double").select(
                col("a"), col("b"), array().cast("array<double>").alias("arr")
            )
            lazy = df.select(transform(col("arr"), lambda e: s(col("a") / col("b"), e)).alias("v"))
            self.assertEqual([[]], [r[0] for r in lazy.collect()])

    def test_udf_transpile_shares_a_nondeterministic_argument_in_a_predicate(self):
        # SPARK-58626: a `where` keeps the shared column when the argument is nondeterministic.
        # PushPredicateThroughNonJoin only pushes a Filter through a Project whose fields are all
        # deterministic, so the draw is not substituted back and the body sees one value. A
        # deterministic argument IS pushed back and evaluated per use -- same rows, just extra work.
        from pyspark.sql.functions import col, rand

        add_self = lambda x: x + x  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            u = UserDefinedFunction(add_self, DoubleType())
            self.assertTrue(u.transpiled)

            nd = self.spark.range(0, 10).where(u(rand()) > 0.5)
            self.assertTrue(self._shares_an_argument(nd), self._optimized_plan(nd))
            self.assertEqual(1, self._draw_count(nd))

            det = self.spark.range(0, 10).where(u((col("id") % 3).cast("double")) > 1.0)
            self.assertFalse(self._shares_an_argument(det), self._optimized_plan(det))

    def test_udf_transpile_udf_as_a_predicate(self):
        # A predicate is transpiled like anything else in a `where`: no Python worker, same answers.
        # In a join condition it is not, since no column can go there -- see below.
        from pyspark.sql.functions import col

        used_twice = lambda a, b: (a + a) if b > 0 else 0.0  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            u = UserDefinedFunction(used_twice, DoubleType())
            self.assertTrue(u.transpiled)
            df = self.spark.createDataFrame([(1.0, 2.0), (4.0, 2.0)], "a double, b double")
            other = self.spark.createDataFrame([(2.0,)], "c double")
            predicate = u(col("a") / col("b"), col("b")) > 2.0

            filtered = df.where(predicate)
            self.assertEqual(0, self._eval_python_count(filtered))
            self.assertEqual([(4.0, 2.0)], [(r[0], r[1]) for r in filtered.collect()])

            # A join has no side to hold the column, so the call goes back to Python -- the same
            # path the predicate takes with transpilation off.
            joined = df.join(other, predicate)
            self.assertGreater(self._eval_python_count(joined), 0, self._optimized_plan(joined))
            self.assertEqual([(4.0, 2.0, 2.0)], [tuple(r) for r in joined.collect()])

    def test_udf_transpile_survives_an_argument_the_analyzer_moved(self):
        # SPARK-58626: PullOutNondeterministic pulls a nondeterministic call into a projection below
        # an operator that cannot hold one, leaving an attribute where the call was -- and so no
        # arguments for the option's references to point at. The call keeps the Python path there
        # rather than failing the query, which is also one evaluation: the projection computes it
        # once. Master lowered these, with the draw spliced into the option body and read twice from
        # the column PullOutNondeterministic left; this gives up that lowering, not correctness.
        from pyspark.sql.functions import col, rand

        add_self = lambda x: x + x  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            u = self._transpiled_udf(add_self, DoubleType())
            rows = self.spark.range(0, 4).select(col("id").cast("double").alias("a"))
            self.assertEqual(4, len(rows.orderBy(u(rand())).collect()))
            self.assertEqual(4, len(rows.repartition(2, u(rand())).collect()))
            self.assertEqual(4, len(rows.groupBy(u(rand())).count().collect()))

    def test_udf_transpile_evaluates_inputs_once_in_a_group_by(self):
        # SPARK-58626: an Aggregate is the awkward one -- a use no aggregate function wraps has to
        # *be* a grouping expression, so it cannot read a pre-evaluated column. `x * x` reads its
        # parameter twice and `a + 1` is not cheap, so all three shapes here keep the interpreted
        # UDF, which evaluates the argument once itself. This pins the answers and that fallback.
        from pyspark.sql.functions import col
        from pyspark.sql.functions import sum as sum_

        square_fn = lambda x: x * x  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            square = UserDefinedFunction(square_fn, LongType())
            self.assertTrue(square.transpiled)
            df = self.spark.createDataFrame([(1,), (2,), (2,)], "a long")

            grouped = df.groupBy(col("a") + 1).agg(square(col("a") + 1).alias("v"))
            self.assertEqual([(2, 4), (3, 9)], sorted((r[0], r[1]) for r in grouped.collect()))

            summed = df.agg(sum_(square(col("a") + 1)).alias("s"))
            self.assertEqual([22], [r[0] for r in summed.collect()])

            mixed = df.groupBy(col("a") + 1).agg(
                square(col("a") + 1).alias("v"), sum_(square(col("a") + 1)).alias("s")
            )
            self.assertEqual([(2, 4, 4), (3, 9, 18)], sorted(tuple(r) for r in mixed.collect()))

    def test_udf_transpile_evaluates_inputs_once_in_a_subquery(self):
        # SPARK-58626: end to end because decorrelation is what makes this interesting. In the last
        # shape the shared argument reads a column of the OUTER query, so the pre-evaluated column
        # lands inside the subquery holding an OuterReference, and decorrelation has to carry it
        # back out. With decorrelation off we decline the column instead -- see ConvertToCatalyst.
        square_fn = lambda x: x * x  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            square = UserDefinedFunction(square_fn, LongType())
            self.assertTrue(square.transpiled)
            # Both registrations go through SQLTestUtils' context managers: ReusedSQLTestCase
            # shares one session across the class, so a failure between a bare register() and the
            # try would leave sq_test_58626 in every later test's session.
            with self.temp_func("sq_test_58626"), self.temp_view("t_58626"):
                self.spark.udf.register("sq_test_58626", square)
                self.spark.range(0, 5).selectExpr("id as a").createOrReplaceTempView("t_58626")
                uncorrelated = self.spark.sql(
                    "SELECT a FROM t_58626 WHERE a < "
                    "(SELECT max(sq_test_58626(a + 1)) FROM t_58626)"
                )
                self.assertEqual([0, 1, 2, 3, 4], [r[0] for r in uncorrelated.collect()])

                correlated = self.spark.sql(
                    "SELECT a FROM t_58626 o WHERE a < "
                    "(SELECT max(sq_test_58626(i.a + 1)) FROM t_58626 i WHERE i.a = o.a)"
                )
                self.assertEqual([0, 1, 2, 3, 4], sorted(r[0] for r in correlated.collect()))

                outer_arg = self.spark.sql(
                    "SELECT a FROM t_58626 o WHERE EXISTS "
                    "(SELECT 1 FROM t_58626 i WHERE sq_test_58626(o.a + 1) = i.a)"
                )
                self.assertEqual([0, 1], sorted(r[0] for r in outer_arg.collect()))

    def test_udf_transpile_puts_no_column_directly_above_an_aggregate(self):
        # SPARK-58626: `splitSubquery` finds the Aggregate a correlated scalar subquery is built on
        # by matching `Filter(_, Aggregate)` -- adjacency, not a search -- in an earlier batch than
        # the pushdown that would collapse a Project away. A column between a HAVING and its
        # Aggregate hid it, `mayHaveCountBug` read false, and the outer rows matching nothing were
        # dropped instead of counted 0: measured as [] where the same plan without the column
        # returns [3, 4]. So nothing goes directly above an Aggregate. The call keeps the Python UDF
        # and raises here exactly as transpilation-off does, because a Python eval node lands
        # between the Filter and the Aggregate just the same -- that half is not ours.
        square_fn = lambda x: x * x  # noqa: E731
        legacy = {**_TRANSPILE_ON, "spark.sql.legacy.scalarSubqueryCountBugBehavior": True}
        # `count(*) + 1` is read twice and is not cheap, so it is what would be owed a column.
        query = (
            "SELECT o.a FROM t_out_58626 o WHERE "
            "(SELECT count(*) FROM t_in_58626 i WHERE i.a = o.a "
            "HAVING sq_test_58626(count(*) + 1) > 0) = 0"
        )
        with self.sql_conf(legacy):
            square = UserDefinedFunction(square_fn, LongType())
            self.assertTrue(square.transpiled)
            with (
                self.temp_func("sq_test_58626"),
                self.temp_view("t_out_58626"),
                self.temp_view("t_in_58626"),
            ):
                self.spark.udf.register("sq_test_58626", square)
                self.spark.range(0, 5).selectExpr("id as a").createOrReplaceTempView("t_out_58626")
                self.spark.range(0, 3).selectExpr("id as a").createOrReplaceTempView("t_in_58626")
                df = self.spark.sql(query)
                plan = self._optimized_plan(df)
                self.assertNotIn(self._SHARED_ARG_ALIAS, plan, plan)
                self.assertEqual(1, self._eval_python_count(df), plan)

    def test_udf_transpile_unused_argument_diverges_under_ansi(self):
        # SPARK-58626: an argument the body never uses never reaches the option, so nothing
        # evaluates it, where the interpreted UDF computes every argument column. Under ANSI
        # that is an error-vs-rows difference rather than merely saved work.
        from pyspark.sql.functions import col, lit

        first = lambda a, b: a  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            u = UserDefinedFunction(first, LongType())
            self.assertTrue(u.transpiled)
            df = self.spark.createDataFrame([(3,)], "x long")
            rows = df.select(u(col("x"), col("x") / lit(0))).collect()
            self.assertEqual([3], [r[0] for r in rows])
        with self.sql_conf(_TRANSPILE_OFF_ANSI_ON):
            u_i = UserDefinedFunction(first, LongType())
            df = self.spark.createDataFrame([(3,)], "x long")
            with self.assertRaises(Exception) as ctx:
                df.select(u_i(col("x"), col("x") / lit(0))).collect()
            self.assertIn("DIVIDE_BY_ZERO", str(ctx.exception))

    def test_udf_transpile_lowers_operators(self):
        # Operators lower to Catalyst and match Python: modulo sign-parity,
        # non-commutative -/* (parameter order), unary nesting, constant
        # body, not(compare), nested boolean, string ==/<, reversed-operand and
        # column-to-column comparisons, if/elif/else, and assigned lambdas.
        L, B = LongType(), BooleanType()
        modulo = lambda x, y: x % y  # noqa: E731
        subtract = lambda a, b: a - b  # noqa: E731
        multiply = lambda a, b: a * b  # noqa: E731
        double_neg = lambda x: --x  # noqa: E731
        unary_pm = lambda x: +(-x)  # noqa: E731
        constant = lambda x: 42  # noqa: E731
        not_pos = lambda x: (not (x > 0)) if x is not None else None  # noqa: E731
        nested = lambda x, y, z: ((x > 0) and (y > 0)) or (z == 0)  # noqa: E731
        str_eq = lambda x: (x == "foo") if x is not None else None  # noqa: E731
        str_lt = lambda x: (x < "m") if x is not None else None  # noqa: E731
        rev_lt = lambda x: (0 < x) if x is not None else None  # noqa: E731
        rev_eq = lambda x: 5 == x  # noqa: E731
        none_eq = lambda x: None == x  # noqa: E711,E731
        col_lt = lambda a, b: (a < b) if a is not None and b is not None else None  # noqa: E731
        assigned = lambda v: v + 1  # noqa: E731

        def if_elif_else(x):
            if x is None:
                return -1
            elif x == 0:
                return 0
            else:
                return 1

        # (func, return_type, schema, rows, expected); arg columns come from the schema.
        cases = [
            (modulo, L, "a long, b long", [(7, 3), (7, -3), (-7, 3), (-7, -3)], [1, -2, 2, -1]),
            (subtract, L, "a long, b long", [(5, 3), (3, 5)], [2, -2]),
            (multiply, L, "a long, b long", [(4, 3), (-2, 5)], [12, -10]),
            (double_neg, L, "a long", [(5,), (-3,)], [5, -3]),
            (unary_pm, L, "a long", [(5,), (-3,)], [-5, 3]),
            (constant, L, "a long", [(1,), (999,)], [42, 42]),
            (not_pos, B, "a long", [(1,), (0,), (-1,), (None,)], [False, True, True, None]),
            (str_eq, B, "a string", [("foo",), ("bar",), (None,)], [True, False, None]),
            (str_lt, B, "a string", [("a",), ("z",), (None,)], [True, False, None]),
            (rev_lt, B, "a long", [(1,), (0,), (-1,)], [True, False, False]),
            (rev_eq, B, "a long", [(5,), (3,), (None,)], [True, False, False]),
            (none_eq, B, "a long", [(None,), (5,)], [True, False]),
            (col_lt, B, "a long, b long", [(1, 2), (2, 1), (1, 1)], [True, False, False]),
            (if_elif_else, L, "a long", [(None,), (0,), (5,), (-3,)], [-1, 0, 1, 1]),
            (assigned, L, "a long", [(1,), (10,)], [2, 11]),
            (
                nested,
                B,
                "a long, b long, c long",
                [(1, 1, 5), (-1, 1, 0), (-1, 1, 5)],
                [True, True, False],
            ),
        ]
        for i, (func, rt, schema, rows, expected) in enumerate(cases):
            with self.subTest(case=i):
                self.assertEqual(self._vals(func, rt, schema, rows), expected, f"case {i}: {rows}")

    def test_udf_transpile_callable_object_drops_its_receiver(self):
        # A callable instance's `self` is dropped before anything indexes the param
        # list, so a/b are _udf_param_0/_udf_param_1 with no offsetting anywhere
        # downstream (the non-commutative body proves the order).
        class SubAB:
            def __call__(self, a, b):
                return a - b

        self.assertEqual(
            self._vals(SubAB(), LongType(), "a long, b long", [(5, 3), (3, 5)]), [2, -2]
        )

    def test_udf_transpile_plan_elision(self):
        # Transpiled UDFs are elided in filter (not just select); a mixed
        # non-convertible -> convertible -> non-convertible chain inlines only
        # the middle UDF, leaving exactly two Python eval nodes.
        offset = 3
        gt5 = lambda x: (x > 5) if x is not None else None  # noqa: E731
        add_offset = lambda x: x + offset  # noqa: E731  closure -> fallback
        plus_one = lambda x: x + 1  # noqa: E731  convertible
        div_two = lambda x: x / 2  # noqa: E731  `/` -> fallback
        with self.sql_conf(_TRANSPILE_ON):
            f = UserDefinedFunction(gt5, BooleanType())
            self.assertTrue(f.transpiled)
            fdf = self.spark.createDataFrame([(3,), (7,), (1,), (None,)], "a long").filter(f("a"))
            self.assertEqual([r[0] for r in fdf.collect()], [7])
            self.assertEqual(0, self._eval_python_count(fdf))

            u1 = UserDefinedFunction(add_offset, LongType())
            u2 = UserDefinedFunction(plus_one, LongType())
            u3 = UserDefinedFunction(div_two, DoubleType())
            self.assertEqual(([], True, []), (u1.transpiled, bool(u2.transpiled), u3.transpiled))
            chained = (
                self.spark.createDataFrame([(10,)], "a long")
                .select(u1("a").alias("x"))
                .select(u2("x").alias("y"))
                .select(u3("y").alias("z"))
            )
            self.assertEqual(chained.first()[0], 7.0)  # ((10 + 3) + 1) / 2
            self.assertEqual(2, self._eval_python_count(chained))

    def test_udf_transpile_config_toggle_no_stale_nodes(self):
        # Built with the flags on, executed with them off -> clean fallback to
        # interpreted Python (the optimizer drops the transpiled node), no error.
        plus_one = lambda x: x + 1  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            u = UserDefinedFunction(plus_one, LongType())
            self.assertTrue(u.transpiled)
        with self.sql_conf(
            {
                "spark.sql.experimental.optimizer.transpilePyUDFs": False,
                "spark.sql.ansi.enabled": False,
            }
        ):
            df = self.spark.createDataFrame([(1,), (5,)], "a long")
            self.assertEqual([r[0] for r in df.select(u("a")).collect()], [2, 6])

    def test_udf_transpile_casts_to_return_type(self):
        # The lowered expression is cast to the declared return type.
        plus_one = lambda x: x + 1  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            d = UserDefinedFunction(plus_one, DoubleType())
            col = self.spark.createDataFrame([(1,)], "a long").select(d("a").alias("r"))
            self.assertEqual(col.schema["r"].dataType, DoubleType())
            self.assertEqual(col.first()[0], 2.0)
        self.assertEqual(self._vals(plus_one, LongType(), "a long", [(1,)]), [2])

    def test_udf_transpile_falls_back(self):
        # Shapes that must NOT transpile (and still compute via Python):
        # inline/wrapped/partial lambdas, default/variadic/keyword-only args, and
        # `%` string formatting. (String `+`/`*` now lower to concat/repeat -- see
        # test_udf_transpile_string_operands -- but `%` as a format is not handled.)
        import functools

        def wrapper(fn):
            return fn

        def with_default(a, b=0):
            return a + 10 * b

        def with_varargs(a, *rest):
            return a

        def with_kwargs(a, **opts):
            return a

        base = lambda v, w: v + w  # noqa: E731
        percent_fmt = lambda x: "n=%d" % x  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            # An inline or wrapped lambda is a call ARGUMENT, so the source is
            # read fine but parses as ``Call`` rather than a definition we can
            # unwrap; ``functools.partial`` has no reachable source at all.
            self.assertEqual([], UserDefinedFunction(lambda v: v + 1, LongType()).transpiled)
            self.assertEqual(
                [], UserDefinedFunction(wrapper(lambda v: v + 1), LongType()).transpiled
            )
            self.assertEqual(
                [], UserDefinedFunction(functools.partial(base, 1), LongType()).transpiled
            )
            # default / variadic / keyword-only args, and `%` string formatting
            for func, rt in [
                (with_default, LongType()),
                (with_varargs, LongType()),
                (with_kwargs, LongType()),
                (percent_fmt, StringType()),
            ]:
                with self.subTest(func=func):
                    self.assertEqual([], UserDefinedFunction(func, rt).transpiled)
            # Fell back -> interpreted Python still computes correctly.
            wd = UserDefinedFunction(with_default, LongType())
            num = self.spark.createDataFrame([(5,)], "a long")
            self.assertEqual(
                [num.select(wd("a")).first()[0], num.select(wd("a", "a")).first()[0]], [5, 55]
            )

    def test_udf_transpile_falls_back_when_a_sibling_lambda_shares_the_line(self):
        # ``inspect.getsource`` works in whole lines, so every lambda on a line hands
        # back the same source and nothing in it says which one we hold. Taking
        # whichever came first was right by position rather than by identity:
        # ``minus_one`` lowered ``x + 1``, and the lambda in the decorator lowered
        # the decorated ``def``'s body -- both silently wrong. Refusing costs
        # ``plus_one``, which the first-match rule happened to get right; being right
        # for 1-of-N by position is not something a caller can rely on (SPARK-58650).
        #
        # ``fmt: off`` keeps the two on one line; asserted below, since the formatter
        # would otherwise split them and quietly make this test vacuous.
        # fmt: off
        plus_one = lambda x: x + 1; minus_one = lambda x: x - 1  # noqa: E702,E731
        # fmt: on
        self.assertEqual(
            plus_one.__code__.co_firstlineno,
            minus_one.__code__.co_firstlineno,
            "fixture must keep both lambdas on ONE line or it proves nothing",
        )

        captured = []

        def capture(g):
            captured.append(g)
            return lambda fn: fn

        @capture(lambda x: x + 1)
        def unrelated(a):
            return a * 12345

        (decorator_lambda,) = captured

        # A class body is the same hazard: master read the whole line and lowered
        # ``helper``, computing 5*100 for a UDF that really returns 5*2.
        class TwoOnALine:
            helper, __call__ = lambda self, x: x * 100, lambda self, x: x * 2

        self.assertEqual(10, TwoOnALine()(5))

        num = self.spark.createDataFrame([(5,)], "a long")
        sibling = "put each lambda on its own line"
        not_a_statement = "does not define it as a statement of its own"
        with self.sql_conf(_TRANSPILE_ON):
            for label, func, expected, needle in [
                # Lowered the WRONG body before -- the bug.
                ("second lambda on the line", minus_one, 4, sibling),
                ("lambda inside a decorator", decorator_lambda, 6, not_a_statement),
                ("__call__ sharing a class-body line", TwoOnALine(), 10, not_a_statement),
                # Lowered correctly before; refused now as acknowledged collateral.
                ("first lambda on the line", plus_one, 6, sibling),
            ]:
                with self.subTest(case=label):
                    u, reasons = self._fallback_reason(func)
                    # Assert the REASON, so relaxing the guard fails here loudly
                    # rather than passing for a new and unrelated cause.
                    self.assertIn(needle, reasons)
                    self.assertEqual(expected, num.select(u("a")).first()[0])

        # The ``def`` itself is NOT collateral: we know we are holding it, so a
        # lambda in its decorator cannot be the body and does not block lowering.
        self.assertEqual(self._vals(unrelated, LongType(), "a long", [(5,)]), [61725])

    def test_udf_transpile_refuses_a_lambda_whose_source_now_reads_as_a_def(self):
        # ``inspect.getsource`` reads the file as it is NOW, so an edit after import
        # can hand back source holding no lambda at all. Refusing only when a SIBLING
        # lambda is in view failed open here, lowering an unrelated ``def`` as the
        # body: 5 * 12345 for a lambda Python evaluates to 6.
        import importlib.util
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "drifted_lambda.py")
            with open(path, "w") as handle:
                handle.write("f = lambda x: x + 1\n")
            spec = importlib.util.spec_from_file_location("drifted_lambda", path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            with open(path, "w") as handle:
                handle.write("def unrelated(a):\n    return a * 12345\n")

            with self.sql_conf(_TRANSPILE_ON):
                u, reasons = self._fallback_reason(module.f)
                # Pin the REASON, or a source read that merely failed would pass too.
                self.assertIn("which lambda to lower cannot be determined", reasons)
                num = self.spark.createDataFrame([(5,)], "a long")
                self.assertEqual(6, num.select(u("a")).first()[0])

    def test_udf_transpile_ambiguity_check_sees_only_rival_lambdas(self):
        # Two ways the check used to misfire or not fire at all.
        from pyspark.sql.transpile import _held_code

        # A lambda nested in the held lambda's own body is not a rival -- it can
        # never be the UDF, and the user cannot split it onto another line. It fell
        # back with the sibling message, advice that could not be acted on.
        nested = lambda x: (lambda y: y + 1)(x)  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            _, reasons = self._fallback_reason(nested)
            self.assertIn("Call", reasons, "must fall back for the body, not for ambiguity")
            self.assertNotIn("put each lambda on its own line", reasons)

        # The mirror: a lambda RETURNED by a one-line lambda. Here the outer one is
        # what the source read locates and the inner one is what we hold, so treating
        # nested lambdas as never-rivals let this through -- and it proceeded with the
        # outer signature, reporting `n` as the UDF's parameter for a UDF whose only
        # parameter is `x`. Matching the located lambda's parameters against the held
        # code object's is what separates this from the case above.
        # fmt: off
        make_adder = lambda n: lambda x: x + n  # noqa: E731
        # fmt: on
        add_three = make_adder(3)
        self.assertEqual(8, add_three(5))
        with self.sql_conf(_TRANSPILE_ON):
            u, reasons = self._fallback_reason(add_three)
            self.assertIn("takes different parameters", reasons)
            self.assertEqual([], u._transpiled_param_names or [])
            self.assertEqual(
                8, self.spark.createDataFrame([(5,)], "a long").select(u("a")).first()[0]
            )

        # ``staticmethod``/``classmethod`` hide ``__code__`` behind the descriptor, so
        # reading it off them left the guard inactive -- skipping the check for a shape
        # it exists to catch. Now that these lower at all, the skip would be a wrong
        # answer: ``helper`` shares the line and takes the same parameter, so it is a
        # true rival and would be lowered instead (5 * 9, not 5 * 2).
        class Wrapped:
            # fmt: off
            helper = lambda x: x * 9; __call__ = staticmethod(lambda x: x * 2)  # noqa: E702,E731
            # fmt: on

        self.assertEqual(
            Wrapped.helper.__code__.co_firstlineno,
            Wrapped.__call__.__code__.co_firstlineno,
            "fixture must keep both lambdas on ONE line or it proves nothing",
        )
        self.assertEqual(
            "<lambda>",
            getattr(_held_code(Wrapped()), "co_name", None),
            "the held code must be found inside the descriptor",
        )
        self.assertEqual(10, Wrapped()(5))
        with self.sql_conf(_TRANSPILE_ON):
            u, reasons = self._fallback_reason(Wrapped())
            self.assertIn("put each lambda on its own line", reasons)
            num = self.spark.createDataFrame([(5,)], "a long")
            self.assertEqual(10, num.select(u("a")).first()[0])

    def test_udf_transpile_lowers_an_annotated_lambda_binding(self):
        # An annotated binding is the same shape as a plain one, and the form a typed
        # codebase writes. Only ``ast.Assign`` was unwrapped, so this was refused as
        # "not a statement of its own" -- while the module docstring told users to
        # bind the lambda to a name and give it a line, which is exactly this.
        from typing import Callable

        annotated: Callable[[int], int] = lambda x: x + 1  # noqa: E731
        self.assertEqual(self._vals(annotated, LongType(), "a long", [(5,)]), [6])

    def test_udf_transpile_recovers_shapes_with_an_unheld_lambda_in_view(self):
        # The ambiguity check applies only when the callable we hold IS a lambda. The
        # lambdas below belong to a ``def`` we are not lowering, so refusing on their
        # account would cost lowering for nothing.
        from typing import Annotated

        def annotated(x: Annotated[int, lambda v: v > 0]) -> int:
            return x + 1

        def returns_annotated(x) -> Annotated[int, lambda v: v > 0]:
            return x + 2

        for label, func, expected in [
            ("lambda in a parameter annotation", annotated, 6),
            ("lambda in the return annotation", returns_annotated, 7),
        ]:
            with self.subTest(case=label):
                self.assertEqual(self._vals(func, LongType(), "a long", [(5,)]), [expected])

    def test_udf_transpile_resolves_call_on_the_type_not_the_instance(self):
        # Python's call protocol looks ``__call__`` up on the TYPE, so an instance
        # attribute of that name is never what runs. ``getattr(obj, "__call__")``
        # finds it anyway, so the transpiler used to lower the shadowing body and
        # return 5*99 where Python returns 5*4 -- silently wrong. The type's
        # ``__call__`` must win, and it must still lower.
        class Shadowed:
            def __call__(self, x):
                return x * 4

        shadowed = Shadowed()
        # Alone on its line: a leading statement on the same line would make the
        # shadowing body unreachable for an unrelated reason and prove nothing.
        shadowed.__call__ = lambda x: x * 99

        self.assertEqual(20, shadowed(5), "the type's __call__ is what Python runs")
        self.assertEqual(self._vals(shadowed, LongType(), "a long", [(5,)]), [20])

    def test_udf_transpile_refuses_a_class_object(self):
        # Calling a CLASS whose metaclass is ``type`` runs ``__init__`` and yields an
        # instance, so its own ``__call__`` is never the body -- but that is the body
        # the old ``getattr(func, "__call__")`` found. Pinned for the
        # dynamically-created case too, where ``getsource`` cannot fall back to a
        # ``ClassDef``. (A class with a custom metaclass IS callable through
        # ``Meta.__call__``, and is resolved through it rather than refused.)
        class Lexical:
            def __init__(self, x):
                self.v = x * 7

            def __call__(self, x):
                return x * 1000

        def impl(self, x):
            return x * 1000

        Dynamic = type("Dynamic", (), {"__call__": impl})

        with self.sql_conf(_TRANSPILE_ON):
            for label, cls in [("lexical class", Lexical), ("type() class", Dynamic)]:
                with self.subTest(case=label):
                    self.assertEqual([], UserDefinedFunction(cls, LongType()).transpiled)

    def test_udf_transpile_strips_a_bound_receiver_by_dispatch_not_by_name(self):
        # The receiver used to be dropped only when literally named ``self``, so a
        # bound ``__call__(this, x)`` or ``@classmethod f(cls, x)`` kept it in the
        # public parameter list. That declares one parameter too many and shifts
        # every ``_udf_param_N``: a two-column call returned column b's value where
        # Python raises TypeError.
        class Recv:
            def __call__(this, x):
                return x + 1

        class Meth:
            def act(this, x):
                return x + 2

        class Cls:
            @classmethod
            def act(kls, x):
                return x + 3

        # A ``__call__`` that is a classmethod, or one that is ALREADY a bound method,
        # also has its receiver spoken for -- the class and the method's own
        # ``__self__`` respectively. Both used to keep it in the public list, which
        # shifts every placeholder. Each expectation below is what Python returns.
        class ClsCall:
            @classmethod
            def __call__(kls, x):
                return x + 4

        class Helper:
            def impl(self, x):
                return x + 5

        class BoundCall:
            __call__ = Helper().impl

        # And the two compose: a ``staticmethod`` prepends nothing, but the method it
        # wraps is already bound, so one parameter is still spoken for. Counting only
        # what the descriptor prepends declared a parameter too many here.
        class StaticBound:
            __call__ = staticmethod(Helper().impl)

        for label, func, expected in [
            ("__call__ receiver not named self", Recv(), 6),
            ("bound method receiver not named self", Meth().act, 7),
            ("classmethod receiver not named self", Cls.act, 8),
            ("classmethod as __call__", ClsCall(), 9),
            ("already-bound method as __call__", BoundCall(), 10),
            ("staticmethod over a bound method", StaticBound(), 10),
        ]:
            with self.subTest(case=label):
                self.assertEqual(func(5), expected, "fixture must match Python's own answer")
                self.assertEqual(self._vals(func, LongType(), "a long", [(5,)]), [expected])

        # Both at once is a callable Python itself rejects: the classmethod prepends
        # the class ON TOP of the method's own receiver, leaving no parameter for the
        # call site. Counting one receiver returned a value where Python raises.
        class ClassBound:
            __call__ = classmethod(Helper().impl)

        with self.assertRaises(TypeError):
            ClassBound()(5)
        with self.sql_conf(_TRANSPILE_ON):
            _, reasons = self._fallback_reason(ClassBound())
            self.assertIn("leaves no parameter for the call site", reasons)

        # The mirror: a ``staticmethod`` ``__call__`` prepends nothing, so its leading
        # ``self`` IS supplied at the call site and both parameters are public.
        # Resolving ``__call__`` on the type (rather than via getattr on the instance,
        # which fires the descriptor) hands back the raw ``staticmethod``, which
        # carries a ``__wrapped__`` of its own -- so for a while on this branch the
        # wraps guard refused every one of them for a decorator that is not there.
        class Static:
            @staticmethod
            def __call__(self, x):
                return self + x

        self.assertEqual(14, Static()(5, 9), "staticmethod __call__ binds no receiver")
        self.assertEqual(
            self._vals(Static(), LongType(), "a long, b long", [(5, 9)]),
            [14],
            "both parameters come from the call site",
        )

    def test_udf_transpile_known_value_divergences(self):
        # Transpile but DIVERGE from Python (documented in transpile.py; pinned so
        # a future fix is noticed): unguarded arithmetic on NULL yields NULL
        # (Python raises TypeError), and NaN > 0 is True (Python False; Spark
        # orders NaN highest). Mixed str/numeric arithmetic is handled or falls
        # back -- see test_udf_transpile_string_operands{,_fall_back}.
        unguarded = lambda x: x + 1  # noqa: E731
        nan_gt = lambda x: (x > 0) if x is not None else None  # noqa: E731
        eq_strlit = lambda x: (x == "5") if x is not None else None  # noqa: E731
        self.assertEqual(self._vals(unguarded, LongType(), "a long", [(None,), (5,)]), [None, 6])
        self.assertEqual(
            self._vals(nan_gt, BooleanType(), "a double", [(float("nan"),), (1.0,)]), [True, True]
        )
        # `x == "5"` used to be pinned as a coercion divergence (int == "5" -> True).
        # The eq category gate now drops the numeric variant, so on a long column the
        # string option is pruned, nothing is left to lower (hence require_lowered
        # =False), and the UDF falls back to interpreted Python -- matching Python's
        # cross-type == (always False).
        self.assertEqual(
            self._vals(eq_strlit, BooleanType(), "a long", [(5,), (3,)], require_lowered=False),
            [False, False],
        )

    def test_udf_transpile_overflow_and_modulo_zero_raise(self):
        # Transpiled arithmetic that raises at runtime: `*` overflow raises under
        # ANSI where Python promotes to a big int (a real divergence, SPARK-55210),
        # while `% 0` raises in both Spark and Python (compatible -- pinned here so
        # it isn't mistaken for a divergence).
        overflow = lambda x: x * x  # noqa: E731
        modulo_zero = lambda x: x % 0  # noqa: E731
        self._raises(overflow, "a long", [(4000000000,)], "overflow")
        self._raises(modulo_zero, "a long", [(5,)], "zero")

    def test_udf_transpile_string_operands(self):
        # Textual `+`/`*` lower to Catalyst string ops and match Python: `str +
        # str` -> concat, and `str * int` / `int * str` -> repeat (including a
        # string column times a numeric literal). The transpiler emits a string-
        # typed variant whose declared categories the JVM matches against the bound
        # column types (see UserDefinedPythonFunction.builder).
        S = StringType()
        add = lambda a, b: a + b  # noqa: E731
        mul = lambda a, b: a * b  # noqa: E731
        mul3 = lambda a: a * 3  # noqa: E731
        concat_right = lambda a: a + "!"  # noqa: E731
        concat_left = lambda a: "pre-" + a  # noqa: E731
        repeat_lit = lambda x: "ab" * x  # noqa: E731
        # (func, return_type, schema, rows, expected); arg columns come from schema.
        cases = [
            (add, S, "a string, b string", [("x", "y"), ("a", "b")], ["xy", "ab"]),
            (mul, S, "a string, b long", [("ab", 3)], ["ababab"]),
            (mul, S, "a long, b string", [(3, "ab")], ["ababab"]),
            (mul3, S, "a string", [("2",), ("ab",)], ["222", "ababab"]),
            (concat_right, S, "a string", [("hi",)], ["hi!"]),
            (concat_left, S, "a string", [("x",)], ["pre-x"]),
            (repeat_lit, S, "a long", [(3,)], ["ababab"]),
        ]
        for i, (func, rt, schema, rows, expected) in enumerate(cases):
            with self.subTest(case=i):
                self.assertEqual(self._vals(func, rt, schema, rows), expected, f"case {i}")

    def test_udf_transpile_string_len(self):
        # SPARK-55214: Empty, ASCII, and concat inputs match Python.
        # Unguarded ``len(NULL)`` raises like CPython (not Spark length's NULL).
        # A proven-non-null branch still returns None for NULL rows. ``len`` on
        # a numeric column has no matching string option, so the UDF falls back
        # and Python raises TypeError.
        L = LongType()
        strlen = lambda x: len(x)  # noqa: E731
        len_concat = lambda a, b: len(a + b)  # noqa: E731
        strlen_guarded = lambda x: len(x) if x is not None else None  # noqa: E731
        self.assertEqual(
            self._vals(strlen, L, "a string", [("",), ("ab",), ("a",)]),
            [0, 2, 1],
        )
        self.assertEqual(
            self._vals(len_concat, L, "a string, b string", [("x", "yz"), ("", "ab")]),
            [3, 2],
        )
        self.assertEqual(
            self._vals(strlen_guarded, L, "a string", [("ab",), (None,)]),
            [2, None],
        )
        self._raises(strlen, "a string", [(None,)], needle="len()")
        self._raises(strlen, "a long", [(5,)], needle="")

    def test_udf_transpile_string_operands_fall_back(self):
        # Operand/type combos with no valid string lowering for the bound column
        # types fall back to the Python UDF, which raises the same way CPython does:
        # `str + int` (and reversed), `str - int`, `str * str`, `str % int`, and a
        # string column plus a numeric literal. The transpiler still emits numeric
        # (and/or concat/repeat) variants, but none match the column types, so the
        # JVM drops them and runs Python -- matching its TypeError.
        add = lambda a, b: a + b  # noqa: E731
        sub = lambda a, b: a - b  # noqa: E731
        mul = lambda a, b: a * b  # noqa: E731
        mod = lambda a, b: a % b  # noqa: E731
        add5 = lambda a: a + 5  # noqa: E731
        # needle="" -> assert only that it raises (the message is CPython's).
        for func, schema, rows in [
            (add, "a string, b long", [("10", 5)]),  # str + int
            (add, "a long, b string", [(5, "10")]),  # int + str
            (sub, "a string, b long", [("10", 5)]),  # str - int
            (mul, "a string, b string", [("a", "b")]),  # str * str
            (mod, "a string, b long", [("10", 3)]),  # str % int
            (add5, "a string", [("10",)]),  # str column + numeric literal
        ]:
            with self.subTest(func=func, schema=schema):
                self._raises(func, schema, rows, needle="")

    def test_udf_transpile_power_falls_back(self):
        # `**` is intentionally not lowered (Spark's pow is DOUBLE and loses
        # precision for large ints), so a UDF using it falls back to interpreted
        # Python. TODO(SPARK-55210): revisit once an exact integer-power lowering
        # exists.
        square = lambda x: x**2  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            self.assertFalse(UserDefinedFunction(square, LongType()).transpiled)

    def test_udf_transpile_non_numeric_constant_falls_back(self):
        # bool/None constants have no faithful numeric/string lowering, so
        # arithmetic against them must fall back rather than emit an option that
        # crashes analysis (`x * True`) or silently returns NULL (`x + None`).
        mul_bool = lambda x: x * True  # noqa: E731
        add_none = lambda x: x + None  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            self.assertFalse(UserDefinedFunction(mul_bool, LongType()).transpiled)
            self.assertFalse(UserDefinedFunction(add_none, LongType()).transpiled)

    def test_udf_transpile_mixed_type_comparison_falls_back(self):
        # Python forbids ordering across types (`a < b` for int/str -> TypeError);
        # Spark would coerce and return a wrong boolean. A comparison whose
        # operand categories differ is dropped (so int-vs-str `<` falls back),
        # while a same-category comparison still transpiles.
        def lt_mixed(a: int, b: str):
            return (a < b) if a is not None and b is not None else None

        def lt_same(a: int, b: int):
            return (a < b) if a is not None and b is not None else None

        with self.sql_conf(_TRANSPILE_ON):
            self.assertFalse(UserDefinedFunction(lt_mixed, BooleanType()).transpiled)
            self.assertTrue(UserDefinedFunction(lt_same, BooleanType()).transpiled)

    def test_udf_transpile_skips_nondeterministic(self):
        # A nondeterministic UDF must not be transpiled: the optimizer could
        # fold/reorder/duplicate the plain expression, dropping the barrier.
        # Holds whether marked at construction or via asNondeterministic().
        plus_one = lambda x: x + 1  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            self.assertTrue(UserDefinedFunction(plus_one, LongType()).transpiled)
            self.assertFalse(
                UserDefinedFunction(plus_one, LongType()).asNondeterministic().transpiled
            )
            self.assertFalse(
                UserDefinedFunction(plus_one, LongType(), deterministic=False).transpiled
            )

    def test_udf_transpile_bool_and_binary_params(self):
        # bool/bytes annotations map to the "bool"/"binary" categories and match
        # Boolean/Binary columns. Identity and same-category comparison transpile
        # (and match Python); boolean arithmetic has no lowering and falls back.
        def bool_ident(x: bool):
            return x

        def bool_lt(a: bool, b: bool):
            return (a < b) if a is not None and b is not None else None

        def bool_add(x: bool):
            return x + 1  # no boolean arithmetic lowering -> fall back

        def bytes_ident(x: bytes):
            return x

        self.assertEqual(
            self._vals(bool_ident, BooleanType(), "a boolean", [(True,), (False,), (None,)]),
            [True, False, None],
        )
        self.assertEqual(
            self._vals(
                bool_lt,
                BooleanType(),
                "a boolean, b boolean",
                [(False, True), (True, False), (True, True)],
            ),
            [True, False, False],
        )
        with self.sql_conf(_TRANSPILE_ON):
            self.assertFalse(UserDefinedFunction(bool_add, LongType()).transpiled)
            self.assertTrue(UserDefinedFunction(bytes_ident, BinaryType()).transpiled)

    def _optimized_plan(self, func, return_type, schema):
        """The optimized plan of ``func`` applied to every column of ``schema``.

        The lowered NULL checks are what these tests are about, so they have to be
        read off the plan: the values a guarded UDF returns are the same whether or
        not the plan carries a check that can never fire.

        Asserting the option was actually APPLIED is what keeps every
        ``assertNotIn(..., plan)`` built on this from passing for the wrong reason
        -- an interpreted plan contains none of the strings we look for, so a UDF
        that quietly fell back would satisfy them all (see ``_vals``, which guards
        the same way for the same reason).
        """
        with self.sql_conf(_TRANSPILE_ON):
            u = self._transpiled_udf(func, return_type)
            df = self.spark.createDataFrame([], schema)
            projected = df.select(u(*df.columns))
            self.assertEqual(0, self._eval_python_count(projected), str(func))
            return projected._jdf.queryExecution().optimizedPlan().toString()

    def test_udf_transpile_drops_null_checks_a_guard_already_made(self):
        # SPARK-58628. Python raises TypeError on `None > 0` where Spark returns
        # NULL, so an ordering comparison carries a check that raises. When the UDF
        # has already tested the parameter the check can never fire, and leaving it
        # in is not free: RaiseError is throwable (SPARK-58627), which stops the
        # optimizer from moving a predicate containing it. Every shape below proves
        # non-NULL-ness, so none of them should lower to a raise -- and all of them
        # must still agree with Python, including on the None row.
        def if_guard(x):
            if x is not None:
                return x > 0
            else:
                return None

        def else_of_is_none(x):
            if x is None:
                return None
            else:
                return x > 0

        def not_is_none(x):
            if not (x is None):  # noqa: E714  the `not` form is the point
                return x > 0
            else:
                return None

        def reversed_test(x):
            if None is not x:
                return x > 0
            else:
                return None

        def ne_none_test(x):
            # The `!= None` spelling of the same guard. PEP 8 prefers `is not None`,
            # but both are common and both must narrow, or the equality form keeps a
            # raising check and gets told to add the guard it already has.
            if x != None:  # noqa: E711
                return x > 0
            else:
                return None

        def eq_none_test(x):
            if x == None:  # noqa: E711
                return None
            else:
                return x > 0

        ternary = lambda x: (x > 0) if x is not None else None  # noqa: E731
        short_circuit_and = lambda x: x is not None and x > 0  # noqa: E731
        short_circuit_or = lambda x: x is None or x > 0  # noqa: E731
        # `<=` / `>=` share `_lower_value_compare` but are otherwise only exercised
        # where a guard IS emitted, so cover the narrowed side for them too.
        lte_guarded = lambda x: (x <= 0) if x is not None else None  # noqa: E731
        gte_guarded = lambda x: (x >= 0) if x is not None else None  # noqa: E731
        lt_guarded = lambda x: (x < 0) if x is not None else None  # noqa: E731

        rows = [(-1,), (0,), (5,), (None,)]
        cases = [
            (if_guard, [False, False, True, None]),
            (else_of_is_none, [False, False, True, None]),
            (not_is_none, [False, False, True, None]),
            (reversed_test, [False, False, True, None]),
            (ne_none_test, [False, False, True, None]),
            (eq_none_test, [False, False, True, None]),
            (ternary, [False, False, True, None]),
            (lte_guarded, [True, True, False, None]),
            (gte_guarded, [False, True, True, None]),
            (lt_guarded, [True, False, False, None]),
            # `and` / `or` return the guard's own result for None, as Python does.
            (short_circuit_and, [False, False, True, False]),
            (short_circuit_or, [False, False, True, True]),
        ]
        for func, expected in cases:
            with self.subTest(func=getattr(func, "__name__", "lambda")):
                self.assertEqual(
                    expected,
                    [func(v) for (v,) in rows],
                    "the expectation should be what Python actually does",
                )
                self.assertEqual(expected, self._vals(func, BooleanType(), "a long", rows))
                plan = self._optimized_plan(func, BooleanType(), "a long")
                self.assertNotIn("raise_error", plan)

    def test_udf_transpile_keeps_the_null_check_it_needs(self):
        # The other side of the previous test: nothing here proves the parameter
        # non-NULL, so the check has to stay and the UDF has to raise like Python.
        # `x is None and ...` is the interesting one -- an `and` being false says
        # only that SOME operand was false, so it narrows nothing.
        unguarded = lambda x: x > 0  # noqa: E731
        wrong_way_and = lambda x: x is None and x > 0  # noqa: E731
        two_columns = lambda a, b: a > b  # noqa: E731

        for func, schema in ((unguarded, "a long"), (two_columns, "a long, b long")):
            with self.subTest(func="lambda", schema=schema):
                plan = self._optimized_plan(func, BooleanType(), schema)
                self.assertIn("raise_error", plan)
        boolean = BooleanType()
        self._raises(unguarded, "a long", [(None,)], "cannot compare null", boolean)
        self._raises(two_columns, "a long, b long", [(1, None)], "cannot compare null", boolean)
        # Each ordering operator names itself in the guard's message and in the
        # warning's label; only `>` was covered before.
        lt_zero = lambda x: x < 0  # noqa: E731
        lte_zero = lambda x: x <= 0  # noqa: E731
        gte_zero = lambda x: x >= 0  # noqa: E731
        for func, op_text in ((lt_zero, "`<`"), (lte_zero, "`<=`"), (gte_zero, "`>=`")):
            with self.subTest(op=op_text):
                with self.sql_conf(_TRANSPILE_ON):
                    _, warned = self._udf_and_warnings(func, boolean)
                self.assertIn(f"comparison {op_text} on x", warned)
                self._raises(func, "a long", [(None,)], f"operator {op_text}", boolean)
        # `x is None and x > 0` is False for every non-None x without evaluating the
        # comparison, and raises for None -- exactly like Python.
        with self.sql_conf(_TRANSPILE_ON):
            u = self._transpiled_udf(wrong_way_and, BooleanType())
            self.assertIn("raise_error", str(u.transpiled[0]))
            df = self.spark.createDataFrame([(1,)], "a long")
            self.assertEqual([False], [r[0] for r in df.select(u("a")).collect()])
        self._raises(wrong_way_and, "a long", [(None,)], "cannot compare null", boolean)

    def test_udf_transpile_narrows_only_the_proven_parameter(self):
        # With two parameters the narrowing has to be per-name: guarding `a` must
        # not license dropping the check on `b`. Both guarded, no check at all.
        one_guarded = lambda a, b: a is not None and a > b  # noqa: E731
        both_guarded = lambda a, b: a is not None and b is not None and a > b  # noqa: E731

        # The same two-parameter guards written as an `if` TEST rather than as the
        # expression itself. This is the only shape that asks `_null_facts` about a
        # composite `and` / `or` node instead of about a single comparison, and it
        # is the idiomatic way to guard two columns.
        def composite_and(a, b):
            if a is not None and b is not None:
                return a > b
            else:
                return None

        def composite_or(a, b):
            if a is None or b is None:
                return None
            else:
                return a > b

        def composite_not_or(a, b):
            if not (a is None or b is None):
                return a > b
            else:
                return None

        schema = "a long, b long"
        partial = self._optimized_plan(one_guarded, BooleanType(), schema)
        self.assertIn("raise_error", partial)
        # Only `b` is still checked. Note `isnull(a` does not match the guard's own
        # `isnotnull(a...)`, which is the test rather than a check on the operand.
        self.assertIn("isnull(b", partial)
        self.assertNotIn("isnull(a", partial)
        rows = [(5, 1), (1, 5), (1, None), (None, 1)]
        for func in (both_guarded, composite_and, composite_or, composite_not_or):
            with self.subTest(func=getattr(func, "__name__", "lambda")):
                self.assertNotIn("raise_error", self._optimized_plan(func, BooleanType(), schema))
                self.assertEqual(
                    [func(a, b) for a, b in rows],
                    self._vals(func, BooleanType(), schema, rows),
                )
        # Partial narrowing was only plan-asserted, but it creates two runtime
        # invariants: the surviving check must still fire for a NULL `b`, and the
        # guard on `a` must still short-circuit a NULL `a` to False without reaching
        # the comparison. This is the one live consumer of the claim that Catalyst's
        # `And` short-circuits on false the way Python does.
        self._raises(one_guarded, schema, [(1, None)], "cannot compare null", BooleanType())
        self.assertEqual([False], self._vals(one_guarded, BooleanType(), schema, [(None, None)]))
        self.assertEqual([False], self._vals(one_guarded, BooleanType(), schema, [(None, 1)]))

        # The UNSOUND direction, end to end. A true `or` says only that ONE operand
        # held, and a false `and` only that one failed, so neither narrows anything --
        # these must KEEP their check and raise like Python. Covered here and not only
        # in `test_null_facts_narrow_by_outcome`, because a plausible "symmetry" edit
        # to `_null_facts` (unioning `or`'s true-facts the way `and`'s are unioned)
        # would delete a needed raise, and a unit test on the fact table sits one
        # remove from the plan that would go wrong.
        def or_test(a, b):
            if a is not None or b is not None:
                return a > 0
            else:
                return None

        def not_and_test(a, b):
            if not (a is None and b is None):
                return a > 0
            else:
                return None

        for func in (or_test, not_and_test):
            with self.subTest(func=func.__name__):
                self.assertIn("raise_error", self._optimized_plan(func, BooleanType(), schema))
                with self.assertRaises(Exception):
                    func(None, 1)
                self._raises(func, schema, [(None, 1)], "cannot compare null", BooleanType())

    def test_udf_transpile_drops_the_coalesce_when_nothing_can_be_null(self):
        # The `if` test and the `not` operand each get coalesced against a literal
        # so that a NULL follows Python's "None is falsy" rule. Neither coalesce
        # does anything once the operand provably cannot be NULL, and dropping it
        # leaves a plainer expression for the optimizer.
        def non_null_test(x):
            # `x is not None` lowers to isNotNull, which is never NULL itself.
            if x is not None:
                return 1
            else:
                return 2

        def nullable_test(x):
            # A ternary arm of literal None can be NULL, so the coalesce stays.
            if (x > 0) if x is not None else None:
                return 1
            else:
                return 2

        # `not (x is None)` on purpose, rather than `x is not None`: it is the `not`
        # arm of _convert_chunk that this case is about.
        negate_compare = lambda x: not (x is None)  # noqa: E714,E731
        # The `not` arm's negative case, and unlike the `if` test above this
        # coalesce is load-bearing: Python's `not None` is True while Catalyst's
        # `~NULL` is NULL, so dropping it here would return NULL for a None row.
        negate_nullable = lambda x: not ((x > 0) if x is not None else None)  # noqa: E731

        with self.sql_conf(_TRANSPILE_ON):
            proven = str(self._transpiled_udf(non_null_test, LongType()).transpiled[0])
            self.assertNotIn("coalesce", proven)
            self.assertIn("isNotNull", proven)
            unproven = str(self._transpiled_udf(nullable_test, LongType()).transpiled[0])
            self.assertIn("coalesce", unproven)
            negated = str(self._transpiled_udf(negate_compare, BooleanType()).transpiled[0])
            self.assertNotIn("coalesce", negated)
            # Positive companion, so this cannot pass on a plan that no longer
            # resembles the `not` lowering at all.
            self.assertIn("!(isNull", negated)
            still = str(self._transpiled_udf(negate_nullable, BooleanType()).transpiled[0])
            self.assertIn("coalesce", still)
        # Values still follow Python, coalesce or not.
        rows = [(5,), (-5,), (None,)]
        for func, return_type in (
            (non_null_test, LongType()),
            (nullable_test, LongType()),
            (negate_compare, BooleanType()),
            (negate_nullable, BooleanType()),
        ):
            with self.subTest(func=getattr(func, "__name__", "lambda")):
                self.assertEqual(
                    [func(v) for (v,) in rows],
                    self._vals(func, return_type, "a long", rows),
                )

    def test_udf_transpile_does_not_null_check_a_literal(self):
        # A literal operand is never NULL, so only the column is checked. This used
        # to emit `isNull(0)` alongside it -- constant-folded later, but it meant a
        # comparison of two literals still produced a guard.
        unguarded = lambda x: x > 0  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            lowered = str(self._transpiled_udf(unguarded, BooleanType()).transpiled[0])
        self.assertIn("isNull(_udf_param_0)", lowered)
        self.assertNotIn("isNull(0)", lowered)

    def test_udf_transpile_narrowing_collapses_eq_null_ladder(self):
        # `==` needs four branches to reproduce Python's None equality, but only
        # while an operand can be NULL. Proven non-NULL, it is just Spark's `=`;
        # against a literal None it folds to the constant Python produces. The
        # optimizer will not do this for us -- it has no branch-local knowledge
        # that an enclosing isnotnull makes the inner isnull false.
        guarded_eq = lambda x: (x == 5) if x is not None else None  # noqa: E731
        guarded_ne = lambda x: (x != 5) if x is not None else None  # noqa: E731
        eq_none = lambda x: x == None  # noqa: E711,E731

        with self.sql_conf(_TRANSPILE_ON):
            for func in (guarded_eq, guarded_ne):
                lowered = str(self._transpiled_udf(func, BooleanType()).transpiled[0])
                with self.subTest(lowered=lowered):
                    self.assertNotIn("isNull", lowered)
        rows = [(5,), (1,), (None,)]
        self.assertEqual(
            [guarded_eq(v) for (v,) in rows],
            self._vals(guarded_eq, BooleanType(), "a long", rows),
        )
        self.assertEqual(
            [guarded_ne(v) for (v,) in rows],
            self._vals(guarded_ne, BooleanType(), "a long", rows),
        )
        # `x == None` keeps ONE check (on the column) rather than four branches,
        # and still answers what Python does.
        self.assertEqual(
            [eq_none(v) for (v,) in rows],
            self._vals(eq_none, BooleanType(), "a long", rows),
        )

        # When BOTH operands are statically decided the ladder resolves with no
        # branch at all: a proven-non-NULL column against a literal None is just
        # Python's False, and two literals fold outright. Both reach the early return
        # in `_lower_eq`, which is a PRECONDITION of building the ladder rather than
        # an optimization -- the ladder's conditions are built eagerly from the
        # undecided operands, and with none left `_all_null([])` has nothing to
        # return. Break the three-way decision itself and the answers go wrong.
        guarded_eq_none = lambda x: (x == None) if x is not None else None  # noqa: E711,E731
        literals_eq = lambda x: 5 == None  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            folded = str(self._transpiled_udf(guarded_eq_none, BooleanType()).transpiled[0])
            self.assertNotIn("isNull", folded)
            self.assertIn("false", folded)
            both_literals = str(self._transpiled_udf(literals_eq, BooleanType()).transpiled[0])
            self.assertNotIn("isNull", both_literals)
            self.assertIn("false", both_literals)
        for func in (guarded_eq_none, literals_eq):
            with self.subTest(func="lambda"):
                self.assertEqual(
                    [func(v) for (v,) in rows],
                    self._vals(func, BooleanType(), "a long", rows),
                )

    def test_udf_transpile_never_folds_away_an_operand_that_can_raise(self):
        # Proving an operand non-NULL is not a licence to DELETE it. `_lower_eq` folds
        # statically-decided branches, and an operand it folds away takes its errors
        # with it: an ordering comparison's `raise_error`, or an ANSI `pmod` on a zero
        # divisor. Each UDF below returned a constant where Python raises until
        # `_is_effect_free` gated the folding, and none of them is a contrived shape --
        # `(x > 0) == None` is a plain, if odd, comparison.
        gt_eq_none = lambda x: (x > 0) == None  # noqa: E711,E731
        mod_zero = lambda x: ((x % 0) == None) if x is not None else None  # noqa: E711,E731
        mod_by_col = lambda a, b: (  # noqa: E731
            ((a % b) != None) if a is not None and b is not None else None  # noqa: E711
        )

        # Mirrored: the raising operand on the RIGHT. Both sides have to be gated --
        # checking only the left still folds these away, and the two paths are
        # otherwise symmetric, so covering one direction proves nothing about the
        # other.
        none_eq_gt = lambda x: None == (x > 0)  # noqa: E711,E731
        mod_zero_right = lambda x: (None == (x % 0)) if x is not None else None  # noqa: E711,E731

        def compare_vs_bool(a, b: bool):
            # No None literal anywhere: `a > 0` is proven non-NULL, which used to drop
            # the ladder rung that was forcing its raise to be evaluated.
            return (a > 0) == b

        def bool_vs_compare(a, b: bool):
            # The same, reversed.
            return b == (a > 0)

        cases = [
            (gt_eq_none, LongType(), "a long", [(None,)]),
            (none_eq_gt, LongType(), "a long", [(None,)]),
            (mod_zero, BooleanType(), "a long", [(5,)]),
            (mod_zero_right, BooleanType(), "a long", [(5,)]),
            (mod_by_col, BooleanType(), "a long, b long", [(5, 0)]),
            (compare_vs_bool, LongType(), "a long, b boolean", [(None, None)]),
            (bool_vs_compare, LongType(), "a long, b boolean", [(None, None)]),
        ]
        for func, return_type, schema, rows in cases:
            with self.subTest(func=getattr(func, "__name__", "lambda")):
                # Every row here raises in Python, so the transpiled form must too.
                for row in rows:
                    with self.assertRaises(Exception):
                        func(*row)
                with self.sql_conf(_TRANSPILE_ON):
                    u = self._transpiled_udf(func, return_type)
                    df = self.spark.createDataFrame(rows, schema)
                    projected = df.select(u(*df.columns))
                    self.assertEqual(0, self._eval_python_count(projected))
                    with self.assertRaises(Exception):
                        projected.collect()

        # The non-raising rows still agree, so the gate did not simply disable the
        # lowering. `(5 > 0) == None` is False in Python; declared LongType, the
        # transpiled cast makes that 0, which is what the interpreted path returns too.
        self.assertEqual([0], self._vals(gt_eq_none, LongType(), "a long", [(5,)]))
        self.assertEqual([True], self._vals(mod_by_col, BooleanType(), "a long, b long", [(7, 2)]))

    def test_udf_transpile_warns_only_when_a_null_check_survives(self):
        # The (gentle) warning SPARK-58628 asks for: the UDF transpiled fine, but
        # the check it kept will stop filters moving, so say so once. A guarded UDF
        # must not warn -- the warning is a nudge toward guarding, and firing it on
        # already-guarded code would train people to ignore it.
        def guarded(x):
            if x is not None:
                return x > 0
            else:
                return None

        unguarded = lambda x: x > 0  # noqa: E731
        half_guarded = lambda a, b: a is not None and a > b  # noqa: E731
        needle = "still checks for NULL"
        with self.sql_conf(_TRANSPILE_ON):
            _, warned = self._udf_and_warnings(unguarded, BooleanType())
            self.assertIn(needle, warned)
            # Actionable: it should name what is checked, and the guard to add.
            self.assertIn("comparison `>` on x", warned)
            self.assertIn("is not None", warned)
            # It must name the parameter still checked, not both -- `a` is guarded
            # here, so advising the reader to guard "the parameter" is only useful
            # if they can tell which one is left.
            _, partial = self._udf_and_warnings(half_guarded, BooleanType())
            self.assertIn("comparison `>` on b", partial)
            # The full label, not " on a": that would match any prose word starting
            # with "a" elsewhere in the message.
            self.assertNotIn("comparison `>` on a", partial)
            # `_transpiled_udf`, not `_udf_and_warnings`: udf.py reports the fallback
            # and this warning through mutually exclusive branches, so a `guarded`
            # that stopped transpiling would satisfy assertNotIn for the wrong
            # reason. Assert it transpiled, then that it stayed quiet.
            quiet_udf, quiet = self._udf_and_warnings(guarded, BooleanType())
            self.assertTrue(quiet_udf.transpiled, f"guarded must transpile: {quiet}")
            self.assertNotIn(needle, quiet)

    def test_udf_transpile_null_check_blocks_predicate_pushdown(self):
        # Why the checks are worth dropping. RaiseError is throwable, and
        # PushPredicateThroughJoin refuses to push a throwable condition to either
        # side (Optimizer.scala). So an unguarded UDF pins its filter above the
        # join while a guarded one pushes below it -- the "filter cannot bubble up"
        # in SPARK-58628.
        def guarded(x):
            if x is not None:
                return x > 0
            else:
                return None

        unguarded = lambda x: x > 0  # noqa: E731

        conf = dict(_TRANSPILE_ON)
        # Broadcast would reshape the plan and hide where the Filter landed.
        conf["spark.sql.autoBroadcastJoinThreshold"] = -1
        with self.sql_conf(conf):
            left = self.spark.createDataFrame([(1,), (2,), (None,)], "a long")
            right = self.spark.createDataFrame([(1,), (2,)], "k long")
            for func, pushed in ((guarded, True), (unguarded, False)):
                with self.subTest(pushed=pushed):
                    u = self._transpiled_udf(func, BooleanType())
                    joined = left.join(right, left["a"] == right["k"]).filter(u("a"))
                    # This method builds its own plan rather than going through
                    # `_optimized_plan`, so it needs the same applied-or-not guard:
                    # a filter on an interpreted UDF would satisfy the pushed case
                    # for the wrong reason.
                    self.assertEqual(0, self._eval_python_count(joined))
                    plan = joined._jdf.queryExecution().optimizedPlan().toString()
                    # Whether the raise is there is what DRIVES the pushdown, so
                    # assert it too: if this half fails the position check below
                    # becomes noise, and the failure says which one broke.
                    self.assertEqual(pushed, "raise_error" not in plan, plan)
                    # The remaining Filters are the join keys' inferred isnotnull,
                    # which sit below the Join either way, so comparing the first
                    # of each reads the UDF's own filter. Assert both nodes are
                    # present first, or `index` raises ValueError and the plan text
                    # this failure needs never gets printed.
                    self.assertIn("Filter", plan, plan)
                    self.assertIn("Join", plan, plan)
                    self.assertEqual(pushed, plan.index("Filter") > plan.index("Join"), plan)

    def test_udf_transpile_non_nullable_column_drops_the_null_check(self):
        # The other half of SPARK-58628, and the half Catalyst already does for us:
        # bound to a non-nullable column the check folds away entirely, because
        # NullPropagation rewrites isnull over a non-nullable child to false and
        # SimplifyConditionals then drops the branch. Pinned here so a reordering
        # of Optimizer.defaultBatches -- ConvertToCatalyst has to run before the
        # operator-optimization batches for this to happen -- cannot quietly
        # regress it.
        unguarded = lambda x: x > 0  # noqa: E731
        with self.sql_conf(_TRANSPILE_ON):
            u = self._transpiled_udf(unguarded, BooleanType())
            self.assertIn("raise_error", str(u.transpiled[0]))
            source = self.spark.range(0, 4)
            self.assertFalse(source.schema["id"].nullable, "range(id) should be non-nullable")
            projected = source.select(u("id"))
            # Without this the test is vacuous under the very regression it pins: an
            # interpreted plan has no `raise_error` and no `CASE WHEN` either, and
            # returns the same values, so all three assertions below would pass
            # while nothing had been lowered at all.
            self.assertEqual(0, self._eval_python_count(projected))
            plan = projected._jdf.queryExecution().optimizedPlan().toString()
            self.assertNotIn("raise_error", plan)
            self.assertNotIn("CASE WHEN", plan)
            self.assertEqual([False, True, True, True], [r[0] for r in projected.collect()])

    def test_null_facts_narrow_by_outcome(self):
        # The fact extractor on its own, so a regression in the table shows up as a
        # unit failure rather than as a plan-shape surprise several layers up.
        import ast as _ast

        from pyspark.sql.transpile import _null_facts

        def facts(source):
            true_facts, false_facts = _null_facts(_ast.parse(source, mode="eval").body)
            return sorted(true_facts), sorted(false_facts)

        self.assertEqual((["x"], []), facts("x is not None"))
        self.assertEqual(([], ["x"]), facts("x is None"))
        # Operand order does not matter -- `None is x` is the same test.
        self.assertEqual((["x"], []), facts("None is not x"))
        self.assertEqual(([], ["x"]), facts("None is x"))
        # `not` swaps the two outcomes.
        self.assertEqual(([], ["x"]), facts("not (x is not None)"))
        # An `and` that held means every operand held; false says only that one of
        # them did not, so it narrows nothing. `or` is the mirror image.
        self.assertEqual((["x", "y"], []), facts("x is not None and y is not None"))
        self.assertEqual(([], ["x", "y"]), facts("x is None or y is None"))
        self.assertEqual(([], []), facts("x is not None or y is not None"))
        self.assertEqual(([], []), facts("x is None and y is None"))
        # Identity checks that are not against None, and anything else, say nothing.
        self.assertEqual(([], []), facts("x is y"))
        self.assertEqual(([], []), facts("x > 0"))
        self.assertEqual(([], []), facts("f(x)"))

    def test_param_category_combos_caps_preserve_typed_pins(self):
        # With more than three untyped params the cap collapses the untyped ones
        # to numeric/string but keeps each typed param pinned (here a: str).
        import ast as _ast

        from pyspark.sql.transpile import _param_category_combos

        fn = _ast.parse("def f(a: str, b, c, d, e): return a").body[0]
        combos = _param_category_combos(fn, ["a", "b", "c", "d", "e"])
        self.assertEqual(len(combos), 2)
        for combo in combos:
            self.assertEqual(combo[0], "string")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
