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
Transpilation of scalar pandas UDFs (``SQL_SCALAR_PANDAS_UDF``).

A scalar pandas UDF is handed one ``pandas.Series`` per argument, so only an element-wise
subset is rewritten; see "Scalar pandas UDFs" in ``python/pyspark/sql/transpile.py``. Both
sides of every differential run at this eval type. Bodies outside the subset must fall back
and behave exactly as before, including the ones that raise; the known differences from
numpy's fixed-width arithmetic are pinned with values.
"""

import warnings

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.tests.arrow.test_arrow_python_udf_transpile import (
    _RAISED,
    _TRANSPILE_OFF,
    _TRANSPILE_ON,
    ArrowUDFTranspileTestsMixin,
    _eval_python_nodes,
    _python_eval_operators,
)
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)
from pyspark.sql.udf import UserDefinedFunction
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.util import PythonEvalType

NAN = float("nan")
LONG_MIN = -(2**63)
_INT_EXT_DTYPE = "spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype"


# --- UDF bodies (supported subset) -----------------------------------------------------


def add_one(s):
    return s + 1


def add_wide_literal(s):
    return s + 1000


def identity(s):
    return s


def subtract_two(s):
    return s - 2


def times_three(s):
    return s * 3


def negate(s):
    return -s


def unary_plus(s):
    return +s


def mod_three(s):
    return s % 3


def modulo(a, b):
    return a % b


def times_one_and_a_half(s):
    return s * 1.5


def combine(a, b):
    return a * b - 1


def ten_a_plus_b(a, b):
    return a * 10 + b


def concat_bang(s):
    return s + "!"


def repeat_twice(s):
    return s * 2


def repeat_by_column(s, n):
    return s * n


def prefixed(s):
    return "x" + s


def is_missing(s):
    return s.isnull()


def is_missing_isna(s):
    return s.isna()


def is_present_notnull(s):
    return s.notnull()


def is_present_notna(s):
    return s.notna()


def add_tenth(s):
    return s + 0.1


def times_inf(s):
    # 1e400 is inf, and 0 * inf is NaN: the one body that makes NaN from non-NaN input.
    return s * 1e400


# --- UDF bodies (outside the subset) ---------------------------------------------------


def greater_than_zero(s):
    return s > 0


def equals_five(s):
    return s == 5


def not_equals_zero(s):
    return s != 0


def is_none(s):
    return s is None


def is_not_none(s):
    return s is not None


def early_none_guard(s):
    if s is None:
        return 0
    return s


def not_is_none(s):
    return not (s is None)  # noqa: E714


def is_not_none_and(s):
    return s is not None and s


def branching(s):
    if s > 0:
        return 1
    return 0


def logical_not(s):
    return not s


def logical_and(s):
    return s and s


def halved(s):
    return s / 2


def absolute(s):
    return abs(s)


def filled(s):
    return s.fillna(0)


def uppercased(s):
    return s.upper()


def isnull_of_expression(s):
    return (s + 1).isnull()


def constant_body(s):
    return 1


def bare_return(s):
    return


def bool_constant(s):
    return True


def free_variable(s):
    return s + _UNDEFINED_NAME  # noqa: F821


def two_statements(s):
    x = s + 1
    return x


def scalar_modulo_by_zero(s):
    return s + 1 % 0


def scalar_modulo(s):
    return s + 5 % 3


class PandasUDFTranspileTestsMixin(ArrowUDFTranspileTestsMixin):
    """The shared differential helpers, run at the scalar pandas eval type."""

    _eval_type = PythonEvalType.SQL_SCALAR_PANDAS_UDF

    def _pandas_udf(self, func, return_type):
        # Not ``pandas_udf``: the bodies are unannotated, so no type hint picks the eval type.
        return UserDefinedFunction(func, return_type, evalType=self._eval_type)

    def _assert_falls_back(self, func, return_type, df, *cols, expect_reason=None):
        """Assert ``func`` is not rewritten AND behaves exactly as with the flag off.

        Several of these bodies raise interpreted, and a rewrite that made them return values
        would be a behavior change.
        """
        name = func.__name__
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with self.sql_conf(_TRANSPILE_ON):
                pudf = self._pandas_udf(func, return_type)
                self.assertEqual([], pudf.transpiled, f"{name} was rewritten")
                on_values, on_error = self._values_or_raised(df.select(pudf(*cols)))
        if expect_reason is not None:
            messages = " ".join(str(w.message) for w in caught)
            self.assertIn(expect_reason, messages, f"{name}: unexpected fallback reason")

        with self.sql_conf(_TRANSPILE_OFF):
            pudf = self._pandas_udf(func, return_type)
            off_values, off_error = self._values_or_raised(df.select(pudf(*cols)))

        if off_values is _RAISED:
            self.assertIs(on_values, _RAISED, f"{name}: fallback stopped it raising")
            self.assertEqual(type(off_error), type(on_error), f"{name}: exception changed")
        else:
            self.assertEqual(off_values, on_values, f"{name}: fallback changed the result")

    def _pin_divergence(self, func, return_type, df, *cols):
        """Return ``(interpreted, transpiled)``, each ``(values, error)``, for sides that differ."""
        with self.sql_conf(_TRANSPILE_OFF):
            interpreted = self._values_or_raised(
                df.select(self._pandas_udf(func, return_type)(*cols))
            )
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(func, return_type)
            self._assert_transpiled(pudf, func.__name__)
            transpiled = self._values_or_raised(df.select(pudf(*cols)))
        return interpreted, transpiled

    def _longs(self, *values):
        return self.spark.createDataFrame([(v,) for v in values], "a bigint")

    def _doubles(self, *values):
        return self.spark.createDataFrame([(v,) for v in values], "a double")

    def _strings(self, *values):
        return self.spark.createDataFrame([(v,) for v in values], "a string")


class PandasUDFTranspileSubsetTests(PandasUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Differentials over the bodies the transpiler is allowed to rewrite."""

    def test_arithmetic_on_integrals(self):
        for dtype in ("tinyint", "smallint", "int", "bigint"):
            df = self.spark.createDataFrame([(1,), (None,), (5,), (-3,), (0,)], f"a {dtype}")
            for func in (
                add_one,
                identity,
                subtract_two,
                times_three,
                negate,
                unary_plus,
                mod_three,
            ):
                with self.subTest(dtype=dtype, func=func.__name__):
                    self._assert_matches_interpreted(func, LongType(), df, "a")

    def test_arithmetic_on_doubles_including_nan(self):
        # Interpreted, NaN in the result reaches Spark as NULL, which the rewrite reproduces.
        df = self._doubles(1.0, None, NAN, -2.5, 0.0)
        for func in (add_one, add_tenth, negate):
            with self.subTest(func=func.__name__):
                self._assert_matches_interpreted(func, DoubleType(), df, "a")
        self._assert_matches_interpreted(times_inf, DoubleType(), self._longs(0, 2), "a")

    def test_modulo(self):
        cases = [
            ("a bigint, b bigint", [(5, 0), (None, 0), (7, -3), (-7, 3)], LongType()),
            ("a double, b double", [(5.0, 0.0), (5.0, -0.0), (7.0, -3.0)], DoubleType()),
            (
                "a double, b double",
                [(-1.0, float("inf")), (1.0, -float("inf")), (-1.0, 1e308), (1.0, -1e308)],
                DoubleType(),
            ),
        ]
        for dtype, minimum in (
            ("tinyint", -(2**7)),
            ("smallint", -(2**15)),
            ("int", -(2**31)),
            ("bigint", LONG_MIN),
        ):
            cases.append(
                (f"a {dtype}, b {dtype}", [(minimum, -1), (1, minimum), (minimum, 3)], LongType())
            )
        for schema, rows, return_type in cases:
            with self.subTest(schema=schema, rows=rows):
                self._assert_matches_interpreted(
                    modulo, return_type, self.spark.createDataFrame(rows, schema), "a", "b"
                )

    def test_noncheap_arguments_still_transpile(self):
        # The NaN normalization and the null check read their input once, so a computed
        # argument does not keep the UDF in Python under an aggregate or a join.
        longs = self.spark.createDataFrame([(0, 1), (0, None), (1, 3)], "k int, v bigint")
        doubles = self.spark.createDataFrame(
            [(0, 1.0), (0, NAN), (1, None), (1, 3.5)], "k int, v double"
        )
        for df, func, return_type in (
            (longs, add_one, LongType()),
            (doubles, add_one, DoubleType()),
            (doubles, is_missing, BooleanType()),
        ):
            with self.subTest(func=func.__name__, schema=df.schema.simpleString()):
                results = []
                for conf in (_TRANSPILE_OFF, _TRANSPILE_ON):
                    with self.sql_conf(conf):
                        pudf = self._pandas_udf(func, return_type)
                        grouped = df.groupBy("k").agg(F.max(pudf(F.col("v") + 1)).alias("m"))
                        left, right = df.alias("l"), df.alias("r")
                        result = pudf(F.col("l.v") + 1).cast("double")
                        joined = left.join(right, result == F.col("r.v"))
                        if conf is _TRANSPILE_ON:
                            self.assertFalse(_python_eval_operators(grouped))
                            self.assertFalse(_python_eval_operators(joined))
                        results.append(
                            (grouped.orderBy("k").collect(), sorted(joined.collect(), key=str))
                        )
                self.assertEqual(results[0], results[1])

    def test_two_argument_body(self):
        df = self.spark.createDataFrame(
            [(2, 3), (None, 4), (5, None), (None, None)], "a bigint, b bigint"
        )
        self._assert_matches_interpreted(combine, LongType(), df, "a", "b")

    def test_string_concat_and_repeat(self):
        df = self._strings("ab", None, "")
        for func in (concat_bang, prefixed, repeat_twice, identity):
            with self.subTest(func=func.__name__):
                self._assert_matches_interpreted(func, StringType(), df, "a")

    def test_string_repeat_by_a_column_falls_back(self):
        # A count column holding a NULL is float64, and `str * float64` raises TypeError, so
        # the interpreted result depends on batching. One partition puts both rows in a batch.
        df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize([("ab", 2), ("ab", None)], 1),
            "a string, n bigint",
        )
        self._assert_falls_back(repeat_by_column, StringType(), df, "a", "n")
        with self.sql_conf(_TRANSPILE_OFF):
            udf = self._pandas_udf(repeat_by_column, StringType())
            values, error = self._values_or_raised(df.select(udf("a", "n")))
        self.assertIs(_RAISED, values)
        self.assertIn("TypeError", str(error))

    def test_series_null_checks(self):
        for df in (self._longs(1, None), self._doubles(1.0, None, NAN)):
            for func in (is_missing, is_missing_isna, is_present_notnull, is_present_notna):
                with self.subTest(func=func.__name__, schema=df.schema.simpleString()):
                    self._assert_matches_interpreted(func, BooleanType(), df, "a")
        # Pinned as well: `isnull()` is True for NaN, which isnull(a) alone would miss.
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(is_missing, BooleanType())
            values = [r[0] for r in self._doubles(1.0, None, NAN).select(pudf("a")).collect()]
        self.assertEqual([False, True, True], values)

    def test_int_extension_dtype_disables_transpilation(self):
        # Under this conf a NaN result stays NaN. It is refused at construction, and
        # ConvertToCatalyst falls back when it is only set afterwards.
        with self.sql_conf({**_TRANSPILE_ON, _INT_EXT_DTYPE: True}):
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                self.assertEqual([], self._pandas_udf(add_one, LongType()).transpiled)
        df = self._longs(0)
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(times_inf, DoubleType())
            self._assert_transpiled(pudf, "times_inf")
            with self.sql_conf({_INT_EXT_DTYPE: True}):
                on = [r[0] for r in df.select(pudf("a")).collect()]
        self.assertEqual("[nan]", repr(on))

    def test_pandas_udf_decorator_path_transpiles(self):
        with self.sql_conf(_TRANSPILE_ON):
            wrapped = pandas_udf(add_one, LongType(), PythonEvalType.SQL_SCALAR_PANDAS_UDF)
            self._assert_transpiled(wrapped._unwrapped, "pandas_udf(add_one)")
            df = self.spark.createDataFrame([Row(a=1)])
            self.assertEqual([2], [r[0] for r in df.select(wrapped("a")).collect()])

    def test_transpiled_pandas_udf_called_with_keyword_arguments(self):
        # The rewrite reads its inputs positionally, so keyword arguments must be reordered.
        # The body is not symmetric in its arguments, so a swapped binding changes the rows.
        df = self.spark.createDataFrame([(2, 3), (None, 4)], "a bigint, b bigint")
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(ten_a_plus_b, LongType())
            called = df.select(pudf(b=df["b"], a=df["a"]))
            self.assertFalse(_python_eval_operators(called))
            self.assertEqual([23, None], [r[0] for r in called.collect()])


class PandasUDFTranspileRefusalTests(PandasUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Bodies outside the subset must fall back, preserving behavior exactly."""

    def test_bodies_outside_the_subset_fall_back(self):
        longs = self._longs(5, None, -1)
        for func, return_type, df, reason in (
            (greater_than_zero, BooleanType(), longs, "a comparison"),
            (equals_five, BooleanType(), longs, "a comparison"),
            (not_equals_zero, BooleanType(), longs, "a comparison"),
            # These test a plain bool, so the `is` test is the reason even under `if`/`not`.
            (is_none, BooleanType(), longs, "an `is` test"),
            (is_not_none, BooleanType(), longs, "an `is` test"),
            (early_none_guard, LongType(), longs, "an `is` test"),
            (not_is_none, LongType(), longs, "an `is` test"),
            (is_not_none_and, LongType(), longs, "an `is` test"),
            (branching, LongType(), longs, "ValueError"),
            (logical_not, LongType(), longs, "ValueError"),
            (logical_and, LongType(), longs, "ValueError"),
            (absolute, LongType(), longs, "function call other than"),
            (filled, LongType(), longs, "function call other than"),
            (uppercased, StringType(), self._strings("a", None), "function call other than"),
            (free_variable, LongType(), longs, "free variable"),
            (two_statements, LongType(), longs, "more than one top-level statement"),
            (scalar_modulo_by_zero, LongType(), longs, "scalar-only modulo"),
            (scalar_modulo, LongType(), longs, "scalar-only modulo"),
            (halved, LongType(), longs, None),
            (isnull_of_expression, BooleanType(), longs, None),
            (times_one_and_a_half, StringType(), self._strings("ab"), None),
            # No parameter is read, so the result is a scalar the interpreted path rejects.
            (constant_body, LongType(), longs, None),
            (bare_return, LongType(), longs, None),
            (bool_constant, LongType(), longs, None),
        ):
            with self.subTest(func=func.__name__):
                self._assert_falls_back(func, return_type, df, "a", expect_reason=reason)

    def test_fractional_body_for_an_integral_return_type_falls_back(self):
        # Interpreted, the safe Arrow conversion fails where the lowered cast would truncate.
        df = self._longs(3, -3, None)
        for return_type in (ByteType(), ShortType(), IntegerType(), LongType()):
            with self.subTest(returnType=return_type.simpleString()):
                self._assert_falls_back(
                    times_one_and_a_half, return_type, df, "a", expect_reason="integral return"
                )
        for return_type in (FloatType(), DoubleType()):
            with self.subTest(returnType=return_type.simpleString()):
                self._assert_matches_interpreted(times_one_and_a_half, return_type, df, "a")

    def test_integral_body_over_a_fractional_column_stays_interpreted(self):
        # `s + 1` declared bigint has an integer variant, but a double column binds only the
        # refused fractional one.
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(add_one, LongType())
            self._assert_transpiled(pudf, "add_one")
            query = self._doubles(1.0, None).select(pudf("a"))
            self.assertEqual({"ArrowEvalPythonExec": 1}, dict(_eval_python_nodes(query)))
            self.assertEqual([2, None], [r[0] for r in query.collect()])

    def test_decimal_return_type_does_not_transpile(self):
        with self.sql_conf(_TRANSPILE_ON), warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            self.assertEqual([], self._pandas_udf(add_one, DecimalType(10, 2)).transpiled)


class PandasUDFTranspileDivergenceTests(PandasUDFTranspileTestsMixin, ReusedSQLTestCase):
    """numpy computes in the input's fixed width where Catalyst is exact or raises."""

    def test_integer_overflow(self):
        # numpy wraps. Catalyst widens a narrow column for `+ 1` and raises past int and bigint.
        for dtype, maximum in (
            ("tinyint", 127),
            ("smallint", 32767),
            ("int", 2**31 - 1),
            ("bigint", 2**63 - 1),
        ):
            with self.subTest(dtype=dtype):
                df = self.spark.createDataFrame([(maximum,)], f"a {dtype}")
                interpreted, transpiled = self._pin_divergence(add_one, LongType(), df, "a")
                self.assertEqual(([-maximum - 1], None), interpreted)
                if dtype in ("int", "bigint"):
                    self.assertIs(_RAISED, transpiled[0])
                    self.assertIn("ARITHMETIC_OVERFLOW", str(transpiled[1]))
                else:
                    self.assertEqual(([maximum + 1], None), transpiled)

    def test_literal_too_wide_for_the_column(self):
        # NumPy 2 refuses it; NumPy 1 widens the result and agrees with the transpiled 1001.
        import numpy as np

        df = self.spark.createDataFrame([(1,)], "a tinyint")
        interpreted, transpiled = self._pin_divergence(add_wide_literal, LongType(), df, "a")
        if int(np.__version__.split(".")[0]) >= 2:
            self.assertIs(_RAISED, interpreted[0])
            self.assertIn("OverflowError", str(interpreted[1]))
        else:
            self.assertEqual(([1001], None), interpreted)
        self.assertEqual(([1001], None), transpiled)

    def test_float32_column_keeps_float32_interpreted(self):
        # NEP 50: float32 + <python float> stays float32. Catalyst promotes to double.
        df = self.spark.createDataFrame([(1.0,)], "a float")
        (interpreted, _), transpiled = self._pin_divergence(add_tenth, DoubleType(), df, "a")
        self.assertEqual(([1.1], None), transpiled)
        self.assertNotEqual([1.1], interpreted)
        self.assertAlmostEqual(1.1, interpreted[0], places=6)

    def test_integral_column_loses_precision_interpreted_when_a_batch_has_a_null(self):
        # A NULL makes the batch float64, rounding 2**53 + 1. One partition shares the batch.
        df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize([(2**53 + 1,), (None,)], 1), "a bigint"
        )
        interpreted, transpiled = self._pin_divergence(add_one, LongType(), df, "a")
        self.assertEqual(([2**53, None], None), interpreted)
        self.assertEqual(([2**53 + 2, None], None), transpiled)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
