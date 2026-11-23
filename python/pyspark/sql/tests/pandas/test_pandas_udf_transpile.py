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
sides of every differential run at this eval type. The suites are:

``...SubsetTests``: differentials over the supported subset.
``...RefusalTests``: bodies outside it are not rewritten AND behave exactly as before,
    including the ones that raise today.
``...DivergenceTests``: the known differences from numpy's fixed-width arithmetic, pinned
    with values.
``...ReturnTypeCastTests``: a fractional body with an integral return type is refused.

UDF bodies are module-level ``def``s, as in the Arrow suite.
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

# 2**53 + 1 is the smallest positive integer float64 cannot represent, so it is where the
# interpreted path's int -> float64 conversion starts losing precision.
UNREPRESENTABLE_IN_FLOAT64 = 2**53 + 1
LONG_MAX = 9223372036854775807
LONG_MIN = -9223372036854775808


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
    # 1e400 is an inf float constant, so this is inside the allowlist, and 0 * inf is NaN.
    # The only body here that produces NaN from non-NaN input, which is what makes it the
    # probe for the two NaN regimes.
    return s * 1e400


# --- UDF bodies (outside the subset) ---------------------------------------------------


def greater_than_zero(s):
    # A bool Series interpreted, and a perfectly reasonable pandas UDF -- but the scalar
    # lowering guards NULL operands with raise_error(), which is right for a Python scalar
    # and wrong for a Series (where a missing value simply compares False).
    return s > 0


def equals_five(s):
    return s == 5


def not_equals_zero(s):
    return s != 0


def is_none(s):
    # Interpreted this is a Python bool, not a Series, so it fails with UDF_RETURN_TYPE.
    # `s.isnull()` is the construct that means per-element.
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
    # Only a bare parameter may be the receiver, so this falls back even though every
    # individual piece is in the subset.
    return (s + 1).isnull()


def constant_body(s):
    # Returns a Python int, not a Series: the interpreted path fails with UDF_RETURN_TYPE.
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


class PandasUDFTranspileTestsMixin(ArrowUDFTranspileTestsMixin):
    """The shared differential helpers, run at the scalar pandas eval type."""

    _eval_type = PythonEvalType.SQL_SCALAR_PANDAS_UDF

    def _pandas_udf(self, func, return_type):
        # Not ``pandas_udf``: the bodies are unannotated on purpose, so neither the eval type
        # nor the transpiler's input-type categories depend on type hints.
        return UserDefinedFunction(func, return_type, evalType=self._eval_type)

    def _assert_falls_back(self, func, return_type, df, *cols, expect_reason=None):
        """Assert ``func`` is not rewritten AND behaves exactly as it does with the flag off.

        The second half is what makes these tests worth having: several of these bodies raise
        interpreted, and "the rewrite made a raising UDF return values" is a behavior change
        even though it looks like an improvement.
        """
        name = getattr(func, "__name__", repr(func))
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with self.sql_conf(_TRANSPILE_ON):
                pudf = self._pandas_udf(func, return_type)
                self.assertEqual(
                    [],
                    pudf.transpiled,
                    f"{name} was rewritten, but its body is outside the element-wise subset "
                    "a pandas Series supports",
                )
                on = self._values_or_raised(df.select(pudf(*cols)))
        if expect_reason is not None:
            messages = " ".join(str(w.message) for w in caught)
            self.assertIn(
                expect_reason,
                messages,
                f"{name}: expected the fallback reason to mention {expect_reason!r}, "
                f"got {messages!r}",
            )

        with self.sql_conf(_TRANSPILE_OFF):
            pudf = self._pandas_udf(func, return_type)
            off = self._values_or_raised(df.select(pudf(*cols)))

        on_values, on_error = on
        off_values, off_error = off
        if off_values is _RAISED:
            self.assertIs(
                on_values,
                _RAISED,
                f"{name}: raises with transpilation off ({off_error!r}) but returned "
                f"{on_values!r} with it on; falling back must preserve the failure",
            )
            self.assertEqual(
                type(off_error),
                type(on_error),
                f"{name}: fallback changed the exception type",
            )
            return
        self.assertEqual(off_values, on_values, f"{name}: fallback changed the result")

    def _pin_divergence(self, func, return_type, df, *cols, conf=None):
        """Return ``(interpreted, transpiled)``, each a value list or a raised exception.

        Deliberately the inverse of ``_assert_matches_interpreted``: here the two sides are
        expected to differ, and the point is to record how.
        """
        extra = conf or {}
        with self.sql_conf({**_TRANSPILE_OFF, **extra}):
            interpreted = self._values_or_raised(
                df.select(self._pandas_udf(func, return_type)(*cols))
            )
        with self.sql_conf({**_TRANSPILE_ON, **extra}):
            pudf = self._pandas_udf(func, return_type)
            self._assert_transpiled(pudf, getattr(func, "__name__", repr(func)))
            transpiled = self._values_or_raised(df.select(pudf(*cols)))
        return interpreted, transpiled

    # --- DataFrame fixtures ---

    def _longs(self, *values):
        return self.spark.createDataFrame([(v,) for v in values], "a bigint")

    def _doubles(self, *values):
        return self.spark.createDataFrame([(v,) for v in values], "a double")

    def _strings(self, *values):
        return self.spark.createDataFrame([(v,) for v in values], "a string")

    def _single_partition_longs(self, *values):
        """One partition, so every row shares a batch.

        The int -> float64 conversion happens per batch, so a NULL only degrades the values
        it is batched with. Tests that pin that behavior must control the partitioning.
        """
        return self.spark.createDataFrame(
            self.spark.sparkContext.parallelize([(v,) for v in values], 1), "a bigint"
        )


class PandasUDFTranspileSubsetTests(PandasUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Differentials over the bodies the transpiler is allowed to rewrite."""

    def test_arithmetic_on_longs(self):
        df = self._longs(1, None, 5, -3, 0)
        for func in (add_one, identity, subtract_two, times_three, negate, unary_plus, mod_three):
            with self.subTest(func=func.__name__):
                self._assert_matches_interpreted(func, LongType(), df, "a")

    def test_arithmetic_on_narrow_integrals(self):
        for dtype in ("tinyint", "smallint", "int"):
            df = self.spark.createDataFrame([(1,), (None,), (-3,), (0,)], f"a {dtype}")
            for func in (add_one, subtract_two, times_three, negate, mod_three):
                with self.subTest(dtype=dtype, func=func.__name__):
                    self._assert_matches_interpreted(func, LongType(), df, "a")

    def test_integral_options_do_not_duplicate_noncheap_arguments(self):
        df = self.spark.createDataFrame([(0, 1), (0, None), (1, 3)], "k int, v bigint")
        results = []
        for conf in (_TRANSPILE_OFF, _TRANSPILE_ON):
            with self.sql_conf(conf):
                pudf = self._pandas_udf(add_one, LongType())
                grouped = df.groupBy("k").agg(F.sum(pudf(F.col("v") + 1)).alias("total"))
                left = df.alias("left")
                right = df.alias("right")
                joined = left.join(right, pudf(F.col("left.v") + 1) == F.col("right.v")).select(
                    F.col("left.v"), F.col("right.v")
                )
                if conf is _TRANSPILE_ON:
                    self.assertEqual(0, sum(_eval_python_nodes(grouped).values()))
                    self.assertEqual(0, sum(_eval_python_nodes(joined).values()))
                    self.assertNotIn(
                        "isnan", grouped._jdf.queryExecution().optimizedPlan().toString()
                    )
                results.append((grouped.orderBy("k").collect(), joined.collect()))
        self.assertEqual(results[0], results[1])

    def test_fractional_options_do_not_duplicate_noncheap_arguments(self):
        # The NaN normalization and the null check read their input once for a float variant,
        # so a non-cheap argument still transpiles under an aggregate.
        df = self.spark.createDataFrame(
            [(0, 1.0), (0, NAN), (1, None), (1, 3.5)], "k int, v double"
        )
        for func, return_type in ((add_one, DoubleType()), (is_missing, BooleanType())):
            with self.subTest(func=func.__name__):
                results = []
                for conf in (_TRANSPILE_OFF, _TRANSPILE_ON):
                    with self.sql_conf(conf):
                        pudf = self._pandas_udf(func, return_type)
                        grouped = df.groupBy("k").agg(F.max(pudf(F.col("v") + 1)).alias("m"))
                        if conf is _TRANSPILE_ON:
                            self.assertEqual(0, sum(_eval_python_nodes(grouped).values()))
                        results.append(grouped.orderBy("k").collect())
                self.assertEqual(results[0], results[1])

    def test_arithmetic_on_doubles_including_nan(self):
        # NaN is the case the isnan-to-NULL normalization exists for: interpreted, the result
        # Series is masked with `series.isnull()`, which is True for NaN, so NaN reaches Spark
        # as NULL. Catalyst would otherwise keep the NaN. NaN propagates identically through
        # every allowlisted operator, so the operator matrix stays on the long column above
        # and this covers the two shapes that differ: an int literal and a float one.
        df = self._doubles(1.0, None, NAN, -2.5, 0.0)
        for func in (add_one, add_tenth, negate):
            with self.subTest(func=func.__name__):
                self._assert_matches_interpreted(func, DoubleType(), df, "a")

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
        # A bigint count repeats element-wise only while its batch has no NULL: with one,
        # pandas holds the column as float64 and `str * float64` raises TypeError, where the
        # lowered repeat(s, cast(n as int)) would return values. So whether the query fails
        # would depend on batching, and the transpiler must refuse. One partition puts both
        # rows in one batch. (A double count never reaches the refusal: `_category` has no
        # string-by-float variant.)
        df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize([("ab", 2), ("ab", None)], 1),
            "a string, n bigint",
        )
        with self.sql_conf(_TRANSPILE_OFF):
            off = self._values_or_raised(
                df.select(self._pandas_udf(repeat_by_column, StringType())("a", "n"))
            )
        self.assertIs(_RAISED, off[0])
        self.assertIn("TypeError", str(off[1]))
        self._assert_falls_back(repeat_by_column, StringType(), df, "a", "n")

    def test_string_repeat_by_a_fractional_literal_falls_back(self):
        # A float literal (`s * 1.5`) is refused too: repeat by a fractional count has no
        # element-wise meaning, and pandas raises TypeError. Falling back preserves that
        # raise, rather than returning the truncated-repeat the scalar eval types still do.
        self._assert_falls_back(times_one_and_a_half, StringType(), self._strings("ab"), "a")

    def test_series_null_check_family(self):
        for func in (is_missing, is_missing_isna, is_present_notnull, is_present_notna):
            with self.subTest(func=func.__name__):
                self._assert_matches_interpreted(func, BooleanType(), self._longs(1, None), "a")

    def test_series_null_check_treats_nan_as_missing(self):
        # ``Series.isnull()`` is True for NaN, and by the time the UDF runs a NULL in a
        # double column has already become NaN, so the lowering needs the isnan arm to agree.
        # The value is pinned as well as the agreement, because `isnull(a)` alone would also
        # agree with an interpreted path that had the same bug.
        df = self._doubles(1.0, None, NAN)
        self._assert_matches_interpreted(is_missing, BooleanType(), df, "a")
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(is_missing, BooleanType())
            self.assertEqual(
                [False, True, True],
                [r[0] for r in df.select(pudf("a")).collect()],
                "NaN must count as missing, like pandas -- isnull(a) alone would say False",
            )

    def test_int_extension_dtype_disables_transpilation(self):
        # Integral inputs become masked arrays whose derived results can retain NaN.
        # Refuse the regime at construction and again if the conf changes afterwards.
        ext_on = {"spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype": True}
        with self.sql_conf({**_TRANSPILE_ON, **ext_on}):
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                self.assertEqual([], self._pandas_udf(add_one, LongType()).transpiled)
                self.assertEqual([], self._pandas_udf(is_missing, BooleanType()).transpiled)

    def test_int_extension_dtype_flipped_after_construction_still_agrees(self):
        # The Python-side gate cannot catch this: the UDF is built under the default regime,
        # so it carries transpiled options, and the conf only changes afterwards. The
        # ConvertToCatalyst re-check is what keeps the result right.
        df = self._longs(0)
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(times_inf, DoubleType())
            self._assert_transpiled(pudf, "times_inf")
            with self.sql_conf(
                {"spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype": True}
            ):
                on = [r[0] for r in df.select(pudf("a")).collect()]
        with self.sql_conf(
            {**_TRANSPILE_OFF, "spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype": True}
        ):
            off = [
                r[0] for r in df.select(self._pandas_udf(times_inf, DoubleType())("a")).collect()
            ]
        # 0 * inf is NaN, and under this conf that NaN reaches Spark as NaN, not NULL.
        self.assertEqual(1, len(off))
        self.assertTrue(off[0] != off[0], f"expected interpreted NaN, got {off!r}")
        self.assertEqual(repr(off), repr(on), "the ConvertToCatalyst re-check did not fall back")

    def test_nan_from_infinite_arithmetic_becomes_null_in_the_default_regime(self):
        # The other half of the pair above: with the conf off, the interpreted NaN really is
        # masked to NULL, which is what the normalization reproduces.
        self._assert_matches_interpreted(times_inf, DoubleType(), self._longs(0, 2), "a")

    def test_pandas_udf_decorator_path_transpiles(self):
        # What users actually write, via the public API rather than UserDefinedFunction.
        with self.sql_conf(_TRANSPILE_ON):
            wrapped = pandas_udf(add_one, LongType(), PythonEvalType.SQL_SCALAR_PANDAS_UDF)
            self.assertEqual(wrapped.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
            self._assert_transpiled(wrapped._unwrapped, "pandas_udf(add_one)")
            df = self.spark.createDataFrame([Row(a=1)])
            self.assertEqual([2], [r[0] for r in df.select(wrapped("a")).collect()])

    def test_transpiled_pandas_udf_called_with_keyword_arguments(self):
        # The rewrite references its inputs positionally as _udf_param_N, so
        # UserDefinedFunction.__call__ resolves kwargs to positional order using the parameter
        # names captured at transpilation time; without that the JVM would splice
        # NamedArgumentExpression nodes into the rewritten tree. Shared with the other eval
        # types but not otherwise covered for any of them, which is why it lives here. The body
        # is not symmetric in its arguments, so a swapped binding changes the rows, and the plan
        # check catches a call that was never rewritten.
        df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize([(2, 3), (None, 4)], 1), "a bigint, b bigint"
        )
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(ten_a_plus_b, LongType())
            self._assert_transpiled(pudf, "ten_a_plus_b")
            called = df.select(pudf(b=df["b"], a=df["a"]))
            self.assertEqual(0, sum(_eval_python_nodes(called).values()))
            on = [r[0] for r in called.collect()]
        with self.sql_conf(_TRANSPILE_OFF):
            pudf = self._pandas_udf(ten_a_plus_b, LongType())
            off = [r[0] for r in df.select(pudf(b=df["b"], a=df["a"])).collect()]
        self.assertEqual(off, on)
        self.assertEqual([23, None], on)


class PandasUDFTranspileRefusalTests(PandasUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Bodies outside the subset must fall back, preserving behavior exactly."""

    def test_scalar_only_modulo_inside_series_body_falls_back(self):
        def raises(s):
            return s + 1 % 0

        def succeeds(s):
            return s + 5 % 3

        for func in (raises, succeeds):
            self._assert_falls_back(
                func, LongType(), self._longs(1, 2), "a", expect_reason="scalar-only modulo"
            )

    def test_comparisons_fall_back(self):
        self._assert_falls_back(
            greater_than_zero,
            BooleanType(),
            self._longs(1, None, -1),
            "a",
            expect_reason="an ordering comparison",
        )
        for func in (equals_five, not_equals_zero):
            with self.subTest(func=func.__name__):
                self._assert_falls_back(
                    func,
                    BooleanType(),
                    self._longs(5, None),
                    "a",
                    expect_reason="an `==` / `!=` comparison",
                )

    def test_is_none_falls_back(self):
        # Interpreted this fails with UDF_RETURN_TYPE: `s is None` is a Python bool, not a
        # Series. A rewrite to isnull(a) would turn that failure into values, which this suite
        # protects. The same holds for `is not`.
        for func, reason in ((is_none, "an `is` test"), (is_not_none, "an `is not` test")):
            with self.subTest(func=func.__name__):
                self._assert_falls_back(
                    func, BooleanType(), self._longs(1, None), "a", expect_reason=reason
                )

    def test_none_test_inside_control_flow_reports_the_is_test(self):
        # What `if`, `not` and `and` test here is a plain bool, so the ValueError reason is wrong.
        for func, reason in (
            (early_none_guard, "an `is` test"),
            (not_is_none, "an `is` test"),
            (is_not_none_and, "an `is not` test"),
        ):
            with self.subTest(func=func.__name__):
                self._assert_falls_back(
                    func, LongType(), self._longs(1, None), "a", expect_reason=reason
                )

    def test_control_flow_falls_back_and_still_raises(self):
        # `if <series>:` raises ValueError interpreted. The rewrite must not turn that into a
        # working query -- _assert_falls_back checks the failure survives.
        for func in (branching, logical_not, logical_and):
            with self.subTest(func=func.__name__):
                self._assert_falls_back(
                    func, LongType(), self._longs(1, None), "a", expect_reason="ValueError"
                )

    def test_unlowered_operator_falls_back(self):
        # `**`, `/` and `//` all fail the same `isinstance(op, _SERIES_SAFE_BINOPS)` check, so
        # one of them exercises the branch; `/` is the one users are most likely to write.
        self._assert_falls_back(halved, LongType(), self._longs(4, None), "a")

    def test_other_calls_fall_back(self):
        for func in (absolute, filled):
            with self.subTest(func=func.__name__):
                self._assert_falls_back(
                    func,
                    LongType(),
                    self._longs(-4, None),
                    "a",
                    expect_reason="function call other than",
                )
        # A str method the scalar transpiler lowers is still not a Series method.
        self._assert_falls_back(
            uppercased,
            StringType(),
            self._strings("a", None),
            "a",
            expect_reason="function call other than",
        )

    def test_null_check_on_non_parameter_receiver_falls_back(self):
        self._assert_falls_back(isnull_of_expression, BooleanType(), self._longs(1, None), "a")

    def test_scalar_bodies_fall_back_and_still_raise(self):
        # A body that never references a parameter produces a Python scalar, and the
        # interpreted path rejects that with UDF_RETURN_TYPE. Returning a value per row would
        # be a nicer outcome and a behavior change, so it must stay a failure. Three distinct
        # refusal branches: no parameter reference, a bare `return`, and a bool constant.
        for func in (constant_body, bare_return, bool_constant):
            with self.subTest(func=func.__name__):
                self._assert_falls_back(func, LongType(), self._longs(1), "a")

    def test_free_variable_falls_back(self):
        self._assert_falls_back(
            free_variable,
            LongType(),
            self._longs(1),
            "a",
            expect_reason="free variable",
        )

    def test_multi_statement_body_reports_the_accurate_reason(self):
        # The Series allowlist walk only visits the first statement, so it must check the
        # statement count first -- otherwise a two-statement body is refused as "an
        # unsupported `Assign` node" (its first statement) rather than for the real reason,
        # and that warning is the only place the user learns why the rewrite did not happen.
        self._assert_falls_back(
            two_statements,
            LongType(),
            self._longs(1, None),
            "a",
            expect_reason="more than one top-level statement",
        )


class PandasUDFTranspileDivergenceTests(PandasUDFTranspileTestsMixin, ReusedSQLTestCase):
    """The known differences, pinned with values so a change fails a test.

    Each is numpy computing in the input's fixed width where ANSI Catalyst is exact. They are
    documented rather than guarded because detecting them needs the runtime values, not the
    types. See the "Scalar pandas UDFs" section of python/pyspark/sql/transpile.py.
    """

    def test_modulo_by_zero_matches_numpy_missing_value(self):
        for schema, values, return_type in (
            ("a bigint, b bigint", [(5, 0), (None, 0), (7, -3), (-7, 3)], LongType()),
            ("a double, b double", [(5.0, 0.0), (5.0, -0.0), (7.0, -3.0)], DoubleType()),
        ):
            with self.subTest(schema=schema):
                df = self.spark.createDataFrame(values, schema)
                self._assert_matches_interpreted(modulo, return_type, df, "a", "b")

    def test_modulo_matches_numpy_at_integral_minima_and_infinite_divisors(self):
        cases = [
            (f"a {dtype}, b {dtype}", [(minimum, -1), (1, minimum), (minimum, 3)], LongType())
            for dtype, minimum in (
                ("tinyint", -(2**7)),
                ("smallint", -(2**15)),
                ("int", -(2**31)),
                ("bigint", LONG_MIN),
            )
        ]
        cases.append(
            (
                "a double, b double",
                [(-1.0, float("inf")), (1.0, -float("inf")), (-1.0, 1e308), (1.0, -1e308)],
                DoubleType(),
            )
        )
        for schema, rows, return_type in cases:
            with self.subTest(schema=schema):
                df = self.spark.createDataFrame(rows, schema)
                with self.sql_conf(_TRANSPILE_ON):
                    pudf = self._pandas_udf(modulo, return_type)
                    self.assertFalse(_eval_python_nodes(df.select(pudf("a", "b"))))
                self._assert_matches_interpreted(modulo, return_type, df, "a", "b")

    def test_narrow_integer_overflow_and_wide_literal(self):
        for dtype, maximum in (("tinyint", 127), ("smallint", 32767), ("int", 2**31 - 1)):
            df = self.spark.createDataFrame([(maximum,)], f"a {dtype}")
            interpreted, transpiled = self._pin_divergence(add_one, LongType(), df, "a")
            self.assertEqual(([-maximum - 1], None), interpreted)
            if dtype == "int":
                self.assertIs(transpiled[0], _RAISED)
                self.assertIn("ARITHMETIC_OVERFLOW", str(transpiled[1]))
            else:
                self.assertEqual(([maximum + 1], None), transpiled)

        # A literal too wide for the column's dtype: NumPy 2 refuses it, NumPy 1 widens the
        # result's dtype (value-based casting) and agrees with the transpiled 1001.
        import numpy as np

        df = self.spark.createDataFrame([(1,)], "a tinyint")
        interpreted, transpiled = self._pin_divergence(add_wide_literal, LongType(), df, "a")
        if int(np.__version__.split(".")[0]) >= 2:
            self.assertIs(interpreted[0], _RAISED)
            self.assertIn("OverflowError", str(interpreted[1]))
        else:
            self.assertEqual(([1001], None), interpreted)
        self.assertEqual(([1001], None), transpiled)

    def test_integer_overflow_raises_where_numpy_wraps(self):
        df = self._longs(LONG_MAX)
        with self.sql_conf(_TRANSPILE_OFF):
            interpreted = [
                r[0] for r in df.select(self._pandas_udf(add_one, LongType())("a")).collect()
            ]
        self.assertEqual(
            [-LONG_MAX - 1],
            interpreted,
            "numpy int64 is expected to wrap; if it started raising, this divergence is gone "
            "and the note in transpile.py should be updated",
        )
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(add_one, LongType())
            self._assert_transpiled(pudf, "add_one")
            with self.assertRaises(Exception) as ctx:
                df.select(pudf("a")).collect()
            self.assertIn("ARITHMETIC_OVERFLOW", str(ctx.exception))

    def test_float32_column_keeps_float32_interpreted(self):
        # NEP 50: float32 + <python float> stays float32, so the interpreted result carries
        # float32 rounding into a double return type. Catalyst promotes to double instead.
        df = self.spark.createDataFrame([(1.0,)], "a float")
        with self.sql_conf(_TRANSPILE_OFF):
            interpreted = [
                r[0] for r in df.select(self._pandas_udf(add_tenth, DoubleType())("a")).collect()
            ]
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(add_tenth, DoubleType())
            self._assert_transpiled(pudf, "add_tenth")
            transpiled = [r[0] for r in df.select(pudf("a")).collect()]
        self.assertEqual([1.1], transpiled)
        self.assertNotEqual(interpreted, transpiled)
        self.assertAlmostEqual(1.1, interpreted[0], places=6)

    def test_integral_column_loses_precision_interpreted_when_a_batch_has_a_null(self):
        # A NULL forces the batch to float64, so a value above 2**53 is rounded before the
        # function ever sees it. One partition, so both rows share a batch -- with the NULL in
        # another partition the interpreted answer would be exact, which is what makes this
        # divergence batch-dependent (and the rewrite the more predictable option).
        df = self._single_partition_longs(UNREPRESENTABLE_IN_FLOAT64, None)
        with self.sql_conf(_TRANSPILE_OFF):
            interpreted = [
                r[0] for r in df.select(self._pandas_udf(add_one, LongType())("a")).collect()
            ]
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(add_one, LongType())
            self._assert_transpiled(pudf, "add_one")
            transpiled = [r[0] for r in df.select(pudf("a")).collect()]
        self.assertEqual([UNREPRESENTABLE_IN_FLOAT64 + 1, None], transpiled)
        self.assertEqual([2**53, None], interpreted)


class PandasUDFTranspileReturnTypeCastTests(PandasUDFTranspileTestsMixin, ReusedSQLTestCase):
    """A possibly fractional body declared with an integral return type is not rewritten.

    Interpreted, the float64 result fails the safe Arrow conversion; the lowered cast would
    truncate toward zero instead. See "The return-type cast" in transpile.py.
    """

    def test_fractional_body_for_an_integral_return_type_falls_back(self):
        # Every variant is refused, and the UDF still raises as it does interpreted.
        df = self._longs(3, -3, None)
        for return_type in (ByteType(), ShortType(), IntegerType(), LongType()):
            with self.subTest(returnType=return_type.simpleString()):
                self._assert_falls_back(
                    times_one_and_a_half,
                    return_type,
                    df,
                    "a",
                    expect_reason="integral return type",
                )
        with self.sql_conf(_TRANSPILE_OFF):
            values, error = self._values_or_raised(
                df.select(self._pandas_udf(times_one_and_a_half, LongType())("a"))
            )
        self.assertIs(values, _RAISED)
        self.assertIn("convertToArrowArraySafely", str(error))

    def test_integral_body_over_a_fractional_column_stays_interpreted(self):
        # `s + 1` declared bigint has an integer variant, but over a double column only the
        # refused fractional one would bind, so the Python UDF is kept.
        df = self._doubles(1.0, None)
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(add_one, LongType())
            self._assert_transpiled(pudf, "add_one")
            query = df.select(pudf("a"))
            self.assertEqual({"ArrowEvalPythonExec": 1}, dict(_eval_python_nodes(query)))
            self.assertEqual([2, None], [r[0] for r in query.collect()])

    def test_fractional_body_agrees_for_a_fractional_return_type(self):
        for return_type in (FloatType(), DoubleType()):
            with self.subTest(returnType=return_type.simpleString()):
                self._assert_matches_interpreted(
                    times_one_and_a_half, return_type, self._longs(3, -3, None), "a"
                )


class PandasUDFTranspileDecimalReturnTests(PandasUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Return types the transpiler declines, so that a pandas UDF is unaffected by them."""

    def test_decimal_return_type_does_not_transpile(self):
        with self.sql_conf(_TRANSPILE_ON):
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                pudf = self._pandas_udf(add_one, DecimalType(10, 2))
            self.assertEqual([], pudf.transpiled)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
