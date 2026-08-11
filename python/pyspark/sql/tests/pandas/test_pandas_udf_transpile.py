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

Unlike the regular Python UDF eval types, a scalar pandas UDF is genuinely vectorized: the
function is handed one ``pandas.Series`` per argument and must return a Series. Most of what
the transpiler lowers for a Python scalar would therefore be WRONG here, so only an
element-wise subset is rewritten (``+`` ``-`` ``*`` ``%``, unary ``+``/``-``, and the
``Series.isnull()`` family) and everything else falls back to interpreted Python. The subset
and the reasoning behind each exclusion live in the "Scalar pandas UDFs" section of
``python/pyspark/sql/transpile.py``; this module is where those claims are checked.

The tests come in three kinds, and the distinction matters when one fails:

``...SubsetTests``
    Differentials over the supported subset. Both sides run at ``SQL_SCALAR_PANDAS_UDF``, so
    the reference is the interpreted behavior of the very UDF being rewritten -- not the
    pickled path, which coerces return values differently.

``...RefusalTests``
    Bodies outside the subset. These assert two things at once: that no Catalyst expression
    was produced, and that behavior is byte-for-byte what it was with the feature off. That
    second half is the important one, because several of these UDFs *raise* today
    (``if <series>:`` raises ValueError, a scalar body raises UDF_RETURN_TYPE) and a rewrite
    that silently made them succeed would be a behavior change, not a fix.

``...DivergenceTests``
    The places a transpiled pandas UDF is known to answer differently, pinned with the actual
    values so that a change is a test failure rather than a surprise in production. They all
    come from numpy computing in the input's fixed width: it wraps on integer overflow, it
    keeps ``float32`` through a ``float`` literal, it converts an integral column to
    ``float64`` as soon as a batch contains a NULL, and it answers ``a % 0`` with a missing
    value. In every case the transpiled expression is the exact or ANSI-correct one. Two are
    worse than merely different interpreted, which is the argument for the rewrite rather
    than against it: the precision one is *batch-dependent* (move the NULL to another
    partition and the answer changes, so it is pinned with an explicit single-partition
    DataFrame), and ``a % 0`` answers NULL or ``0`` depending on one conf.

UDF bodies are module-level ``def``s. ``inspect.getsource`` on a lambda passed as a call
argument returns the enclosing statement, whose top-level node is a call rather than a lambda,
so transpilation silently falls back and a differential would compare interpreted against
interpreted and pass while asserting nothing. ``_assert_transpiled`` catches that.
"""

import unittest
import warnings

from pyspark.sql import Row
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
)
from pyspark.sql.udf import UserDefinedFunction
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import PythonEvalType, is_remote_only


# Transpilation requires both flags, at construction and again in ConvertToCatalyst.
_TRANSPILE_ON = {
    "spark.sql.experimental.optimizer.transpilePyUDFs": True,
    "spark.sql.ansi.enabled": True,
}

# ANSI stays on for the interpreted side so a differential isolates the rewrite rather than
# also flipping overflow semantics.
_TRANSPILE_OFF = {
    "spark.sql.experimental.optimizer.transpilePyUDFs": False,
    "spark.sql.ansi.enabled": True,
}

# A differential that silently fell back would compare interpreted against interpreted and
# pass for the wrong reason, so these warnings are fatal in the subset tests.
_BAD_TRANSPILE_WARNING_MARKERS = (
    "Unable to transpile",
    "Exception transpiling",
    "is only supported when ANSI mode is enabled",
)

# "This side raised instead of returning rows."
_RAISED = object()

NAN = float("nan")

# 2**53 + 1 is the smallest positive integer float64 cannot represent, so it is where the
# interpreted path's int -> float64 conversion starts losing precision.
UNREPRESENTABLE_IN_FLOAT64 = 2**53 + 1
LONG_MAX = 9223372036854775807
LONG_MIN = -9223372036854775808


# --- UDF bodies (supported subset) -----------------------------------------------------


def add_one(s):
    return s + 1


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


def is_none(s):
    # Always False interpreted: the Series object is not None. `s.isnull()` is the
    # construct that means per-element.
    return s is None


def ternary_none_guard(s):
    return 0 if s is None else s


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


@unittest.skipIf(is_remote_only(), "UDF transpilation is only supported in non-Connect Spark.")
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,  # type: ignore[arg-type]
)
class PandasUDFTranspileTestsMixin:
    """Helpers shared by the suites in this module."""

    def _pandas_udf(self, func, return_type):
        """Build a scalar pandas UDF without going through type-hint inference.

        ``pandas_udf`` needs either annotations or an explicit functionType, and the bodies
        here are deliberately unannotated so that neither the eval type nor the transpiler's
        input-type categories depend on them.
        """
        return UserDefinedFunction(func, return_type, evalType=PythonEvalType.SQL_SCALAR_PANDAS_UDF)

    def _assert_transpiled(self, pudf, what):
        self.assertTrue(
            pudf.transpiled,
            f"expected {what} to transpile, but no Catalyst expression was produced; "
            "a parity assertion against the interpreted path would be vacuous",
        )

    def _values_or_raised(self, df):
        try:
            return [row[0] for row in df.collect()], None
        except Exception as e:  # the exception itself is what we compare
            return _RAISED, e

    def _differential(self, func, return_type, df, *cols, conf=None):
        """Run ``func`` with transpilation on and off; return ``(values, error)`` for each.

        Both sides run at ``SQL_SCALAR_PANDAS_UDF``, so the reference is the interpreted
        behavior of the very UDF being rewritten.
        """
        extra = conf or {}
        name = getattr(func, "__name__", repr(func))

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with self.sql_conf({**_TRANSPILE_ON, **extra}):
                pudf = self._pandas_udf(func, return_type)
                self._assert_transpiled(pudf, name)
                on = self._values_or_raised(df.select(pudf(*cols)))
        bad = [
            str(w.message)
            for w in caught
            if any(m in str(w.message) for m in _BAD_TRANSPILE_WARNING_MARKERS)
        ]
        self.assertFalse(bad, f"unexpected transpile warnings for {name}: {bad}")

        with self.sql_conf({**_TRANSPILE_OFF, **extra}):
            pudf = self._pandas_udf(func, return_type)
            off = self._values_or_raised(df.select(pudf(*cols)))

        return on, off

    def _assert_matches_interpreted(self, func, return_type, df, *cols, conf=None):
        """Assert transpiled and interpreted agree, including on whether they raise."""
        (on_values, on_error), (off_values, off_error) = self._differential(
            func, return_type, df, *cols, conf=conf
        )
        name = getattr(func, "__name__", repr(func))
        if off_values is _RAISED:
            self.assertIs(
                on_values,
                _RAISED,
                f"{name}: interpreted raised {off_error!r} but transpiled returned {on_values!r}",
            )
            return
        self.assertIsNot(
            on_values,
            _RAISED,
            f"{name}: transpiled raised {on_error!r} but interpreted returned {off_values!r}",
        )
        self.assertEqual(off_values, on_values, f"{name}: transpiled != interpreted")

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

    def test_arithmetic_on_doubles_including_nan(self):
        # NaN is the case the isnan-to-NULL normalization exists for: interpreted, the result
        # Series is masked with `series.isnull()`, which is True for NaN, so NaN reaches Spark
        # as NULL. Catalyst would otherwise keep the NaN. NaN propagates identically through
        # every allowlisted operator, so the operator matrix stays on the long column above
        # and this covers the two shapes that differ: an int literal and a float one.
        df = self._doubles(1.0, None, NAN, -2.5, 0.0)
        for func in (add_one, add_tenth):
            with self.subTest(func=func.__name__):
                self._assert_matches_interpreted(func, DoubleType(), df, "a")

    def test_nan_result_becomes_null_not_nan(self):
        # The explicit form of the above: pin the value, not just the agreement, so a
        # regression that dropped the normalization is legible.
        df = self._doubles(1.0, NAN, None)
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(add_one, DoubleType())
            self._assert_transpiled(pudf, "add_one on doubles")
            self.assertEqual([2.0, None, None], [r[0] for r in df.select(pudf("a")).collect()])

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

    def test_string_repeat_by_a_literal_int_transpiles(self):
        # The one safe repeat count: a literal int can be neither NaN nor NULL and is fixed
        # at transpile time. `repeat_twice` (s * 2) is also in test_string_concat_and_repeat;
        # pinned again here as the positive counterpart to the refusals below.
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(repeat_twice, StringType())
            self._assert_transpiled(pudf, "s * 2")
        self._assert_matches_interpreted(repeat_twice, StringType(), self._strings("ab", None), "a")

    def test_string_repeat_by_a_column_falls_back(self):
        # A Series count makes `str * numeric` unsound in every batch shape: pandas raises
        # TypeError on any genuine float (so it only succeeds, as NULL, for an all-missing
        # batch), while the lowered repeat(s, cast(n as int)) returns a repeated string for a
        # real value and CAST_OVERFLOW for a NaN. So an all-missing batch would turn a
        # NULL-returning query into an error -- the cardinal sin. The transpiler must refuse.
        df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize([("ab", float("nan"))], 1), "a string, b double"
        )
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(repeat_by_column, StringType())
            self.assertEqual([], pudf.transpiled, "string-repeat by a column must not transpile")
            on = self._values_or_raised(df.select(pudf("a", "b")))
        with self.sql_conf(_TRANSPILE_OFF):
            off = self._values_or_raised(
                df.select(self._pandas_udf(repeat_by_column, StringType())("a", "b"))
            )
        # The all-NaN batch: interpreted masks it to NULL, and falling back preserves that.
        self.assertEqual(([None], None), off)
        self.assertEqual(off, on)

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

    def test_series_null_check_on_string_column(self):
        # A string column needs no isnan arm; its missing values are exactly the NULLs.
        self._assert_matches_interpreted(is_missing, BooleanType(), self._strings("x", None), "a")

    def test_int_extension_dtype_disables_transpilation(self):
        # The isnan-to-NULL normalization models the default dtype regime, where the result
        # Series is numpy-backed and masked with isnull(). Under this conf the Series is a
        # pandas masked extension array, handed to Arrow with mask=None and whose isnull() is
        # False for NaN anyway, so a NaN result arrives as NaN rather than NULL and the
        # normalization would be exactly backwards. No single expression covers both regimes,
        # so the whole eval type is refused here -- in udf.py and again in ConvertToCatalyst,
        # since the conf can be flipped after the UDF is built.
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
        # types but not otherwise covered for any of them, which is why it lives here.
        df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize([(2, 3), (None, 4)], 1), "a bigint, b bigint"
        )
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(combine, LongType())
            self._assert_transpiled(pudf, "combine")
            on = [r[0] for r in df.select(pudf(b=df["b"], a=df["a"])).collect()]
        with self.sql_conf(_TRANSPILE_OFF):
            pudf = self._pandas_udf(combine, LongType())
            off = [r[0] for r in df.select(pudf(b=df["b"], a=df["a"])).collect()]
        self.assertEqual(off, on)
        self.assertEqual([5, None], on)


class PandasUDFTranspileRefusalTests(PandasUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Bodies outside the subset must fall back, preserving behavior exactly."""

    def test_comparisons_fall_back(self):
        self._assert_falls_back(
            greater_than_zero,
            BooleanType(),
            self._longs(1, None, -1),
            "a",
            expect_reason="a comparison",
        )
        self._assert_falls_back(equals_five, BooleanType(), self._longs(5, None), "a")

    def test_is_none_falls_back(self):
        # Interpreted this returns False for every row (the Series object is not None), so a
        # rewrite to isnull(a) would change results rather than fix a bug.
        self._assert_falls_back(
            is_none,
            BooleanType(),
            self._longs(1, None),
            "a",
            expect_reason="`is` / `is not` test",
        )

    def test_control_flow_falls_back_and_still_raises(self):
        # `if <series>:` raises ValueError interpreted. The rewrite must not turn that into a
        # working query -- _assert_falls_back checks the failure survives.
        for func in (branching, ternary_none_guard, logical_not, logical_and):
            with self.subTest(func=func.__name__):
                self._assert_falls_back(func, LongType(), self._longs(1, None), "a")

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

    def _mods(self, *pairs):
        return self.spark.createDataFrame(
            self.spark.sparkContext.parallelize(list(pairs), 1), "a bigint, b bigint"
        )

    def test_modulo_by_zero_raises_where_numpy_yields_a_missing_value(self):
        # pandas promotes int64 to float64 for `% 0` and yields NaN, which the return path
        # masks to NULL. ANSI raises REMAINDER_BY_ZERO, and there is no single interpreted answer
        # to reproduce anyway -- see the extension-dtype case below.
        interpreted, transpiled = self._pin_divergence(
            modulo, LongType(), self._mods((5, 0)), "a", "b"
        )
        self.assertEqual(([None], None), interpreted)
        self.assertIs(transpiled[0], _RAISED)
        self.assertIn("REMAINDER_BY_ZERO", str(transpiled[1]))

    def test_modulo_by_zero_yields_zero_under_int_extension_dtype(self):
        # The same body and data, one conf apart, and pandas answers 0 rather than NULL. That
        # inconsistency is the reason `% 0` is documented rather than reproduced -- a guard
        # emitting NULL would match one regime and contradict the other, as well as ANSI.
        # Nothing is transpiled under this conf (see
        # test_int_extension_dtype_disables_transpilation), so this pins the interpreted side
        # only; the transpiled REMAINDER_BY_ZERO is pinned by the default-regime test above.
        with self.sql_conf(
            {
                **_TRANSPILE_OFF,
                "spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype": True,
            }
        ):
            interpreted = self._values_or_raised(
                self._mods((5, 0)).select(self._pandas_udf(modulo, LongType())("a", "b"))
            )
        self.assertEqual(([0], None), interpreted)

    def test_modulo_overflow_boundary_raises_where_numpy_wraps(self):
        # The lowering is `sign(b) * pmod(sign(b) * a, abs(b))`, whose intermediate negation
        # overflows for a = Long.MinValue with b < 0. Same class as the arithmetic overflow
        # above, and already noted on the Mod lowering for the scalar eval types.
        interpreted, transpiled = self._pin_divergence(
            modulo, LongType(), self._mods((LONG_MIN, -1)), "a", "b"
        )
        self.assertEqual(([0], None), interpreted)
        self.assertIs(transpiled[0], _RAISED)
        self.assertIn("ARITHMETIC_OVERFLOW", str(transpiled[1]))

    def test_modulo_agrees_away_from_the_boundaries(self):
        # The sign rule itself matches: numpy `%` takes the divisor's sign, like Python, which
        # is what the pmod-based lowering reproduces. Pinned so the boundary tests above are
        # not mistaken for "modulo is broken".
        self._assert_matches_interpreted(
            modulo,
            LongType(),
            self._mods((7, -3), (-7, 3), (7, 3), (-7, -3), (LONG_MIN, 3), (LONG_MAX, -1)),
            "a",
            "b",
        )

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

    def test_float32_column_agrees_when_the_return_type_is_float(self):
        # The same body with a FloatType return type agrees, because the cast back to float32
        # discards exactly the difference above. Pinned so the divergence above is understood
        # as narrow rather than "float columns are broken".
        df = self.spark.createDataFrame([(1.0,)], "a float")
        self._assert_matches_interpreted(add_tenth, FloatType(), df, "a")

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

    def test_no_precision_loss_under_int_extension_dtype(self):
        # The interpreted path is exact once the UDF receives a nullable Int64 rather than
        # float64, so there is nothing left for the rewrite to improve on -- which is
        # convenient, because transpilation is refused under this conf for the unrelated NaN
        # reason. Pinned to record that the precision divergence above is a property of the
        # default regime, not of pandas UDFs generally.
        df = self._single_partition_longs(UNREPRESENTABLE_IN_FLOAT64, None)
        with self.sql_conf(
            {
                **_TRANSPILE_ON,
                "spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype": True,
            }
        ):
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                pudf = self._pandas_udf(add_one, LongType())
            self.assertEqual([], pudf.transpiled)
            self.assertEqual(
                [UNREPRESENTABLE_IN_FLOAT64 + 1, None],
                [r[0] for r in df.select(pudf("a")).collect()],
            )

    def test_fractional_result_for_an_integral_return_type(self):
        # `s * 1.5` on a bigint column with a bigint return type: interpreted, the float64
        # result fails the Arrow conversion; transpiled, the Cast truncates. Accepted as the
        # same class as the documented return-type-cast behavior for the scalar eval types --
        # the rewrite succeeds where the interpreted path errors out.
        df = self._longs(3, None)
        with self.sql_conf(_TRANSPILE_OFF):
            interpreted = self._values_or_raised(
                df.select(self._pandas_udf(times_one_and_a_half, LongType())("a"))
            )
        with self.sql_conf(_TRANSPILE_ON):
            pudf = self._pandas_udf(times_one_and_a_half, LongType())
            self._assert_transpiled(pudf, "times_one_and_a_half")
            transpiled = [r[0] for r in df.select(pudf("a")).collect()]
        self.assertEqual([4, None], transpiled)
        self.assertIs(
            interpreted[0],
            _RAISED,
            "the interpreted float64 -> int64 conversion was expected to fail; if it now "
            "succeeds, compare the values and update the note in transpile.py",
        )

    def test_fractional_result_agrees_for_a_double_return_type(self):
        # Same body, declared honestly: no divergence.
        self._assert_matches_interpreted(
            times_one_and_a_half, DoubleType(), self._longs(3, None), "a"
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
