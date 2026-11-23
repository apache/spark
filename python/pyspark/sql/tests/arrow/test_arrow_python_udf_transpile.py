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
Transpilation of Arrow-optimized Python UDFs (``SQL_ARROW_BATCHED_UDF``).

Arrow optimization is the default for regular Python UDFs, so this is the eval type a plain
``functions.udf(f, "bigint")`` gets. Arrow is only the transport: the worker still calls the
function once per row with Python scalars, unlike the genuinely vectorized
``SQL_SCALAR_ARROW_UDF``, which must never be transpiled.

The pickled path NULLs a return value of the wrong type where the Arrow path raises or
converts, so every differential here compares transpiled-Arrow against interpreted-Arrow.

UDF bodies are named ``def``s: ``inspect.getsource`` cannot recover a lambda passed as a call
argument, so it would silently fall back and the test would assert nothing.
"""

import tempfile
import unittest
import warnings
from collections import Counter

from pyspark.errors import PythonException, StreamingQueryException
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    CalendarIntervalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    _parse_datatype_json_string,
)
from pyspark.sql.udf import (
    _TRANSPILABLE_EVAL_TYPES,
    UserDefinedFunction,
)
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

# A differential that silently falls back would compare interpreted against interpreted, so
# the warnings udf.py emits when a rewrite does not happen are fatal there.
_BAD_TRANSPILE_WARNING_MARKERS = (
    "Unable to transpile",
    "Exception transpiling",
    "is only supported when ANSI mode is enabled",
)

# "This side raised instead of returning rows."
_RAISED = object()


# --- UDF bodies ------------------------------------------------------------------------


def add_one(x):
    return x + 1


def add_four(x):
    return x + 4


def identity(x):
    return x


def is_null(x):
    return x is None


def square(x):
    return x * x


def mod_zero(x):
    return x % 0


def add_half(x):
    return x + 0.5


def times_thousand(x):
    return x * 1000


def floor_halve(x):
    # Floor division is not lowered: the canonical "must stay interpreted" body.
    return x // 2


def floor_third(x):
    return x // 3


def guarded_add_one(x):
    # Single top-level statement; the transpiler rejects more than one. The implicit
    # else yields NULL, which is also what the interpreted path returns.
    if x is not None:
        return x + 1


def num_add(x):
    return x + 3


def num_sub(x):
    return x - 3


def num_mul(x):
    return x * 3


def num_mod(x):
    return x % 3


def num_neg(x):
    return -x


def num_pos(x):
    return +x


_NUMERIC_ARITHMETIC = (num_add, num_sub, num_mul, num_mod, num_neg, num_pos)


def cmp_lt(x):
    return x < 2


def cmp_le(x):
    return x <= 2


def cmp_gt(x):
    return x > 2


def cmp_ge(x):
    return x >= 2


def cmp_eq(x):
    return x == 2


def cmp_ne(x):
    return x != 2


_NUMERIC_COMPARISONS = (cmp_lt, cmp_le, cmp_gt, cmp_ge, cmp_eq, cmp_ne)


# Annotations pin a parameter's input-type category; an unannotated parameter is only tried
# as numeric and string.
def str_concat(x: str):
    return x + x


def repeat_negated(x):
    return "ab" * -x


def repeat_positive(x):
    return "ab" * +x


def repeat_negated_left(x, s: str):
    return -x * s


def repeat_negative_float(s: str):
    return s * -1.5


def negated_equals_two(x):
    return -x == 2


def guarded_negated_equals_two(x):
    return False if x is None else -x == 2


def str_repeat_right(x: str):
    return x * 3


def str_repeat_left(x: str):
    return 3 * x


_STRING_BODIES = (str_concat, str_repeat_right, str_repeat_left)


# `not`/`and`/`or` need a statically-boolean operand, which a bare parameter is not (see
# _is_definitely_boolean); those fall back, as test_udf_transpile_{not,and_or}_bare_param_*
# in the non-Arrow unit suite pin. Wrapping a comparison is what makes them lowerable.
def not_compare(x):
    return not (x > 2)


def and_compare(x):
    return (x > 0) and (x < 10)


def or_compare(x):
    return (x < 0) or (x > 10)


_BOOL_LOGIC_BODIES = (not_compare, and_compare, or_compare)


def bytes_identity(x: bytes):
    return x


def _arrow_udf(func, return_type):
    """Build a UDF pinned to the Arrow-optimized eval type, whatever the type hints say."""
    return UserDefinedFunction(func, return_type, evalType=PythonEvalType.SQL_ARROW_BATCHED_UDF)


def _pickled_udf(func, return_type):
    """Build a UDF pinned to the pickled eval type, for contrast with ``_arrow_udf``."""
    return UserDefinedFunction(func, return_type, evalType=PythonEvalType.SQL_BATCHED_UDF)


def _python_eval_operators(df):
    """The Python-eval physical operators in ``df``'s executed plan."""
    operators = []
    stack = [df._jdf.queryExecution().executedPlan()]
    while stack:
        node = stack.pop()
        name = node.getClass().getSimpleName()
        if name == "AdaptiveSparkPlanExec":
            stack.append(node.executedPlan())
            continue
        if name.endswith("EvalPythonExec"):
            operators.append(node)
        children = node.children()
        stack.extend(children.apply(i) for i in range(children.size()))
    return operators


def _eval_python_nodes(df):
    """Count the Python-eval operators in ``df``'s executed plan, by class name."""
    return Counter(node.getClass().getSimpleName() for node in _python_eval_operators(df))


def _optimized_type(df):
    """Type of ``df``'s only output column in the optimized plan, where the UDF is replaced."""
    schema = df._jdf.queryExecution().optimizedPlan().schema().json()
    return _parse_datatype_json_string(schema).fields[0].dataType


def _python_udf_expr_count(df):
    """Count the PythonUDF expressions in ``df``'s Python-eval operators.

    A chain of UDFs is evaluated in one operator, so only this count tells whether a call in
    the chain was lowered.
    """
    total = 0
    for operator in _python_eval_operators(df):
        udfs = operator.udfs()
        exprs = [udfs.apply(i) for i in range(udfs.size())]
        while exprs:
            expr = exprs.pop()
            total += expr.getClass().getSimpleName() == "PythonUDF"
            children = expr.children()
            exprs.extend(children.apply(i) for i in range(children.size()))
    return total


@unittest.skipIf(is_remote_only(), "UDF transpilation is only supported in non-Connect Spark.")
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,  # type: ignore[arg-type]
)
class ArrowUDFTranspileTestsMixin:
    """Helpers shared by the transpile suites; the pandas ones reuse them."""

    # The eval type a differential runs at unless told otherwise.
    _eval_type = PythonEvalType.SQL_ARROW_BATCHED_UDF

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

    def _differential(self, func, return_type, df, *cols, eval_type=None, conf=None):
        """Run ``func`` with transpilation on and off; return ``(values, error)`` for each.

        Both sides use the same eval type, so the reference is the interpreted behavior of
        the very UDF being rewritten. ``values`` is the produced list, or ``_RAISED``.
        """
        eval_type = eval_type or self._eval_type
        extra = conf or {}
        name = getattr(func, "__name__", repr(func))

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with self.sql_conf({**_TRANSPILE_ON, **extra}):
                pudf = UserDefinedFunction(func, return_type, evalType=eval_type)
                self._assert_transpiled(pudf, name)
                on = self._values_or_raised(df.select(pudf(*cols)))
        bad = [
            str(w.message)
            for w in caught
            if any(m in str(w.message) for m in _BAD_TRANSPILE_WARNING_MARKERS)
        ]
        self.assertFalse(bad, f"unexpected transpile warnings for {name}: {bad}")

        with self.sql_conf({**_TRANSPILE_OFF, **extra}):
            pudf = UserDefinedFunction(func, return_type, evalType=eval_type)
            off = self._values_or_raised(df.select(pudf(*cols)))

        return on, off

    def _assert_matches_interpreted(self, func, return_type, df, *cols, eval_type=None, conf=None):
        """Assert transpiled and interpreted agree, including on whether they raise."""
        (on_values, on_error), (off_values, off_error) = self._differential(
            func, return_type, df, *cols, eval_type=eval_type, conf=conf
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
        # repr(), so that a genuine NaN on both sides compares equal.
        self.assertEqual(repr(off_values), repr(on_values), f"{name}: transpiled != interpreted")


class ArrowUDFTranspileEvalTypeGateTests(ArrowUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Which eval types may be transpiled at all."""

    def test_functions_udf_transpiles_on_default_configuration(self):
        # The motivating case: no useArrow argument, so functions.udf picks the
        # Arrow-optimized eval type from the default of
        # spark.sql.execution.pythonUDF.arrow.enabled (true) -- what users write. Only the
        # experimental transpile flag is set; the arrow conf is left at its default so this
        # actually exercises it rather than pinning it.
        with self.sql_conf(_TRANSPILE_ON):
            wrapped = udf(add_four, LongType())
            self.assertEqual(wrapped.evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF)
            self._assert_transpiled(wrapped._unwrapped, "functions.udf on default config")
            df = self.spark.createDataFrame([Row(a=1)])
            self.assertEqual([5], [r[0] for r in df.select(wrapped("a")).collect()])

    def test_eval_type_allowlist_is_exhaustive(self):
        # Try every eval type Spark defines rather than listing the excluded ones, so a newly
        # added eval type that transpiles shows up here instead of being silently covered.
        # `add_one` is inside the element-wise subset, which is why the pandas type appears.
        all_eval_types = {
            name: value
            for name, value in vars(PythonEvalType).items()
            if name.startswith("SQL_") and isinstance(value, int)
        }
        self.assertGreater(len(all_eval_types), 20, "eval-type introspection found almost none")
        with self.sql_conf(_TRANSPILE_ON):
            transpiling = {
                value
                for value in all_eval_types.values()
                if UserDefinedFunction(add_one, LongType(), evalType=value).transpiled
            }
        self.assertEqual(
            set(_TRANSPILABLE_EVAL_TYPES),
            transpiling,
            "the set of transpiling eval types changed. An eval type that hands the function "
            "a pandas.Series or pyarrow.Array must be audited for batch semantics first; see "
            "the 'Scalar pandas UDFs' section of python/pyspark/sql/transpile.py.",
        )

    def test_nondeterministic_arrow_udf_does_not_transpile(self):
        # A plain Catalyst expression could be folded, reordered or duplicated, discarding
        # the nondeterminism barrier.
        with self.sql_conf(_TRANSPILE_ON):
            pudf = _arrow_udf(add_one, LongType()).asNondeterministic()
            self.assertEqual([], pudf.transpiled)

    def test_ansi_disabled_warns_and_does_not_transpile(self):
        for build in (_pickled_udf, _arrow_udf):
            with self.subTest(build=build.__name__):
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    with self.sql_conf(
                        {
                            "spark.sql.experimental.optimizer.transpilePyUDFs": True,
                            "spark.sql.ansi.enabled": False,
                        }
                    ):
                        pudf = build(add_one, LongType())
                self.assertEqual([], pudf.transpiled)
                self.assertTrue(
                    any("ANSI mode" in str(w.message) for w in caught),
                    f"expected the ANSI-mode warning, got {[str(w.message) for w in caught]}",
                )

    def test_return_type_unrepresentable_in_arrow_raises_cleanly(self):
        # The transpile block reads self.returnType, which runs _check_return_type inside a
        # broad `except Exception`. Without a narrower handler the real error would surface
        # as a bogus "Exception transpiling UDF" warning first.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with self.sql_conf(_TRANSPILE_ON):
                with self.assertRaises(Exception) as ctx:
                    _arrow_udf(identity, CalendarIntervalType())
        self.assertIn("Invalid return type with Arrow-optimized Python UDF", str(ctx.exception))
        spurious = [str(w.message) for w in caught if "Exception transpiling" in str(w.message)]
        self.assertFalse(spurious, f"spurious transpilation warning: {spurious}")


class ArrowUDFTranspilePlanShapeTests(ArrowUDFTranspileTestsMixin, ReusedSQLTestCase):
    """What the executed plan looks like once a UDF is (or is not) rewritten."""

    def test_transpiled_arrow_udf_leaves_no_python_eval_operator(self):
        with self.sql_conf(_TRANSPILE_ON):
            pudf = _arrow_udf(add_one, LongType())
            self._assert_transpiled(pudf, "an Arrow-optimized UDF")
            df = self.spark.range(3).select(pudf("id").alias("v"))
            self.assertEqual(Counter(), _eval_python_nodes(df))
            self.assertEqual([1, 2, 3], [r[0] for r in df.collect()])

    def test_untranspilable_arrow_udf_keeps_arrow_eval_python(self):
        # Must stay interpreted, and stay on the Arrow operator rather than degrading to
        # the pickled one.
        with self.sql_conf(_TRANSPILE_ON):
            pudf = _arrow_udf(floor_halve, LongType())
            self.assertEqual([], pudf.transpiled)
            counts = _eval_python_nodes(self.spark.range(4).select(pudf("id")))
            self.assertEqual(1, counts["ArrowEvalPythonExec"])
            self.assertEqual(0, counts["BatchEvalPythonExec"])

    def test_middle_of_arrow_udf_chain_is_not_elided(self):
        # ConvertToCatalyst keeps a transpilable UDF whose inputs are all Python UDFs,
        # because UDF -> UDF -> UDF pipelines into one Arrow batch.
        #
        # Lowering the middle call would leave outer(cast(inner(id) + 1)). ExtractPythonUDFs
        # chains a UDF only when its single child is itself a Python UDF, so inner and outer
        # would land in two ArrowEvalPythonExec operators holding two PythonUDF expressions.
        # Kept, all three run in one operator, so both counts below catch a regression.
        with self.sql_conf(_TRANSPILE_ON):
            outer = _arrow_udf(floor_halve, LongType())
            middle = _arrow_udf(add_one, LongType())
            inner = _arrow_udf(floor_third, LongType())
            self._assert_transpiled(middle, "the middle UDF of the chain")
            df = self.spark.range(10).select(outer(middle(inner("id"))))
            self.assertEqual(1, _eval_python_nodes(df)["ArrowEvalPythonExec"])
            self.assertEqual(3, _python_udf_expr_count(df))
            # The contrast: the same middle UDF over a column rather than a UDF IS lowered,
            # so only the outer call is left. Without this the assertion above would also
            # pass if nothing were ever lowered.
            lowered = self.spark.range(10).select(outer(middle("id")))
            self.assertEqual(1, _python_udf_expr_count(lowered))

    def test_output_schema_is_identical_with_and_without_transpilation(self):
        # ConvertToCatalyst substitutes the option with no cast back to the declared return
        # type, relying on the transpiler having cast already. The analyzed schema (what
        # `.schema` reads) still reports the Python UDF's type, so read the optimized plan.
        # Only the type is compared: the option may be non-nullable where the UDF never is.
        for return_type in (ByteType(), IntegerType(), LongType(), DoubleType()):
            with self.subTest(returnType=return_type):
                with self.sql_conf(_TRANSPILE_ON):
                    df = self.spark.range(1).select(_arrow_udf(add_one, return_type)("id"))
                    self.assertEqual(0, len(_python_eval_operators(df)))
                    on = _optimized_type(df)
                with self.sql_conf(_TRANSPILE_OFF):
                    off = _optimized_type(
                        self.spark.range(1).select(_arrow_udf(add_one, return_type)("id"))
                    )
                self.assertEqual(off, on)

    def test_config_toggled_after_construction_leaves_no_stale_nodes(self):
        # TranspiledPythonUDF is Unevaluable, so ConvertToCatalyst must strip it when the
        # confs flip between construction and execution.
        with self.sql_conf(_TRANSPILE_ON):
            pudf = _arrow_udf(add_one, LongType())
        for off in (
            {"spark.sql.experimental.optimizer.transpilePyUDFs": False},
            {"spark.sql.ansi.enabled": False},
        ):
            with self.subTest(conf=off):
                with self.sql_conf({**_TRANSPILE_ON, **off}):
                    df = self.spark.range(3).select(pudf("id"))
                    self.assertEqual([1, 2, 3], [r[0] for r in df.collect()])


class ArrowUDFTranspileDifferentialTests(ArrowUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Transpiled Arrow UDFs vs the same UDFs interpreted on the Arrow path."""

    # Column type paired with a return type in the same family. Keeping the body's value
    # domain and the declared return type aligned isolates the operator lowering from the
    # separate question of what a cross-family cast does.
    _INTEGRAL_CASES = tuple((d, 7, LongType()) for d in ("tinyint", "smallint", "int", "bigint"))
    _FLOATING_CASES = tuple((d, 7.0, DoubleType()) for d in ("float", "double"))

    def _one_row(self, dtype, value):
        return self.spark.createDataFrame([(value,)], schema=f"a {dtype}")

    def test_modulo_keeps_narrow_product_at_integer_width(self):
        def product(a, b):
            return (a % b) * b

        for dtype, maximum, minimum in (("tinyint", 127, -128), ("smallint", 32767, -32768)):
            with self.subTest(dtype=dtype):
                df = self.spark.createDataFrame([(maximum, minimum)], f"a {dtype}, b {dtype}")
                with self.sql_conf(_TRANSPILE_ON):
                    pudf = _arrow_udf(product, LongType())
                    self.assertFalse(_python_eval_operators(df.select(pudf("a", "b"))))
                self._assert_matches_interpreted(product, LongType(), df, "a", "b")

    def test_modulo_minima_and_large_divisors_match_interpreted(self):
        # The pickled eval type is pinned against Python's own `%` in test_udf_transpile_unit.
        def modulo(a, b):
            return a % b

        cases = [
            (f"a {dtype}, b {dtype}", [(minimum, -1), (1, minimum), (minimum, 3)], LongType())
            for dtype, minimum in (
                ("tinyint", -(2**7)),
                ("smallint", -(2**15)),
                ("int", -(2**31)),
                ("bigint", -(2**63)),
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
                    pudf = _arrow_udf(modulo, return_type)
                    self.assertFalse(_python_eval_operators(df.select(pudf("a", "b"))))
                self._assert_matches_interpreted(modulo, return_type, df, "a", "b")

    def test_numeric_arithmetic_matches_interpreted_arrow(self):
        for dtype, value, return_type in self._INTEGRAL_CASES + self._FLOATING_CASES:
            df = self._one_row(dtype, value)
            for body in _NUMERIC_ARITHMETIC:
                with self.subTest(dtype=dtype, body=body.__name__):
                    self._assert_matches_interpreted(body, return_type, df, "a")

    def test_unary_float_repeat_falls_back_and_raises(self):
        for eval_type in (PythonEvalType.SQL_BATCHED_UDF, PythonEvalType.SQL_ARROW_BATCHED_UDF):
            for dtype in ("float", "double"):
                df = self.spark.createDataFrame([(-2.5, "ab"), (1.5, "cd")], f"a {dtype}, s string")
                for body, cols in (
                    (repeat_negated, ("a",)),
                    (repeat_positive, ("a",)),
                    (repeat_negated_left, ("a", "s")),
                    (repeat_negative_float, ("s",)),
                ):
                    with self.subTest(eval_type=eval_type, dtype=dtype, body=body.__name__):
                        for conf in (_TRANSPILE_OFF, _TRANSPILE_ON):
                            with self.sql_conf(conf):
                                pudf = UserDefinedFunction(body, StringType(), evalType=eval_type)
                                query = df.select(pudf(*cols))
                                self.assertGreater(len(_python_eval_operators(query)), 0)
                                with self.assertRaisesRegex(Exception, "TypeError"):
                                    query.collect()

    def test_unary_integral_repeat_and_numeric_equality(self):
        for eval_type in (PythonEvalType.SQL_BATCHED_UDF, PythonEvalType.SQL_ARROW_BATCHED_UDF):
            integers = self.spark.createDataFrame([(-2,), (1,)], "a bigint")
            with self.sql_conf(_TRANSPILE_ON):
                pudf = UserDefinedFunction(repeat_negated, StringType(), evalType=eval_type)
                query = integers.select(pudf("a"))
                self.assertEqual(0, len(_python_eval_operators(query)))
                self.assertEqual(["abab", ""], [r[0] for r in query.collect()])
            self._assert_matches_interpreted(
                repeat_negated, StringType(), integers, "a", eval_type=eval_type
            )
            for df in (
                integers,
                self.spark.createDataFrame([(-2.0,), (float("nan"),)], "a double"),
            ):
                self._assert_matches_interpreted(
                    negated_equals_two, BooleanType(), df, "a", eval_type=eval_type
                )

            nullable = self.spark.createDataFrame([(-2.0,), (float("nan"),), (None,)], "a double")
            self._assert_matches_interpreted(
                guarded_negated_equals_two, BooleanType(), nullable, "a", eval_type=eval_type
            )

    def test_numeric_comparisons_match_interpreted_arrow(self):
        for value in (1, 2, 3):
            df = self._one_row("bigint", value)
            for body in _NUMERIC_COMPARISONS:
                with self.subTest(body=body.__name__, value=value):
                    self._assert_matches_interpreted(body, BooleanType(), df, "a")

    def test_bool_logic_matches_interpreted_arrow(self):
        for value in (-1, 5, 20):
            df = self._one_row("bigint", value)
            for body in _BOOL_LOGIC_BODIES:
                with self.subTest(body=body.__name__, value=value):
                    self._assert_matches_interpreted(body, BooleanType(), df, "a")

    def test_null_guarded_branch_matches_interpreted_arrow(self):
        # The idiomatic NULL-safe UDF: the guard means the interpreted path never does
        # arithmetic on None, so both sides agree on the NULL row.
        df = self.spark.createDataFrame([(1,), (None,), (5,)], schema="a bigint")
        self._assert_matches_interpreted(guarded_add_one, LongType(), df, "a")

    def test_string_ops_match_interpreted_arrow(self):
        df = self.spark.createDataFrame([("ab",), ("",)], schema="a string")
        for body in _STRING_BODIES:
            with self.subTest(body=body.__name__):
                self._assert_matches_interpreted(body, StringType(), df, "a")

    def test_binary_identity_matches_interpreted_arrow(self):
        df = self.spark.createDataFrame([(b"abc",), (b"",)], schema="a binary")
        for as_bytes in (True, False):
            with self.subTest(binaryAsBytes=as_bytes):
                self._assert_matches_interpreted(
                    bytes_identity,
                    BinaryType(),
                    df,
                    "a",
                    conf={"spark.sql.execution.pyspark.binaryAsBytes": as_bytes},
                )

    def test_overflow_and_modulo_zero_raise_on_both_paths(self):
        # Pinned as an AGREEMENT so it is not later mistaken for a divergence.
        big = self._one_row("bigint", 2**62)
        (on_values, _), (off_values, _) = self._differential(square, LongType(), big, "a")
        self.assertIs(_RAISED, on_values)
        self.assertIs(_RAISED, off_values)

        one = self._one_row("bigint", 1)
        (on_values, _), (off_values, _) = self._differential(mod_zero, LongType(), one, "a")
        self.assertIs(_RAISED, on_values)
        self.assertIs(_RAISED, off_values)

    def test_known_divergence_arithmetic_on_null_input(self):
        # Documented divergence: unguarded `x + 1` propagates NULL when transpiled, while
        # the interpreted path runs Python's `None + 1` and raises. Transpilation turns a
        # query failure into a NULL.
        df = self.spark.createDataFrame([(None,)], schema="a bigint")
        (on_values, _), (off_values, off_error) = self._differential(add_one, LongType(), df, "a")
        self.assertEqual([None], on_values)
        self.assertIs(_RAISED, off_values)
        self.assertIsInstance(off_error, PythonException)

    # The return-type cast, kept by decision: see "The return-type cast" in transpile.py.
    # 1, -2 and 3 plus 0.5 are 1.5, -1.5 and 3.5, which only truncation toward zero turns
    # into 1, -1 and 3 -- floor gives -2 for the second, and rounding gives 2, -2 and 4.
    _HALVES = (1, -2, 3)
    _HALVES_TRUNCATED_TOWARD_ZERO = [1, -1, 3]

    def _halves(self):
        return self.spark.createDataFrame([(v,) for v in self._HALVES], "a bigint")

    def test_float_literal_body_with_integral_return_pickled_diverges(self):
        # `x + 0.5` declared LongType() passes the body-vs-return-type check, which accepts
        # any numeric body for a non-decimal numeric return type, and lowers to a
        # truncating cast. The interpreted PICKLED path instead NULLs the fractional
        # result, since makeFromJava accepts only integral Java types for LongType.
        (on_values, _), (off_values, _) = self._differential(
            add_half, LongType(), self._halves(), "a", eval_type=PythonEvalType.SQL_BATCHED_UDF
        )
        self.assertEqual(
            self._HALVES_TRUNCATED_TOWARD_ZERO, on_values, "the transpiled cast truncates"
        )
        self.assertEqual(
            [None, None, None], off_values, "the pickled path NULLs a fractional result"
        )

    def test_float_literal_body_with_integral_return_agrees_on_arrow(self):
        # Same body on the Arrow path AGREES: Arrow truncates toward zero just as the ANSI
        # cast does, and does so regardless of convertToArrowArraySafely. So the int/float
        # bucketing gap above is specific to the pickled regime, and widening
        # transpilation to Arrow UDFs does not introduce it here.
        for safe in (True, False):
            with self.subTest(convertToArrowArraySafely=safe):
                (on_values, _), (off_values, _) = self._differential(
                    add_half,
                    LongType(),
                    self._halves(),
                    "a",
                    conf={"spark.sql.execution.pandas.convertToArrowArraySafely": safe},
                )
                self.assertEqual(self._HALVES_TRUNCATED_TOWARD_ZERO, on_values)
                self.assertEqual(self._HALVES_TRUNCATED_TOWARD_ZERO, off_values)


class ArrowUDFTranspileStreamingRestartTests(ArrowUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Restarting a stateful streaming query after its transpilation settings changed.

    `x is None` lowers to `isnull`, which is non-nullable where the Python UDF is always
    nullable, so a grouping key built from it changes nullability with the settings. Whether
    that reaches the state schema recorded in the checkpoint is not up to the transpiler: see
    "Nullability and streaming state" in python/pyspark/sql/transpile.py.
    """

    _ALWAYS_NULLABLE = "spark.sql.streaming.statefulOperator.alwaysNullableOutput.enabled"

    def _run_to_completion(self, source, checkpoint, conf, expect_transpiled, expect_rows=4):
        """Feed the query one more file of four rows and run it until its input is processed.

        ``availableNow`` rather than a rate source and a sleep: the query ends by itself, and
        a failure to restart comes out of ``awaitTermination``.
        """
        self.spark.range(4).selectExpr("id AS value").write.mode("append").parquet(source)
        with self.sql_conf(conf):
            pudf = _arrow_udf(is_null, BooleanType())
            self.assertEqual(expect_transpiled, bool(pudf.transpiled))
            keyed = self.spark.readStream.schema("value bigint").parquet(source)
            query = (
                keyed.groupBy(pudf("value").alias("missing"))
                .count()
                .writeStream.outputMode("update")
                .format("noop")
                .option("checkpointLocation", checkpoint)
                .trigger(availableNow=True)
                .start()
            )
            try:
                query.awaitTermination()
                self.assertEqual(
                    expect_rows,
                    sum(p["numInputRows"] for p in query.recentProgress),
                    "the restart did not process the input it was given, so it proves nothing",
                )
            finally:
                query.stop()

    def test_restart_after_transpilation_changes(self):
        # Either direction: the state schema is nullable
        # whichever way the key was computed.
        with tempfile.TemporaryDirectory(prefix="transpile_stream") as tmp:
            source, checkpoint = f"{tmp}/source", f"{tmp}/checkpoint"
            for conf, transpiled in (
                (_TRANSPILE_ON, True),
                (_TRANSPILE_OFF, False),
                (_TRANSPILE_ON, True),
            ):
                with self.subTest(conf=conf):
                    self._run_to_completion(source, checkpoint, conf, transpiled)

    def test_restart_needs_the_same_settings_without_nullable_state_schemas(self):
        # With stateful operators left to record the plan's own nullability, the first run
        # stores a non-nullable key and a nullable one is refused from then on. Interpreted
        # first is fine: a nullable stored key accepts both.
        legacy = {self._ALWAYS_NULLABLE: False}
        started_with = {**_TRANSPILE_ON, **legacy}
        with tempfile.TemporaryDirectory(prefix="transpile_stream") as tmp:
            source, checkpoint = f"{tmp}/source", f"{tmp}/checkpoint"
            self._run_to_completion(source, checkpoint, started_with, True)
            with self.assertRaises(StreamingQueryException) as ctx:
                self._run_to_completion(source, checkpoint, {**_TRANSPILE_OFF, **legacy}, False)
            self.assertIn("STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE", str(ctx.exception))
            # Back on the settings it started with, it picks up where it failed: the file
            # the failed restart never got through, and the new one.
            self._run_to_completion(source, checkpoint, started_with, True, expect_rows=8)
        with tempfile.TemporaryDirectory(prefix="transpile_stream") as tmp:
            source, checkpoint = f"{tmp}/source", f"{tmp}/checkpoint"
            for conf, transpiled in ((_TRANSPILE_OFF, False), (_TRANSPILE_ON, True)) * 2:
                self._run_to_completion(source, checkpoint, {**conf, **legacy}, transpiled)


class ArrowUDFTranspileConfInteractionTests(ArrowUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Confs that change what the interpreted Arrow path actually computes."""

    _LEGACY_PANDAS = "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled"
    _SAFE_ARROW = "spark.sql.execution.pandas.convertToArrowArraySafely"

    def test_legacy_pandas_conversion_disables_transpilation(self):
        # Under the legacy conversion a NULL in an integer column reaches the function as NaN,
        # so `x is None` is False where a transpiled isnull(a) is True.
        df = self.spark.createDataFrame([(None,)], schema="a bigint")
        legacy = {self._LEGACY_PANDAS: True}
        with self.sql_conf({**_TRANSPILE_OFF, **legacy}):
            interpreted = df.select(_arrow_udf(is_null, BooleanType())("a")).collect()
        self.assertEqual([False], [r[0] for r in interpreted])
        with self.sql_conf({**_TRANSPILE_ON, **legacy}):
            self.assertEqual([], _arrow_udf(is_null, BooleanType()).transpiled)
        # Set only after the UDF was built, ConvertToCatalyst has to fall back.
        with self.sql_conf(_TRANSPILE_ON):
            pudf = _arrow_udf(is_null, BooleanType())
            self._assert_transpiled(pudf, "is_null")
            with self.sql_conf(legacy):
                self.assertEqual([False], [r[0] for r in df.select(pudf("a")).collect()])

    def test_downcast_overflow_raises_transpiled_whatever_the_arrow_safety_conf(self):
        # With the safe conversion both paths raise. Without it the interpreted Arrow path
        # wraps silently where the ANSI cast still raises; that is not gated against, since
        # asking for unsafe conversion together with ANSI is self-contradictory.
        df = self.spark.createDataFrame([(3_000_000,)], schema="a bigint")
        for safe in (True, False):
            with self.subTest(convertToArrowArraySafely=safe):
                (on_values, _), (off_values, _) = self._differential(
                    times_thousand, IntegerType(), df, "a", conf={self._SAFE_ARROW: safe}
                )
                self.assertIs(_RAISED, on_values)
                self.assertEqual(safe, off_values is _RAISED)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
