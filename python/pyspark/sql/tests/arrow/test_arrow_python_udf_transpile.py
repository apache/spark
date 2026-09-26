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

Arrow optimization is the default for regular Python UDFs, so a plain
``functions.udf(f, "bigint")`` gets ``SQL_ARROW_BATCHED_UDF``. Until that eval type was
admitted to ``_TRANSPILABLE_EVAL_TYPES`` in ``python/pyspark/sql/udf.py``, the transpilation
flag did nothing on default configuration.

This is a separate module from ``pyspark.sql.tests.test_udf_transpile_unit`` because the
reference semantics differ. The pickled path coerces a UDF's return value through
``EvaluatePython.makeFromJava``, which NULLs a type mismatch; the Arrow path builds a
``pa.array`` and falls back to a cast (``python/pyspark/worker.py``), which can raise or
convert instead. So every differential here compares transpiled-Arrow against
interpreted-**Arrow**: the question is whether the rewrite matches what this UDF would have
done in its own eval mode. The cell-by-cell matrix for both regimes lives in the golden
files under ``python/pyspark/sql/tests/coercion/``.

Naming trap: ``SQL_ARROW_BATCHED_UDF`` (101) is the Arrow-*optimized* regular UDF -- Arrow
is only the transport, and the worker still calls the function once per row with Python
scalars. ``SQL_SCALAR_ARROW_UDF`` (250) is genuinely vectorized and must never be
transpiled. ``test_eval_type_allowlist_is_exhaustive`` guards the distinction.

UDF bodies here are all named ``def``s. The transpiler recovers source with
``inspect.getsource``, which for a lambda passed as a call argument returns the enclosing
statement -- whose top-level node is a call, not a lambda -- so transpilation silently falls
back and the test passes while asserting nothing. ``_assert_transpiled`` catches that.
"""

import unittest
import warnings
from collections import Counter

from pyspark.errors import PythonException
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    CalendarIntervalType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
)
from pyspark.sql.udf import UserDefinedFunction
from pyspark.testing.objects import ExamplePoint, ExamplePointUDT
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import PythonEvalType, is_remote_only

if have_pandas:
    import pandas as pd


# Kept as a constant so ``test_arrow_transpile_flag_matches_reality`` fails in either
# direction if the eval-type gate in python/pyspark/sql/udf.py and this file disagree.
ARROW_TRANSPILE_SUPPORTED = True

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

# A differential that silently falls back compares interpreted against interpreted and
# passes for the wrong reason, so these warnings are fatal. These are exactly the three
# warnings udf.py emits when a rewrite does not happen ("Unable to transpile UDF ...",
# "Exception transpiling UDF ...", and the ANSI-disabled one); kept in sync with the
# identical tuple in pyspark.sql.tests.pandas.test_pandas_udf_transpile.
_BAD_TRANSPILE_WARNING_MARKERS = (
    "Unable to transpile",
    "Exception transpiling",
    "is only supported when ANSI mode is enabled",
)

# "This side raised instead of returning rows."
_RAISED = object()

# Eval types that hand the function a whole pandas.Series or pyarrow.Array and are still
# refused outright. SQL_SCALAR_PANDAS_UDF is deliberately absent: it is also vectorized, but
# it is transpiled for the element-wise subset of bodies covered by
# ``pyspark.sql.tests.pandas.test_pandas_udf_transpile``.
_VECTORIZED_SCALAR_EVAL_TYPES = (
    PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
    PythonEvalType.SQL_SCALAR_ARROW_UDF,
    PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF,
)

_GROUPED_AGG_EVAL_TYPES = (
    PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
    PythonEvalType.SQL_GROUPED_AGG_PANDAS_ITER_UDF,
    PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF,
    PythonEvalType.SQL_GROUPED_AGG_ARROW_ITER_UDF,
)


# --- UDF bodies ------------------------------------------------------------------------


def add_one(x):
    return x + 1


def add_four(x):
    return x + 4


def identity(x):
    return x


def constant_42(x):
    return 42


def gt_one(x):
    return x > 1


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


def str_repeat_right(x: str):
    return x * 3


def str_repeat_left(x: str):
    return 3 * x


_STRING_BODIES = (str_concat, str_repeat_right, str_repeat_left)


def str_is_abc(x: str):
    return x == "abc"


def bool_identity(x: bool):
    return x


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
    """Build a UDF pinned to the Arrow-optimized eval type.

    ``UserDefinedFunction`` defaults to ``SQL_BATCHED_UDF``, and constructing it directly
    also bypasses ``_create_py_udf``'s type-hint eval-type inference -- which matters for
    the annotated bodies above, whose annotations are category hints rather than a
    vectorized signature.
    """
    return UserDefinedFunction(func, return_type, evalType=PythonEvalType.SQL_ARROW_BATCHED_UDF)


def _pickled_udf(func, return_type):
    """Build a UDF pinned to the pickled eval type, for contrast with ``_arrow_udf``."""
    return UserDefinedFunction(func, return_type, evalType=PythonEvalType.SQL_BATCHED_UDF)


def _eval_python_nodes(df):
    """Count Python-eval physical operators in ``df``'s executed plan, by class name.

    Counting nodes rather than substring-matching the plan string matters because
    ``"ArrowEvalPython"`` contains ``"EvalPython"``. Traversal follows
    ``_assert_columnar_arrow_eval`` in test_arrow_python_udf_cached.py.
    """
    counts: Counter = Counter()
    stack = [df._jdf.queryExecution().executedPlan()]
    while stack:
        node = stack.pop()
        name = node.getClass().getSimpleName()
        if name == "AdaptiveSparkPlanExec":
            stack.append(node.executedPlan())
            continue
        if name.endswith("EvalPythonExec"):
            counts[name] += 1
        children = node.children()
        for i in range(children.size()):
            stack.append(children.apply(i))
    return counts


def _python_udf_expr_count(df):
    """Count PythonUDF expression nodes across every Python-eval operator in ``df``.

    Operator counts alone no longer say whether a call was lowered: a chain of UDFs is
    evaluated as a pipeline inside ONE operator, so three chained UDFs and one UDF both
    show a single ArrowEvalPythonExec. What still distinguishes them is how many PythonUDF
    expressions that operator holds -- a lowered call is gone from the tree, while the alias
    naming it survives, so this counts nodes rather than matching the plan string.
    """
    total = 0
    stack = [df._jdf.queryExecution().executedPlan()]
    while stack:
        node = stack.pop()
        name = node.getClass().getSimpleName()
        if name == "AdaptiveSparkPlanExec":
            stack.append(node.executedPlan())
            continue
        if name.endswith("EvalPythonExec"):
            udfs = node.udfs()
            for i in range(udfs.size()):
                exprs = [udfs.apply(i)]
                while exprs:
                    expr = exprs.pop()
                    if expr.getClass().getSimpleName() == "PythonUDF":
                        total += 1
                    expr_children = expr.children()
                    for j in range(expr_children.size()):
                        exprs.append(expr_children.apply(j))
        children = node.children()
        for i in range(children.size()):
            stack.append(children.apply(i))
    return total


@unittest.skipIf(is_remote_only(), "UDF transpilation is only supported in non-Connect Spark.")
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,  # type: ignore[arg-type]
)
class ArrowUDFTranspileTestsMixin:
    """Helpers shared by the suites in this module."""

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
        eval_type = eval_type or PythonEvalType.SQL_ARROW_BATCHED_UDF
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
        self.assertEqual(off_values, on_values, f"{name}: transpiled != interpreted")


class ArrowUDFTranspileEvalTypeGateTests(ArrowUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Which eval types may be transpiled at all."""

    def test_arrow_transpile_flag_matches_reality(self):
        with self.sql_conf(_TRANSPILE_ON):
            pudf = _arrow_udf(add_one, LongType())
            self.assertEqual(
                bool(pudf.transpiled),
                ARROW_TRANSPILE_SUPPORTED,
                "ARROW_TRANSPILE_SUPPORTED disagrees with the eval-type gate in "
                "python/pyspark/sql/udf.py; update whichever one is wrong.",
            )

    def test_arrow_batched_eval_type_transpiles_and_computes(self):
        with self.sql_conf(_TRANSPILE_ON):
            pudf = _arrow_udf(add_four, LongType())
            self._assert_transpiled(pudf, "an Arrow-optimized UDF")
            df = self.spark.createDataFrame([Row(a=1)])
            self.assertEqual([5], [r[0] for r in df.select(pudf("a")).collect()])

    def test_functions_udf_transpiles_on_default_configuration(self):
        # The motivating case: no useArrow argument, no conf override -- what users write.
        with self.sql_conf({**_TRANSPILE_ON, "spark.sql.execution.pythonUDF.arrow.enabled": True}):
            wrapped = udf(add_four, LongType())
            self.assertEqual(wrapped.evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF)
            self._assert_transpiled(wrapped._unwrapped, "functions.udf on default config")
            df = self.spark.createDataFrame([Row(a=1)])
            self.assertEqual([5], [r[0] for r in df.select(wrapped("a")).collect()])

    def test_pickled_eval_type_still_transpiles(self):
        # Regression guard on rewriting the gate as set membership.
        with self.sql_conf(_TRANSPILE_ON):
            pudf = _pickled_udf(add_four, LongType())
            self._assert_transpiled(pudf, "a pickled UDF")
            df = self.spark.createDataFrame([Row(a=1)])
            self.assertEqual([5], [r[0] for r in df.select(pudf("a")).collect()])

    def test_vectorized_scalar_eval_types_do_not_transpile(self):
        # `x + 1` on a Series is a whole-batch operation, not the per-row lowering the
        # transpiler would emit.
        with self.sql_conf(_TRANSPILE_ON):
            for eval_type in _VECTORIZED_SCALAR_EVAL_TYPES:
                with self.subTest(evalType=eval_type):
                    pudf = UserDefinedFunction(add_one, LongType(), evalType=eval_type)
                    self.assertEqual([], pudf.transpiled)

    def test_grouped_agg_eval_types_do_not_transpile(self):
        with self.sql_conf(_TRANSPILE_ON):
            for eval_type in _GROUPED_AGG_EVAL_TYPES:
                with self.subTest(evalType=eval_type):
                    pudf = UserDefinedFunction(add_one, LongType(), evalType=eval_type)
                    self.assertEqual([], pudf.transpiled)

    def test_eval_type_allowlist_is_exhaustive(self):
        # Enumerate every eval type Spark defines rather than listing the excluded ones, so
        # a newly added eval type shows up here instead of being silently covered.
        expected = {PythonEvalType.SQL_BATCHED_UDF}
        if ARROW_TRANSPILE_SUPPORTED:
            expected.add(PythonEvalType.SQL_ARROW_BATCHED_UDF)
        # The one vectorized eval type that is transpiled, for the element-wise subset only.
        # ``add_one``'s body (`x + 1`) is inside that subset, which is why it appears here;
        # the subset itself is pinned by
        # ``pyspark.sql.tests.pandas.test_pandas_udf_transpile``.
        expected.add(PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        all_eval_types = {
            name: value
            for name, value in vars(PythonEvalType).items()
            if name.startswith("SQL_") and isinstance(value, int)
        }
        self.assertGreater(len(all_eval_types), 20, "eval-type introspection found almost none")

        transpiling = {}
        with self.sql_conf(_TRANSPILE_ON):
            for name, value in sorted(all_eval_types.items(), key=lambda kv: kv[1]):
                pudf = UserDefinedFunction(add_one, LongType(), evalType=value)
                if pudf.transpiled:
                    transpiling[name] = value

        self.assertEqual(
            expected,
            set(transpiling.values()),
            f"the set of transpiling eval types changed: {sorted(transpiling.items())}. "
            "A per-row scalar Python UDF belongs in _TRANSPILABLE_EVAL_TYPES in "
            "python/pyspark/sql/udf.py and in this test. An eval type that hands the "
            "function a pandas.Series or pyarrow.Array needs more than that: the lowerings "
            "are written against Python scalars, so admitting one means auditing every "
            "construct for batch semantics first (see the 'Scalar pandas UDFs' section of "
            "python/pyspark/sql/transpile.py for how that was done for "
            "SQL_SCALAR_PANDAS_UDF).",
        )

    def test_annotated_vectorized_function_via_functions_udf_transpiles(self):
        # ``functions.udf`` infers SQL_SCALAR_PANDAS_UDF from the pandas annotations, and
        # `s + 1` is inside the element-wise subset, so this is transpiled. The pandas
        # semantics are covered in pyspark.sql.tests.pandas.test_pandas_udf_transpile; what
        # matters here is that arriving via ``functions.udf`` rather than ``pandas_udf``
        # reaches the same gate.
        def series_add_one(s: pd.Series) -> pd.Series:
            return s + 1

        with self.sql_conf(_TRANSPILE_ON):
            wrapped = udf(series_add_one, LongType())
            self.assertEqual(wrapped.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
            self._assert_transpiled(wrapped._unwrapped, "an annotated pandas UDF")
            df = self.spark.createDataFrame([Row(a=1)])
            self.assertEqual([2], [r[0] for r in df.select(wrapped("a")).collect()])

    def test_scalar_annotations_warn_about_eval_type_inference(self):
        # Pins a pre-existing wart rather than endorsing it: a scalar annotation is not a
        # vectorized signature, so inference raises and degrades to a warning. Annotations
        # are how callers pin an input-type category, so users will hit this and read it as
        # a transpiler bug.
        def scalar_add_one(x: int) -> int:
            return x + 1

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": True}):
                wrapped = udf(scalar_add_one, LongType())
        self.assertEqual(wrapped.evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF)
        self.assertTrue(
            any("Cannot infer the eval type from type hints" in str(w.message) for w in caught),
            f"expected the eval-type inference warning, got {[str(w.message) for w in caught]}",
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

    def test_untranspilable_pickled_udf_keeps_batch_eval_python(self):
        with self.sql_conf(_TRANSPILE_ON):
            pudf = _pickled_udf(floor_halve, LongType())
            self.assertEqual([], pudf.transpiled)
            counts = _eval_python_nodes(self.spark.range(4).select(pudf("id")))
            self.assertEqual(1, counts["BatchEvalPythonExec"])
            self.assertEqual(0, counts["ArrowEvalPythonExec"])

    def test_transpiled_arrow_udf_in_filter_has_no_python_eval(self):
        with self.sql_conf(_TRANSPILE_ON):
            pudf = _arrow_udf(gt_one, BooleanType())
            self._assert_transpiled(pudf, "a boolean Arrow-optimized UDF")
            df = self.spark.range(4).filter(pudf("id"))
            self.assertEqual(Counter(), _eval_python_nodes(df))
            self.assertEqual([2, 3], [r[0] for r in df.collect()])

    def test_middle_of_arrow_udf_chain_is_not_elided(self):
        # ConvertToCatalyst keeps a transpilable UDF whose inputs are all Python UDFs,
        # because UDF -> UDF -> UDF pipelines into one Arrow batch.
        #
        # Asserted on the PythonUDF expression count, not the operator count: the chain is
        # evaluated as a pipeline inside a single ArrowEvalPythonExec, so the operator count
        # is 1 whether or not the middle call was lowered. Three PythonUDF expressions means
        # all three still run in Python; lowering the middle one leaves 1, since the option
        # replaces it and the outer call's argument becomes plain arithmetic.
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
        # type, relying on the transpiler having cast already.
        for return_type in (ByteType(), IntegerType(), LongType(), DoubleType()):
            with self.subTest(returnType=return_type):
                with self.sql_conf(_TRANSPILE_ON):
                    on = self.spark.range(1).select(_arrow_udf(add_one, return_type)("id")).schema
                with self.sql_conf(_TRANSPILE_OFF):
                    off = self.spark.range(1).select(_arrow_udf(add_one, return_type)("id")).schema
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

    def test_numeric_arithmetic_matches_interpreted_arrow(self):
        for dtype, value, return_type in self._INTEGRAL_CASES + self._FLOATING_CASES:
            df = self._one_row(dtype, value)
            for body in _NUMERIC_ARITHMETIC:
                with self.subTest(dtype=dtype, body=body.__name__):
                    self._assert_matches_interpreted(body, return_type, df, "a")

    def test_numeric_boundary_values_match_interpreted_arrow(self):
        # Identity, because at the boundary any operation would overflow (pinned below).
        for dtype, value in (
            ("tinyint", -128),
            ("tinyint", 127),
            ("bigint", -(2**63)),
            ("bigint", 2**63 - 1),
        ):
            with self.subTest(dtype=dtype, value=value):
                self._assert_matches_interpreted(
                    identity, LongType(), self._one_row(dtype, value), "a"
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

    def test_bool_input_matches_interpreted_arrow(self):
        df = self.spark.createDataFrame([(True,), (False,), (None,)], schema="a boolean")
        self._assert_matches_interpreted(bool_identity, BooleanType(), df, "a")

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

    def test_string_comparison_matches_interpreted_arrow(self):
        df = self.spark.createDataFrame([("abc",), ("ABC",), ("zzz",)], schema="a string")
        self._assert_matches_interpreted(str_is_abc, BooleanType(), df, "a")

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

    def test_float_literal_body_with_integral_return_pickled_diverges(self):
        # `x + 0.5` declared LongType() passes the body-vs-return-type check because
        # _category lumps int and float into one "numeric" bucket, and lowers to a
        # truncating cast. The interpreted PICKLED path instead NULLs the fractional
        # result, since makeFromJava accepts only integral Java types for LongType.
        df = self._one_row("bigint", 1)
        (on_values, _), (off_values, _) = self._differential(
            add_half, LongType(), df, "a", eval_type=PythonEvalType.SQL_BATCHED_UDF
        )
        self.assertEqual([1], on_values, "the transpiled cast truncates")
        self.assertEqual([None], off_values, "the pickled path NULLs a fractional result")

    def test_float_literal_body_with_integral_return_agrees_on_arrow(self):
        # Same body on the Arrow path AGREES: Arrow truncates 1.5 to 1 just as the ANSI
        # cast does, and does so regardless of convertToArrowArraySafely. So the int/float
        # bucketing gap above is specific to the pickled regime, and widening
        # transpilation to Arrow UDFs does not introduce it here.
        df = self._one_row("bigint", 1)
        for safe in (True, False):
            with self.subTest(convertToArrowArraySafely=safe):
                self._assert_matches_interpreted(
                    add_half,
                    LongType(),
                    df,
                    "a",
                    conf={"spark.sql.execution.pandas.convertToArrowArraySafely": safe},
                )


class ArrowUDFTranspileConfInteractionTests(ArrowUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Confs that change what the interpreted Arrow path actually computes."""

    _LEGACY_PANDAS = "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled"
    _SAFE_ARROW = "spark.sql.execution.pandas.convertToArrowArraySafely"
    _INT_TO_DECIMAL = "spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled"
    _FALLBACK_ON_UDT = "spark.sql.execution.pythonUDF.arrow.legacy.fallbackOnUDT"

    def test_legacy_pandas_conversion_disables_transpilation(self):
        with self.sql_conf({**_TRANSPILE_ON, self._LEGACY_PANDAS: True}):
            pudf = _arrow_udf(add_one, LongType())
            self.assertEqual([], pudf.transpiled)

    def test_legacy_pandas_conversion_does_not_disable_the_pickled_path(self):
        # The gate must be specific to the Arrow eval type.
        with self.sql_conf({**_TRANSPILE_ON, self._LEGACY_PANDAS: True}):
            pudf = _pickled_udf(add_one, LongType())
            self._assert_transpiled(pudf, "a pickled UDF under the legacy pandas conf")

    def test_legacy_pandas_conversion_null_int_column_is_the_reason(self):
        # The evidence for the gate, on the interpreted path. Under the legacy regime an
        # integer column with NULL goes through pandas, which has no integer NA and upcasts
        # to float64, so the function receives nan and `x is None` is False -- where a
        # transpiled isnull(a) says True.
        df = self.spark.createDataFrame([(None,)], schema="a bigint")
        with self.sql_conf({**_TRANSPILE_OFF, self._LEGACY_PANDAS: True}):
            legacy = df.select(_arrow_udf(is_null, BooleanType())("a")).collect()[0][0]
        with self.sql_conf({**_TRANSPILE_OFF, self._LEGACY_PANDAS: False}):
            non_legacy = df.select(_arrow_udf(is_null, BooleanType())("a")).collect()[0][0]

        self.assertFalse(
            legacy,
            "expected the legacy pandas conversion to hand the UDF nan rather than None; "
            "if this is now True the gate in python/pyspark/sql/udf.py may be removable",
        )
        self.assertTrue(non_legacy, "the non-legacy Arrow path should pass None through")

    def test_legacy_pandas_conversion_toggled_after_construction_still_correct(self):
        # The Python gate reads the conf at construction, so it cannot see a later flip;
        # ConvertToCatalyst re-checks for exactly this case.
        df = self.spark.createDataFrame([(None,)], schema="a bigint")
        with self.sql_conf({**_TRANSPILE_ON, self._LEGACY_PANDAS: False}):
            pudf = _arrow_udf(is_null, BooleanType())
            self._assert_transpiled(pudf, "a UDF built under the non-legacy conf")
            with self.sql_conf({self._LEGACY_PANDAS: True}):
                value = df.select(pudf("a")).collect()[0][0]
        self.assertFalse(
            value,
            "with legacy pandas conversion on at execution time the query must match the "
            "legacy interpreted answer (False), so ConvertToCatalyst has to strip the "
            "transpiled option rather than use it",
        )

    def test_downcast_raises_on_both_paths_with_safe_arrow_conversion(self):
        df = self.spark.createDataFrame([(3_000_000,)], schema="a bigint")
        (on_values, _), (off_values, _) = self._differential(
            times_thousand, IntegerType(), df, "a", conf={self._SAFE_ARROW: True}
        )
        self.assertIs(_RAISED, on_values)
        self.assertIs(_RAISED, off_values)

    def test_known_divergence_downcast_with_unsafe_arrow_conversion(self):
        # Not gated against: the conf is internal, defaults safe, and asking for unsafe
        # conversion together with ANSI is self-contradictory.
        df = self.spark.createDataFrame([(3_000_000,)], schema="a bigint")
        (on_values, _), (off_values, _) = self._differential(
            times_thousand, IntegerType(), df, "a", conf={self._SAFE_ARROW: False}
        )
        self.assertIs(_RAISED, on_values, "the transpiled ANSI cast should reject the overflow")
        self.assertIsNot(
            _RAISED,
            off_values,
            "expected the interpreted Arrow path to wrap silently; if it now raises, this "
            "divergence is gone and the test should assert agreement",
        )

    def test_int_to_decimal_coercion_does_not_open_decimal_return_types(self):
        # Arrow's int-to-decimal coercion makes the interpreted path accept an int result,
        # which could look like an invitation to allow the rewrite. It is not: Arrow
        # rescales with ROUND_HALF_EVEN, which a Catalyst cast need not reproduce.
        for enabled in (True, False):
            for build in (_arrow_udf, _pickled_udf):
                with self.subTest(intToDecimal=enabled, build=build.__name__):
                    with self.sql_conf({**_TRANSPILE_ON, self._INT_TO_DECIMAL: enabled}):
                        pudf = build(add_one, DecimalType(10, 0))
                        self.assertEqual([], pudf.transpiled)

    def test_fallback_on_udt_cannot_change_the_regime_under_a_transpiled_option(self):
        # correctEvalType can flip an Arrow UDF back to the pickled eval type when a UDT is
        # involved, which would swap the coercion regime under a transpiled option. That is
        # unreachable: a UDT return type is outside the return-type allowlist, and a UDT
        # column matches no input category so every option is pruned.
        with self.sql_conf({**_TRANSPILE_ON, self._FALLBACK_ON_UDT: True}):
            pudf = _arrow_udf(identity, ExamplePointUDT())
            self.assertEqual([], pudf.transpiled, "a UDT return type must not transpile")

            df = self.spark.createDataFrame([Row(a=ExamplePoint(1.0, 2.0))])
            constant = _arrow_udf(constant_42, LongType())
            self.assertEqual(42, df.select(constant("a")).collect()[0][0])

    def test_exotic_input_columns_fall_back(self):
        with self.sql_conf(_TRANSPILE_ON):
            constant = _arrow_udf(constant_42, LongType())
            df = self.spark.range(1).selectExpr(
                "array(1, 2) as arr", "map('a', 1) as m", "struct(1 as f) as s"
            )
            for column in ("arr", "m", "s"):
                with self.subTest(column=column):
                    self.assertEqual(42, df.select(constant(column)).collect()[0][0])

    def test_arrow_concurrency_level_does_not_change_results(self):
        df = self.spark.createDataFrame([(1,), (2,), (3,)], schema="a bigint")
        self._assert_matches_interpreted(
            add_one,
            LongType(),
            df,
            "a",
            conf={"spark.sql.execution.pythonUDF.arrow.concurrency.level": 4},
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
