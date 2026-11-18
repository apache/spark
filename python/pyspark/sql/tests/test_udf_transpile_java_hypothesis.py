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
Differential / property-based tests for the ``java`` UDF transpilation target (SPARK-55209).

The same harness as ``test_udf_transpile_hypothesis.py`` (and the same ``RUN_HYPOTHESIS``
gate; see that module's docstring for how to run it), with only the ``java`` target named.
The Catalyst suite's bodies are mostly ones the Java target declines (``and`` / ``or``,
truthiness), so this one has its own, aimed at the places where Python and Java disagree
and ``TranspiledJavaUDFHelpers`` has to make up the difference:

* ``//`` and ``%`` floor where Java truncates, for ints and for floats;
* ``/`` on two ints rounds the exact quotient once, past 2**53 as well;
* an int compares exactly against a float, rather than being widened to one;
* ``NaN``, infinities and ``-0.0`` flow through as CPython's IEEE values;
* text orders by code point, which is not Java's UTF-16 order once a character is
  outside the BMP;
* locals, several statements and early returns, which only this target lowers.

Each run also checks the optimized plan for ``TranspiledJavaUDF``: an option that the
analyzer pruned would otherwise compare interpreted Python against itself.
"""

import math
import unittest

from pyspark.sql import Row
from pyspark.sql.tests.test_udf_transpile_hypothesis import (
    TranspileDifferentialMixin,
    _have_hypothesis,
    _hypothesis_enabled,
    _regular_spark,
    _skip_reason,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase

_JAVA_ONLY = {"spark.sql.experimental.optimizer.pyTranspilers": "java"}
_PLAN_MARKER = "TranspiledJavaUDF"

if _have_hypothesis:
    from hypothesis import given
    from hypothesis import strategies as st

    from pyspark.sql.tests.test_udf_transpile_hypothesis import (
        _LONG_ARITH_PAIR_EDGES,
        _LONG_BOUND,
        _LONG_PAIR_EDGES,
        _hyp_settings,
        _long_arith_strategy,
        _long_strategy,
        _seed_examples,
        _seed_pair_examples,
    )

    _TWO_53 = 2**53

    # Every double, including NaN, both infinities, -0.0 and subnormals.
    _double_strategy = st.one_of(st.none(), st.floats(allow_nan=True, allow_infinity=True))

    # Default `st.text` never yields a lone surrogate (which Spark cannot carry either) but
    # does reach past the BMP, which is where code point and UTF-16 order part ways.
    _text_strategy = st.one_of(st.none(), st.text(max_size=8))

    # Zero divisors, the floor-vs-truncate sign combinations, and quotients past 2**53.
    # `-_LONG_BOUND // -1` is in range; Long.MIN_VALUE is never generated (see the
    # Catalyst suite on why overflowing results are kept out).
    _DIVISION_PAIR_EDGES = (
        *_LONG_PAIR_EDGES,
        (7, 0),
        (0, 0),
        (7, 3),
        (-7, 3),
        (7, -3),
        (-7, -3),
        (_LONG_BOUND, -1),
        (-_LONG_BOUND, -1),
        (_TWO_53 + 1, 3),
        (_TWO_53 + 1, 2),
        (_LONG_BOUND, 3),
        (0, -(_TWO_53 + 1)),
    )

    _DOUBLE_PAIR_EDGES = (
        (None, 1.0),
        (1.0, None),
        (7.0, 0.0),
        (7.0, -0.0),
        (-7.0, 3.0),
        (7.0, -3.0),
        (5.5, 1.1),
        (-1.0, -1.0),
        (0.0, -1.0),
        (-1.0, math.inf),
        (1.0, -math.inf),
        (math.inf, 1.0),
        (math.nan, 1.0),
        (1e308, 1e-308),
    )

    _MIXED_PAIR_EDGES = (
        (None, None),
        (None, 1.0),
        (1, None),
        (_TWO_53 + 1, float(_TWO_53)),
        (_TWO_53, float(_TWO_53)),
        (_LONG_BOUND, 9223372036854775808.0),
        (-_LONG_BOUND, -9223372036854775808.0),
        (0, -0.0),
        (1, math.nan),
        (1, math.inf),
        (1, -math.inf),
    )

    _TEXT_PAIR_EDGES = (
        (None, "a"),
        ("a", None),
        ("", ""),
        ("a", "ab"),
        # U+FFFF sorts after U+10000 in UTF-16 (whose surrogates start at 0xD800) but
        # before it by code point, which is Python's order.
        ("\uffff", "\U00010000"),
        ("\U00010000", "\uffff"),
        ("\u00e9", "e\u0301"),
    )


# ---- The UDF templates we exercise --------------------------------------
#
# Module-level so ``inspect.getsource`` works. Annotated, which pins one input-type
# option per UDF instead of trying integral, fractional and string for each parameter.


def floor_div(a: int, b: int):
    return a // b


def floor_mod(a: int, b: int):
    return a % b


def true_div(a: int, b: int):
    return a / b


def float_floor_div(a: float, b: float):
    return a // b


def float_mod(a: float, b: float):
    return a % b


def long_vs_double(a: int, b: float):
    # Both operators in one job. NaN is neither less than nor equal to anything.
    if a < b:
        return -1
    if a == b:
        return 0
    return 1


def text_lt(a: str, b: str):
    return a < b


def divmod_mix(a: int, b: int):
    # Locals and an early return, which the Catalyst target does not lower.
    if b == 0:
        return None
    q = a // b
    r = a % b
    if r == 0:
        return q
    return q * 2 + r


def framed_repeat(s: str, n: int):
    if s is None:
        return "none"
    framed = "<" + s + ">"
    if n is None:
        return framed
    return framed * n


# ----------------------------------------------------------------------------


def _same(transpiled, interpreted) -> bool:
    """Equality that tells ``-0.0`` from ``0.0`` and treats NaN as equal to itself."""
    if isinstance(transpiled, float) and isinstance(interpreted, float):
        if math.isnan(transpiled) or math.isnan(interpreted):
            return math.isnan(transpiled) and math.isnan(interpreted)
        return transpiled == interpreted and math.copysign(1.0, transpiled) == math.copysign(
            1.0, interpreted
        )
    return transpiled == interpreted


@unittest.skipUnless(_have_hypothesis and _hypothesis_enabled and _regular_spark, _skip_reason)
class JavaUDFTranspileHypothesisTests(TranspileDifferentialMixin, ReusedSQLTestCase):
    """Compare the ``java`` target against interpreted Python on generated inputs."""

    def _pair_df(self, x, y, x_type, y_type):
        schema = StructType(
            [
                StructField("a", x_type, nullable=True),
                StructField("b", y_type, nullable=True),
            ]
        )
        return self.spark.createDataFrame([Row(a=x, b=y)], schema=schema)

    def _check(self, func, return_type, x, y, x_type, y_type):
        df = self._pair_df(x, y, x_type, y_type)
        transpiled, interpreted = self._run(
            func,
            return_type,
            df,
            "a",
            "b",
            transpile_conf=_JAVA_ONLY,
            plan_marker=_PLAN_MARKER,
        )
        self.assertTrue(
            _same(transpiled, interpreted),
            f"{func.__name__} mismatch on ({x!r}, {y!r}): "
            f"java={transpiled!r} python={interpreted!r}",
        )

    if _have_hypothesis:

        @_hyp_settings
        @given(x=_long_strategy, y=_long_strategy)
        @_seed_pair_examples(_DIVISION_PAIR_EDGES)
        def test_floor_div_matches_python(self, x, y):
            self._check(floor_div, LongType(), x, y, LongType(), LongType())

        @_hyp_settings
        @given(x=_long_strategy, y=_long_strategy)
        @_seed_pair_examples(_DIVISION_PAIR_EDGES)
        def test_floor_mod_matches_python(self, x, y):
            self._check(floor_mod, LongType(), x, y, LongType(), LongType())

        @_hyp_settings
        @given(x=_long_strategy, y=_long_strategy)
        @_seed_pair_examples(_DIVISION_PAIR_EDGES)
        def test_true_div_matches_python(self, x, y):
            self._check(true_div, DoubleType(), x, y, LongType(), LongType())

        @_hyp_settings
        @given(x=_double_strategy, y=_double_strategy)
        @_seed_pair_examples(_DOUBLE_PAIR_EDGES)
        def test_float_floor_div_matches_python(self, x, y):
            self._check(float_floor_div, DoubleType(), x, y, DoubleType(), DoubleType())

        @_hyp_settings
        @given(x=_double_strategy, y=_double_strategy)
        @_seed_pair_examples(_DOUBLE_PAIR_EDGES)
        def test_float_mod_matches_python(self, x, y):
            self._check(float_mod, DoubleType(), x, y, DoubleType(), DoubleType())

        @_hyp_settings
        @given(x=_long_strategy, y=_double_strategy)
        @_seed_pair_examples(_MIXED_PAIR_EDGES)
        def test_long_vs_double_matches_python(self, x, y):
            self._check(long_vs_double, LongType(), x, y, LongType(), DoubleType())

        @_hyp_settings
        @given(x=st.integers(min_value=-_LONG_BOUND, max_value=_LONG_BOUND))
        @_seed_examples((_TWO_53 - 1, _TWO_53, _TWO_53 + 1, _LONG_BOUND, -_LONG_BOUND), "x")
        def test_long_against_its_own_rounded_double(self, x):
            # Random pairs almost never land on the near-ties where widening a long to a
            # double changes the answer; pairing a long with its own rounding always does.
            self._check(long_vs_double, LongType(), x, float(x), LongType(), DoubleType())

        @_hyp_settings
        @given(x=_text_strategy, y=_text_strategy)
        @_seed_pair_examples(_TEXT_PAIR_EDGES)
        def test_text_lt_matches_python_code_point_order(self, x, y):
            self._check(text_lt, BooleanType(), x, y, StringType(), StringType())

        @_hyp_settings
        @given(x=_long_arith_strategy, y=_long_arith_strategy)
        @_seed_pair_examples((*_LONG_ARITH_PAIR_EDGES, (7, 0), (-7, 2), (7, -2), (6, 3)))
        def test_multi_statement_divmod_matches_python(self, x, y):
            # The arithmetic bound keeps `q * 2 + r` in range; an overflowing Python result
            # is wrapped by the interpreted runner rather than raised.
            self._check(divmod_mix, LongType(), x, y, LongType(), LongType())

        @_hyp_settings
        @given(
            x=_text_strategy,
            y=st.one_of(st.none(), st.integers(min_value=-3, max_value=50)),
        )
        @_seed_pair_examples(((None, 2), ("ab", None), ("", 0), ("ab", -1), ("\U0001f600", 3)))
        def test_multi_statement_text_matches_python(self, x, y):
            self._check(framed_repeat, StringType(), x, y, StringType(), LongType())


if __name__ == "__main__":
    from pyspark.testing import main

    main()
