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
Unit tests for the ``java`` UDF transpilation target.

Two kinds of test live here. The lowering tests call the transpiler directly and assert on
the Java source it produces, which is cheap and says exactly what changed when one breaks.
The end-to-end tests run queries and compare against interpreted Python, which is the only
thing that can catch a body that lowers but computes the wrong answer.

The companion suites for the Catalyst target are ``test_udf_transpile_unit.py`` and
``test_udf_transpile_hypothesis.py``; ``test_udf_transpile_parity.py`` re-runs the shared
UDF mixins with this target enabled.
"""

import ast
import textwrap
import unittest

from pyspark.errors import UnsupportedOperationException
from pyspark.sql.functions import col, lit, rand
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

# Transpilation needs both flags; ANSI because the lowerings target ANSI semantics. The
# `java` target additionally has to be named in the transpiler list, which is what makes it
# opt-in -- the default stays `catalyst`.
_JAVA_ONLY = {
    "spark.sql.experimental.optimizer.transpilePyUDFs": True,
    "spark.sql.ansi.enabled": True,
    "spark.sql.experimental.optimizer.pyTranspilers": "java",
}

_CATALYST_THEN_JAVA = dict(
    _JAVA_ONLY,
    **{
        "spark.sql.experimental.optimizer.pyTranspilers": "catalyst,java",
    },
)

_CATALYST_ONLY = dict(
    _JAVA_ONLY,
    **{
        "spark.sql.experimental.optimizer.pyTranspilers": "catalyst",
    },
)

# A true interpreted baseline. Naming no transpiler is not enough to get one: the Catalyst
# target lowers more than it might seem -- `%`, and a body that just returns a parameter --
# so a test that wants interpreted Python has to turn transpilation off rather than assume
# some particular body defeats it.
_TRANSPILE_OFF = {
    "spark.sql.experimental.optimizer.transpilePyUDFs": False,
    "spark.sql.ansi.enabled": True,
}


def _lower(source: str, return_type, categories: dict) -> str:
    """Lower ``source``'s function to Java statements, without a session or a JVM.

    Drives the transpiler below its ``_build_column`` step, which is the only part that
    needs a live JVM, so the generated source can be asserted on directly.
    """
    from pyspark.sql.transpile import _get_function_from_ast
    from pyspark.sql.transpile_java import JavaTranspiler, _return_category

    tree = ast.parse(textwrap.dedent(source))
    function_ast, error = _get_function_from_ast(tree, None)
    assert function_ast is not None, error
    transpiler = JavaTranspiler()
    transpiler._param_categories = dict(categories)
    transpiler._params = [arg.arg for arg in function_ast.args.args]
    transpiler._arg_names = [f"_udf_arg_{i}" for i in range(len(transpiler._params))]
    transpiler._locals = {}
    transpiler._local_java_names = {}
    return "\n".join(transpiler._lower_body(function_ast.body, _return_category(return_type)))


class JavaTranspileLoweringTests(unittest.TestCase):
    """Tests over the generated source. No session, no JVM."""

    def test_arithmetic_goes_through_the_helpers(self):
        # Never a bare Java operator: `+` would wrap silently where ANSI must raise, and
        # `%` and `/` have the wrong sign and the wrong zero behaviour for Python.
        lowered = _lower("def f(x):\n    return x + 1", LongType(), {0: "integral"})
        self.assertIn("TranspiledJavaUDFHelpers.addLong(_udf_arg_0", lowered)
        self.assertNotIn("_udf_arg_0 +", lowered)

    def test_floor_divide_and_mod_use_the_floor_helpers(self):
        floor_div = _lower("def f(x):\n    return x // 3", LongType(), {0: "integral"})
        self.assertIn("floorDivideLong", floor_div)
        mod = _lower("def f(x):\n    return x % 3", LongType(), {0: "integral"})
        self.assertIn("modLong", mod)

    def test_true_divide_of_ints_produces_a_float(self):
        # Python's `/` is always float division, so an int body under a declared double is
        # the correct shape and an int return type is refused.
        lowered = _lower("def f(x):\n    return x / 2", DoubleType(), {0: "integral"})
        self.assertIn("divideLong", lowered)
        with self.assertRaises(UnsupportedOperationException):
            _lower("def f(x):\n    return x / 2", LongType(), {0: "integral"})

    def test_multi_statement_body_with_local_and_early_return(self):
        lowered = _lower(
            """
            def f(x):
                doubled = x * 2
                if doubled > 10:
                    return 10
                return doubled + 1
            """,
            LongType(),
            {0: "integral"},
        )
        self.assertIn("_doubled =", lowered)
        self.assertIn("Long _udf_local_0", lowered)
        self.assertIn("return Long.valueOf(10L);", lowered)

    def test_locals_needing_the_same_sanitised_name_stay_distinct(self):
        # Sanitising to Java's character set is not injective on its own: a non-ASCII name
        # and an underscore name can reduce to the same text, and two locals sharing one
        # Java variable would be a silently wrong answer rather than a failure. The numbering
        # is what prevents it.
        # The identifier is spelled with an escape so this file stays ASCII, per the
        # project's convention; what `ast.parse` sees is still a non-ASCII name.
        accented = "caf\u00e9"
        lowered = _lower(
            f"""
            def f(x):
                {accented} = x + 1
                caf_ = x + 2
                return {accented} + caf_
            """,
            LongType(),
            {0: "integral"},
        )
        self.assertIn("_udf_local_0_caf_", lowered)
        self.assertIn("_udf_local_1_caf_", lowered)

    def test_no_unreachable_return_when_every_path_returns(self):
        # Java rejects an unreachable statement, so the trailing `return null` that stands
        # for Python falling off the end must not be emitted when it cannot be reached.
        both = _lower(
            """
            def f(x):
                if x > 0:
                    return 1
                else:
                    return 2
            """,
            LongType(),
            {0: "integral"},
        )
        # Two returns, one per branch, and crucially no third one after the `if`.
        self.assertEqual(2, both.count("return "), both)
        self.assertNotIn("null", both)

        # ... and must be emitted when it can, since `if` alone falls through.
        falls_through = _lower(
            """
            def f(x):
                if x > 0:
                    return 1
            """,
            LongType(),
            {0: "integral"},
        )
        self.assertIn("return ((Long) null);", falls_through)

    def test_int_promotes_to_float_when_mixed(self):
        lowered = _lower(
            "def f(a, b):\n    return a + b", DoubleType(), {0: "integral", 1: "fractional"}
        )
        self.assertIn("toDouble(_udf_arg_0)", lowered)
        self.assertIn("addDouble", lowered)

    def test_string_concat_and_repeat(self):
        concat = _lower("def f(s):\n    return s + 'x'", StringType(), {0: "string"})
        self.assertIn("TranspiledJavaUDFHelpers.concat", concat)
        self.assertIn('UTF8String.fromString("x")', concat)
        repeat = _lower(
            "def f(s, n):\n    return s * n", StringType(), {0: "string", 1: "integral"}
        )
        self.assertIn("TranspiledJavaUDFHelpers.repeat", repeat)

    def test_repeat_refuses_a_fractional_count(self):
        # The divergence the Catalyst target documents: there a fractional count from a
        # column is truncated by the cast it inserts, where Python raises TypeError. Here
        # the count's category is known before any source is emitted, so it is declined.
        with self.assertRaises(UnsupportedOperationException):
            _lower(
                "def f(s, n):\n    return s * n",
                StringType(),
                {0: "string", 1: "fractional"},
            )

    def test_non_ascii_string_literal_is_escaped(self):
        lowered = _lower("def f(s):\n    return s + '\u00e9'", StringType(), {0: "string"})
        self.assertIn("\\u00e9", lowered)
        # The generated source stays ASCII so no compiler has to be asked how it decodes.
        self.assertTrue(lowered.isascii(), lowered)

    def test_is_none_lowers_but_eq_none_declines(self):
        guarded = _lower(
            """
            def f(x):
                if x is not None:
                    return x + 1
            """,
            LongType(),
            {0: "integral"},
        )
        self.assertIn("_udf_arg_0 != null", guarded)
        # `x == None` is False in Python but NULL in SQL. The Catalyst target lowers it
        # specially and is tried first, so declining here costs nothing.
        with self.assertRaises(UnsupportedOperationException):
            _lower(
                "def f(x):\n    if x == None:\n        return 1",
                LongType(),
                {0: "integral"},
            )

    def test_each_ordering_operator_keeps_its_own_operand_order(self):
        # `>` gets its own helper rather than `<` with the operands swapped. Swapping is correct
        # for the result but reverses the order Java evaluates the two sides in, so when both can
        # raise the wrong one wins -- `x // 0 > x * x` would report the overflow, where Python and
        # a Catalyst GreaterThan both report the division.
        lowered = _lower("def f(x):\n    return x > 5", BooleanType(), {0: "integral"})
        self.assertIn("greaterThanLong(_udf_arg_0, Long.valueOf(5L))", lowered)
        lowered = _lower("def f(x):\n    return x >= 5", BooleanType(), {0: "integral"})
        self.assertIn("greaterThanOrEqualLong(_udf_arg_0, Long.valueOf(5L))", lowered)
        lowered = _lower("def f(x):\n    return x < 5", BooleanType(), {0: "integral"})
        self.assertIn("lessThanLong(_udf_arg_0, Long.valueOf(5L))", lowered)

    def test_shapes_whose_python_meaning_would_be_changed_are_declined(self):
        # Each of these lowered to something plausible-looking and wrong before, so they are
        # pinned as refusals rather than left to a reviewer to notice.
        cases = {
            # Python short-circuits `and`; a helper call cannot, so the guard would not guard.
            "and": (
                "def f(a, b):\n    return b != 0 and a > 1",
                BooleanType(),
                {0: "integral", 1: "integral"},
            ),
            "or": (
                "def f(a, b):\n    return b == 0 or a > 1",
                BooleanType(),
                {0: "integral", 1: "integral"},
            ),
            # Python's `not None` is True; SQL's `NOT NULL` is NULL.
            "not None": ("def f(x):\n    return not None", BooleanType(), {0: "integral"}),
            # `None == x` is as much a None comparison as `x == None`.
            "None on the left": ("def f(x):\n    return None == x", BooleanType(), {0: "integral"}),
            # An annotation is not a cast: converting would lose precision past 2**53.
            "annotation as cast": (
                "def f(x):\n    y: float = x\n    return y",
                DoubleType(),
                {0: "integral"},
            ),
        }
        for label, (source, return_type, categories) in cases.items():
            with self.subTest(case=label):
                with self.assertRaises(UnsupportedOperationException):
                    _lower(source, return_type, categories)

    def test_no_statement_is_emitted_after_a_definite_return(self):
        # Python just never runs these; Java rejects them as unreachable (JLS 14.21), and a body
        # that does not compile is a failed query rather than a fallback once the option is in the
        # plan. Both shapes below occur in real code -- a defensive trailing return, and dead code.
        after_if_else = _lower(
            """
            def f(x):
                if x > 0:
                    return 1
                else:
                    return 2
                return 0
            """,
            LongType(),
            {0: "integral"},
        )
        # Matched with the `return` and the `;`: the condition `x > 0` legitimately contains a
        # bare `Long.valueOf(0L)`, so the looser check would fail on correct output.
        self.assertNotIn("return Long.valueOf(0L);", after_if_else)
        after_return = _lower(
            """
            def f(x):
                return x
                y = x + 1
            """,
            LongType(),
            {0: "integral"},
        )
        self.assertNotIn("_udf_local", after_return)

    def test_refusals_fall_back_rather_than_guess(self):
        # Each of these is a shape whose Python meaning the target will not reproduce.
        cases = {
            "bitwise": "def f(x):\n    return x & 1",
            "chained comparison": "def f(x):\n    return 1 < x < 5",
            "loop": "def f(x):\n    for i in range(3):\n        x = x + 1\n    return x",
            "try": "def f(x):\n    try:\n        return x\n    except Exception:\n        return 0",
            "truthiness": "def f(x):\n    if x:\n        return 1\n    return 0",
            "free variable": "def f(x):\n    return x + undefined_name",
            "attribute": "def f(x):\n    return x.bit_length()",
            "augmented assign": "def f(x):\n    x += 1\n    return x",
            "unary minus on text": "def f(s):\n    return -s",
            "while": "def f(x):\n    while x > 0:\n        x = x - 1\n    return x",
        }
        for label, source in cases.items():
            with self.subTest(case=label):
                with self.assertRaises(UnsupportedOperationException):
                    category = "string" if label == "unary minus on text" else "integral"
                    return_type = StringType() if category == "string" else LongType()
                    _lower(source, return_type, {0: category})

    def test_local_reassigned_to_another_category_declines(self):
        # A Java local has one type. Rather than rename per assignment, decline.
        with self.assertRaises(UnsupportedOperationException):
            _lower(
                """
                def f(x):
                    y = x + 1
                    y = 'text'
                    return y
                """,
                StringType(),
                {0: "integral"},
            )

    def test_local_first_bound_inside_a_branch_declines(self):
        # Python would have it bound after the `if`; a Java local declared in the block
        # would not exist there. Declining is the fail-closed choice.
        with self.assertRaises(UnsupportedOperationException):
            _lower(
                """
                def f(x):
                    if x > 0:
                        y = 1
                    return y
                """,
                LongType(),
                {0: "integral"},
            )

    def test_category_combos_split_numeric_and_pin_annotations(self):
        from pyspark.sql.transpile_java import JavaTranspiler

        annotated = ast.parse("def f(a: int, b: float, c: str): return a").body[0]
        combos = JavaTranspiler()._param_category_combos(annotated, ["a", "b", "c"])
        self.assertEqual([{0: "integral", 1: "fractional", 2: "string"}], combos)

        # One untyped parameter is tried as all three categories.
        untyped = ast.parse("def f(a): return a").body[0]
        combos = JavaTranspiler()._param_category_combos(untyped, ["a"])
        self.assertEqual(
            [{0: "integral"}, {0: "fractional"}, {0: "string"}],
            combos,
        )

    def test_category_combos_cap_engages_past_two_untyped(self):
        from pyspark.sql.transpile_java import JavaTranspiler

        # Both sides of the boundary, because the earlier version of this test used four untyped
        # parameters -- collapsed under either threshold -- so the cap could have moved from two to
        # three, or vanished, without failing. Two untyped is the largest uncapped case and the one
        # that costs the most driver-side compiles.
        two = ast.parse("def f(a, b): return a").body[0]
        self.assertEqual(9, len(JavaTranspiler()._param_category_combos(two, ["a", "b"])))

        three = ast.parse("def f(a, b, c): return a").body[0]
        self.assertEqual(3, len(JavaTranspiler()._param_category_combos(three, ["a", "b", "c"])))

        # And a pinned parameter stays pinned once the cap engages.
        fn = ast.parse("def f(a: str, b, c, d, e): return a").body[0]
        combos = JavaTranspiler()._param_category_combos(fn, ["a", "b", "c", "d", "e"])
        self.assertEqual(3, len(combos))
        for combo in combos:
            self.assertEqual("string", combo[0])

    def test_a_none_valued_conditional_keeps_its_condition(self):
        # `_coerce` used to replace a "none"-category value with a bare `null`, discarding the code
        # that produced it -- so this body lowered to `return ((Long) null);` and the divide
        # vanished, returning NULL where CPython raises ZeroDivisionError on b = 0.
        lowered = _lower(
            "def f(a, b):\n    return None if a // b > 0 else None",
            LongType(),
            {0: "integral", 1: "integral"},
        )
        self.assertIn("floorDivideLong", lowered)

    def test_unsupported_return_type_declines(self):
        from pyspark.sql.types import DecimalType, TimestampType

        for return_type in (DecimalType(10, 2), TimestampType()):
            with self.subTest(return_type=return_type.simpleString()):
                with self.assertRaises(UnsupportedOperationException):
                    _lower("def f(x):\n    return x", return_type, {0: "integral"})


@unittest.skipIf(
    is_remote_only(),
    "UDF transpilation is only supported in regular (non-Connect) Spark.",
)
class JavaTranspileEndToEndTests(ReusedSQLTestCase):
    """Tests that actually run the generated code and compare against interpreted Python."""

    def _plan(self, df) -> str:
        return df._jdf.queryExecution().optimizedPlan().toString()

    def test_java_target_is_off_by_default(self):
        # The whole point of the opt-in: with the default transpiler list, naming nothing
        # new, a body only this target can lower must still run as interpreted Python.
        def multi(x):
            doubled = x * 2
            return doubled + 1

        with self.sql_conf(_CATALYST_ONLY):
            self.assertEqual([], UserDefinedFunction(multi, LongType()).transpiled)

    def test_catalyst_wins_when_both_can_lower(self):
        # Ordering is what makes `catalyst,java` safe: a UDF Catalyst can lower keeps the
        # Catalyst option, which the optimizer can see into, and never the opaque one.
        def plus_one(x):
            if x is not None:
                return x + 1

        df = self.spark.range(3).select(col("id").alias("a"))
        with self.sql_conf(_CATALYST_THEN_JAVA):
            udf = UserDefinedFunction(plus_one, LongType())
            plan = self._plan(df.select(udf(col("a"))))
            self.assertNotIn("TranspiledJavaUDF", plan)
            self.assertNotIn("BatchEvalPython", plan)

        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(plus_one, LongType())
            plan = self._plan(df.select(udf(col("a"))))
            self.assertIn("TranspiledJavaUDF", plan)

    def test_multi_statement_lowers_only_with_java(self):
        # The capability that justifies a second target.
        def clamp(x):
            doubled = x * 2
            if doubled > 10:
                return 10
            return doubled + 1

        df = self.spark.createDataFrame([(3,), (9,)], "a bigint")
        with self.sql_conf(_CATALYST_ONLY):
            udf = UserDefinedFunction(clamp, LongType())
            self.assertEqual([], udf.transpiled)
            interpreted = df.select(udf(col("a"))).collect()

        with self.sql_conf(_CATALYST_THEN_JAVA):
            udf = UserDefinedFunction(clamp, LongType())
            self.assertTrue(udf.transpiled)
            result = df.select(udf(col("a")))
            self.assertIn("TranspiledJavaUDF", self._plan(result))
            self.assertNotIn("BatchEvalPython", self._plan(result))
            self.assertEqual([r[0] for r in interpreted], [r[0] for r in result.collect()])

    def test_repeated_parameter_read_adds_no_projection(self):
        # A body reading one parameter three times: the reads are inside the generated
        # source, so the plan holds one child and nothing has to be hoisted into a Project
        # to keep the argument to one evaluation.
        def thrice(x):
            return x + x + x

        df = self.spark.range(3).select((col("id") * 2).alias("a"))
        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(thrice, LongType())
            result = df.select(udf(col("a")))
            plan = self._plan(result)
            self.assertIn("TranspiledJavaUDF", plan)
            self.assertNotIn("BatchEvalPython", plan)
            self.assertEqual([0, 6, 12], [r[0] for r in result.collect()])

    def test_results_match_interpreted_python(self):
        # The check that matters: the same function, run both ways, over values chosen to
        # hit the sign and zero cases where Python, Java and SQL disagree.
        def arithmetic(x):
            if x is not None:
                return x // 3

        def modulo(x):
            if x is not None:
                return x % 3

        def promoted(x):
            if x is not None:
                return x / 4

        rows = [(-7,), (-1,), (0,), (7,), (8,)]
        df = self.spark.createDataFrame(rows, "a bigint")
        for func, return_type in (
            (arithmetic, LongType()),
            (modulo, LongType()),
            (promoted, DoubleType()),
        ):
            with self.subTest(func=func.__name__):
                with self.sql_conf(_TRANSPILE_OFF):
                    plain = UserDefinedFunction(func, return_type)
                    self.assertEqual([], plain.transpiled)
                    expected = [r[0] for r in df.select(plain(col("a"))).collect()]
                with self.sql_conf(_JAVA_ONLY):
                    lowered = UserDefinedFunction(func, return_type)
                    self.assertTrue(lowered.transpiled)
                    actual = [r[0] for r in df.select(lowered(col("a"))).collect()]
                self.assertEqual(expected, actual)

    def test_python_floor_and_mod_signs(self):
        # Pinned values, not just agreement: -7 // 3 is -3 and -7 % 3 is 2 in Python, where
        # Java's own operators give -2 and -1.
        def floor_div(x):
            if x is not None:
                return x // 3

        def modulo(x):
            if x is not None:
                return x % 3

        df = self.spark.createDataFrame([(-7,)], "a bigint")
        with self.sql_conf(_JAVA_ONLY):
            self.assertEqual(
                -3, df.select(UserDefinedFunction(floor_div, LongType())(col("a"))).first()[0]
            )
            self.assertEqual(
                2, df.select(UserDefinedFunction(modulo, LongType())(col("a"))).first()[0]
            )

    def test_null_propagates_like_the_catalyst_target(self):
        # An unguarded body over a NULL gives NULL on both transpiled targets, where
        # interpreted Python raises TypeError. Documented, and the same on both, so which
        # backend fired is not observable from the result.
        def unguarded(x):
            return x + 1

        df = self.spark.createDataFrame([(None,), (1,)], "a bigint")
        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(unguarded, LongType())
            self.assertTrue(udf.transpiled)
            self.assertEqual([None, 2], [r[0] for r in df.select(udf(col("a"))).collect()])

    def test_equality_over_null_is_null_safe_like_python(self):
        # Python's `==` is not three-valued: `None == None` is True and `None == 1` is False, never
        # None. Propagating NULL instead made `x != 1` return NULL over a NULL column where Python
        # returns True, and `if x != 1` take the wrong branch. Three statements, so Catalyst
        # declines and the java option is what runs.
        def not_one(x):
            y = 1
            if x != 1:
                y = 10
            return y

        def both_none(a, b):
            same = a == b
            if same:
                return 1
            return 0

        df = self.spark.createDataFrame([(None,)], "a bigint")
        pair = self.spark.createDataFrame([(None, None)], "a bigint, b bigint")
        with self.sql_conf(_TRANSPILE_OFF):
            expected_ne = df.select(UserDefinedFunction(not_one, LongType())(col("a"))).first()[0]
            expected_eq = pair.select(
                UserDefinedFunction(both_none, LongType())(col("a"), col("b"))
            ).first()[0]
        with self.sql_conf(_JAVA_ONLY):
            udf_ne = UserDefinedFunction(not_one, LongType())
            self.assertTrue(udf_ne.transpiled)
            self.assertEqual(expected_ne, df.select(udf_ne(col("a"))).first()[0])
            udf_eq = UserDefinedFunction(both_none, LongType())
            self.assertTrue(udf_eq.transpiled)
            self.assertEqual(expected_eq, pair.select(udf_eq(col("a"), col("b"))).first()[0])
        # Pinned, not just agreed: `None != 1` is True so the branch is taken, and
        # `None == None` is True.
        self.assertEqual(10, expected_ne)
        self.assertEqual(1, expected_eq)

    def test_a_bare_none_local_declines_with_a_python_level_message(self):
        # `y = None` is ordinary Python. Declining is fine; the message has to name the construct
        # rather than leak the internal "no Java type for category 'none'".
        def maybe(x):
            y = None
            if x is not None:
                y = x
            return y

        with self.sql_conf(_JAVA_ONLY):
            import warnings as _w

            with _w.catch_warnings(record=True) as caught:
                _w.simplefilter("always")
                udf = UserDefinedFunction(maybe, LongType())
            self.assertEqual([], udf.transpiled)
            messages = " ".join(str(w.message) for w in caught)
            self.assertIn("bare None", messages)
            self.assertNotIn("no Java type for category", messages)

    def test_both_eval_paths_agree(self):
        # `eval` and `doGenCode` come off one source string; this is what says so from the
        # outside. Codegen off exercises the per-JVM compile in the interpreted path.
        def clamp(x):
            doubled = x * 2
            if doubled > 10:
                return 10
            return doubled + 1

        df = self.spark.createDataFrame([(1,), (3,), (9,)], "a bigint")
        results = []
        for factory_mode in ("CODEGEN_ONLY", "NO_CODEGEN"):
            with self.sql_conf(dict(_JAVA_ONLY, **{"spark.sql.codegen.factoryMode": factory_mode})):
                udf = UserDefinedFunction(clamp, LongType())
                results.append([r[0] for r in df.select(udf(col("a"))).collect()])
        self.assertEqual(results[0], results[1])
        self.assertEqual([3, 7, 10], results[0])

    def test_a_literal_argument_works(self):
        # `foldable` is deliberately not overridden, so ConstantFolding does not fire on this node
        # -- the interpreted path exists for NO_CODEGEN, not for folding. What this pins is that a
        # literal argument still produces the right answer.
        def plus_one(x):
            return x + 1

        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(plus_one, LongType())
            result = self.spark.range(1).select(udf(lit(41)).alias("v"))
            self.assertEqual(42, result.first()[0])

    def test_float_mod_is_exact_not_just_correctly_signed(self):
        # The bug this replaced got the sign right and the value wrong for almost every inexact
        # pair, returning 0.0 for `5.5 % 1.1`. Compared against interpreted Python rather than a
        # hand-written constant.
        def remainder(x):
            if x is not None:
                return x % 1.1

        rows = [(5.5,), (100.0,), (0.7,), (-5.5,), (1e16,)]
        df = self.spark.createDataFrame(rows, "a double")
        with self.sql_conf(_TRANSPILE_OFF):
            expected = [
                r[0]
                for r in df.select(UserDefinedFunction(remainder, DoubleType())(col("a"))).collect()
            ]
        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(remainder, DoubleType())
            self.assertTrue(udf.transpiled)
            actual = [r[0] for r in df.select(udf(col("a"))).collect()]
        self.assertEqual(expected, actual)

    def test_large_int_division_rounds_once(self):
        # Widening both operands to double first rounds twice and lands an ULP away.
        def halve(x):
            if x is not None:
                return x / 3

        df = self.spark.createDataFrame([(9007199254740993,)], "a bigint")
        with self.sql_conf(_TRANSPILE_OFF):
            expected = df.select(UserDefinedFunction(halve, DoubleType())(col("a"))).first()[0]
        with self.sql_conf(_JAVA_ONLY):
            actual = df.select(UserDefinedFunction(halve, DoubleType())(col("a"))).first()[0]
        self.assertEqual(expected, actual)
        self.assertEqual(3002399751580331.0, actual)

    def test_ordering_against_null_raises_like_the_catalyst_target(self):
        # Returning NULL here would take the false branch and hand back a confident wrong answer.
        # Both transpiled targets raise; interpreted Python raises TypeError. Three statements, so
        # Catalyst declines the body and the java option is the one that runs.
        def classify(x):
            y = 1
            if x > 0:
                y = 2
            return y

        df = self.spark.createDataFrame([(None,)], "a bigint")
        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(classify, LongType())
            self.assertTrue(udf.transpiled)
            with self.assertRaises(Exception) as caught:
                df.select(udf(col("a"))).collect()
            self.assertIn("cannot compare NULL", str(caught.exception))

    def test_arithmetic_errors_carry_spark_error_classes(self):
        # The same failure through the Catalyst target is [ARITHMETIC_OVERFLOW] / [DIVIDE_BY_ZERO];
        # a bare java.lang.ArithmeticException would make the error class depend on which target
        # lowered the UDF.
        def add_one(x):
            if x is not None:
                return x + 1

        def divide(x):
            if x is not None:
                return x // 0

        with self.sql_conf(_JAVA_ONLY):
            overflowing = self.spark.createDataFrame([(2**63 - 1,)], "a bigint")
            with self.assertRaises(Exception) as caught:
                overflowing.select(UserDefinedFunction(add_one, LongType())(col("a"))).collect()
            self.assertIn("ARITHMETIC_OVERFLOW", str(caught.exception))

            df = self.spark.createDataFrame([(1,)], "a bigint")
            with self.assertRaises(Exception) as caught:
                df.select(UserDefinedFunction(divide, LongType())(col("a"))).collect()
            self.assertIn("DIVIDE_BY_ZERO", str(caught.exception))

    def test_one_draw_per_row_for_a_repeated_parameter(self):
        # The no-double-draws claim. The body reads its parameter twice and subtracts, so a
        # second draw would show up as a non-zero result.
        def difference(x):
            return x - x

        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(difference, DoubleType())
            self.assertTrue(udf.transpiled)
            values = [r[0] for r in self.spark.range(20).select(udf(rand(1)).alias("v")).collect()]
            self.assertTrue(all(v == 0.0 for v in values), values)

    def test_two_draws_stay_two_draws(self):
        # The other half: two arguments are two parameters, so `f(rand(), rand())` draws
        # twice, which is what the interpreted UDF does too.
        def difference(a, b):
            return a - b

        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(difference, DoubleType())
            values = [
                r[0]
                for r in self.spark.range(20).select(udf(rand(1), rand(2)).alias("v")).collect()
            ]
            self.assertTrue(any(v != 0.0 for v in values), values)

    def test_every_argument_is_evaluated_even_when_unread(self):
        # Each parameter is a child, so every argument is computed whether or not the body
        # reads it. That matches the interpreted UDF, which evaluates its arguments in a
        # projection feeding the worker. A Catalyst lowering does not: it inlines a
        # parameter only where the body reads it, so an argument no branch reads never
        # reaches the plan. All three behaviours are asserted, because the contrast is the
        # point -- and because the Catalyst arm is the one that would quietly change if that
        # target ever started evaluating unread arguments.
        def first_only(a, b):
            return a

        df = self.spark.createDataFrame([(1, 0)], "a bigint, b bigint")
        # `/` under ANSI raises on a zero divisor. (`//` is not defined on Column.)
        unread = col("a") / col("b")

        with self.sql_conf(_TRANSPILE_OFF):
            interpreted = UserDefinedFunction(first_only, LongType())
            self.assertEqual([], interpreted.transpiled)
            with self.assertRaises(Exception):
                df.select(interpreted(col("a"), unread)).collect()

        with self.sql_conf(_JAVA_ONLY):
            lowered = UserDefinedFunction(first_only, LongType())
            self.assertTrue(lowered.transpiled)
            with self.assertRaises(Exception):
                df.select(lowered(col("a"), unread)).collect()

        with self.sql_conf(_CATALYST_ONLY):
            catalyst = UserDefinedFunction(first_only, LongType())
            self.assertTrue(catalyst.transpiled)
            # No exception: the divide is not in the plan at all.
            self.assertEqual(1, df.select(catalyst(col("a"), unread)).first()[0])

    def test_a_nondeterministic_udf_is_never_transpiled(self):
        def plus_one(x):
            return x + 1

        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(plus_one, LongType()).asNondeterministic()
            self.assertEqual([], udf.transpiled)

    def test_a_string_column_falls_back_for_a_numeric_body(self):
        # Type pruning: `x // 3` is only lowered for numbers, so over text no option
        # survives and the interpreted UDF runs -- which is what raises TypeError, the way
        # Python does, rather than coercing.
        def floor_div(x):
            return x // 3

        df = self.spark.createDataFrame([("abc",)], "a string")
        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(floor_div, LongType())
            plan = self._plan(df.select(udf(col("a"))))
            self.assertNotIn("TranspiledJavaUDF", plan)
            self.assertIn("BatchEvalPython", plan)

    def test_int_and_float_columns_pick_different_options(self):
        # The integral/fractional split: one untyped body, two column types, and the value
        # has to come out right for each rather than being truncated or widened wrongly.
        def halve(x):
            return x / 2

        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(halve, DoubleType())
            integral = self.spark.createDataFrame([(5,)], "a bigint")
            self.assertEqual(2.5, integral.select(udf(col("a"))).first()[0])
            fractional = self.spark.createDataFrame([(5.5,)], "a double")
            self.assertEqual(2.75, fractional.select(udf(col("a"))).first()[0])

    def test_string_comparison_matches_python_codepoint_order(self):
        # Codepoint order, so "Z" < "a". Spark disables UTF8String.compareTo in favour of
        # binaryCompare, and only the latter is collation-free -- this is the end-to-end
        # check that the helper reaches for the right one.
        def before_lowercase_a(s):
            if s is not None:
                return s < "a"

        df = self.spark.createDataFrame([("Z",), ("b",)], "a string")
        with self.sql_conf(_TRANSPILE_OFF):
            interpreted = UserDefinedFunction(before_lowercase_a, BooleanType())
            expected = [r[0] for r in df.select(interpreted(col("a"))).collect()]
        with self.sql_conf(_JAVA_ONLY):
            lowered = UserDefinedFunction(before_lowercase_a, BooleanType())
            self.assertTrue(lowered.transpiled)
            actual = [r[0] for r in df.select(lowered(col("a"))).collect()]
        self.assertEqual([True, False], expected)
        self.assertEqual(expected, actual)

    def test_the_generated_body_stays_out_of_the_plan(self):
        # The default Expression.toString prints every case-class field, which for this node
        # means the whole multi-line Java method lands in the plan string and mangles
        # EXPLAIN. The plan should name the node and its arguments and nothing else. This
        # also makes the `TranspiledJavaUDF` assertions elsewhere in this file mean what they
        # say: before the override they matched `TranspiledJavaUDFHelpers` inside the dumped
        # body rather than the node.
        def clamp(x):
            doubled = x * 2
            if doubled > 10:
                return 10
            return doubled + 1

        df = self.spark.createDataFrame([(3,)], "a bigint")
        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(clamp, LongType())
            result = df.select(udf(col("a")))
            plan = self._plan(result)
            self.assertIn("TranspiledJavaUDF(clamp,", plan)
            self.assertNotIn("TranspiledJavaUDFHelpers", plan)
            self.assertNotIn("_udf_local", plan)
            self.assertNotIn("Long.valueOf", plan)
            # The user-facing column name is the UDF's, as it is for an interpreted one.
            self.assertEqual(["clamp(a)"], result.columns)

    def test_a_zero_argument_udf_lowers(self):
        # Exercises every empty-collection path at once: no parameters in the generated
        # method, no arguments in the call it emits (a stray comma would not compile), and
        # empty lists across the Py4J bridge.
        def answer():
            return 42

        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(answer, LongType())
            self.assertTrue(udf.transpiled)
            result = self.spark.range(2).select(udf().alias("v"))
            self.assertIn("TranspiledJavaUDF(answer,", self._plan(result))
            self.assertEqual([42, 42], [r[0] for r in result.collect()])

    def test_bytes_are_lowered_for_the_identity_body_only(self):
        # `binary` is a category this target names a Java type for (`byte[]`), but no
        # operator is lowered over it, so the reachable case is passing bytes through. Worth
        # a test because the ABI claims to support the category: the cast in the generated
        # invoker and the method's return type both have to be `byte[]`.
        # Annotated, because an untyped parameter is only tried as integral, fractional and
        # string -- bool and binary have to be pinned, exactly as for the Catalyst target.
        def passthrough(b: bytes):
            return b

        df = self.spark.createDataFrame([(bytearray(b"ab"),)], "a binary")
        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(passthrough, BinaryType())
            self.assertTrue(udf.transpiled)
            result = df.select(udf(col("a")))
            self.assertIn("TranspiledJavaUDF", self._plan(result))
            self.assertEqual(bytearray(b"ab"), result.first()[0])

    def test_a_wide_integral_column_is_not_truncated(self):
        # Arguments are upcast into the category's type, so an int column and a bigint
        # column both arrive as a Long and a value near the long bound survives.
        def identity(x):
            return x

        with self.sql_conf(_JAVA_ONLY):
            udf = UserDefinedFunction(identity, LongType())
            for value in (2**31, 2**62):
                df = self.spark.createDataFrame([(value,)], "a bigint")
                self.assertEqual(value, df.select(udf(col("a"))).first()[0])


class GetTranspilersTests(unittest.TestCase):
    """The conf parsing in ``_get_transpilers``. No session -- a stub answers the one conf read."""

    class _StubSession:
        def __init__(self, value):
            self.conf = self

        def get(self, key, default=None):
            # Signature matches RuntimeConfig.get, which is all _get_transpilers uses.
            return self._value

    def _transpilers(self, conf_value):
        import warnings as _w

        from pyspark.sql.transpile import _get_transpilers

        session = self._StubSession(conf_value)
        session._value = conf_value
        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            names = [t.variety for t in _get_transpilers(session)]
        return names, [str(w.message) for w in caught]

    def test_names_are_stripped(self):
        # A comma-separated conf is something a human types, so "catalyst, java" is the natural
        # spelling. Without the strip the second entry matches no registered variety and the
        # target goes silently missing.
        names, warnings_seen = self._transpilers("catalyst, java")
        self.assertEqual(["catalyst", "java"], names)
        self.assertEqual([], warnings_seen)

    def test_an_unknown_name_warns_and_keeps_the_rest(self):
        names, warnings_seen = self._transpilers("catalyst,jvaa")
        self.assertEqual(["catalyst"], names)
        self.assertTrue(any("jvaa" in m for m in warnings_seen), warnings_seen)

    def test_warnings_as_errors_does_not_disable_transpilation(self):
        # The warning is raised inside ``_transpile_func``'s blanket handler, whose meaning is
        # "no transpilation at all", so under -W error a single typo would take the working
        # transpilers down with it.
        import warnings as _w

        from pyspark.sql.transpile import _get_transpilers

        session = self._StubSession("catalyst,jvaa")
        session._value = "catalyst,jvaa"
        with _w.catch_warnings():
            _w.simplefilter("error")
            names = [t.variety for t in _get_transpilers(session)]
        self.assertEqual(["catalyst"], names)

    def test_an_empty_conf_yields_nothing(self):
        self.assertEqual(([], []), self._transpilers(""))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
