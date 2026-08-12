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
Parity tests that re-run the existing UDF test suites with Python-to-Catalyst
transpilation enabled.

Transpilation is only attempted when both
``spark.sql.experimental.optimizer.transpilePyUDFs`` and
``spark.sql.ansi.enabled`` are true, and it is designed to fall back to
interpreted Python rather than risk semantic drift. These classes re-run the
shared UDF mixins under that configuration so we can confirm that turning on the
experimental feature does not change UDF results compared with the default
(transpilation off) runs covered by the original concrete classes
(``UDFTests``, ``UDFCombinationsTests``, ``UnifiedUDFTests``).

Transpilation is currently only supported in regular (non-Connect) Spark, so
these classes are guarded with ``is_remote_only()`` and are intentionally not
inherited into the Spark Connect parity tests. The companion suites that test
the transpiler directly live in ``test_udf_transpile_unit.py`` and
``test_udf_transpile_hypothesis.py``.

Note on configuration: enabling transpilation requires ANSI mode, so an "on"
run is unavoidably also an ANSI run. Inherited tests pass as-is apart from the
two join-condition tests overridden in ``TranspiledUDFParityTests``, which
assert restrictions that only apply while a Python UDF survives in the plan. If
a future change makes an inherited test diverge purely due to ANSI semantics,
because transpilation bypasses a Python-side effect, or because lowering removes
a Python-UDF-specific planner restriction (rather than a genuine result change),
override it here with a documented ``unittest.skip`` rather than editing the
inherited test body.
"""

import unittest

from pyspark.sql.tests.test_udf import BaseUDFTestsMixin
from pyspark.sql.tests.test_udf_combinations import UDFCombinationsTestsMixin
from pyspark.sql.tests.test_unified_udf import UnifiedUDFTestsMixin
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import is_remote_only


# Transpilation is gated on both of these being enabled, both at UDF
# construction time (python/pyspark/sql/udf.py) and again in the Catalyst
# optimizer (the ConvertToCatalyst rule).
# spark.conf.set requires strings, so we use "true" rather than Python True here.
_TRANSPILE_CONF = {
    "spark.sql.experimental.optimizer.transpilePyUDFs": "true",
    "spark.sql.ansi.enabled": "true",
}

_NON_CONNECT_ONLY = "UDF transpilation is only supported in regular (non-Connect) Spark."


def _enable_transpilation(cls):
    for key, value in _TRANSPILE_CONF.items():
        cls.spark.conf.set(key, value)


@unittest.skipIf(is_remote_only(), _NON_CONNECT_ONLY)
class TranspiledUDFParityTests(BaseUDFTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")
        _enable_transpilation(cls)

    # Both of these assert restrictions that exist BECAUSE a Python UDF is
    # opaque to the planner: a UDF spanning both sides of a join has to be
    # pulled out as Filter + Cross join (so a cartesian-product error is
    # expected), and a Python UDF in the ON clause of a non-inner join is
    # rejected outright. Their UDFs are inline lambdas, which SPARK-58650 now
    # identifies and lowers, so no Python UDF survives and neither restriction
    # applies.
    #
    # Be precise about what that costs, because it is not purely a plan-shape
    # change. With ``spark.sql.crossJoin.enabled=false``, a UDF that was the SOLE
    # join condition used to raise "Detected implicit cartesian product"; lowered,
    # the CASE WHEN references both sides so CheckCartesianProducts no longer
    # flags it, yet no equi-join key is extractable, so the query still executes
    # as an O(n*m) CartesianProduct -- the guard is defeated rather than
    # satisfied. That is a real consequence of lowering, tracked separately; it
    # is not something these skips can assert either way.
    @unittest.skip(
        "Transpilation lowers the inline lambda to a native join condition, so the "
        "implicit-cartesian-product error this asserts no longer occurs (SPARK-58650). "
        "NOTE the query still runs as a CartesianProduct -- the guard stops firing "
        "rather than the cross join going away."
    )
    def test_udf_in_join_condition(self):
        pass

    @unittest.skip(
        "Transpilation lowers the inline lambda to a native ON condition, so the "
        "PYTHON_UDF_IN_ON_CLAUSE error this asserts no longer occurs (SPARK-58650). "
        "Replaced by test_transpiled_udf_join_condition_matches_python and "
        "test_non_lowerable_udf_still_refused_in_on_clause."
    )
    def test_udf_not_supported_in_join_condition(self):
        pass

    def test_transpiled_udf_join_condition_matches_python(self):
        """Positive replacement for the two skips above.

        The contract a transpiled UDF must meet is that it agrees with the
        INTERPRETED Python UDF -- not with the equivalent native SQL predicate.
        The distinction is invisible on null-free keys and decisive with a NULL:
        Python's ``None == None`` is True, so ``lambda a, b: a == b`` matches a
        NULL pair, while SQL's ``a = b`` yields NULL and matches nothing. The
        fixture therefore includes a NULL key.

        For an INNER join the interpreted UDF is legal (cross join plus filter),
        so it is used directly as the reference. For non-inner joins Spark
        REFUSES a Python UDF in the ON clause, so no interpreted baseline exists
        for the shape this change newly makes reachable -- the expected rows are
        pinned explicitly instead, and they deliberately differ from the native
        predicate for the NULL row.
        """
        from pyspark.sql import Row
        from pyspark.sql.functions import udf
        from pyspark.sql.types import BooleanType
        from pyspark.testing.utils import assertDataFrameEqual

        left = self.spark.createDataFrame([Row(a=1, a1=1), Row(a=None, a1=5)])
        right = self.spark.createDataFrame([Row(b=1, b1=1), Row(b=None, b1=5)])
        eq = udf(lambda a, b: a == b, BooleanType())
        cond = [eq("a", "b"), left.a1 == right.b1]

        # INNER: compare against the interpreted UDF, the actual contract.
        with self.subTest(how="inner", reference="interpreted"):
            lowered = left.join(right, cond, "inner")
            self.assertNotIn("EvalPython", lowered._jdf.queryExecution().executedPlan().toString())
            lowered_rows = lowered.collect()
            with self.sql_conf({"spark.sql.experimental.optimizer.transpilePyUDFs": False}):
                interpreted = left.join(
                    right,
                    [udf(lambda a, b: a == b, BooleanType())("a", "b"), left.a1 == right.b1],
                    "inner",
                )
                self.assertIn(
                    "EvalPython", interpreted._jdf.queryExecution().executedPlan().toString()
                )
                assertDataFrameEqual(lowered_rows, interpreted.collect())
            # Python semantics: the NULL pair matches, which SQL's `a = b` never
            # would. Pinned so a future change to null handling is visible.
            assertDataFrameEqual(
                lowered_rows, [Row(a=1, a1=1, b=1, b1=1), Row(a=None, a1=5, b=None, b1=5)]
            )

        # Non-inner: no interpreted baseline exists (Python UDFs are refused in
        # a non-inner ON clause), so pin the rows Python semantics produce.
        expected = {
            "leftouter": [Row(a=1, a1=1, b=1, b1=1), Row(a=None, a1=5, b=None, b1=5)],
            "rightouter": [Row(a=1, a1=1, b=1, b1=1), Row(a=None, a1=5, b=None, b1=5)],
            "fullouter": [Row(a=1, a1=1, b=1, b1=1), Row(a=None, a1=5, b=None, b1=5)],
            "leftsemi": [Row(a=1, a1=1), Row(a=None, a1=5)],
            "leftanti": [],
        }
        for how, want in expected.items():
            with self.subTest(how=how, reference="pinned-python-semantics"):
                lowered = left.join(right, cond, how)
                self.assertNotIn(
                    "EvalPython", lowered._jdf.queryExecution().executedPlan().toString()
                )
                assertDataFrameEqual(lowered, want)

    def test_non_lowerable_udf_still_refused_in_on_clause(self):
        # A UDF that does NOT lower (a closure -- the common fallback case) must
        # still be rejected in a non-inner ON clause under transpilation, exactly
        # as before. Skipping test_udf_not_supported_in_join_condition dropped
        # the only assertion of this; keep it with a deliberately non-lowerable
        # UDF so a planner regression that dropped the check would be caught.
        from pyspark.sql import Row
        from pyspark.errors import AnalysisException
        from pyspark.sql.functions import udf
        from pyspark.sql.types import BooleanType

        left = self.spark.createDataFrame([Row(a=1, a1=1)])
        right = self.spark.createDataFrame([Row(b=1, b1=1)])
        captured = 0
        # Free variable -> closure -> not transpilable, so it stays a Python UDF.
        # The raise itself is the proof it stayed one: a lowered native condition
        # would be legal here and would not raise. Cover every non-inner join the
        # skipped test_udf_not_supported_in_join_condition enumerated, not just
        # one, so a transpile-config planner regression on any of them is caught.
        closure_udf = udf(lambda a, b: a == b or captured > 0, BooleanType())
        for how in ("leftouter", "rightouter", "fullouter", "leftanti", "leftsemi"):
            with self.subTest(how=how):
                with self.assertRaisesRegex(AnalysisException, "Python UDF in the ON clause"):
                    left.join(right, closure_udf("a", "b"), how).collect()


@unittest.skipIf(is_remote_only(), _NON_CONNECT_ONLY)
class TranspiledUDFCombinationsParityTests(UDFCombinationsTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")
        _enable_transpilation(cls)


@unittest.skipIf(is_remote_only(), _NON_CONNECT_ONLY)
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class TranspiledUnifiedUDFParityTests(UnifiedUDFTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()
        _enable_transpilation(cls)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
