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

import datetime
import unittest
from typing import Iterator

from pyspark.sql import Row
from pyspark.sql.functions import arrow_udf, col, pandas_udf, udf
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    assertDataFrameEqual,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,  # type: ignore[arg-type]
)
class ArrowPythonUDFCachedInputTests(ReusedSQLTestCase):
    """
    Arrow-optimized Python UDFs reading columnar input from an Arrow-cached relation.

    The Arrow-cached scan is a columnar Arrow-backed source, so it drives the columnar Arrow
    Python runner (ColumnarArrowPythonWithNamedArgumentRunner) end to end -- the path an
    Arrow-backed DSv2 source would also take. This pins the init-message protocol between that
    runner and the worker: a mismatched section there desyncs the worker's init parse and hangs
    the task (SPARK-58241). It also pins the columnar fast path's shape gate: the cached vectors
    are forwarded verbatim only when their physical shape matches what the stream's schema
    declares, and fall back to row-based re-encoding otherwise.

    This suite runs in its own module because spark.sql.cache.serializer is a static conf latched
    into a JVM-wide singleton on first cache materialization.
    """

    @classmethod
    def conf(cls):
        conf = super().conf()
        # Static conf: must be set before the session is created.
        conf.set(
            "spark.sql.cache.serializer",
            "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer",
        )
        return conf

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")
        # The columnar input path, which this suite tests, is disabled by default.
        cls.spark.conf.set("spark.sql.execution.arrow.pythonUDF.columnarInput.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled")
            cls.spark.conf.unset("spark.sql.execution.arrow.pythonUDF.columnarInput.enabled")
        finally:
            super().tearDownClass()

    def _assert_columnar_arrow_eval(self, df, expected_nodes, pass_through_columns=None):
        # Assert the executed plan contains exactly `expected_nodes` ArrowEvalPythonExec
        # operators and every one of them takes the columnar input path -- otherwise the
        # test silently stops exercising the code this suite exists to pin. If
        # `pass_through_columns` is given, also assert that every one of them passes through
        # the child columns of those names.
        nodes = []
        stack = [df._jdf.queryExecution().executedPlan()]
        while stack:
            node = stack.pop()
            name = node.getClass().getSimpleName()
            if name == "AdaptiveSparkPlanExec":
                stack.append(node.executedPlan())
                continue
            if name == "ArrowEvalPythonExec":
                nodes.append(node)
            children = node.children()
            for i in range(children.size()):
                stack.append(children.apply(i))
        self.assertEqual(len(nodes), expected_nodes, "unexpected number of ArrowEvalPythonExec")
        for node in nodes:
            self.assertTrue(node.supportsColumnar(), "expected the columnar input path")
            if pass_through_columns is not None:
                required = node.requiredChildOutput()
                names = [required.apply(i).name() for i in range(required.size())]
                self.assertEqual(names, pass_through_columns, "unexpected pass-through columns")

    def _assert_udf_on_cached_strings(self):
        df = self.spark.createDataFrame(
            [("hello",), (None,), ("world",)], schema="v string"
        ).cache()
        try:
            result = df.select(udf(lambda x: x.upper() if x is not None else None)(col("v")))
            self._assert_columnar_arrow_eval(result, expected_nodes=1)
            assertDataFrameEqual(result, [Row("HELLO"), Row(None), Row("WORLD")])
        finally:
            df.unpersist()

    def test_udf_on_cached_strings(self):
        # Cache-scan vectors use standard 32-bit var-width offsets, matching the default
        # interchange schema: the columnar fast path forwards them verbatim.
        self._assert_udf_on_cached_strings()

    def test_udf_on_cached_strings_with_large_var_types(self):
        # With useLargeVarTypes=true the UDF stream schema declares LargeUtf8 (64-bit offsets)
        # but the cache always stores standard Utf8, so forwarding the cache-scan vector
        # verbatim would produce a stream whose header and body disagree. The shape gate must
        # route this through the row-based re-encoding fallback and still produce correct
        # results.
        with self.sql_conf({"spark.sql.execution.arrow.useLargeVarTypes": "true"}):
            self._assert_udf_on_cached_strings()

    def test_udf_on_cached_numeric_column_alongside_unselected_string(self):
        # Only the UDF's input columns are serialized to the worker; the column that is not a
        # UDF input rides through the pass-through recombination, so select it alongside the
        # UDF result to observe that its values line up with the corresponding rows.
        df = self.spark.createDataFrame([(1, "a"), (2, "b")], schema="i long, s string").cache()
        try:
            result = df.select(col("s"), udf(lambda x: x + 1, "long")(col("i")))
            self._assert_columnar_arrow_eval(result, expected_nodes=1, pass_through_columns=["s"])
            assertDataFrameEqual(result, [Row("a", 2), Row("b", 3)])
        finally:
            df.unpersist()

    def test_udf_on_cached_input_with_output_batch_slicing(self):
        # The 10-row result of the single cached batch is sliced under the configs below, and
        # must still line up with its rows, which are paired with the results by row.
        df = self.spark.range(0, 10, 1, 1).cache()
        try:
            for conf in [
                {"spark.sql.execution.arrow.maxRecordsPerOutputBatch": "3"},
                {"spark.sql.execution.arrow.maxBytesPerOutputBatch": "3"},
            ]:
                with self.subTest(conf=conf), self.sql_conf(conf):
                    result = df.select(col("id"), udf(lambda x: x + 1, "long")(col("id")))
                    self._assert_columnar_arrow_eval(result, expected_nodes=1)
                    assertDataFrameEqual(result, [Row(i, i + 1) for i in range(10)])
        finally:
            df.unpersist()

    def test_udf_on_cached_map_column(self):
        # Canonical map vectors from the cache pass the gate (their entry-struct children carry
        # the canonical "key"/"value" names in order) and ride the columnar path verbatim. The
        # rejection of a source whose entry children are swapped is pinned at the unit level
        # (ArrowUtilsSuite), since no in-tree producer can build such a vector.
        df = self.spark.createDataFrame(
            [({"a": 1},), ({"b": 2, "c": 3},)], schema="m map<string,long>"
        ).cache()
        try:
            result = df.select(udf(lambda m: len(m), "long")(col("m")))
            self._assert_columnar_arrow_eval(result, expected_nodes=1)
            assertDataFrameEqual(result, [Row(1), Row(2)])
        finally:
            df.unpersist()

    def test_udf_on_timestamps_cached_under_different_time_zone(self):
        # Pins the timestamp pass-through end to end: the plan takes the columnar path and
        # the stored epoch round-trips unchanged across a session-timezone change between
        # cache materialization and the UDF query. Note the cache rebuilds its Arrow schema
        # at read time with the query's session timezone, so the actual and declared timezone
        # labels the gate compares are equal by construction here; the layout-equal
        # label-mismatch case (e.g. an external DSv2 Arrow source labeling UTC while the
        # stream schema declares the session timezone) has no in-tree columnar producer and
        # is pinned by the ArrowUtilsSuite congruence tests instead.
        ts = datetime.datetime(2025, 1, 6, 12, 30, 45)
        with self.sql_conf({"spark.sql.session.timeZone": "UTC"}):
            df = self.spark.createDataFrame([(ts,)], schema="ts timestamp").cache()
            df.count()  # materialize: the cached vectors are labeled with the UTC session tz
        try:
            with self.sql_conf({"spark.sql.session.timeZone": "America/Los_Angeles"}):
                result = df.select(udf(lambda t: t, "timestamp")(col("ts")))
                self._assert_columnar_arrow_eval(result, expected_nodes=1)
                # The stored epoch round-trips; naive-datetime wall time is preserved because
                # createDataFrame and collect both convert against the machine-local timezone.
                assertDataFrameEqual(result, [Row(ts)])
        finally:
            df.unpersist()

    def test_udfs_without_pass_through_columns(self):
        # Only the results are selected, so no column is passed through, and the result batches
        # of Python are the output batches, including those of iterator UDFs, which may return
        # batches of other sizes than their input.
        import pandas as pd
        import pyarrow as pa
        import pyarrow.compute as pc

        @pandas_udf("long")
        def pandas_plus_one(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for s in it:
                yield s + 1

        @arrow_udf("long")
        def arrow_plus_one(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
            for a in it:
                # Splits each batch in two.
                half = len(a) // 2
                yield pc.add(a[:half], 1)
                yield pc.add(a[half:], 1)

        @pandas_udf("long")
        def pandas_times_two(s: pd.Series) -> pd.Series:
            return s * 2

        num_rows = 10000
        df = self.spark.range(0, num_rows, 1, 2).selectExpr("id", "id * 10 AS v").cache()
        cases = [
            ("pandas_iter", pandas_plus_one, lambda i: i + 1),
            ("arrow_iter", arrow_plus_one, lambda i: i + 1),
            ("pandas", pandas_times_two, lambda i: i * 2),
            ("udf", udf(lambda x: x + 1, "long"), lambda i: i + 1),
        ]
        try:
            with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": "1000"}):
                for name, f, g in cases:
                    with self.subTest(udf=name):
                        expected = [Row(r=g(i)) for i in range(num_rows)]
                        result = df.select(f(col("id")).alias("r"))
                        self._assert_columnar_arrow_eval(
                            result, expected_nodes=1, pass_through_columns=[]
                        )
                        assertDataFrameEqual(result, expected)
                        # With a filter on the result above.
                        filtered = result.where(col("r") % 7 == 0)
                        assertDataFrameEqual(filtered, [r for r in expected if r.r % 7 == 0])
        finally:
            df.unpersist()

    def test_stacked_udfs_without_pass_through_columns(self):
        # The outer UDF reads the result batches of the inner one, which are loaded into the
        # same vectors for every batch, and neither passes through a column.
        import pandas as pd

        @pandas_udf("long")
        def pandas_times_two(s: pd.Series) -> pd.Series:
            return s * 2

        plus_one = udf(lambda x: x + 1, "long")
        df = self.spark.range(0, 10000, 1, 2).cache()
        try:
            with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": "1000"}):
                result = df.select(pandas_times_two(plus_one(col("id"))).alias("r"))
                self._assert_columnar_arrow_eval(result, expected_nodes=2, pass_through_columns=[])
                assertDataFrameEqual(result, [Row(r=(i + 1) * 2) for i in range(10000)])
        finally:
            df.unpersist()

    def test_cache_results_of_udf_without_pass_through_columns(self):
        # The results of Python are the output batches, which are cached by copying them.
        df = self.spark.range(0, 10000, 1, 2).cache()
        try:
            result = df.select(udf(lambda x: x + 1, "long")(col("id")).alias("r"))
            self._assert_columnar_arrow_eval(result, expected_nodes=1, pass_through_columns=[])
            cached = result.cache()
            try:
                self.assertEqual(cached.count(), 10000)
                assertDataFrameEqual(cached, [Row(r=i + 1) for i in range(10000)])
            finally:
                cached.unpersist()
        finally:
            df.unpersist()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
