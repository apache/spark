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

import unittest

from pyspark.errors import AnalysisException
from pyspark.sql import functions as sf
from pyspark.sql.tests.test_column import ColumnTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class ColumnParityTests(ColumnTestsMixin, ReusedConnectTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.analyzer.strictDataFrameColumnResolution", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.analyzer.strictDataFrameColumnResolution")
        finally:
            super().tearDownClass()

    @unittest.skip("Requires JVM access.")
    def test_validate_column_types(self):
        super().test_validate_column_types()

    def test_resolve_after_union(self):
        # Connect diverges from Classic here: Union is treated as a leaf when
        # walking the plan tree for plan-id resolution, so the left-side plan
        # id is never found and CANNOT_RESOLVE_DATAFRAME_COLUMN is thrown
        # before any name-based fallback - in both strict and lenient modes.
        df1 = self.spark.sql("SELECT 1 AS c")
        df2 = self.spark.sql("SELECT 2 AS c")
        with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
            df1.union(df2).select(df1.c).collect()

    # zip merges the two column-projected sides into a single plan, so the
    # per-DataFrame plan-id tags do not survive ResolveZip. A tagged left/right
    # reference can no longer be found and raises in both strict and lenient
    # modes - the throw precedes any name-based fallback - unlike Classic, which
    # resolves by the attribute id that ResolveZip preserves. A base-side
    # reference (df.zip(right).select(df.a)) raises too when the base's tagged
    # plan root is a Project that the rewrite dissolves (createDataFrame,
    # overridden below); only a bare relation root like range survives as the
    # merged base and resolves (test_resolve_after_zip_bare_base_side,
    # inherited with no override).

    def test_resolve_after_zip(self):
        df = self.spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["a", "b"])
        left = df.select((df.a + 1).alias("x"))
        right = df.select((df.b * 2).alias("y"))
        with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
            left.zip(right).select(left.x).collect()

    def test_resolve_after_zip_reordered(self):
        df = self.spark.createDataFrame([(1, 10), (2, 20)], ["a", "b"])
        left = df.select(df.a.alias("x"))
        right = df.select(df.b.alias("y"))
        with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
            left.zip(right).select(right.y, left.x).collect()

    def test_resolve_after_zip_duplicate_names(self):
        df = self.spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        left = df.select(df.a.alias("v"))
        right = df.select(df.b.alias("v"))
        with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
            left.zip(right).select(left.v).collect()

    def test_resolve_after_zip_shared_producer(self):
        df = self.spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        left = df.select(df.a.alias("v"))
        right = df.select(df.a.alias("v"))
        with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
            left.zip(right).select(right.v).collect()

    def test_resolve_after_zip_in_expression(self):
        df = self.spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["a", "b"])
        left = df.select(df.a.alias("x"))
        right = df.select(df.b.alias("y"))
        with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
            left.zip(right).select((left.x + right.y).alias("s")).collect()

    def test_resolve_after_zip_in_filter(self):
        df = self.spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["a", "b"])
        left = df.select(df.a.alias("x"))
        right = df.select(df.b.alias("y"))
        with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
            left.zip(right).filter(left.x >= 2).collect()

    def test_resolve_after_zip_chained(self):
        df = self.spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
        left0 = df.select("a", "b")
        left = left0.select((left0.a + 1).alias("x"))
        right0 = df.select("b", "c")
        right = right0.select((right0.c * 2).alias("y"))
        with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
            left.zip(right).select(left.x, right.y).collect()

    def test_resolve_after_zip_base_side(self):
        df = self.spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["a", "b"])
        right = df.select((df.b * 2).alias("y"))
        with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
            df.zip(right).select(df.a).collect()

    def test_df_col_resolution_mode(self):
        self.assertEqual(
            self.spark.conf.get("spark.sql.analyzer.strictDataFrameColumnResolution"),
            "true",
        )


class ColumnParityTestsWithNonStrictDFColResolution(ColumnParityTests):
    """Re-run the Column parity tests with
    `spark.sql.analyzer.strictDataFrameColumnResolution=false` to exercise the
    name-based fallback path for tagged UnresolvedAttributes."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.analyzer.strictDataFrameColumnResolution", "false")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.analyzer.strictDataFrameColumnResolution")
        finally:
            super().tearDownClass()

    def test_df_col_resolution_mode(self):
        self.assertEqual(
            self.spark.conf.get("spark.sql.analyzer.strictDataFrameColumnResolution"),
            "false",
        )

    # The shadowing trio diverges in lenient mode: where Classic and Connect
    # strict raise, lenient resolves the tagged reference by name against the
    # current (shadowed) output.

    def test_resolve_after_chained_withcolumn_shadow(self):
        df = self.spark.sql("SELECT 1 AS c")
        rows = (
            df.withColumn("c", sf.col("c").cast("string"))
            .withColumn("c", sf.col("c").cast("int"))
            .select(df.c)
            .collect()
        )
        self.assertEqual([r.c for r in rows], [1])

    def test_resolve_after_select_alias_shadow(self):
        df = self.spark.sql("SELECT 1 AS c")
        rows = df.select(df.c.cast("string").alias("c")).select(df.c).collect()
        self.assertEqual([r.c for r in rows], ["1"])

    def test_resolve_after_agg_alias_shadow(self):
        df = self.spark.sql("SELECT 1 AS c")
        rows = df.groupBy().agg(sf.sum("c").alias("c")).select(df.c).collect()
        self.assertEqual([r.c for r in rows], [1])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
