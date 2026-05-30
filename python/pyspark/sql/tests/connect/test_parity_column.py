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
        df = self.spark.sql("SELECT 1 AS x")
        rows = df.groupBy().agg(sf.sum("x").alias("c")).select(df["c"]).collect()
        self.assertEqual([r.c for r in rows], [1])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
