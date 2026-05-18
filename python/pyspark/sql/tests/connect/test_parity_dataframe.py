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
import uuid

from pyspark.sql.tests.test_dataframe import DataFrameTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class DataFrameParityTests(DataFrameTestsMixin, ReusedConnectTestCase):
    def test_help_command(self):
        df = self.spark.createDataFrame(data=[{"foo": "bar"}, {"foo": "baz"}])
        super().check_help_command(df)

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_toDF_with_schema_string(self):
        super().test_toDF_with_schema_string()

    @unittest.skip("This test is dedicated for classic mode.")
    def test_query_execution_unsupported_in_classic(self):
        pass

    @unittest.skip("This test is dedicated for classic mode.")
    def test_to_json(self):
        pass

    # --- Connect-only layered DataFrame regression programs ------------------
    #
    # These tests exercise mixed-surface DataFrame pipelines (4-6 chained
    # operators combining filters, joins, aggregations, set ops, window
    # functions, UDFs and temporary views) end-to-end. The intent is to catch
    # regressions in Connect's plan-id propagation through analyzer rules
    # that single-operator tests miss when rules interact.
    #
    # Each program runs under the non-strict mode of
    # `spark.sql.analyzer.strictDataFrameColumnResolution` (the historical
    # Connect contract that customer workflows depend on) and asserts the
    # expected output. Under strict mode the same program either succeeds
    # identically (when no tagged reference crosses a shadowing boundary) or
    # fails at the first divergence.
    #
    # Tests live here intentionally: they must not be moved to Classic-shared
    # suites where they'd be removed as "diverging from Classic" during
    # routine cleanup.

    def test_layered_filter_join_agg_shadow(self):
        from pyspark.sql import functions as sf

        with self.sql_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": "false"}):
            df = self.spark.createDataFrame(
                [(1, 10), (1, 20), (2, 30), (2, 40), (3, 50)], ["c", "v"]
            )
            # filter -> self-join on c -> groupBy/agg shadowing c -> withColumn
            # shadowing c again -> select(df["c"]) which routes through name
            # fallback in lenient mode.
            result = (
                df.filter(df["v"] > 10)
                .alias("a")
                .join(df.alias("b"), sf.col("a.c") == sf.col("b.c"))
                .groupBy(sf.col("a.c").alias("c"))
                .agg(sf.sum(sf.col("b.v")).alias("s"))
                .withColumn("c", sf.col("c").cast("string"))
                .select(df["c"], "s")
                .collect()
            )
            self.assertEqual(
                sorted((r.c, r.s) for r in result),
                [("1", 60), ("2", 140)],
            )

    def test_layered_temp_view_subquery_udf(self):
        from pyspark.sql import functions as sf
        from pyspark.sql.types import IntegerType

        with self.sql_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": "false"}):
            view_name = f"layered_view_{uuid.uuid4().hex}"
            df = self.spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["c", "v"])
            df.createOrReplaceTempView(view_name)
            try:
                # createOrReplaceTempView -> SQL with subquery referencing the
                # view -> join back to the original DataFrame -> apply a UDF
                # over a shadowed column -> select via the tagged df["c"]
                # (resolved by name fallback since the view+SQL path emits new
                # attribute ids).
                double_udf = sf.udf(lambda x: x * 2 if x is not None else None, IntegerType())
                sub = self.spark.sql(
                    f"SELECT c, v FROM {view_name} WHERE v IN (SELECT v FROM {view_name} WHERE c > 1)"
                )
                result = (
                    sub.join(df, sub["c"] == df["c"])
                    .withColumn("v", double_udf(sub["v"]))
                    .select(df["c"], "v")
                    .collect()
                )
                self.assertEqual(
                    sorted((r.c, r.v) for r in result),
                    [(2, 40), (3, 60)],
                )
            finally:
                self.spark.sql(f"DROP VIEW IF EXISTS {view_name}")

    def test_layered_union_window_pivot_shadow(self):
        from pyspark.sql import functions as sf
        from pyspark.sql.window import Window

        with self.sql_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": "false"}):
            df1 = self.spark.createDataFrame([(1, "a", 10), (1, "b", 20)], ["c", "k", "v"])
            df2 = self.spark.createDataFrame([(2, "a", 30), (2, "b", 40)], ["c", "k", "v"])
            unioned = df1.unionByName(df2)
            # union -> window aggregation -> pivot -> withColumn shadow ->
            # select via original df1["c"] tagged reference.
            w = Window.partitionBy("c")
            result = (
                unioned.withColumn("rank_v", sf.row_number().over(w.orderBy("v")))
                .groupBy("c")
                .pivot("k", ["a", "b"])
                .sum("v")
                .withColumn("c", sf.col("c").cast("string"))
                .select(df1["c"], "a", "b")
                .collect()
            )
            self.assertEqual(
                sorted((r.c, r.a, r.b) for r in result),
                [("1", 10, 20), ("2", 30, 40)],
            )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
