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
DataFrame API golden file tests for GROUP BY and aggregate operations.

The cases are modelled on the GROUP BY cases of the SQL golden tests
(``sql/core/src/test/resources/sql-tests/inputs/group-by.sql``).  Their expected
outputs live in ``../results/test_group_by.py.out``; see
``pyspark.sql.tests.df_golden.df_golden`` for the framework and how to
regenerate them.
"""

from decimal import Decimal

from pyspark.sql import Window
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    bool_and,
    bool_or,
    col,
    count,
    count_if,
    every,
    first,
    kurtosis,
    lit,
    max,
    min,
    round,
    skewness,
    some,
    stddev,
    struct,
    sum,
    variance,
    when,
)
from pyspark.sql.tests.df_golden.df_golden import DFGoldenTestMixin, unordered
from pyspark.testing.connectutils import ReusedConnectTestCase


class GroupByGoldenTests(DFGoldenTestMixin, ReusedConnectTestCase):
    """Test the aggregate operator."""

    @classmethod
    def setup_session(cls, spark):
        spark.sql("""CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
            (1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
            AS testData(a, b)""")
        spark.sql("""CREATE OR REPLACE TEMPORARY VIEW test_agg AS SELECT * FROM VALUES
            (1, true), (1, false),
            (2, true),
            (3, false), (3, null),
            (4, null), (4, null),
            (5, null), (5, true), (5, false) AS test_agg(k, v)""")

    def _test_select_non_grouping_column(self, spark):
        """Select a non-grouping column without a GROUP BY (error)."""
        return spark.table("testData").select(col("a"), count(col("b")))

    def _test_agg_counts_no_grouping(self, spark):
        """Aggregate with empty GroupBy expressions."""
        return spark.table("testData").agg(count(col("a")), count(col("b")))

    @unordered
    def _test_group_by_count(self, spark):
        """Aggregate with non-empty GroupBy expressions."""
        return spark.table("testData").groupBy(col("a")).agg(count(col("b")))

    def _test_group_by_missing_aggregation(self, spark):
        """Non-aggregate column not in GROUP BY (error)."""
        return spark.table("testData").groupBy(col("b")).agg(col("a"), count(col("b")))

    @unordered
    def _test_group_by_multiple_counts(self, spark):
        """Group by with multiple aggregates."""
        return spark.table("testData").groupBy(col("a")).agg(count(col("a")), count(col("b")))

    def _test_group_by_literal(self, spark):
        """Aggregate grouped by literal."""
        return spark.table("testData").groupBy(lit("foo")).agg(count(col("a")))

    def _test_group_by_literal_hash_agg(self, spark):
        """Aggregate grouped by literal (hash aggregate)."""
        return (
            spark.table("testData")
            .filter(col("a") == 0)
            .groupBy(lit("foo"))
            .agg(approx_count_distinct(col("a")))
        )

    def _test_group_by_literal_sort_agg(self, spark):
        """Aggregate grouped by literal (sort aggregate)."""
        return (
            spark.table("testData")
            .filter(col("a") == 0)
            .groupBy(lit("foo"))
            .agg(max(struct(col("a"))))
        )

    @unordered
    def _test_group_by_complex_expr(self, spark):
        """Aggregate with complex GroupBy expression."""
        return spark.table("testData").groupBy(col("a") + col("b")).agg(count(col("b")))

    @unordered
    def _test_group_by_struct(self, spark):
        """struct() in group by."""
        return (
            spark.table("testData")
            .groupBy(struct((col("a") + 0.1).alias("aa")))
            .agg(count(lit(1)))
        )

    def _test_agg_stats_with_nulls(self, spark):
        """Aggregate with nulls."""
        return spark.table("testData").agg(
            round(skewness(col("a")), 12),
            round(kurtosis(col("a")), 12),
            min(col("a")),
            max(col("a")),
            round(avg(col("a")), 12),
            round(variance(col("a")), 12),
            round(stddev(col("a")), 12),
            sum(col("a")),
            count(col("a")),
        )

    def _test_empty_input_group_by(self, spark):
        """Aggregate with empty input and non-empty GroupBy expressions."""
        return spark.table("testData").filter(lit(False)).groupBy(col("a")).agg(count(lit(1)))

    def _test_empty_input_agg(self, spark):
        """Aggregate with empty input and empty GroupBy expressions."""
        return spark.table("testData").filter(lit(False)).agg(count(lit(1)))

    def _test_empty_input_agg_select(self, spark):
        """Aggregate with empty input and constant projection."""
        return spark.table("testData").filter(lit(False)).agg(count(lit(1))).select(lit(1))

    def _test_bool_agg_empty_table(self, spark):
        """Bool aggregates over empty table."""
        return self._bool_aggs(spark.table("test_agg").filter(lit(1) == 0))

    def _test_bool_agg_all_nulls(self, spark):
        """Bool aggregates over all null values."""
        return self._bool_aggs(spark.table("test_agg").filter(col("k") == 4))

    def _test_bool_agg_null_filtering(self, spark):
        """Bool aggregates null filtering."""
        return self._bool_aggs(spark.table("test_agg").filter(col("k") == 5))

    @unordered
    def _test_bool_agg_group_by(self, spark):
        """Bool aggregates with group by."""
        return self._bool_aggs(spark.table("test_agg").groupBy(col("k")))

    @unordered
    def _test_bool_agg_having_false(self, spark):
        """Bool aggregates with having false."""
        return (
            spark.table("test_agg")
            .groupBy(col("k"))
            .agg(every(col("v")).alias("every_v"))
            .filter(col("every_v") == False)  # noqa: E712
            .select(col("k"), col("every_v"))
        )

    def _test_bool_agg_having_null(self, spark):
        """Bool aggregates with having null."""
        return (
            spark.table("test_agg")
            .groupBy(col("k"))
            .agg(every(col("v")).alias("every_v"))
            .filter(col("every_v").isNull())
            .select(col("k"), col("every_v"))
        )

    def _test_every_int_error(self, spark):
        """every() input type checking: int (error)."""
        return spark.table("test_agg").select(every(lit(1)))

    def _test_some_int_error(self, spark):
        """some() input type checking: int (error)."""
        return spark.table("test_agg").select(some(lit(1)))

    def _test_bool_or_int_error(self, spark):
        """bool_or() input type checking: int (error)."""
        return spark.table("test_agg").select(bool_or(lit(1)))

    def _test_every_string(self, spark):
        """every() input type checking: string."""
        return spark.table("test_agg").select(every(lit("true")))

    def _test_bool_and_decimal_error(self, spark):
        """bool_and() input type checking: decimal (error)."""
        return spark.table("test_agg").select(bool_and(lit(Decimal("1.0"))))

    def _test_bool_or_double_error(self, spark):
        """bool_or() input type checking: double (error)."""
        return spark.table("test_agg").select(bool_or(lit(1.0)))

    @unordered
    def _test_every_window(self, spark):
        """every() as window expression."""
        return self._bool_agg_window(spark, every)

    @unordered
    def _test_some_window(self, spark):
        """some() as window expression."""
        return self._bool_agg_window(spark, some)

    @unordered
    def _test_bool_or_window(self, spark):
        """bool_or() as window expression."""
        return self._bool_agg_window(spark, bool_or)

    @unordered
    def _test_bool_and_window(self, spark):
        """bool_and() as window expression."""
        return self._bool_agg_window(spark, bool_and)

    @unordered
    def _test_bool_or_window_repeated(self, spark):
        """bool_or() as window expression (repeated)."""
        return self._bool_agg_window(spark, bool_or)

    def _test_having_agg_count(self, spark):
        """Having referencing aggregate expression."""
        return (
            spark.table("test_agg")
            .agg(count(col("k")).alias("cnt"))
            .filter(col("cnt") > 1)
            .select(col("cnt"))
        )

    @unordered
    def _test_having_max_v(self, spark):
        """Having on aliased max."""
        return (
            spark.table("test_agg")
            .groupBy(col("k"))
            .agg(max(col("v")).alias("max_v"))
            .filter(col("max_v") == True)  # noqa: E712
            .select(col("k"), col("max_v"))
        )

    def _test_agg_alias_filter(self, spark):
        """Aggregate expression referenced through alias."""
        return spark.table("test_agg").agg(count(col("k")).alias("cnt")).filter(col("cnt") > 1)

    @unordered
    def _test_grouping_exprs_not_optimized(self, spark):
        """SPARK-34581: do not optimize out grouping expressions.

        They must survive in aggregate expressions without aggregate function.
        """
        return (
            spark.table("testData")
            .groupBy(col("a").isNull())
            .agg(~col("a").isNull(), count("*").alias("c"))
        )

    @unordered
    def _test_pull_out_grouping_exprs(self, spark):
        """PullOutGroupingExpressions pulls out grouping expressions.

        It pulls them out from inside AggregateExpressions too.
        """
        return (
            spark.table("testData")
            .groupBy(col("a").isNull())
            .agg(when(~col("a").isNull(), 0).otherwise(1), first(col("a").isNull()).alias("c"))
        )

    @unordered
    def _test_count_if_group_by(self, spark):
        """count_if() with grouping expression reference."""
        return (
            spark.table("testData")
            .groupBy(col("a") + 1)
            .agg(count_if((col("a") + 1) == col("b")))
        )

    @staticmethod
    def _bool_aggs(relation):
        """Aggregate ``v`` with every boolean aggregate, over a DataFrame or GroupedData."""
        return relation.agg(
            every(col("v")),
            some(col("v")),
            bool_or(col("v")),
            bool_and(col("v")),
            bool_or(col("v")),
        )

    @staticmethod
    def _bool_agg_window(spark, bool_agg):
        """Apply *bool_agg* to ``v`` as a window expression over ``test_agg``."""
        return spark.table("test_agg").select(
            col("k"), col("v"), bool_agg(col("v")).over(Window.partitionBy("k").orderBy("v"))
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
