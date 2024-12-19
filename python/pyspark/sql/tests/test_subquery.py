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

from pyspark.errors import AnalysisException, QueryContextType, SparkRuntimeException
from pyspark.sql import functions as sf
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import ReusedSQLTestCase


class SubqueryTestsMixin:
    @property
    def df1(self):
        return self.spark.createDataFrame(
            [
                (1, 1.0),
                (1, 2.0),
                (2, 1.0),
                (2, 2.0),
                (3, 3.0),
                (None, None),
                (None, 5.0),
                (6, None),
            ],
            ["a", "b"],
        )

    @property
    def df2(self):
        return self.spark.createDataFrame(
            [(2, 3.0), (2, 3.0), (3, 2.0), (4, 1.0), (None, None), (None, 5.0), (6, None)],
            ["c", "d"],
        )

    def test_noop_outer(self):
        assertDataFrameEqual(
            self.spark.range(1).select(sf.col("id").outer()),
            self.spark.range(1).select(sf.col("id")),
        )

        with self.assertRaises(AnalysisException) as pe:
            self.spark.range(1).select(sf.col("outer_col").outer()).collect()

        self.check_error(
            exception=pe.exception,
            errorClass="UNRESOLVED_COLUMN.WITH_SUGGESTION",
            messageParameters={"objectName": "`outer_col`", "proposal": "`id`"},
            query_context_type=QueryContextType.DataFrame,
            fragment="col",
        )

    def test_simple_uncorrelated_scalar_subquery(self):
        assertDataFrameEqual(
            self.spark.range(1).select(self.spark.range(1).select(sf.lit(1)).scalar().alias("b")),
            self.spark.sql("""select (select 1 as b) as b"""),
        )

        assertDataFrameEqual(
            self.spark.range(1).select(
                self.spark.range(1)
                .select(self.spark.range(1).select(sf.lit(1)).scalar() + 1)
                .scalar()
                + 1
            ),
            self.spark.sql("""select (select (select 1) + 1) + 1"""),
        )

        # string type
        assertDataFrameEqual(
            self.spark.range(1).select(self.spark.range(1).select(sf.lit("s")).scalar().alias("b")),
            self.spark.sql("""select (select 's' as s) as b"""),
        )

        # 0 rows
        assertDataFrameEqual(
            self.spark.range(1).select(
                self.spark.range(1).select(sf.lit("s")).limit(0).scalar().alias("b")
            ),
            self.spark.sql("""select (select 's' as s limit 0) as b"""),
        )

    def test_uncorrelated_scalar_subquery_with_view(self):
        with self.tempView("subqueryData"):
            df = self.spark.createDataFrame(
                [(1, "one"), (2, "two"), (3, "three")], ["key", "value"]
            )
            df.createOrReplaceTempView("subqueryData")

            assertDataFrameEqual(
                self.spark.range(1).select(
                    self.spark.table("subqueryData")
                    .select("key")
                    .where(sf.col("key") > 2)
                    .orderBy("key")
                    .limit(1)
                    .scalar()
                    + 1
                ),
                self.spark.sql(
                    """
                    select (select key from subqueryData where key > 2 order by key limit 1) + 1
                    """
                ),
            )

            assertDataFrameEqual(
                self.spark.range(1).select(
                    (-self.spark.table("subqueryData").select(sf.max("key")).scalar()).alias(
                        "negative_max_key"
                    )
                ),
                self.spark.sql(
                    """select -(select max(key) from subqueryData) as negative_max_key"""
                ),
            )

            assertDataFrameEqual(
                self.spark.range(1).select(
                    self.spark.table("subqueryData").select("value").limit(0).scalar()
                ),
                self.spark.sql("""select (select value from subqueryData limit 0)"""),
            )

            assertDataFrameEqual(
                self.spark.range(1).select(
                    self.spark.table("subqueryData")
                    .where(
                        sf.col("key")
                        == self.spark.table("subqueryData").select(sf.max("key")).scalar() - 1
                    )
                    .select(sf.min("value"))
                    .scalar()
                ),
                self.spark.sql(
                    """
                    select (
                        select min(value) from subqueryData
                        where key = (select max(key) from subqueryData) - 1
                    )
                    """
                ),
            )

    def test_scalar_subquery_against_local_relations(self):
        with self.tempView("t1", "t2"):
            self.spark.createDataFrame([(1, 1), (2, 2)], ["c1", "c2"]).createOrReplaceTempView("t1")
            self.spark.createDataFrame([(1, 1), (2, 2)], ["c1", "c2"]).createOrReplaceTempView("t2")

            assertDataFrameEqual(
                self.spark.table("t1").select(
                    self.spark.range(1).select(sf.lit(1).alias("col")).scalar()
                ),
                self.spark.sql("""SELECT (select 1 as col) from t1"""),
            )

            assertDataFrameEqual(
                self.spark.table("t1").select(self.spark.table("t2").select(sf.max("c1")).scalar()),
                self.spark.sql("""SELECT (select max(c1) from t2) from t1"""),
            )

            assertDataFrameEqual(
                self.spark.table("t1").select(
                    sf.lit(1) + self.spark.range(1).select(sf.lit(1).alias("col")).scalar()
                ),
                self.spark.sql("""SELECT 1 + (select 1 as col) from t1"""),
            )

            assertDataFrameEqual(
                self.spark.table("t1").select(
                    "c1", self.spark.table("t2").select(sf.max("c1")).scalar() + sf.col("c2")
                ),
                self.spark.sql("""SELECT c1, (select max(c1) from t2) + c2 from t1"""),
            )

            assertDataFrameEqual(
                self.spark.table("t1").select(
                    "c1",
                    (
                        self.spark.table("t2")
                        .where(sf.col("t1.c2").outer() == sf.col("t2.c2"))
                        .select(sf.max("c1"))
                        .scalar()
                    ),
                ),
                self.spark.sql(
                    """SELECT c1, (select max(c1) from t2 where t1.c2 = t2.c2) from t1"""
                ),
            )

    def test_correlated_scalar_subquery(self):
        with self.tempView("l", "r"):
            self.df1.createOrReplaceTempView("l")
            self.df2.createOrReplaceTempView("r")

            with self.subTest("in where"):
                for cond in [
                    sf.col("a").outer() == sf.col("c"),
                    (sf.col("a") == sf.col("c")).outer(),
                    sf.expr("a = c").outer(),
                ]:
                    with self.subTest(cond=cond):
                        assertDataFrameEqual(
                            self.spark.table("l").where(
                                sf.col("b")
                                < self.spark.table("r").where(cond).select(sf.max("d")).scalar()
                            ),
                            self.spark.sql(
                                """select * from l where b < (select max(d) from r where a = c)"""
                            ),
                        )

            with self.subTest("in select"):
                df1 = self.spark.table("l").alias("t1")
                df2 = self.spark.table("l").alias("t2")

                for cond in [
                    sf.col("t1.a") == sf.col("t2.a").outer(),
                    (sf.col("t1.a") == sf.col("t2.a")).outer(),
                    sf.expr("t1.a = t2.a").outer(),
                ]:
                    with self.subTest(cond=cond):
                        assertDataFrameEqual(
                            df1.select(
                                "a",
                                df2.where(cond).select(sf.sum("b")).scalar().alias("sum_b"),
                            ),
                            self.spark.sql(
                                """
                                select
                                    a, (select sum(b) from l t2 where t2.a = t1.a) sum_b
                                from l t1
                                """
                            ),
                        )

            with self.subTest("without .outer()"):
                assertDataFrameEqual(
                    self.spark.table("l").select(
                        "a",
                        (
                            self.spark.table("r")
                            .where(sf.col("b") == sf.col("a").outer())
                            .select(sf.sum("d"))
                            .scalar()
                            .alias("sum_d")
                        ),
                    ),
                    self.spark.sql(
                        """select a, (select sum(d) from r where b = l.a) sum_d from l"""
                    ),
                )

            with self.subTest("in select (null safe)"):
                df1 = self.spark.table("l").alias("t1")
                df2 = self.spark.table("l").alias("t2")

                assertDataFrameEqual(
                    df1.select(
                        "a",
                        (
                            df2.where(sf.col("t2.a").eqNullSafe(sf.col("t1.a").outer()))
                            .select(sf.sum("b"))
                            .scalar()
                            .alias("sum_b")
                        ),
                    ),
                    self.spark.sql(
                        """
                        select a, (select sum(b) from l l2 where l2.a <=> l1.a) sum_b from l l1
                        """
                    ),
                )

            with self.subTest("in aggregate"):
                assertDataFrameEqual(
                    self.spark.table("l")
                    .groupBy(
                        "a",
                        (
                            self.spark.table("r")
                            .where(sf.col("a").outer() == sf.col("c"))
                            .select(sf.sum("d"))
                            .scalar()
                            .alias("sum_d")
                        ),
                    )
                    .agg({}),
                    self.spark.sql(
                        """
                        select a, (select sum(d) from r where a = c) sum_d from l l1 group by 1, 2
                        """
                    ),
                )

            with self.subTest("non-aggregated"):
                df1 = self.spark.table("l").alias("t1")
                df2 = self.spark.table("l").alias("t2")

                with self.assertRaises(SparkRuntimeException) as pe:
                    df1.select(
                        "a",
                        df2.where(sf.col("t1.a") == sf.col("t2.a").outer()).select("b").scalar(),
                    ).collect()

                self.check_error(
                    exception=pe.exception,
                    errorClass="SCALAR_SUBQUERY_TOO_MANY_ROWS",
                    messageParameters={},
                )

            with self.subTest("non-equal"):
                df1 = self.spark.table("l").alias("t1")
                df2 = self.spark.table("l").alias("t2")

                assertDataFrameEqual(
                    df1.select(
                        "a",
                        (
                            df2.where(sf.col("t2.a") < sf.col("t1.a").outer())
                            .select(sf.sum("b"))
                            .scalar()
                            .alias("sum_b")
                        ),
                    ),
                    self.spark.sql(
                        """select a, (select sum(b) from l t2 where t2.a < t1.a) sum_b from l t1"""
                    ),
                )

            with self.subTest("disjunctive"):
                assertDataFrameEqual(
                    self.spark.table("l")
                    .where(
                        self.spark.table("r")
                        .where(
                            ((sf.col("a").outer() == sf.col("c")) & (sf.col("d") == sf.lit(2.0)))
                            | ((sf.col("a").outer() == sf.col("c")) & (sf.col("d") == sf.lit(1.0)))
                        )
                        .select(sf.count(sf.lit(1)))
                        .scalar()
                        > 0
                    )
                    .select("a"),
                    self.spark.sql(
                        """
                        select a
                        from   l
                        where  (select count(*)
                                from   r
                                where (a = c and d = 2.0) or (a = c and d = 1.0)) > 0
                        """
                    ),
                )

    def test_exists_subquery(self):
        with self.tempView("l", "r"):
            self.df1.createOrReplaceTempView("l")
            self.df2.createOrReplaceTempView("r")

            with self.subTest("EXISTS"):
                for cond in [
                    sf.col("a").outer() == sf.col("c"),
                    (sf.col("a") == sf.col("c")).outer(),
                    sf.expr("a = c").outer(),
                ]:
                    with self.subTest(cond=cond):
                        assertDataFrameEqual(
                            self.spark.table("l").where(self.spark.table("r").where(cond).exists()),
                            self.spark.sql(
                                """select * from l where exists (select * from r where l.a = r.c)"""
                            ),
                        )

                        assertDataFrameEqual(
                            self.spark.table("l").where(
                                self.spark.table("r").where(cond).exists()
                                & (sf.col("a") <= sf.lit(2))
                            ),
                            self.spark.sql(
                                """
                        select * from l where exists (select * from r where l.a = r.c) and l.a <= 2
                        """
                            ),
                        )

            with self.subTest("NOT EXISTS"):
                assertDataFrameEqual(
                    self.spark.table("l").where(
                        ~self.spark.table("r").where(sf.col("a").outer() == sf.col("c")).exists()
                    ),
                    self.spark.sql(
                        """select * from l where not exists (select * from r where l.a = r.c)"""
                    ),
                )

                assertDataFrameEqual(
                    self.spark.table("l").where(
                        ~(
                            self.spark.table("r")
                            .where(
                                (sf.col("a").outer() == sf.col("c"))
                                & (sf.col("b").outer() < sf.col("d"))
                            )
                            .exists()
                        )
                    ),
                    self.spark.sql(
                        """
                        select * from l
                            where not exists (select * from r where l.a = r.c and l.b < r.d)
                        """
                    ),
                )

            with self.subTest("EXISTS within OR"):
                assertDataFrameEqual(
                    self.spark.table("l").where(
                        self.spark.table("r").where(sf.col("a").outer() == sf.col("c")).exists()
                        | self.spark.table("r").where(sf.col("a").outer() == sf.col("c")).exists()
                    ),
                    self.spark.sql(
                        """
                        select * from l where exists (select * from r where l.a = r.c)
                            or exists (select * from r where l.a = r.c)
                        """
                    ),
                )

                assertDataFrameEqual(
                    self.spark.table("l").where(
                        self.spark.table("r")
                        .where(
                            (sf.col("a").outer() == sf.col("c"))
                            & (sf.col("b").outer() < sf.col("d"))
                        )
                        .exists()
                        | self.spark.table("r").where(sf.col("a").outer() == sf.col("c")).exists()
                    ),
                    self.spark.sql(
                        """
                        select * from l where exists (select * from r where l.a = r.c and l.b < r.d)
                            or exists (select * from r where l.a = r.c)
                        """
                    ),
                )

    def test_scalar_subquery_with_missing_outer_reference(self):
        with self.tempView("l", "r"):
            self.df1.createOrReplaceTempView("l")
            self.df2.createOrReplaceTempView("r")

            with self.assertRaises(AnalysisException) as pe:
                self.spark.table("l").select(
                    "a",
                    (
                        self.spark.table("r")
                        .where(sf.col("c") == sf.col("a"))
                        .select(sf.sum("d"))
                        .scalar()
                    ),
                ).collect()

            self.check_error(
                exception=pe.exception,
                errorClass="UNRESOLVED_COLUMN.WITH_SUGGESTION",
                messageParameters={"objectName": "`a`", "proposal": "`c`, `d`"},
                query_context_type=QueryContextType.DataFrame,
                fragment="col",
            )

    def table1(self):
        t1 = self.spark.sql("VALUES (0, 1), (1, 2) AS t1(c1, c2)")
        t1.createOrReplaceTempView("t1")
        return self.spark.table("t1")

    def table2(self):
        t2 = self.spark.sql("VALUES (0, 2), (0, 3) AS t2(c1, c2)")
        t2.createOrReplaceTempView("t2")
        return self.spark.table("t2")

    def table3(self):
        t3 = self.spark.sql(
            "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4)) AS t3(c1, c2)"
        )
        t3.createOrReplaceTempView("t3")
        return self.spark.table("t3")

    def test_lateral_join_with_single_column_select(self):
        with self.tempView("t1", "t2"):
            t1 = self.table1()
            t2 = self.table2()

            assertDataFrameEqual(
                t1.lateralJoin(self.spark.range(1).select(sf.col("c1").outer())),
                self.spark.sql("""SELECT * FROM t1, LATERAL (SELECT c1)"""),
            )
            assertDataFrameEqual(
                t1.lateralJoin(t2.select(sf.col("t1.c1").outer())),
                self.spark.sql("""SELECT * FROM t1, LATERAL (SELECT t1.c1 FROM t2)"""),
            )
            assertDataFrameEqual(
                t1.lateralJoin(t2.select(sf.col("t1.c1").outer() + sf.col("t2.c1"))),
                self.spark.sql("""SELECT * FROM t1, LATERAL (SELECT t1.c1 + t2.c1 FROM t2)"""),
            )

    def test_lateral_join_with_different_join_types(self):
        with self.tempView("t1"):
            t1 = self.table1()

            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.range(1).select(
                        (sf.col("c1").outer() + sf.col("c2").outer()).alias("c3")
                    ),
                    sf.col("c2") == sf.col("c3"),
                ),
                self.spark.sql(
                    """SELECT * FROM t1 JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3"""
                ),
            )
            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.range(1).select(
                        (sf.col("c1").outer() + sf.col("c2").outer()).alias("c3")
                    ),
                    sf.col("c2") == sf.col("c3"),
                    "left",
                ),
                self.spark.sql(
                    """SELECT * FROM t1 LEFT JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3"""
                ),
            )
            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.range(1).select(
                        (sf.col("c1").outer() + sf.col("c2").outer()).alias("c3")
                    ),
                    how="cross",
                ),
                self.spark.sql("""SELECT * FROM t1 CROSS JOIN LATERAL (SELECT c1 + c2 AS c3)"""),
            )

            with self.assertRaises(AnalysisException) as pe:
                t1.lateralJoin(
                    self.spark.range(1).select(
                        (sf.col("c1").outer() + sf.col("c2").outer()).alias("c3")
                    ),
                    how="right",
                ).collect()

            self.check_error(
                pe.exception,
                errorClass="UNSUPPORTED_JOIN_TYPE",
                messageParameters={
                    "typ": "right",
                    "supported": "'inner', 'leftouter', 'left', 'left_outer', 'cross'",
                },
            )

    def test_lateral_join_with_correlated_predicates(self):
        with self.tempView("t1", "t2"):
            t1 = self.table1()
            t2 = self.table2()

            assertDataFrameEqual(
                t1.lateralJoin(
                    t2.where(sf.col("t1.c1").outer() == sf.col("t2.c1")).select(sf.col("c2"))
                ),
                self.spark.sql(
                    """SELECT * FROM t1, LATERAL (SELECT c2 FROM t2 WHERE t1.c1 = t2.c1)"""
                ),
            )
            assertDataFrameEqual(
                t1.lateralJoin(
                    t2.where(sf.col("t1.c1").outer() < sf.col("t2.c1")).select(sf.col("c2"))
                ),
                self.spark.sql(
                    """SELECT * FROM t1, LATERAL (SELECT c2 FROM t2 WHERE t1.c1 < t2.c1)"""
                ),
            )

    def test_lateral_join_with_aggregation_and_correlated_predicates(self):
        with self.tempView("t1", "t2"):
            t1 = self.table1()
            t2 = self.table2()

            assertDataFrameEqual(
                t1.lateralJoin(
                    t2.where(sf.col("t1.c2").outer() < sf.col("t2.c2")).select(
                        sf.max(sf.col("c2")).alias("m")
                    )
                ),
                self.spark.sql(
                    """
                    SELECT * FROM t1, LATERAL (SELECT max(c2) AS m FROM t2 WHERE t1.c2 < t2.c2)
                    """
                ),
            )

    def test_lateral_join_reference_preceding_from_clause_items(self):
        with self.tempView("t1", "t2"):
            t1 = self.table1()
            t2 = self.table2()

            assertDataFrameEqual(
                t1.join(t2).lateralJoin(
                    self.spark.range(1).select(sf.col("t1.c2").outer() + sf.col("t2.c2").outer())
                ),
                self.spark.sql("""SELECT * FROM t1 JOIN t2 JOIN LATERAL (SELECT t1.c2 + t2.c2)"""),
            )

    def test_multiple_lateral_joins(self):
        with self.tempView("t1"):
            t1 = self.table1()

            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.range(1).select(
                        (sf.col("c1").outer() + sf.col("c2").outer()).alias("a")
                    )
                )
                .lateralJoin(
                    self.spark.range(1).select(
                        (sf.col("c1").outer() - sf.col("c2").outer()).alias("b")
                    )
                )
                .lateralJoin(
                    self.spark.range(1).select(
                        (sf.col("a").outer() * sf.col("b").outer()).alias("c")
                    )
                ),
                self.spark.sql(
                    """
                    SELECT * FROM t1,
                    LATERAL (SELECT c1 + c2 AS a),
                    LATERAL (SELECT c1 - c2 AS b),
                    LATERAL (SELECT a * b AS c)
                    """
                ),
            )

    def test_lateral_join_in_between_regular_joins(self):
        with self.tempView("t1", "t2"):
            t1 = self.table1()
            t2 = self.table2()

            assertDataFrameEqual(
                t1.lateralJoin(
                    t2.where(sf.col("t1.c1").outer() == sf.col("t2.c1")).select(sf.col("c2")),
                    how="left",
                ).join(t1.alias("t3"), sf.col("t2.c2") == sf.col("t3.c2"), how="left"),
                self.spark.sql(
                    """
                    SELECT * FROM t1
                    LEFT OUTER JOIN LATERAL (SELECT c2 FROM t2 WHERE t1.c1 = t2.c1) s
                    LEFT OUTER JOIN t1 t3 ON s.c2 = t3.c2
                    """
                ),
            )

    def test_nested_lateral_joins(self):
        with self.tempView("t1", "t2"):
            t1 = self.table1()
            t2 = self.table2()

            assertDataFrameEqual(
                t1.lateralJoin(t2.lateralJoin(self.spark.range(1).select(sf.col("c1").outer()))),
                self.spark.sql(
                    """SELECT * FROM t1, LATERAL (SELECT * FROM t2, LATERAL (SELECT c1))"""
                ),
            )
            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.range(1)
                    .select((sf.col("c1").outer() + sf.lit(1)).alias("c1"))
                    .lateralJoin(self.spark.range(1).select(sf.col("c1").outer()))
                ),
                self.spark.sql(
                    """
                    SELECT * FROM t1,
                    LATERAL (SELECT * FROM (SELECT c1 + 1 AS c1), LATERAL (SELECT c1))
                    """
                ),
            )

    def test_scalar_subquery_inside_lateral_join(self):
        with self.tempView("t1", "t2"):
            t1 = self.table1()
            t2 = self.table2()

            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.range(1).select(
                        sf.col("c2").outer(), t2.select(sf.min(sf.col("c2"))).scalar()
                    )
                ),
                self.spark.sql(
                    """SELECT * FROM t1, LATERAL (SELECT c2, (SELECT MIN(c2) FROM t2))"""
                ),
            )
            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.range(1)
                    .select(sf.col("c1").outer().alias("a"))
                    .select(
                        t2.where(sf.col("c1") == sf.col("a").outer())
                        .select(sf.sum(sf.col("c2")))
                        .scalar()
                    )
                ),
                self.spark.sql(
                    """
                    SELECT * FROM t1, LATERAL (
                        SELECT (SELECT SUM(c2) FROM t2 WHERE c1 = a) FROM (SELECT c1 AS a)
                    )
                    """
                ),
            )

    def test_lateral_join_inside_subquery(self):
        with self.tempView("t1", "t2"):
            t1 = self.table1()
            t2 = self.table2()

            assertDataFrameEqual(
                t1.where(
                    sf.col("c1")
                    == (
                        t2.lateralJoin(self.spark.range(1).select(sf.col("c1").outer().alias("a")))
                        .select(sf.min(sf.col("a")))
                        .scalar()
                    )
                ),
                self.spark.sql(
                    """
                    SELECT * FROM t1 WHERE c1 = (SELECT MIN(a) FROM t2, LATERAL (SELECT c1 AS a))
                    """
                ),
            )
            assertDataFrameEqual(
                t1.where(
                    sf.col("c1")
                    == (
                        t2.lateralJoin(self.spark.range(1).select(sf.col("c1").outer().alias("a")))
                        .where(sf.col("c1") == sf.col("t1.c1").outer())
                        .select(sf.min(sf.col("a")))
                        .scalar()
                    )
                ),
                self.spark.sql(
                    """
                    SELECT * FROM t1
                    WHERE c1 = (SELECT MIN(a) FROM t2, LATERAL (SELECT c1 AS a) WHERE c1 = t1.c1)
                    """
                ),
            )

    def test_lateral_join_with_table_valued_functions(self):
        with self.tempView("t1", "t3"):
            t1 = self.table1()
            t3 = self.table3()

            assertDataFrameEqual(
                t1.lateralJoin(self.spark.tvf.range(3)),
                self.spark.sql("""SELECT * FROM t1, LATERAL RANGE(3)"""),
            )
            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.tvf.explode(sf.array(sf.col("c1").outer(), sf.col("c2").outer()))
                ).toDF("c1", "c2", "c3"),
                self.spark.sql("""SELECT * FROM t1, LATERAL EXPLODE(ARRAY(c1, c2)) t2(c3)"""),
            )
            assertDataFrameEqual(
                t3.lateralJoin(self.spark.tvf.explode_outer(sf.col("c2").outer())).toDF(
                    "c1", "c2", "v"
                ),
                self.spark.sql("""SELECT * FROM t3, LATERAL EXPLODE_OUTER(c2) t2(v)"""),
            )
            assertDataFrameEqual(
                self.spark.tvf.explode(sf.array(sf.lit(1), sf.lit(2)))
                .toDF("v")
                .lateralJoin(self.spark.range(1).select((sf.col("v").outer() + 1).alias("v"))),
                self.spark.sql(
                    """SELECT * FROM EXPLODE(ARRAY(1, 2)) t(v), LATERAL (SELECT v + 1 AS v)"""
                ),
            )

    def test_lateral_join_with_table_valued_functions_and_join_conditions(self):
        with self.tempView("t1", "t3"):
            t1 = self.table1()
            t3 = self.table3()

            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.tvf.explode(sf.array(sf.col("c1").outer(), sf.col("c2").outer())),
                    sf.col("c1") == sf.col("col"),
                ).toDF("c1", "c2", "c3"),
                self.spark.sql(
                    """SELECT * FROM t1 JOIN LATERAL EXPLODE(ARRAY(c1, c2)) t(c3) ON t1.c1 = c3"""
                ),
            )
            assertDataFrameEqual(
                t3.lateralJoin(
                    self.spark.tvf.explode(sf.col("c2").outer()),
                    sf.col("c1") == sf.col("col"),
                ).toDF("c1", "c2", "c3"),
                self.spark.sql("""SELECT * FROM t3 JOIN LATERAL EXPLODE(c2) t(c3) ON t3.c1 = c3"""),
            )
            assertDataFrameEqual(
                t3.lateralJoin(
                    self.spark.tvf.explode(sf.col("c2").outer()),
                    sf.col("c1") == sf.col("col"),
                    "left",
                ).toDF("c1", "c2", "c3"),
                self.spark.sql(
                    """SELECT * FROM t3 LEFT JOIN LATERAL EXPLODE(c2) t(c3) ON t3.c1 = c3"""
                ),
            )

    def test_subquery_with_generator_and_tvf(self):
        with self.tempView("t1"):
            t1 = self.table1()

            assertDataFrameEqual(
                self.spark.range(1).select(sf.explode(t1.select(sf.collect_list("c2")).scalar())),
                self.spark.sql("""SELECT EXPLODE((SELECT COLLECT_LIST(c2) FROM t1))"""),
            )
            assertDataFrameEqual(
                self.spark.tvf.explode(t1.select(sf.collect_list("c2")).scalar()),
                self.spark.sql("""SELECT * FROM EXPLODE((SELECT COLLECT_LIST(c2) FROM t1))"""),
            )

    def test_subquery_in_join_condition(self):
        with self.tempView("t1", "t2"):
            t1 = self.table1()
            t2 = self.table2()

            assertDataFrameEqual(
                t1.join(t2, sf.col("t1.c1") == t1.select(sf.max("c1")).scalar()),
                self.spark.sql("""SELECT * FROM t1 JOIN t2 ON t1.c1 = (SELECT MAX(c1) FROM t1)"""),
            )

    def test_subquery_in_unpivot(self):
        self.check_subquery_in_unpivot(QueryContextType.DataFrame, "exists")

    def check_subquery_in_unpivot(self, query_context_type, fragment):
        with self.tempView("t1", "t2"):
            t1 = self.table1()
            t2 = self.table2()

            with self.assertRaises(AnalysisException) as pe:
                t1.unpivot("c1", t2.exists(), "c1", "c2").collect()

            self.check_error(
                exception=pe.exception,
                errorClass=(
                    "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_IN_EXISTS_SUBQUERY"
                ),
                messageParameters={"treeNode": "Expand.*"},
                query_context_type=query_context_type,
                fragment=fragment,
                matchPVals=True,
            )

    def test_subquery_in_transpose(self):
        with self.tempView("t1"):
            t1 = self.table1()

            with self.assertRaises(AnalysisException) as pe:
                t1.transpose(t1.select(sf.max("c1")).scalar()).collect()

            self.check_error(
                exception=pe.exception,
                errorClass="TRANSPOSE_INVALID_INDEX_COLUMN",
                messageParameters={"reason": "Index column must be an atomic attribute"},
            )

    def test_subquery_in_with_columns(self):
        with self.tempView("t1"):
            t1 = self.table1()

            assertDataFrameEqual(
                t1.withColumn(
                    "scalar",
                    self.spark.range(1)
                    .select(sf.col("c1").outer() + sf.col("c2").outer())
                    .scalar(),
                ),
                t1.withColumn("scalar", sf.col("c1") + sf.col("c2")),
            )

    def test_subquery_in_drop(self):
        with self.tempView("t1"):
            t1 = self.table1()

            assertDataFrameEqual(t1.drop(self.spark.range(1).select(sf.lit("c1")).scalar()), t1)

    def test_subquery_in_repartition(self):
        with self.tempView("t1"):
            t1 = self.table1()

            assertDataFrameEqual(t1.repartition(self.spark.range(1).select(sf.lit(1)).scalar()), t1)


class SubqueryTests(SubqueryTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_subquery import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
