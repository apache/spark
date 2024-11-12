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

    def test_unanalyzable_expression(self):
        sub = self.spark.range(1).where(sf.col("id") == sf.col("id").outer())

        with self.assertRaises(AnalysisException) as pe:
            sub.schema

        self.check_error(
            exception=pe.exception,
            errorClass="UNANALYZABLE_EXPRESSION",
            messageParameters={"expr": '"outer(id)"'},
            query_context_type=QueryContextType.DataFrame,
            fragment="outer",
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
                        .where(sf.col("c2").outer() == sf.col("c2"))
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
                assertDataFrameEqual(
                    self.spark.table("l").where(
                        sf.col("b")
                        < (
                            self.spark.table("r")
                            .where(sf.col("a").outer() == sf.col("c"))
                            .select(sf.max("d"))
                            .scalar()
                        )
                    ),
                    self.spark.sql(
                        """select * from l where b < (select max(d) from r where a = c)"""
                    ),
                )

            with self.subTest("in select"):
                assertDataFrameEqual(
                    self.spark.table("l").select(
                        "a",
                        (
                            self.spark.table("l")
                            .where(sf.col("a") == sf.col("a").outer())
                            .select(sf.sum("b"))
                            .scalar()
                            .alias("sum_b")
                        ),
                    ),
                    self.spark.sql(
                        """select a, (select sum(b) from l l2 where l2.a = l1.a) sum_b from l l1"""
                    ),
                )

            with self.subTest("in select (null safe)"):
                assertDataFrameEqual(
                    self.spark.table("l").select(
                        "a",
                        (
                            self.spark.table("l")
                            .where(sf.col("a").eqNullSafe(sf.col("a").outer()))
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
                with self.assertRaises(SparkRuntimeException) as pe:
                    self.spark.table("l").select(
                        "a",
                        (
                            self.spark.table("l")
                            .where(sf.col("a") == sf.col("a").outer())
                            .select("b")
                            .scalar()
                        ),
                    ).collect()

                self.check_error(
                    exception=pe.exception,
                    errorClass="SCALAR_SUBQUERY_TOO_MANY_ROWS",
                    messageParameters={},
                )

            with self.subTest("non-equal"):
                assertDataFrameEqual(
                    self.spark.table("l").select(
                        "a",
                        (
                            self.spark.table("l")
                            .where(sf.col("a") < sf.col("a").outer())
                            .select(sf.sum("b"))
                            .scalar()
                            .alias("sum_b")
                        ),
                    ),
                    self.spark.sql(
                        """select a, (select sum(b) from l l2 where l2.a < l1.a) sum_b from l l1"""
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
                assertDataFrameEqual(
                    self.spark.table("l").where(
                        self.spark.table("r").where(sf.col("a").outer() == sf.col("c")).exists()
                    ),
                    self.spark.sql(
                        """select * from l where exists (select * from r where l.a = r.c)"""
                    ),
                )

                assertDataFrameEqual(
                    self.spark.table("l").where(
                        self.spark.table("r").where(sf.col("a").outer() == sf.col("c")).exists()
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

    def test_scalar_subquery_with_outer_reference_errors(self):
        with self.tempView("l", "r"):
            self.df1.createOrReplaceTempView("l")
            self.df2.createOrReplaceTempView("r")

            with self.subTest("missing `outer()`"):
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

            with self.subTest("extra `outer()`"):
                with self.assertRaises(AnalysisException) as pe:
                    self.spark.table("l").select(
                        "a",
                        (
                            self.spark.table("r")
                            .where(sf.col("c").outer() == sf.col("a").outer())
                            .select(sf.sum("d"))
                            .scalar()
                        ),
                    ).collect()

                self.check_error(
                    exception=pe.exception,
                    errorClass="UNRESOLVED_COLUMN.WITH_SUGGESTION",
                    messageParameters={"objectName": "`c`", "proposal": "`a`, `b`"},
                    query_context_type=QueryContextType.DataFrame,
                    fragment="outer",
                )

            with self.subTest("missing `outer()` for another outer"):
                with self.assertRaises(AnalysisException) as pe:
                    self.spark.table("l").select(
                        "a",
                        (
                            self.spark.table("r")
                            .where(sf.col("b") == sf.col("a").outer())
                            .select(sf.sum("d"))
                            .scalar()
                        ),
                    ).collect()

                self.check_error(
                    exception=pe.exception,
                    errorClass="UNRESOLVED_COLUMN.WITH_SUGGESTION",
                    messageParameters={"objectName": "`b`", "proposal": "`c`, `d`"},
                    query_context_type=QueryContextType.DataFrame,
                    fragment="col",
                )


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
