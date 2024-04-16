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

from pyspark.sql import Row
from pyspark.sql import functions as sf
from pyspark.errors import AnalysisException
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual


class GroupTestsMixin:
    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)  # type: ignore
    def test_agg_func(self):
        data = [Row(key=1, value=10), Row(key=1, value=20), Row(key=1, value=30)]
        df = self.spark.createDataFrame(data)
        g = df.groupBy("key")
        self.assertEqual(g.max("value").collect(), [Row(**{"key": 1, "max(value)": 30})])
        self.assertEqual(g.min("value").collect(), [Row(**{"key": 1, "min(value)": 10})])
        self.assertEqual(g.sum("value").collect(), [Row(**{"key": 1, "sum(value)": 60})])
        self.assertEqual(g.count().collect(), [Row(key=1, count=3)])
        self.assertEqual(g.mean("value").collect(), [Row(**{"key": 1, "avg(value)": 20.0})])

        data = [
            Row(electronic="Smartphone", year=2018, sales=150000),
            Row(electronic="Tablet", year=2018, sales=120000),
            Row(electronic="Smartphone", year=2019, sales=180000),
            Row(electronic="Tablet", year=2019, sales=50000),
        ]

        df_pivot = self.spark.createDataFrame(data)
        assertDataFrameEqual(
            df_pivot.groupBy("year").pivot("electronic", ["Smartphone", "Tablet"]).sum("sales"),
            df_pivot.groupBy("year").pivot("electronic").sum("sales"),
        )

    def test_aggregator(self):
        df = self.df
        g = df.groupBy()
        self.assertEqual([99, 100], sorted(g.agg({"key": "max", "value": "count"}).collect()[0]))
        self.assertEqual([Row(**{"AVG(key#0)": 49.5})], g.mean().collect())

        from pyspark.sql import functions

        self.assertEqual(
            (0, "99"), tuple(g.agg(functions.first(df.key), functions.last(df.value)).first())
        )
        self.assertTrue(95 < g.agg(functions.approx_count_distinct(df.key)).first()[0])
        # test deprecated countDistinct
        self.assertEqual(100, g.agg(functions.countDistinct(df.value)).first()[0])

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)  # type: ignore
    def test_group_by_ordinal(self):
        spark = self.spark
        df = spark.createDataFrame(
            [
                (1, 1),
                (1, 2),
                (2, 1),
                (2, 2),
                (3, 1),
                (3, 2),
            ],
            ["a", "b"],
        )

        with self.tempView("v"):
            df.createOrReplaceTempView("v")

            # basic case
            df1 = spark.sql("select a, sum(b) from v group by 1;")
            df2 = df.groupBy(1).agg(sf.sum("b"))
            assertSchemaEqual(df1.schema, df2.schema)
            assertDataFrameEqual(df1, df2)

            # constant case
            df1 = spark.sql("select 1, 2, sum(b) from v group by 1, 2;")
            df2 = df.select(sf.lit(1), sf.lit(2), "b").groupBy(1, 2).agg(sf.sum("b"))
            assertSchemaEqual(df1.schema, df2.schema)
            assertDataFrameEqual(df1, df2)

            # duplicate group by column
            df1 = spark.sql("select a, 1, sum(b) from v group by a, 1;")
            df2 = df.select("a", sf.lit(1), "b").groupBy("a", 2).agg(sf.sum("b"))
            assertSchemaEqual(df1.schema, df2.schema)
            assertDataFrameEqual(df1, df2)

            df1 = spark.sql("select a, 1, sum(b) from v group by 1, 2;")
            df2 = df.select("a", sf.lit(1), "b").groupBy(1, 2).agg(sf.sum("b"))
            assertSchemaEqual(df1.schema, df2.schema)
            assertDataFrameEqual(df1, df2)

            # group by a non-aggregate expression's ordinal
            df1 = spark.sql("select a, b + 2, count(2) from v group by a, 2;")
            df2 = df.select("a", df.b + 2).groupBy(1, 2).agg(sf.count(sf.lit(2)))
            assertSchemaEqual(df1.schema, df2.schema)
            assertDataFrameEqual(df1, df2)

            # negative cases: ordinal out of range
            with self.assertRaises(IndexError):
                df.groupBy(0).agg(sf.sum("b"))

            with self.assertRaises(IndexError):
                df.groupBy(-1).agg(sf.sum("b"))

            with self.assertRaises(IndexError):
                df.groupBy(3).agg(sf.sum("b"))

            with self.assertRaises(IndexError):
                df.groupBy(10).agg(sf.sum("b"))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)  # type: ignore
    def test_order_by_ordinal(self):
        spark = self.spark
        df = spark.createDataFrame(
            [
                (1, 1),
                (1, 2),
                (2, 1),
                (2, 2),
                (3, 1),
                (3, 2),
            ],
            ["a", "b"],
        )

        with self.tempView("v"):
            df.createOrReplaceTempView("v")

            df1 = spark.sql("select * from v order by 1 desc;")
            df2 = df.orderBy(-1)
            assertSchemaEqual(df1.schema, df2.schema)
            assertDataFrameEqual(df1, df2)

            df1 = spark.sql("select * from v order by 1 desc, b desc;")
            df2 = df.orderBy(-1, df.b.desc())
            assertSchemaEqual(df1.schema, df2.schema)
            assertDataFrameEqual(df1, df2)

            df1 = spark.sql("select * from v order by 1 desc, 2 desc;")
            df2 = df.orderBy(-1, -2)
            assertSchemaEqual(df1.schema, df2.schema)
            assertDataFrameEqual(df1, df2)

            # groupby ordinal with orderby ordinal
            df1 = spark.sql("select a, 1, sum(b) from v group by 1, 2 order by 1;")
            df2 = df.select("a", sf.lit(1), "b").groupBy(1, 2).agg(sf.sum("b")).sort(1)
            assertSchemaEqual(df1.schema, df2.schema)
            assertDataFrameEqual(df1, df2)

            df1 = spark.sql("select a, 1, sum(b) from v group by 1, 2 order by 3, 1;")
            df2 = df.select("a", sf.lit(1), "b").groupBy(1, 2).agg(sf.sum("b")).sort(3, 1)
            assertSchemaEqual(df1.schema, df2.schema)
            assertDataFrameEqual(df1, df2)

            # negative cases: ordinal out of range
            with self.assertRaises(IndexError):
                df.sort(0)

            with self.assertRaises(IndexError):
                df.orderBy(3)

            with self.assertRaises(IndexError):
                df.orderBy(-3)

        def test_pivot_exceed_max_values(self):
            with self.assertRaises(AnalysisException):
                spark.range(100001).groupBy(sf.lit(1)).pivot("id").count().show()


class GroupTests(GroupTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_group import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
