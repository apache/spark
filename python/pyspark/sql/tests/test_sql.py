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

from mpmath.rational import mpq_3_2
from pyspark.sql import Row
from pyspark.testing.sqlutils import ReusedSQLTestCase


class SQLTestsMixin:
    def test_simple(self):
        res = self.spark.sql("SELECT 1 + 1").collect()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][0], 2)

    def test_args_dict(self):
        with self.tempView("test"):
            self.spark.range(10).createOrReplaceTempView("test")
            df = self.spark.sql(
                "SELECT * FROM IDENTIFIER(:table_name)",
                args={"table_name": "test"},
            )

            self.assertEqual(df.count(), 10)
            self.assertEqual(df.limit(5).count(), 5)
            self.assertEqual(df.offset(5).count(), 5)

            self.assertEqual(df.take(1), [Row(id=0)])
            self.assertEqual(df.tail(1), [Row(id=9)])

    def test_args_list(self):
        with self.tempView("test"):
            self.spark.range(10).createOrReplaceTempView("test")
            df = self.spark.sql(
                "SELECT * FROM test WHERE ? < id AND id < ?",
                args=[1, 6],
            )

            self.assertEqual(df.count(), 4)
            self.assertEqual(df.limit(3).count(), 3)
            self.assertEqual(df.offset(3).count(), 1)

            self.assertEqual(df.take(1), [Row(id=2)])
            self.assertEqual(df.tail(1), [Row(id=5)])

    def test_kwargs_literal(self):
        with self.tempView("test"):
            self.spark.range(10).createOrReplaceTempView("test")

            df = self.spark.sql(
                "SELECT * FROM IDENTIFIER(:table_name) WHERE {m1} < id AND id < {m2} OR id = {m3}",
                args={"table_name": "test"},
                m1=3,
                m2=7,
                m3=9,
            )

            self.assertEqual(df.count(), 4)
            self.assertEqual(df.collect(), [Row(id=4), Row(id=5), Row(id=6), Row(id=9)])
            self.assertEqual(df.take(1), [Row(id=4)])
            self.assertEqual(df.tail(1), [Row(id=9)])

    def test_kwargs_literal_multiple_ref(self):
        with self.tempView("test"):
            self.spark.range(10).createOrReplaceTempView("test")

            df = self.spark.sql(
                "SELECT * FROM IDENTIFIER(:table_name) WHERE {m} = id OR id > {m} OR {m} < 0",
                args={"table_name": "test"},
                m=6,
            )

            self.assertEqual(df.count(), 4)
            self.assertEqual(df.collect(), [Row(id=6), Row(id=7), Row(id=8), Row(id=9)])
            self.assertEqual(df.take(1), [Row(id=6)])
            self.assertEqual(df.tail(1), [Row(id=9)])

    def test_kwargs_dataframe(self):
        df0 = self.spark.range(10)
        df1 = self.spark.sql(
            "SELECT * FROM {df} WHERE id > 4",
            df=df0,
        )

        self.assertEqual(df0.schema, df1.schema)
        self.assertEqual(df1.count(), 5)
        self.assertEqual(df1.take(1), [Row(id=5)])
        self.assertEqual(df1.tail(1), [Row(id=9)])

    def test_kwargs_dataframe_with_column(self):
        df0 = self.spark.range(10)
        df1 = self.spark.sql(
            "SELECT * FROM {df} WHERE {df.id} > :m1 AND {df[id]} < :m2",
            {"m1": 4, "m2": 9},
            df=df0,
        )

        self.assertEqual(df0.schema, df1.schema)
        self.assertEqual(df1.count(), 4)
        self.assertEqual(df1.take(1), [Row(id=5)])
        self.assertEqual(df1.tail(1), [Row(id=8)])

    def test_nested_view(self):
        with self.tempView("v1", "v2", "v3", "v4"):
            self.spark.range(10).createOrReplaceTempView("v1")
            self.spark.sql(
                "SELECT * FROM IDENTIFIER(:view) WHERE id > :m",
                args={"view": "v1", "m": 1},
            ).createOrReplaceTempView("v2")
            self.spark.sql(
                "SELECT * FROM IDENTIFIER(:view) WHERE id > :m",
                args={"view": "v2", "m": 2},
            ).createOrReplaceTempView("v3")
            self.spark.sql(
                "SELECT * FROM IDENTIFIER(:view) WHERE id > :m",
                args={"view": "v3", "m": 3},
            ).createOrReplaceTempView("v4")

            df = self.spark.sql("select * from v4")
            self.assertEqual(df.count(), 6)
            self.assertEqual(df.take(1), [Row(id=4)])
            self.assertEqual(df.tail(1), [Row(id=9)])

    def test_nested_dataframe(self):
        df0 = self.spark.range(10)
        df1 = self.spark.sql(
            "SELECT * FROM {df} WHERE id > ?",
            args=[1],
            df=df0,
        )
        df2 = self.spark.sql(
            "SELECT * FROM {df} WHERE id > ?",
            args=[2],
            df=df1,
        )
        df3 = self.spark.sql(
            "SELECT * FROM {df} WHERE id > ?",
            args=[3],
            df=df2,
        )

        self.assertEqual(df0.schema, df1.schema)
        self.assertEqual(df1.count(), 8)
        self.assertEqual(df1.take(1), [Row(id=2)])
        self.assertEqual(df1.tail(1), [Row(id=9)])

        self.assertEqual(df0.schema, df2.schema)
        self.assertEqual(df2.count(), 7)
        self.assertEqual(df2.take(1), [Row(id=3)])
        self.assertEqual(df2.tail(1), [Row(id=9)])

        self.assertEqual(df0.schema, df3.schema)
        self.assertEqual(df3.count(), 6)
        self.assertEqual(df3.take(1), [Row(id=4)])
        self.assertEqual(df3.tail(1), [Row(id=9)])


class SQLTests(SQLTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_sql import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
