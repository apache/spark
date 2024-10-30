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
from pyspark.testing.connectutils import should_test_connect
from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase

if should_test_connect:
    from pyspark.sql import functions as SF
    from pyspark.sql.connect import functions as CF


class SparkConnectCollectionTests(SparkConnectSQLTestCase):
    def test_collect(self):
        cdf = self.connect.read.table(self.tbl_name)
        sdf = self.spark.read.table(self.tbl_name)

        data = cdf.limit(10).collect()
        self.assertEqual(len(data), 10)
        # Check Row has schema column names.
        self.assertTrue("name" in data[0])
        self.assertTrue("id" in data[0])

        cdf = cdf.select(
            CF.log("id"), CF.log("id"), CF.struct("id", "name"), CF.struct("id", "name")
        ).limit(10)
        sdf = sdf.select(
            SF.log("id"), SF.log("id"), SF.struct("id", "name"), SF.struct("id", "name")
        ).limit(10)

        self.assertEqual(
            cdf.collect(),
            sdf.collect(),
        )

    def test_collect_timestamp(self):
        query = """
            SELECT * FROM VALUES
            (TIMESTAMP('2022-12-25 10:30:00'), 1),
            (TIMESTAMP('2022-12-25 10:31:00'), 2),
            (TIMESTAMP('2022-12-25 10:32:00'), 1),
            (TIMESTAMP('2022-12-25 10:33:00'), 2),
            (TIMESTAMP('2022-12-26 09:30:00'), 1),
            (TIMESTAMP('2022-12-26 09:35:00'), 3)
            AS tab(date, val)
            """

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        self.assertEqual(cdf.schema, sdf.schema)

        self.assertEqual(cdf.collect(), sdf.collect())

        self.assertEqual(
            cdf.select(CF.date_trunc("year", cdf.date).alias("year")).collect(),
            sdf.select(SF.date_trunc("year", sdf.date).alias("year")).collect(),
        )

    def test_head(self):
        # SPARK-41002: test `head` API in Python Client
        df = self.connect.read.table(self.tbl_name)
        self.assertIsNotNone(len(df.head()))
        self.assertIsNotNone(len(df.head(1)))
        self.assertIsNotNone(len(df.head(5)))
        df2 = self.connect.read.table(self.tbl_name_empty)
        self.assertIsNone(df2.head())

    def test_first(self):
        # SPARK-41002: test `first` API in Python Client
        df = self.connect.read.table(self.tbl_name)
        self.assertIsNotNone(len(df.first()))
        df2 = self.connect.read.table(self.tbl_name_empty)
        self.assertIsNone(df2.first())

    def test_take(self) -> None:
        # SPARK-41002: test `take` API in Python Client
        df = self.connect.read.table(self.tbl_name)
        self.assertEqual(5, len(df.take(5)))
        df2 = self.connect.read.table(self.tbl_name_empty)
        self.assertEqual(0, len(df2.take(5)))

    def test_to_pandas(self):
        # SPARK-41005: Test to pandas
        query = """
            SELECT * FROM VALUES
            (false, 1, NULL),
            (false, NULL, float(2.0)),
            (NULL, 3, float(3.0))
            AS tab(a, b, c)
            """

        self.assert_eq(
            self.connect.sql(query).toPandas(),
            self.spark.sql(query).toPandas(),
        )

        query = """
            SELECT * FROM VALUES
            (1, 1, NULL),
            (2, NULL, float(2.0)),
            (3, 3, float(3.0))
            AS tab(a, b, c)
            """

        self.assert_eq(
            self.connect.sql(query).toPandas(),
            self.spark.sql(query).toPandas(),
        )

        query = """
            SELECT * FROM VALUES
            (double(1.0), 1, "1"),
            (NULL, NULL, NULL),
            (double(2.0), 3, "3")
            AS tab(a, b, c)
            """

        self.assert_eq(
            self.connect.sql(query).toPandas(),
            self.spark.sql(query).toPandas(),
        )

        query = """
            SELECT * FROM VALUES
            (float(1.0), double(1.0), 1, "1"),
            (float(2.0), double(2.0), 2, "2"),
            (float(3.0), double(3.0), 3, "3")
            AS tab(a, b, c, d)
            """

        self.assert_eq(
            self.connect.sql(query).toPandas(),
            self.spark.sql(query).toPandas(),
        )

    def test_collect_nested_type(self):
        query = """
            SELECT * FROM VALUES
            (1, 4, 0, 8, true, true, ARRAY(1, NULL, 3), MAP(1, 2, 3, 4)),
            (2, 5, -1, NULL, false, NULL, ARRAY(1, 3), MAP(1, NULL, 3, 4)),
            (3, 6, NULL, 0, false, NULL, ARRAY(NULL), NULL)
            AS tab(a, b, c, d, e, f, g, h)
            """

        # +---+---+----+----+-----+----+------------+-------------------+
        # |  a|  b|   c|   d|    e|   f|           g|                  h|
        # +---+---+----+----+-----+----+------------+-------------------+
        # |  1|  4|   0|   8| true|true|[1, null, 3]|   {1 -> 2, 3 -> 4}|
        # |  2|  5|  -1|NULL|false|NULL|      [1, 3]|{1 -> null, 3 -> 4}|
        # |  3|  6|NULL|   0|false|NULL|      [null]|               NULL|
        # +---+---+----+----+-----+----+------------+-------------------+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # test collect array
        # +--------------+-------------+------------+
        # |array(a, b, c)|  array(e, f)|           g|
        # +--------------+-------------+------------+
        # |     [1, 4, 0]| [true, true]|[1, null, 3]|
        # |    [2, 5, -1]|[false, null]|      [1, 3]|
        # |  [3, 6, null]|[false, null]|      [null]|
        # +--------------+-------------+------------+
        self.assertEqual(
            cdf.select(CF.array("a", "b", "c"), CF.array("e", "f"), CF.col("g")).collect(),
            sdf.select(SF.array("a", "b", "c"), SF.array("e", "f"), SF.col("g")).collect(),
        )

        # test collect nested array
        # +-----------------------------------+-------------------------+
        # |array(array(a), array(b), array(c))|array(array(e), array(f))|
        # +-----------------------------------+-------------------------+
        # |                    [[1], [4], [0]]|         [[true], [true]]|
        # |                   [[2], [5], [-1]]|        [[false], [null]]|
        # |                 [[3], [6], [null]]|        [[false], [null]]|
        # +-----------------------------------+-------------------------+
        self.assertEqual(
            cdf.select(
                CF.array(CF.array("a"), CF.array("b"), CF.array("c")),
                CF.array(CF.array("e"), CF.array("f")),
            ).collect(),
            sdf.select(
                SF.array(SF.array("a"), SF.array("b"), SF.array("c")),
                SF.array(SF.array("e"), SF.array("f")),
            ).collect(),
        )

        # test collect array of struct, map
        # +----------------+---------------------+
        # |array(struct(a))|             array(h)|
        # +----------------+---------------------+
        # |           [{1}]|   [{1 -> 2, 3 -> 4}]|
        # |           [{2}]|[{1 -> null, 3 -> 4}]|
        # |           [{3}]|               [null]|
        # +----------------+---------------------+
        self.assertEqual(
            cdf.select(CF.array(CF.struct("a")), CF.array("h")).collect(),
            sdf.select(SF.array(SF.struct("a")), SF.array("h")).collect(),
        )

        # test collect map
        # +-------------------+-------------------+
        # |                  h|    map(a, b, b, c)|
        # +-------------------+-------------------+
        # |   {1 -> 2, 3 -> 4}|   {1 -> 4, 4 -> 0}|
        # |{1 -> null, 3 -> 4}|  {2 -> 5, 5 -> -1}|
        # |               NULL|{3 -> 6, 6 -> null}|
        # +-------------------+-------------------+
        self.assertEqual(
            cdf.select(CF.col("h"), CF.create_map("a", "b", "b", "c")).collect(),
            sdf.select(SF.col("h"), SF.create_map("a", "b", "b", "c")).collect(),
        )

        # test collect map of struct, array
        # +-------------------+------------------------+
        # |          map(a, g)|    map(a, struct(b, g))|
        # +-------------------+------------------------+
        # |{1 -> [1, null, 3]}|{1 -> {4, [1, null, 3]}}|
        # |      {2 -> [1, 3]}|      {2 -> {5, [1, 3]}}|
        # |      {3 -> [null]}|      {3 -> {6, [null]}}|
        # +-------------------+------------------------+
        self.assertEqual(
            cdf.select(CF.create_map("a", "g"), CF.create_map("a", CF.struct("b", "g"))).collect(),
            sdf.select(SF.create_map("a", "g"), SF.create_map("a", SF.struct("b", "g"))).collect(),
        )

        # test collect struct
        # +------------------+--------------------------+
        # |struct(a, b, c, d)|           struct(e, f, g)|
        # +------------------+--------------------------+
        # |      {1, 4, 0, 8}|{true, true, [1, null, 3]}|
        # |  {2, 5, -1, null}|     {false, null, [1, 3]}|
        # |   {3, 6, null, 0}|     {false, null, [null]}|
        # +------------------+--------------------------+
        self.assertEqual(
            cdf.select(CF.struct("a", "b", "c", "d"), CF.struct("e", "f", "g")).collect(),
            sdf.select(SF.struct("a", "b", "c", "d"), SF.struct("e", "f", "g")).collect(),
        )

        # test collect nested struct
        # +------------------------------------------+--------------------------+----------------------------+ # noqa
        # |struct(a, struct(a, struct(c, struct(d))))|struct(a, b, struct(c, d))|     struct(e, f, struct(g))| # noqa
        # +------------------------------------------+--------------------------+----------------------------+ # noqa
        # |                        {1, {1, {0, {8}}}}|            {1, 4, {0, 8}}|{true, true, {[1, null, 3]}}| # noqa
        # |                    {2, {2, {-1, {null}}}}|        {2, 5, {-1, null}}|     {false, null, {[1, 3]}}| # noqa
        # |                     {3, {3, {null, {0}}}}|         {3, 6, {null, 0}}|     {false, null, {[null]}}| # noqa
        # +------------------------------------------+--------------------------+----------------------------+ # noqa
        self.assertEqual(
            cdf.select(
                CF.struct("a", CF.struct("a", CF.struct("c", CF.struct("d")))),
                CF.struct("a", "b", CF.struct("c", "d")),
                CF.struct("e", "f", CF.struct("g")),
            ).collect(),
            sdf.select(
                SF.struct("a", SF.struct("a", SF.struct("c", SF.struct("d")))),
                SF.struct("a", "b", SF.struct("c", "d")),
                SF.struct("e", "f", SF.struct("g")),
            ).collect(),
        )

        # test collect struct containing array, map
        # +--------------------------------------------+
        # |  struct(a, struct(a, struct(g, struct(h))))|
        # +--------------------------------------------+
        # |{1, {1, {[1, null, 3], {{1 -> 2, 3 -> 4}}}}}|
        # |   {2, {2, {[1, 3], {{1 -> null, 3 -> 4}}}}}|
        # |                  {3, {3, {[null], {null}}}}|
        # +--------------------------------------------+
        self.assertEqual(
            cdf.select(
                CF.struct("a", CF.struct("a", CF.struct("g", CF.struct("h")))),
            ).collect(),
            sdf.select(
                SF.struct("a", SF.struct("a", SF.struct("g", SF.struct("h")))),
            ).collect(),
        )


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_collection import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
