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
from pyspark.sql import Row
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


class DataFrameZipTestsMixin:
    """Tests for DataFrame.zip(). Currently only the classic path is supported;
    Spark Connect raises ``NOT_IMPLEMENTED``."""

    def test_zip_select_different_columns(self):
        df = self.spark.createDataFrame([(1, 2, 3), (4, 5, 6), (7, 8, 9)], ["a", "b", "c"])
        zipped = df.select("a").zip(df.select("b"))
        self.assertEqual(zipped.columns, ["a", "b"])
        self.assertEqual(
            sorted(zipped.collect()),
            [Row(a=1, b=2), Row(a=4, b=5), Row(a=7, b=8)],
        )

    def test_zip_with_expressions(self):
        df = self.spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["a", "b"])
        left = df.select((sf.col("a") + 1).alias("a_plus_1"))
        right = df.select((sf.col("b") * 2).alias("b_times_2"))
        self.assertEqual(
            sorted(left.zip(right).collect()),
            [
                Row(a_plus_1=2, b_times_2=20),
                Row(a_plus_1=3, b_times_2=40),
                Row(a_plus_1=4, b_times_2=60),
            ],
        )

    def test_zip_one_side_is_base(self):
        df = self.spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        right = df.select((sf.col("a") + sf.col("b")).alias("sum"))
        self.assertEqual(
            sorted(df.zip(right).collect()),
            [Row(a=1, b=2, sum=3), Row(a=3, b=4, sum=7)],
        )

    def test_zip_with_python_udf(self):
        df = self.spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["a", "b"])
        plus_one = sf.udf(lambda x: x + 1, IntegerType())
        left = df.select(plus_one(sf.col("a")).alias("a_plus_1"))
        right = df.select(plus_one(sf.col("b")).alias("b_plus_1"))
        self.assertEqual(
            sorted(left.zip(right).collect()),
            [
                Row(a_plus_1=2, b_plus_1=11),
                Row(a_plus_1=3, b_plus_1=21),
                Row(a_plus_1=4, b_plus_1=31),
            ],
        )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_zip_with_pandas_udf(self):
        import pandas as pd

        @sf.pandas_udf(IntegerType())
        def plus_one(s: pd.Series) -> pd.Series:
            return s + 1

        df = self.spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["a", "b"])
        left = df.select(plus_one(sf.col("a")).alias("a_plus_1"))
        right = df.select(plus_one(sf.col("b")).alias("b_plus_1"))
        self.assertEqual(
            sorted(left.zip(right).collect()),
            [
                Row(a_plus_1=2, b_plus_1=11),
                Row(a_plus_1=3, b_plus_1=21),
                Row(a_plus_1=4, b_plus_1=31),
            ],
        )

    def test_zip_different_bases_throws(self):
        df1 = self.spark.createDataFrame([(1, 2)], ["a", "b"])
        df2 = self.spark.createDataFrame([(3, 4, 5)], ["x", "y", "z"])
        with self.assertRaises(AnalysisException) as ctx:
            df1.select("a").zip(df2.select("x")).schema
        self.assertEqual(ctx.exception.getCondition(), "ZIP_PLANS_NOT_MERGEABLE")

    def test_zip_different_range_bases_throws(self):
        df1 = self.spark.range(10).toDF("id1")
        df2 = self.spark.range(20).toDF("id2")
        with self.assertRaises(AnalysisException) as ctx:
            df1.zip(df2).schema
        self.assertEqual(ctx.exception.getCondition(), "ZIP_PLANS_NOT_MERGEABLE")

    def test_zip_with_withColumn(self):
        df = self.spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["a", "b"])
        left = df.withColumn("a_plus_1", sf.col("a") + 1)
        right = df.withColumn("b_times_2", sf.col("b") * 2)
        zipped = left.zip(right)
        # Schema has duplicates (a, b appear twice) since withColumn keeps original columns.
        self.assertEqual(zipped.columns, ["a", "b", "a_plus_1", "a", "b", "b_times_2"])
        rows = sorted(zipped.collect(), key=lambda r: r[0])
        self.assertEqual(
            [tuple(r) for r in rows],
            [(1, 10, 2, 1, 10, 20), (2, 20, 3, 2, 20, 40), (3, 30, 4, 3, 30, 60)],
        )

    def test_zip_with_withColumnRenamed(self):
        df = self.spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        left = df.withColumnRenamed("a", "a1")
        right = df.withColumnRenamed("b", "b1")
        self.assertEqual(
            sorted(left.zip(right).collect()),
            [Row(a1=1, b=2, a=1, b1=2), Row(a1=3, b=4, a=3, b1=4)],
        )

    def test_zip_chained_withColumn(self):
        # Stack two withColumn calls on left (two Project layers) and one on right.
        df = self.spark.createDataFrame([(1, 10), (2, 20)], ["a", "b"])
        left = df.withColumn("a_plus_1", sf.col("a") + 1).withColumn("a_plus_2", sf.col("a") + 2)
        right = df.withColumn("b_times_2", sf.col("b") * 2)
        zipped = left.zip(right)
        self.assertEqual(
            zipped.columns,
            ["a", "b", "a_plus_1", "a_plus_2", "a", "b", "b_times_2"],
        )
        rows = sorted(zipped.collect(), key=lambda r: r[0])
        self.assertEqual(
            [tuple(r) for r in rows],
            [(1, 10, 2, 3, 1, 10, 20), (2, 20, 3, 4, 2, 20, 40)],
        )

    def test_zip_longer_chain(self):
        # Left has three nested Projects; right has one.
        df = self.spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
        left = df.select("a", "b", "c").select("a", "b").select("a")
        right = df.select("c")
        self.assertEqual(
            sorted(left.zip(right).collect()),
            [Row(a=1, c=3), Row(a=4, c=6)],
        )

    def test_zip_parent_with_chained_child(self):
        # df.zip(<chained projection of df>) -- the parent has no Project, child has many.
        df = self.spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        child = df.select((sf.col("a") + 1).alias("a_plus_1")).select(
            (sf.col("a_plus_1") * 2).alias("doubled")
        )
        self.assertEqual(
            sorted(df.zip(child).collect()),
            [Row(a=1, b=2, doubled=4), Row(a=3, b=4, doubled=8)],
        )


class DataFrameZipTests(DataFrameZipTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
