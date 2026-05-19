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


class DataFrameZipTests(DataFrameZipTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
