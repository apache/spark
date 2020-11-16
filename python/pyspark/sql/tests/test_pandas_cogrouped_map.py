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
import sys

from pyspark.sql.functions import array, explode, col, lit, udf, sum, pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StructType, StructField, Row
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, have_pyarrow, \
    pandas_requirement_message, pyarrow_requirement_message
from pyspark.testing.utils import QuietTest

if have_pandas:
    import pandas as pd
    from pandas.util.testing import assert_frame_equal, assert_series_equal

if have_pyarrow:
    import pyarrow as pa


# Tests below use pd.DataFrame.assign that will infer mixed types (unicode/str) for column names
# From kwargs w/ Python 2, so need to set check_column_type=False and avoid this check
_check_column_type = sys.version >= '3'


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
class CogroupedMapInPandasTests(ReusedSQLTestCase):

    @property
    def data1(self):
        return self.spark.range(10).toDF('id') \
            .withColumn("ks", array([lit(i) for i in range(20, 30)])) \
            .withColumn("k", explode(col('ks')))\
            .withColumn("v", col('k') * 10)\
            .drop('ks')

    @property
    def data2(self):
        return self.spark.range(10).toDF('id') \
            .withColumn("ks", array([lit(i) for i in range(20, 30)])) \
            .withColumn("k", explode(col('ks'))) \
            .withColumn("v2", col('k') * 100) \
            .drop('ks')

    def test_simple(self):
        self._test_merge(self.data1, self.data2)

    def test_left_group_empty(self):
        left = self.data1.where(col("id") % 2 == 0)
        self._test_merge(left, self.data2)

    def test_right_group_empty(self):
        right = self.data2.where(col("id") % 2 == 0)
        self._test_merge(self.data1, right)

    def test_different_schemas(self):
        right = self.data2.withColumn('v3', lit('a'))
        self._test_merge(self.data1, right, 'id long, k int, v int, v2 int, v3 string')

    def test_complex_group_by(self):
        left = pd.DataFrame.from_dict({
            'id': [1, 2, 3],
            'k':  [5, 6, 7],
            'v': [9, 10, 11]
        })

        right = pd.DataFrame.from_dict({
            'id': [11, 12, 13],
            'k': [5, 6, 7],
            'v2': [90, 100, 110]
        })

        left_gdf = self.spark\
            .createDataFrame(left)\
            .groupby(col('id') % 2 == 0)

        right_gdf = self.spark \
            .createDataFrame(right) \
            .groupby(col('id') % 2 == 0)

        def merge_pandas(l, r):
            return pd.merge(l[['k', 'v']], r[['k', 'v2']], on=['k'])

        result = left_gdf \
            .cogroup(right_gdf) \
            .applyInPandas(merge_pandas, 'k long, v long, v2 long') \
            .sort(['k']) \
            .toPandas()

        expected = pd.DataFrame.from_dict({
            'k': [5, 6, 7],
            'v': [9, 10, 11],
            'v2': [90, 100, 110]
        })

        assert_frame_equal(expected, result, check_column_type=_check_column_type)

    def test_empty_group_by(self):
        left = self.data1
        right = self.data2

        def merge_pandas(l, r):
            return pd.merge(l, r, on=['id', 'k'])

        result = left.groupby().cogroup(right.groupby())\
            .applyInPandas(merge_pandas, 'id long, k int, v int, v2 int') \
            .sort(['id', 'k']) \
            .toPandas()

        left = left.toPandas()
        right = right.toPandas()

        expected = pd \
            .merge(left, right, on=['id', 'k']) \
            .sort_values(by=['id', 'k'])

        assert_frame_equal(expected, result, check_column_type=_check_column_type)

    def test_mixed_scalar_udfs_followed_by_cogrouby_apply(self):
        df = self.spark.range(0, 10).toDF('v1')
        df = df.withColumn('v2', udf(lambda x: x + 1, 'int')(df['v1'])) \
            .withColumn('v3', pandas_udf(lambda x: x + 2, 'int')(df['v1']))

        result = df.groupby().cogroup(df.groupby()) \
            .applyInPandas(lambda x, y: pd.DataFrame([(x.sum().sum(), y.sum().sum())]),
                           'sum1 int, sum2 int').collect()

        self.assertEquals(result[0]['sum1'], 165)
        self.assertEquals(result[0]['sum2'], 165)

    def test_with_key_left(self):
        self._test_with_key(self.data1, self.data1, isLeft=True)

    def test_with_key_right(self):
        self._test_with_key(self.data1, self.data1, isLeft=False)

    def test_with_key_left_group_empty(self):
        left = self.data1.where(col("id") % 2 == 0)
        self._test_with_key(left, self.data1, isLeft=True)

    def test_with_key_right_group_empty(self):
        right = self.data1.where(col("id") % 2 == 0)
        self._test_with_key(self.data1, right, isLeft=False)

    def test_with_key_complex(self):

        def left_assign_key(key, l, _):
            return l.assign(key=key[0])

        result = self.data1 \
            .groupby(col('id') % 2 == 0)\
            .cogroup(self.data2.groupby(col('id') % 2 == 0)) \
            .applyInPandas(left_assign_key, 'id long, k int, v int, key boolean') \
            .sort(['id', 'k']) \
            .toPandas()

        expected = self.data1.toPandas()
        expected = expected.assign(key=expected.id % 2 == 0)

        assert_frame_equal(expected, result, check_column_type=_check_column_type)

    def test_wrong_return_type(self):
        # Test that we get a sensible exception invalid values passed to apply
        left = self.data1
        right = self.data2
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    NotImplementedError,
                    'Invalid return type.*MapType'):
                left.groupby('id').cogroup(right.groupby('id')).applyInPandas(
                    lambda l, r: l, 'id long, v map<int, int>')

    def test_wrong_args(self):
        left = self.data1
        right = self.data2
        with self.assertRaisesRegexp(ValueError, 'Invalid function'):
            left.groupby('id').cogroup(right.groupby('id')) \
                .applyInPandas(lambda: 1, StructType([StructField("d", DoubleType())]))

    def test_case_insensitive_grouping_column(self):
        # SPARK-31915: case-insensitive grouping column should work.
        df1 = self.spark.createDataFrame([(1, 1)], ("column", "value"))

        row = df1.groupby("ColUmn").cogroup(
            df1.groupby("COLUMN")
        ).applyInPandas(lambda r, l: r + l, "column long, value long").first()
        self.assertEquals(row.asDict(), Row(column=2, value=2).asDict())

        df2 = self.spark.createDataFrame([(1, 1)], ("column", "value"))

        row = df1.groupby("ColUmn").cogroup(
            df2.groupby("COLUMN")
        ).applyInPandas(lambda r, l: r + l, "column long, value long").first()
        self.assertEquals(row.asDict(), Row(column=2, value=2).asDict())

    @staticmethod
    def _test_with_key(left, right, isLeft):

        def right_assign_key(key, l, r):
            return l.assign(key=key[0]) if isLeft else r.assign(key=key[0])

        result = left \
            .groupby('id') \
            .cogroup(right.groupby('id')) \
            .applyInPandas(right_assign_key, 'id long, k int, v int, key long') \
            .toPandas()

        expected = left.toPandas() if isLeft else right.toPandas()
        expected = expected.assign(key=expected.id)

        assert_frame_equal(expected, result, check_column_type=_check_column_type)

    @staticmethod
    def _test_merge(left, right, output_schema='id long, k int, v int, v2 int'):

        def merge_pandas(l, r):
            return pd.merge(l, r, on=['id', 'k'])

        result = left \
            .groupby('id') \
            .cogroup(right.groupby('id')) \
            .applyInPandas(merge_pandas, output_schema)\
            .sort(['id', 'k']) \
            .toPandas()

        left = left.toPandas()
        right = right.toPandas()

        expected = pd \
            .merge(left, right, on=['id', 'k']) \
            .sort_values(by=['id', 'k'])

        assert_frame_equal(expected, result, check_column_type=_check_column_type)


if __name__ == "__main__":
    from pyspark.sql.tests.test_pandas_cogrouped_map import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
