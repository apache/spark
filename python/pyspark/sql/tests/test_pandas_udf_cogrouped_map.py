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

import datetime
import unittest
import sys

from collections import OrderedDict
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql.functions import array, explode, col, lit, udf, sum, pandas_udf, PandasUDFType
from pyspark.sql.types import *
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, have_pyarrow, \
    pandas_requirement_message, pyarrow_requirement_message
from pyspark.testing.utils import QuietTest

if have_pandas:
    import pandas as pd
    from pandas.util.testing import assert_frame_equal

if have_pyarrow:
    import pyarrow as pa


"""
Tests below use pd.DataFrame.assign that will infer mixed types (unicode/str) for column names
from kwargs w/ Python 2, so need to set check_column_type=False and avoid this check
"""
if sys.version < '3':
    _check_column_type = False
else:
    _check_column_type = True


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
class CoGroupedMapPandasUDFTests(ReusedSQLTestCase):

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
        self._test_merge(self.data1, right, output_schema='id long, k int, v int, v2 int, v3 string')

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

        left_df =  self.spark\
            .createDataFrame(left)\
            .groupby(col('id') % 2 == 0)

        right_df = self.spark \
            .createDataFrame(right) \
            .groupby(col('id') % 2 == 0)

        @pandas_udf('k long, v long, v2 long', PandasUDFType.COGROUPED_MAP)
        def merge_pandas(l, r):
            return pd.merge(l[['k', 'v']], r[['k', 'v2']], on=['k'])

        result = left_df \
            .cogroup(right_df) \
            .apply(merge_pandas) \
            .sort(['k']) \
            .toPandas()

        expected = pd.DataFrame.from_dict({
            'k': [5, 6, 7],
            'v': [9, 10, 11],
            'v2': [90, 100, 110]
        })

        assert_frame_equal(expected, result, check_column_type=_check_column_type)

    def _test_merge(self, left, right, output_schema='id long, k int, v int, v2 int'):

        @pandas_udf(output_schema, PandasUDFType.COGROUPED_MAP)
        def merge_pandas(l, r):
            return pd.merge(l, r, on=['id', 'k'])

        result = left \
            .groupby('id') \
            .cogroup(right.groupby('id')) \
            .apply(merge_pandas)\
            .sort(['id', 'k']) \
            .toPandas()

        left = left.toPandas()
        right = right.toPandas()

        expected = pd \
            .merge(left, right, on=['id', 'k']) \
            .sort_values(by=['id', 'k'])

        assert_frame_equal(expected, result, check_column_type=_check_column_type)

