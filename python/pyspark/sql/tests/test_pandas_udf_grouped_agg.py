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

from pyspark.rdd import PythonEvalType
from pyspark.sql import Row
from pyspark.sql.functions import array, explode, col, lit, mean, sum, \
    udf, pandas_udf, PandasUDFType
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, have_pyarrow, \
    pandas_requirement_message, pyarrow_requirement_message
from pyspark.testing.utils import QuietTest

if have_pandas:
    import pandas as pd
    from pandas.util.testing import assert_frame_equal


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
class GroupedAggPandasUDFTests(ReusedSQLTestCase):

    @property
    def data(self):
        return self.spark.range(10).toDF('id') \
            .withColumn("vs", array([lit(i * 1.0) + col('id') for i in range(20, 30)])) \
            .withColumn("v", explode(col('vs'))) \
            .drop('vs') \
            .withColumn('w', lit(1.0))

    @property
    def python_plus_one(self):
        @udf('double')
        def plus_one(v):
            assert isinstance(v, (int, float))
            return v + 1
        return plus_one

    @property
    def pandas_scalar_plus_two(self):
        @pandas_udf('double', PandasUDFType.SCALAR)
        def plus_two(v):
            assert isinstance(v, pd.Series)
            return v + 2
        return plus_two

    @property
    def pandas_agg_mean_udf(self):
        @pandas_udf('double', PandasUDFType.GROUPED_AGG)
        def avg(v):
            return v.mean()
        return avg

    @property
    def pandas_agg_sum_udf(self):
        @pandas_udf('double', PandasUDFType.GROUPED_AGG)
        def sum(v):
            return v.sum()
        return sum

    @property
    def pandas_agg_weighted_mean_udf(self):
        import numpy as np

        @pandas_udf('double', PandasUDFType.GROUPED_AGG)
        def weighted_mean(v, w):
            return np.average(v, weights=w)
        return weighted_mean

    def test_manual(self):
        df = self.data
        sum_udf = self.pandas_agg_sum_udf
        mean_udf = self.pandas_agg_mean_udf
        mean_arr_udf = pandas_udf(
            self.pandas_agg_mean_udf.func,
            ArrayType(self.pandas_agg_mean_udf.returnType),
            self.pandas_agg_mean_udf.evalType)

        result1 = df.groupby('id').agg(
            sum_udf(df.v),
            mean_udf(df.v),
            mean_arr_udf(array(df.v))).sort('id')
        expected1 = self.spark.createDataFrame(
            [[0, 245.0, 24.5, [24.5]],
             [1, 255.0, 25.5, [25.5]],
             [2, 265.0, 26.5, [26.5]],
             [3, 275.0, 27.5, [27.5]],
             [4, 285.0, 28.5, [28.5]],
             [5, 295.0, 29.5, [29.5]],
             [6, 305.0, 30.5, [30.5]],
             [7, 315.0, 31.5, [31.5]],
             [8, 325.0, 32.5, [32.5]],
             [9, 335.0, 33.5, [33.5]]],
            ['id', 'sum(v)', 'avg(v)', 'avg(array(v))'])

        assert_frame_equal(expected1.toPandas(), result1.toPandas())

    def test_basic(self):
        df = self.data
        weighted_mean_udf = self.pandas_agg_weighted_mean_udf

        # Groupby one column and aggregate one UDF with literal
        result1 = df.groupby('id').agg(weighted_mean_udf(df.v, lit(1.0))).sort('id')
        expected1 = df.groupby('id').agg(mean(df.v).alias('weighted_mean(v, 1.0)')).sort('id')
        assert_frame_equal(expected1.toPandas(), result1.toPandas())

        # Groupby one expression and aggregate one UDF with literal
        result2 = df.groupby((col('id') + 1)).agg(weighted_mean_udf(df.v, lit(1.0)))\
            .sort(df.id + 1)
        expected2 = df.groupby((col('id') + 1))\
            .agg(mean(df.v).alias('weighted_mean(v, 1.0)')).sort(df.id + 1)
        assert_frame_equal(expected2.toPandas(), result2.toPandas())

        # Groupby one column and aggregate one UDF without literal
        result3 = df.groupby('id').agg(weighted_mean_udf(df.v, df.w)).sort('id')
        expected3 = df.groupby('id').agg(mean(df.v).alias('weighted_mean(v, w)')).sort('id')
        assert_frame_equal(expected3.toPandas(), result3.toPandas())

        # Groupby one expression and aggregate one UDF without literal
        result4 = df.groupby((col('id') + 1).alias('id'))\
            .agg(weighted_mean_udf(df.v, df.w))\
            .sort('id')
        expected4 = df.groupby((col('id') + 1).alias('id'))\
            .agg(mean(df.v).alias('weighted_mean(v, w)'))\
            .sort('id')
        assert_frame_equal(expected4.toPandas(), result4.toPandas())

    def test_unsupported_types(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegexp(NotImplementedError, 'not supported'):
                pandas_udf(
                    lambda x: x,
                    ArrayType(ArrayType(TimestampType())),
                    PandasUDFType.GROUPED_AGG)

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(NotImplementedError, 'not supported'):
                @pandas_udf('mean double, std double', PandasUDFType.GROUPED_AGG)
                def mean_and_std_udf(v):
                    return v.mean(), v.std()

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(NotImplementedError, 'not supported'):
                @pandas_udf(MapType(DoubleType(), DoubleType()), PandasUDFType.GROUPED_AGG)
                def mean_and_std_udf(v):
                    return {v.mean(): v.std()}

    def test_alias(self):
        df = self.data
        mean_udf = self.pandas_agg_mean_udf

        result1 = df.groupby('id').agg(mean_udf(df.v).alias('mean_alias'))
        expected1 = df.groupby('id').agg(mean(df.v).alias('mean_alias'))

        assert_frame_equal(expected1.toPandas(), result1.toPandas())

    def test_mixed_sql(self):
        """
        Test mixing group aggregate pandas UDF with sql expression.
        """
        df = self.data
        sum_udf = self.pandas_agg_sum_udf

        # Mix group aggregate pandas UDF with sql expression
        result1 = (df.groupby('id')
                   .agg(sum_udf(df.v) + 1)
                   .sort('id'))
        expected1 = (df.groupby('id')
                     .agg(sum(df.v) + 1)
                     .sort('id'))

        # Mix group aggregate pandas UDF with sql expression (order swapped)
        result2 = (df.groupby('id')
                     .agg(sum_udf(df.v + 1))
                     .sort('id'))

        expected2 = (df.groupby('id')
                       .agg(sum(df.v + 1))
                       .sort('id'))

        # Wrap group aggregate pandas UDF with two sql expressions
        result3 = (df.groupby('id')
                   .agg(sum_udf(df.v + 1) + 2)
                   .sort('id'))
        expected3 = (df.groupby('id')
                     .agg(sum(df.v + 1) + 2)
                     .sort('id'))

        assert_frame_equal(expected1.toPandas(), result1.toPandas())
        assert_frame_equal(expected2.toPandas(), result2.toPandas())
        assert_frame_equal(expected3.toPandas(), result3.toPandas())

    def test_mixed_udfs(self):
        """
        Test mixing group aggregate pandas UDF with python UDF and scalar pandas UDF.
        """
        df = self.data
        plus_one = self.python_plus_one
        plus_two = self.pandas_scalar_plus_two
        sum_udf = self.pandas_agg_sum_udf

        # Mix group aggregate pandas UDF and python UDF
        result1 = (df.groupby('id')
                   .agg(plus_one(sum_udf(df.v)))
                   .sort('id'))
        expected1 = (df.groupby('id')
                     .agg(plus_one(sum(df.v)))
                     .sort('id'))

        # Mix group aggregate pandas UDF and python UDF (order swapped)
        result2 = (df.groupby('id')
                   .agg(sum_udf(plus_one(df.v)))
                   .sort('id'))
        expected2 = (df.groupby('id')
                     .agg(sum(plus_one(df.v)))
                     .sort('id'))

        # Mix group aggregate pandas UDF and scalar pandas UDF
        result3 = (df.groupby('id')
                   .agg(sum_udf(plus_two(df.v)))
                   .sort('id'))
        expected3 = (df.groupby('id')
                     .agg(sum(plus_two(df.v)))
                     .sort('id'))

        # Mix group aggregate pandas UDF and scalar pandas UDF (order swapped)
        result4 = (df.groupby('id')
                   .agg(plus_two(sum_udf(df.v)))
                   .sort('id'))
        expected4 = (df.groupby('id')
                     .agg(plus_two(sum(df.v)))
                     .sort('id'))

        # Wrap group aggregate pandas UDF with two python UDFs and use python UDF in groupby
        result5 = (df.groupby(plus_one(df.id))
                   .agg(plus_one(sum_udf(plus_one(df.v))))
                   .sort('plus_one(id)'))
        expected5 = (df.groupby(plus_one(df.id))
                     .agg(plus_one(sum(plus_one(df.v))))
                     .sort('plus_one(id)'))

        # Wrap group aggregate pandas UDF with two scala pandas UDF and user scala pandas UDF in
        # groupby
        result6 = (df.groupby(plus_two(df.id))
                   .agg(plus_two(sum_udf(plus_two(df.v))))
                   .sort('plus_two(id)'))
        expected6 = (df.groupby(plus_two(df.id))
                     .agg(plus_two(sum(plus_two(df.v))))
                     .sort('plus_two(id)'))

        assert_frame_equal(expected1.toPandas(), result1.toPandas())
        assert_frame_equal(expected2.toPandas(), result2.toPandas())
        assert_frame_equal(expected3.toPandas(), result3.toPandas())
        assert_frame_equal(expected4.toPandas(), result4.toPandas())
        assert_frame_equal(expected5.toPandas(), result5.toPandas())
        assert_frame_equal(expected6.toPandas(), result6.toPandas())

    def test_multiple_udfs(self):
        """
        Test multiple group aggregate pandas UDFs in one agg function.
        """
        df = self.data
        mean_udf = self.pandas_agg_mean_udf
        sum_udf = self.pandas_agg_sum_udf
        weighted_mean_udf = self.pandas_agg_weighted_mean_udf

        result1 = (df.groupBy('id')
                   .agg(mean_udf(df.v),
                        sum_udf(df.v),
                        weighted_mean_udf(df.v, df.w))
                   .sort('id')
                   .toPandas())
        expected1 = (df.groupBy('id')
                     .agg(mean(df.v),
                          sum(df.v),
                          mean(df.v).alias('weighted_mean(v, w)'))
                     .sort('id')
                     .toPandas())

        assert_frame_equal(expected1, result1)

    def test_complex_groupby(self):
        df = self.data
        sum_udf = self.pandas_agg_sum_udf
        plus_one = self.python_plus_one
        plus_two = self.pandas_scalar_plus_two

        # groupby one expression
        result1 = df.groupby(df.v % 2).agg(sum_udf(df.v))
        expected1 = df.groupby(df.v % 2).agg(sum(df.v))

        # empty groupby
        result2 = df.groupby().agg(sum_udf(df.v))
        expected2 = df.groupby().agg(sum(df.v))

        # groupby one column and one sql expression
        result3 = df.groupby(df.id, df.v % 2).agg(sum_udf(df.v)).orderBy(df.id, df.v % 2)
        expected3 = df.groupby(df.id, df.v % 2).agg(sum(df.v)).orderBy(df.id, df.v % 2)

        # groupby one python UDF
        result4 = df.groupby(plus_one(df.id)).agg(sum_udf(df.v))
        expected4 = df.groupby(plus_one(df.id)).agg(sum(df.v))

        # groupby one scalar pandas UDF
        result5 = df.groupby(plus_two(df.id)).agg(sum_udf(df.v))
        expected5 = df.groupby(plus_two(df.id)).agg(sum(df.v))

        # groupby one expression and one python UDF
        result6 = df.groupby(df.v % 2, plus_one(df.id)).agg(sum_udf(df.v))
        expected6 = df.groupby(df.v % 2, plus_one(df.id)).agg(sum(df.v))

        # groupby one expression and one scalar pandas UDF
        result7 = df.groupby(df.v % 2, plus_two(df.id)).agg(sum_udf(df.v)).sort('sum(v)')
        expected7 = df.groupby(df.v % 2, plus_two(df.id)).agg(sum(df.v)).sort('sum(v)')

        assert_frame_equal(expected1.toPandas(), result1.toPandas())
        assert_frame_equal(expected2.toPandas(), result2.toPandas())
        assert_frame_equal(expected3.toPandas(), result3.toPandas())
        assert_frame_equal(expected4.toPandas(), result4.toPandas())
        assert_frame_equal(expected5.toPandas(), result5.toPandas())
        assert_frame_equal(expected6.toPandas(), result6.toPandas())
        assert_frame_equal(expected7.toPandas(), result7.toPandas())

    def test_complex_expressions(self):
        df = self.data
        plus_one = self.python_plus_one
        plus_two = self.pandas_scalar_plus_two
        sum_udf = self.pandas_agg_sum_udf

        # Test complex expressions with sql expression, python UDF and
        # group aggregate pandas UDF
        result1 = (df.withColumn('v1', plus_one(df.v))
                   .withColumn('v2', df.v + 2)
                   .groupby(df.id, df.v % 2)
                   .agg(sum_udf(col('v')),
                        sum_udf(col('v1') + 3),
                        sum_udf(col('v2')) + 5,
                        plus_one(sum_udf(col('v1'))),
                        sum_udf(plus_one(col('v2'))))
                   .sort('id')
                   .toPandas())

        expected1 = (df.withColumn('v1', df.v + 1)
                     .withColumn('v2', df.v + 2)
                     .groupby(df.id, df.v % 2)
                     .agg(sum(col('v')),
                          sum(col('v1') + 3),
                          sum(col('v2')) + 5,
                          plus_one(sum(col('v1'))),
                          sum(plus_one(col('v2'))))
                     .sort('id')
                     .toPandas())

        # Test complex expressions with sql expression, scala pandas UDF and
        # group aggregate pandas UDF
        result2 = (df.withColumn('v1', plus_one(df.v))
                   .withColumn('v2', df.v + 2)
                   .groupby(df.id, df.v % 2)
                   .agg(sum_udf(col('v')),
                        sum_udf(col('v1') + 3),
                        sum_udf(col('v2')) + 5,
                        plus_two(sum_udf(col('v1'))),
                        sum_udf(plus_two(col('v2'))))
                   .sort('id')
                   .toPandas())

        expected2 = (df.withColumn('v1', df.v + 1)
                     .withColumn('v2', df.v + 2)
                     .groupby(df.id, df.v % 2)
                     .agg(sum(col('v')),
                          sum(col('v1') + 3),
                          sum(col('v2')) + 5,
                          plus_two(sum(col('v1'))),
                          sum(plus_two(col('v2'))))
                     .sort('id')
                     .toPandas())

        # Test sequential groupby aggregate
        result3 = (df.groupby('id')
                   .agg(sum_udf(df.v).alias('v'))
                   .groupby('id')
                   .agg(sum_udf(col('v')))
                   .sort('id')
                   .toPandas())

        expected3 = (df.groupby('id')
                     .agg(sum(df.v).alias('v'))
                     .groupby('id')
                     .agg(sum(col('v')))
                     .sort('id')
                     .toPandas())

        assert_frame_equal(expected1, result1)
        assert_frame_equal(expected2, result2)
        assert_frame_equal(expected3, result3)

    def test_retain_group_columns(self):
        with self.sql_conf({"spark.sql.retainGroupColumns": False}):
            df = self.data
            sum_udf = self.pandas_agg_sum_udf

            result1 = df.groupby(df.id).agg(sum_udf(df.v))
            expected1 = df.groupby(df.id).agg(sum(df.v))
            assert_frame_equal(expected1.toPandas(), result1.toPandas())

    def test_array_type(self):
        df = self.data

        array_udf = pandas_udf(lambda x: [1.0, 2.0], 'array<double>', PandasUDFType.GROUPED_AGG)
        result1 = df.groupby('id').agg(array_udf(df['v']).alias('v2'))
        self.assertEquals(result1.first()['v2'], [1.0, 2.0])

    def test_invalid_args(self):
        df = self.data
        plus_one = self.python_plus_one
        mean_udf = self.pandas_agg_mean_udf

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    AnalysisException,
                    'nor.*aggregate function'):
                df.groupby(df.id).agg(plus_one(df.v)).collect()

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    AnalysisException,
                    'aggregate function.*argument.*aggregate function'):
                df.groupby(df.id).agg(mean_udf(mean_udf(df.v))).collect()

        with QuietTest(self.sc):
            with self.assertRaisesRegexp(
                    AnalysisException,
                    'mixture.*aggregate function.*group aggregate pandas UDF'):
                df.groupby(df.id).agg(mean_udf(df.v), mean(df.v)).collect()

    def test_register_vectorized_udf_basic(self):
        sum_pandas_udf = pandas_udf(
            lambda v: v.sum(), "integer", PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF)

        self.assertEqual(sum_pandas_udf.evalType, PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF)
        group_agg_pandas_udf = self.spark.udf.register("sum_pandas_udf", sum_pandas_udf)
        self.assertEqual(group_agg_pandas_udf.evalType, PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF)
        q = "SELECT sum_pandas_udf(v1) FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2"
        actual = sorted(map(lambda r: r[0], self.spark.sql(q).collect()))
        expected = [1, 5]
        self.assertEqual(actual, expected)

    def test_grouped_with_empty_partition(self):
        data = [Row(id=1, x=2), Row(id=1, x=3), Row(id=2, x=4)]
        expected = [Row(id=1, sum=5), Row(id=2, x=4)]
        num_parts = len(data) + 1
        df = self.spark.createDataFrame(self.sc.parallelize(data, numSlices=num_parts))

        f = pandas_udf(lambda x: x.sum(),
                       'int', PandasUDFType.GROUPED_AGG)

        result = df.groupBy('id').agg(f(df['x']).alias('sum')).collect()
        self.assertEqual(result, expected)


if __name__ == "__main__":
    from pyspark.sql.tests.test_pandas_udf_grouped_agg import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
