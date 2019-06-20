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
    def data(self):
        return self.spark.range(10).toDF('id') \
            .withColumn("vs", array([lit(i) for i in range(20, 30)])) \
            .withColumn("v", explode(col('vs'))).drop('vs')

    def test_supported_types(self):

        df1 = self.spark.createDataFrame(
            pd.DataFrame.from_dict({
            'id' : [1,1,10, 10, 1,1],
            'x' : [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        }))

        df2 = self.spark.createDataFrame(
            pd.DataFrame.from_dict({
                'id2': [1,1,10, 10, 1,1],
                'a': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
            }))

        output_schema = StructType([
            StructField("id", LongType()),
            StructField("x", DoubleType()),
        ])

        @pandas_udf(output_schema, functionType=PandasUDFType.COGROUPED_MAP)
        def foo(left, right):
            print("hello")
            print(left)
            print("goodbye")
            print(right)
            return left

        output_schema2 = StructType([
            StructField("id", LongType())
        ])
        @pandas_udf(output_schema, functionType=PandasUDFType.GROUPED_MAP)
        def foo2(key, df):
            print('key is ' + str(key))
            print(df)
            return df


        df1.groupby(col("id") > 5)\
            .apply(foo2)\
            .show()




if __name__ == "__main__":
    from pyspark.sql.tests.test_pandas_udf_cogrouped_map import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
