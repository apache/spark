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

    def test_simple(self):

        pdf1 = pd.DataFrame.from_dict({
            'id': ['a', 'a', 'b', 'b'],
            't': [1.0, 2.0, 1.0, 2.0],
            'x': [10, 10, 30, 40]

        })

        pdf2 = pd.DataFrame.from_dict({
            'id2': ['a', 'b'],
            't': [0.5, 0.5],
            'y': [7.0, 8.0]
        })

        output_schema = StructType([
            StructField("id", StringType()),
            StructField("t", DoubleType()),
            StructField("x", IntegerType()),
            StructField("y", DoubleType()),
        ])


        @pandas_udf(output_schema, functionType=PandasUDFType.COGROUPED_MAP)
        def pandas_merge(left, right):
            print(left)
            print("#########")
            print(right)
            print("#########")
            import pandas as pd
            left.sort_values(by='t', inplace=True)
            right.sort_values(by='t', inplace=True)
            result = pd.merge_asof(left, right, on='t').reset_index()
            print(result)
            return result


        df1 = self.spark.createDataFrame(pdf1)
        df2 = self.spark.createDataFrame(pdf2)

        gd1 = df1.groupby('id')
        gd2 = df2.groupby('id2')

        gd1\
            .cogroup(gd2)\
            .apply(pandas_merge)\
            .explain()

