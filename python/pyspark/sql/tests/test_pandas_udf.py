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

from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.types import *
from pyspark.sql.utils import ParseException
from pyspark.rdd import PythonEvalType
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, have_pyarrow, \
    pandas_requirement_message, pyarrow_requirement_message
from pyspark.testing.utils import QuietTest

from py4j.protocol import Py4JJavaError


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)
class PandasUDFTests(ReusedSQLTestCase):

    def test_pandas_udf_basic(self):
        udf = pandas_udf(lambda x: x, DoubleType())
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, DoubleType(), PandasUDFType.SCALAR)
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, 'double', PandasUDFType.SCALAR)
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, StructType([StructField("v", DoubleType())]),
                         PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, 'v double', PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, 'v double',
                         functionType=PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, returnType='v double',
                         functionType=PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

    def test_pandas_udf_decorator(self):
        @pandas_udf(DoubleType())
        def foo(x):
            return x
        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        @pandas_udf(returnType=DoubleType())
        def foo(x):
            return x
        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        schema = StructType([StructField("v", DoubleType())])

        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x
        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        @pandas_udf('v double', PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x
        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        @pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x
        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        @pandas_udf(returnType='double', functionType=PandasUDFType.SCALAR)
        def foo(x):
            return x
        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        @pandas_udf(returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x
        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

    def test_udf_wrong_arg(self):
        with QuietTest(self.sc):
            with self.assertRaises(ParseException):
                @pandas_udf('blah')
                def foo(x):
                    return x
            with self.assertRaisesRegexp(ValueError, 'Invalid returnType.*None'):
                @pandas_udf(functionType=PandasUDFType.SCALAR)
                def foo(x):
                    return x
            with self.assertRaisesRegexp(ValueError, 'Invalid functionType'):
                @pandas_udf('double', 100)
                def foo(x):
                    return x

            with self.assertRaisesRegexp(ValueError, '0-arg pandas_udfs.*not.*supported'):
                pandas_udf(lambda: 1, LongType(), PandasUDFType.SCALAR)
            with self.assertRaisesRegexp(ValueError, '0-arg pandas_udfs.*not.*supported'):
                @pandas_udf(LongType(), PandasUDFType.SCALAR)
                def zero_with_type():
                    return 1

            with self.assertRaisesRegexp(TypeError, 'Invalid returnType'):
                @pandas_udf(returnType=PandasUDFType.GROUPED_MAP)
                def foo(df):
                    return df
            with self.assertRaisesRegexp(TypeError, 'Invalid returnType'):
                @pandas_udf(returnType='double', functionType=PandasUDFType.GROUPED_MAP)
                def foo(df):
                    return df
            with self.assertRaisesRegexp(ValueError, 'Invalid function'):
                @pandas_udf(returnType='k int, v double', functionType=PandasUDFType.GROUPED_MAP)
                def foo(k, v, w):
                    return k

    def test_stopiteration_in_udf(self):
        def foo(x):
            raise StopIteration()

        def foofoo(x, y):
            raise StopIteration()

        exc_message = "Caught StopIteration thrown from user's code; failing the task"
        df = self.spark.range(0, 100)

        # plain udf (test for SPARK-23754)
        self.assertRaisesRegexp(
            Py4JJavaError,
            exc_message,
            df.withColumn('v', udf(foo)('id')).collect
        )

        # pandas scalar udf
        self.assertRaisesRegexp(
            Py4JJavaError,
            exc_message,
            df.withColumn(
                'v', pandas_udf(foo, 'double', PandasUDFType.SCALAR)('id')
            ).collect
        )

        # pandas grouped map
        self.assertRaisesRegexp(
            Py4JJavaError,
            exc_message,
            df.groupBy('id').apply(
                pandas_udf(foo, df.schema, PandasUDFType.GROUPED_MAP)
            ).collect
        )

        self.assertRaisesRegexp(
            Py4JJavaError,
            exc_message,
            df.groupBy('id').apply(
                pandas_udf(foofoo, df.schema, PandasUDFType.GROUPED_MAP)
            ).collect
        )

        # pandas grouped agg
        self.assertRaisesRegexp(
            Py4JJavaError,
            exc_message,
            df.groupBy('id').agg(
                pandas_udf(foo, 'double', PandasUDFType.GROUPED_AGG)('id')
            ).collect
        )

    def test_pandas_udf_detect_unsafe_type_conversion(self):
        import pandas as pd
        import numpy as np

        values = [1.0] * 3
        pdf = pd.DataFrame({'A': values})
        df = self.spark.createDataFrame(pdf).repartition(1)

        @pandas_udf(returnType="int")
        def udf(column):
            return pd.Series(np.linspace(0, 1, len(column)))

        # Since 0.11.0, PyArrow supports the feature to raise an error for unsafe cast.
        with self.sql_conf({
                "spark.sql.execution.pandas.arrowSafeTypeConversion": True}):
            with self.assertRaisesRegexp(Exception,
                                         "Exception thrown when converting pandas.Series"):
                df.select(['A']).withColumn('udf', udf('A')).collect()

        # Disabling Arrow safe type check.
        with self.sql_conf({
                "spark.sql.execution.pandas.arrowSafeTypeConversion": False}):
            df.select(['A']).withColumn('udf', udf('A')).collect()

    def test_pandas_udf_arrow_overflow(self):
        import pandas as pd

        df = self.spark.range(0, 1)

        @pandas_udf(returnType="byte")
        def udf(column):
            return pd.Series([128] * len(column))

        # When enabling safe type check, Arrow 0.11.0+ disallows overflow cast.
        with self.sql_conf({
                "spark.sql.execution.pandas.arrowSafeTypeConversion": True}):
            with self.assertRaisesRegexp(Exception,
                                         "Exception thrown when converting pandas.Series"):
                df.withColumn('udf', udf('id')).collect()

        # Disabling safe type check, let Arrow do the cast anyway.
        with self.sql_conf({"spark.sql.execution.pandas.arrowSafeTypeConversion": False}):
            df.withColumn('udf', udf('id')).collect()


if __name__ == "__main__":
    from pyspark.sql.tests.test_pandas_udf import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
