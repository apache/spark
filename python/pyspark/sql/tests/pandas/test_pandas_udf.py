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
import datetime
from typing import cast

from pyspark.sql.functions import udf, pandas_udf, PandasUDFType, assert_true, lit
from pyspark.sql.types import (
    DoubleType,
    StructType,
    StructField,
    LongType,
    DayTimeIntervalType,
    VariantType,
)
from pyspark.errors import ParseException, PythonException, PySparkTypeError
from pyspark.util import PythonEvalType
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class PandasUDFTestsMixin:
    def test_pandas_udf_basic(self):
        udf = pandas_udf(lambda x: x, DoubleType())
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, VariantType())
        self.assertEqual(udf.returnType, VariantType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, DoubleType(), PandasUDFType.SCALAR)
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, VariantType(), PandasUDFType.SCALAR)
        self.assertEqual(udf.returnType, VariantType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(
            lambda x: x, StructType([StructField("v", DoubleType())]), PandasUDFType.GROUPED_MAP
        )
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(
            lambda x: x, StructType([StructField("v", VariantType())]), PandasUDFType.GROUPED_MAP
        )
        self.assertEqual(udf.returnType, StructType([StructField("v", VariantType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

    def test_pandas_udf_basic_with_return_type_string(self):
        udf = pandas_udf(lambda x: x, "double", PandasUDFType.SCALAR)
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, "variant", PandasUDFType.SCALAR)
        self.assertEqual(udf.returnType, VariantType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, "v double", PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, "v variant", PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", VariantType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, "v double", functionType=PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, "v variant", functionType=PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", VariantType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(lambda x: x, returnType="v double", functionType=PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType())]))
        self.assertEqual(udf.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        udf = pandas_udf(
            lambda x: x, returnType="v variant", functionType=PandasUDFType.GROUPED_MAP
        )
        self.assertEqual(udf.returnType, StructType([StructField("v", VariantType())]))
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

        @pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x

        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        @pandas_udf(returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x

        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

    def test_pandas_udf_decorator_with_return_type_string(self):
        schema = StructType([StructField("v", DoubleType())])

        @pandas_udf("v double", PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x

        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)

        @pandas_udf(returnType="double", functionType=PandasUDFType.SCALAR)
        def foo(x):
            return x

        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

    def test_udf_wrong_arg(self):
        with self.quiet():
            self.check_udf_wrong_arg()

            with self.assertRaises(ParseException):

                @pandas_udf("blah")
                def foo(x):
                    return x

            with self.assertRaises(PySparkTypeError) as pe:

                @pandas_udf(returnType="double", functionType=PandasUDFType.GROUPED_MAP)
                def foo(df):
                    return df

            self.check_error(
                exception=pe.exception,
                errorClass="INVALID_RETURN_TYPE_FOR_PANDAS_UDF",
                messageParameters={
                    "eval_type": "SQL_GROUPED_MAP_PANDAS_UDF "
                    "or SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE",
                    "return_type": "DoubleType()",
                },
            )

            with self.assertRaisesRegex(ValueError, "Invalid function"):

                @pandas_udf(returnType="k int, v double", functionType=PandasUDFType.GROUPED_MAP)
                def foo(k, v, w):
                    return k

    def check_udf_wrong_arg(self):
        with self.assertRaises(PySparkTypeError) as pe:

            @pandas_udf(functionType=PandasUDFType.SCALAR)
            def foo(x):
                return x

        self.check_error(
            exception=pe.exception,
            errorClass="CANNOT_BE_NONE",
            messageParameters={"arg_name": "returnType"},
        )

        with self.assertRaises(PySparkTypeError) as pe:

            @pandas_udf("double", 100)
            def foo(x):
                return x

        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_PANDAS_UDF_TYPE",
            messageParameters={"arg_name": "functionType", "arg_type": "100"},
        )

        with self.assertRaisesRegex(ValueError, "0-arg pandas_udfs.*not.*supported"):
            pandas_udf(lambda: 1, LongType(), PandasUDFType.SCALAR)
        with self.assertRaisesRegex(ValueError, "0-arg pandas_udfs.*not.*supported"):

            @pandas_udf(LongType(), PandasUDFType.SCALAR)
            def zero_with_type():
                return 1

        with self.assertRaises(PySparkTypeError) as pe:

            @pandas_udf(returnType=PandasUDFType.GROUPED_MAP)
            def foo(df):
                return df

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_DATATYPE_OR_STR",
            messageParameters={"arg_name": "returnType", "arg_type": "int"},
        )

    def test_stopiteration_in_udf(self):
        def foo(x):
            raise StopIteration()

        exc_message = "StopIteration"
        df = self.spark.range(0, 100)

        # plain udf (test for SPARK-23754)
        self.assertRaisesRegex(
            PythonException, exc_message, df.withColumn("v", udf(foo)("id")).collect
        )

        # pandas scalar udf
        self.assertRaisesRegex(
            PythonException,
            exc_message,
            df.withColumn("v", pandas_udf(foo, "double", PandasUDFType.SCALAR)("id")).collect,
        )

    def test_stopiteration_in_grouped_map(self):
        def foo(x):
            raise StopIteration()

        def foofoo(x, y):
            raise StopIteration()

        exc_message = "StopIteration"
        df = self.spark.range(0, 100)

        # pandas grouped map
        self.assertRaisesRegex(
            PythonException,
            exc_message,
            df.groupBy("id").apply(pandas_udf(foo, df.schema, PandasUDFType.GROUPED_MAP)).collect,
        )

        self.assertRaisesRegex(
            PythonException,
            exc_message,
            df.groupBy("id")
            .apply(pandas_udf(foofoo, df.schema, PandasUDFType.GROUPED_MAP))
            .collect,
        )

    def test_stopiteration_in_grouped_agg(self):
        def foo(x):
            raise StopIteration()

        exc_message = "StopIteration"
        df = self.spark.range(0, 100)

        # pandas grouped agg
        self.assertRaisesRegex(
            PythonException,
            exc_message,
            df.groupBy("id")
            .agg(pandas_udf(foo, "double", PandasUDFType.GROUPED_AGG)("id"))
            .collect,
        )

    def test_pandas_udf_detect_unsafe_type_conversion(self):
        import pandas as pd
        import numpy as np

        values = [1.0] * 3
        pdf = pd.DataFrame({"A": values})
        df = self.spark.createDataFrame(pdf).repartition(1)

        @pandas_udf(returnType="int")
        def udf(column):
            return pd.Series(np.linspace(0, 1, len(column)))

        # Since 0.11.0, PyArrow supports the feature to raise an error for unsafe cast.
        with self.sql_conf({"spark.sql.execution.pandas.convertToArrowArraySafely": True}):
            with self.assertRaisesRegex(
                Exception, "Exception thrown when converting pandas.Series"
            ):
                df.select(["A"]).withColumn("udf", udf("A")).collect()

        # Disabling Arrow safe type check.
        with self.sql_conf({"spark.sql.execution.pandas.convertToArrowArraySafely": False}):
            df.select(["A"]).withColumn("udf", udf("A")).collect()

    def test_pandas_udf_arrow_overflow(self):
        import pandas as pd

        df = self.spark.range(0, 1)

        @pandas_udf(returnType="byte")
        def udf(column):
            return pd.Series([128] * len(column))

        # When enabling safe type check, Arrow 0.11.0+ disallows overflow cast.
        with self.sql_conf({"spark.sql.execution.pandas.convertToArrowArraySafely": True}):
            with self.assertRaisesRegex(
                Exception, "Exception thrown when converting pandas.Series"
            ):
                df.withColumn("udf", udf("id")).collect()

        # Disabling safe type check, let Arrow do the cast anyway.
        with self.sql_conf({"spark.sql.execution.pandas.convertToArrowArraySafely": False}):
            df.withColumn("udf", udf("id")).collect()

    def test_pandas_udf_timestamp_ntz(self):
        # SPARK-36626: Test TimestampNTZ in pandas UDF
        @pandas_udf(returnType="timestamp_ntz")
        def noop(s):
            assert s.iloc[0] == datetime.datetime(1970, 1, 1, 0, 0)
            return s

        with self.sql_conf({"spark.sql.session.timeZone": "Asia/Hong_Kong"}):
            df = self.spark.createDataFrame(
                [(datetime.datetime(1970, 1, 1, 0, 0),)], schema="dt timestamp_ntz"
            ).select(noop("dt").alias("dt"))

            df.selectExpr("assert_true('1970-01-01 00:00:00' == CAST(dt AS STRING))").collect()
            self.assertEqual(df.schema[0].dataType.typeName(), "timestamp_ntz")
            self.assertEqual(df.first()[0], datetime.datetime(1970, 1, 1, 0, 0))

    def test_pandas_udf_day_time_interval_type(self):
        # SPARK-37277: Test DayTimeIntervalType in pandas UDF
        import pandas as pd

        @pandas_udf(DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))
        def noop(s: pd.Series) -> pd.Series:
            assert s.iloc[0] == datetime.timedelta(microseconds=123)
            return s

        df = self.spark.createDataFrame(
            [(datetime.timedelta(microseconds=123),)], schema="td interval day to second"
        ).select(noop("td").alias("td"))

        df.select(
            assert_true(lit("INTERVAL '0 00:00:00.000123' DAY TO SECOND") == df.td.cast("string"))
        ).collect()
        self.assertEqual(df.schema[0].dataType.simpleString(), "interval day to second")
        self.assertEqual(df.first()[0], datetime.timedelta(microseconds=123))

    def test_pandas_udf_return_type_error(self):
        import pandas as pd

        @pandas_udf("s string")
        def upper(s: pd.Series) -> pd.Series:
            return s.str.upper()

        df = self.spark.createDataFrame([("a",)], schema="s string")

        self.assertRaisesRegex(
            PythonException, "Invalid return type", df.select(upper("s")).collect
        )

    def test_pandas_udf_empty_frame(self):
        import pandas as pd

        empty_df = self.spark.createDataFrame([], "id long")

        @pandas_udf("long")
        def add1(x: pd.Series) -> pd.Series:
            return x + 1

        result = empty_df.select(add1("id"))
        self.assertEqual(result.collect(), [])


class PandasUDFTests(PandasUDFTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_pandas_udf import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
