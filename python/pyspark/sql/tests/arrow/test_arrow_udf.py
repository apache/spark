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

# TODO: import arrow_udf from public API
from pyspark.sql.pandas.functions import arrow_udf, ArrowUDFType, PandasUDFType
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StructType,
    StructField,
    LongType,
    DayTimeIntervalType,
    VariantType,
)
from pyspark.errors import ParseException, PySparkTypeError
from pyspark.util import PythonEvalType
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pyarrow,
    pyarrow_requirement_message,
)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowUDFTestsMixin:
    def test_arrow_udf_basic(self):
        udf = arrow_udf(lambda x: x, DoubleType())
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        udf = arrow_udf(lambda x: x, VariantType())
        self.assertEqual(udf.returnType, VariantType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        udf = arrow_udf(lambda x: x, DoubleType(), ArrowUDFType.SCALAR)
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        udf = arrow_udf(lambda x: x, VariantType(), ArrowUDFType.SCALAR)
        self.assertEqual(udf.returnType, VariantType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

    def test_arrow_udf_basic_with_return_type_string(self):
        udf = arrow_udf(lambda x: x, "double", ArrowUDFType.SCALAR)
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        udf = arrow_udf(lambda x: x, "variant", ArrowUDFType.SCALAR)
        self.assertEqual(udf.returnType, VariantType())
        self.assertEqual(udf.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

    def test_arrow_udf_decorator(self):
        @arrow_udf(DoubleType())
        def foo(x):
            return x

        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        @arrow_udf(returnType=DoubleType())
        def foo(x):
            return x

        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

    def test_arrow_udf_decorator_with_return_type_string(self):
        schema = StructType([StructField("v", DoubleType())])

        @arrow_udf("v double", ArrowUDFType.SCALAR)
        def foo(x):
            return x

        self.assertEqual(foo.returnType, schema)
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        @arrow_udf(returnType="double", functionType=ArrowUDFType.SCALAR)
        def foo(x):
            return x

        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

    def test_arrow_udf_wrong_arg(self):
        with self.quiet():
            with self.assertRaises(ParseException):

                @arrow_udf("blah")
                def foo(x):
                    return x

            with self.assertRaises(PySparkTypeError) as pe:

                @arrow_udf(returnType="double", functionType=PandasUDFType.SCALAR)
                def foo(df):
                    return df

            self.check_error(
                exception=pe.exception,
                errorClass="INVALID_PANDAS_UDF_TYPE",
                messageParameters={
                    "arg_name": "functionType",
                    "arg_type": "200",
                },
            )

            with self.assertRaises(PySparkTypeError) as pe:

                @arrow_udf(functionType=ArrowUDFType.SCALAR)
                def foo(x):
                    return x

            self.check_error(
                exception=pe.exception,
                errorClass="CANNOT_BE_NONE",
                messageParameters={"arg_name": "returnType"},
            )

            with self.assertRaises(PySparkTypeError) as pe:

                @arrow_udf("double", 100)
                def foo(x):
                    return x

            self.check_error(
                exception=pe.exception,
                errorClass="INVALID_PANDAS_UDF_TYPE",
                messageParameters={"arg_name": "functionType", "arg_type": "100"},
            )

            with self.assertRaises(PySparkTypeError) as pe:

                @arrow_udf(returnType=PandasUDFType.GROUPED_MAP)
                def foo(df):
                    return df

            self.check_error(
                exception=pe.exception,
                errorClass="INVALID_PANDAS_UDF_TYPE",
                messageParameters={"arg_name": "functionType", "arg_type": "201"},
            )

            with self.assertRaisesRegex(ValueError, "0-arg arrow_udfs.*not.*supported"):
                arrow_udf(lambda: 1, LongType(), ArrowUDFType.SCALAR)

            with self.assertRaisesRegex(ValueError, "0-arg arrow_udfs.*not.*supported"):

                @arrow_udf(LongType(), ArrowUDFType.SCALAR)
                def zero_with_type():
                    return 1

    def test_arrow_udf_timestamp_ntz(self):
        import pyarrow as pa

        @arrow_udf(returnType="timestamp_ntz")
        def noop(s):
            assert isinstance(s, pa.Array)
            assert s[0].as_py() == datetime.datetime(1970, 1, 1, 0, 0)
            return s

        with self.sql_conf({"spark.sql.session.timeZone": "Asia/Hong_Kong"}):
            df = self.spark.createDataFrame(
                [(datetime.datetime(1970, 1, 1, 0, 0),)], schema="dt timestamp_ntz"
            ).select(noop("dt").alias("dt"))

            df.selectExpr("assert_true('1970-01-01 00:00:00' == CAST(dt AS STRING))").collect()
            self.assertEqual(df.schema[0].dataType.typeName(), "timestamp_ntz")
            self.assertEqual(df.first()[0], datetime.datetime(1970, 1, 1, 0, 0))

    def test_arrow_udf_day_time_interval_type(self):
        import pyarrow as pa

        @arrow_udf(DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))
        def noop(s: pa.Array) -> pa.Array:
            assert isinstance(s, pa.Array)
            assert s[0].as_py() == datetime.timedelta(microseconds=123)
            return s

        df = self.spark.createDataFrame(
            [(datetime.timedelta(microseconds=123),)], schema="td interval day to second"
        ).select(noop("td").alias("td"))

        df.select(
            F.assert_true(
                F.lit("INTERVAL '0 00:00:00.000123' DAY TO SECOND") == df.td.cast("string")
            )
        ).collect()
        self.assertEqual(df.schema[0].dataType.simpleString(), "interval day to second")
        self.assertEqual(df.first()[0], datetime.timedelta(microseconds=123))


class ArrowUDFTests(ArrowUDFTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_udf import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
