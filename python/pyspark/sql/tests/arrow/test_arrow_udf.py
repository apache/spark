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

import os
import time
import unittest
import datetime
from typing import Iterator

from pyspark.sql.functions import arrow_udf, ArrowUDFType, PandasUDFType
from pyspark.sql import functions as F, Row
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

    def test_time_zone_against_map_in_arrow(self):
        import pyarrow as pa

        for tz in [
            "Asia/Shanghai",
            "Asia/Hong_Kong",
            "America/Los_Angeles",
            "Pacific/Honolulu",
            "Europe/Amsterdam",
            "US/Pacific",
        ]:
            with self.sql_conf({"spark.sql.session.timeZone": tz}):
                # There is a time-zone conversion in df.collect:
                # ts.astimezone().replace(tzinfo=None)
                # it is controlled by env os.environ["TZ"].
                # Note that if the env is not equvilent to spark.sql.session.timeZone,
                # than there is a mismatch between the internal arrow data and df.collect.
                os.environ["TZ"] = tz
                time.tzset()

                df = self.spark.sql("SELECT TIMESTAMP('2019-04-12 15:50:01') AS ts")

                def check_value(t):
                    assert isinstance(t, pa.Array)
                    assert isinstance(t, pa.TimestampArray)
                    assert isinstance(t[0], pa.Scalar)
                    assert isinstance(t[0], pa.TimestampScalar)
                    ts = t[0].as_py()
                    assert isinstance(ts, datetime.datetime)
                    assert ts.year == 2019
                    assert ts.month == 4
                    assert ts.day == 12
                    assert ts.hour == 15
                    assert ts.minute == 50
                    assert ts.second == 1
                    # the timezone is still kept in the internal arrow data
                    assert ts.tzinfo is not None
                    assert str(ts.tzinfo) == tz, str(ts.tzinfo)

                @arrow_udf("timestamp")
                def identity(t):
                    check_value(t)
                    return t

                expected = [Row(ts=datetime.datetime(2019, 4, 12, 15, 50, 1))]
                self.assertEqual(expected, df.collect())

                result1 = df.select(identity("ts").alias("ts"))
                self.assertEqual(expected, result1.collect())

                def identity2(iter):
                    for batch in iter:
                        t = batch["ts"]
                        check_value(t)
                        yield batch

                result2 = df.mapInArrow(identity2, "ts timestamp")
                self.assertEqual(expected, result2.collect())

    def test_arrow_udf_wrong_arg(self):
        with self.quiet():
            with self.assertRaises(ParseException):

                @arrow_udf("blah")
                def _(x):
                    return x

            with self.assertRaises(PySparkTypeError) as pe:

                @arrow_udf(returnType="double", functionType=PandasUDFType.SCALAR)
                def _(df):
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
                def _(x):
                    return x

            self.check_error(
                exception=pe.exception,
                errorClass="CANNOT_BE_NONE",
                messageParameters={"arg_name": "returnType"},
            )

            with self.assertRaises(PySparkTypeError) as pe:

                @arrow_udf("double", 100)
                def _(x):
                    return x

            self.check_error(
                exception=pe.exception,
                errorClass="INVALID_PANDAS_UDF_TYPE",
                messageParameters={"arg_name": "functionType", "arg_type": "100"},
            )

            with self.assertRaises(PySparkTypeError) as pe:

                @arrow_udf(returnType=PandasUDFType.GROUPED_MAP)
                def _(df):
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
                def _():
                    return 1

            with self.assertRaisesRegex(ValueError, "0-arg arrow_udfs.*not.*supported"):

                @arrow_udf(LongType(), ArrowUDFType.SCALAR_ITER)
                def _():
                    yield 1
                    yield 2

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

    def test_scalar_arrow_udf_with_specified_eval_type(self):
        import pyarrow as pa

        df = self.spark.range(10).selectExpr("id", "id as v")
        expected = df.selectExpr("(v + 1) as plus_one").collect()

        @arrow_udf("long", ArrowUDFType.SCALAR)
        def plus_one(v: pa.Array):
            return pa.compute.add(v, 1)

        result1 = df.select(plus_one("v").alias("plus_one")).collect()
        self.assertEqual(expected, result1)

        @arrow_udf("long", ArrowUDFType.SCALAR_ITER)
        def plus_one_iter(it: Iterator[pa.Array]):
            for v in it:
                yield pa.compute.add(v, 1)

        result2 = df.select(plus_one_iter("v").alias("plus_one")).collect()
        self.assertEqual(expected, result2)

    def test_agg_arrow_udf_with_specified_eval_type(self):
        import pyarrow as pa

        df = self.spark.range(10).selectExpr("id", "id as v")
        expected = df.selectExpr("max(v) as m").collect()

        @arrow_udf("long", ArrowUDFType.GROUPED_AGG)
        def calc_max(v: pa.Array):
            return pa.compute.max(v)

        result = df.select(calc_max("v").alias("m")).collect()
        self.assertEqual(expected, result)


class ArrowUDFTests(ArrowUDFTestsMixin, ReusedSQLTestCase):
    def setUp(self):
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_udf import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
