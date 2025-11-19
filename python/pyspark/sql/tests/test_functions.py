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

from contextlib import redirect_stdout
import datetime
from enum import Enum
from inspect import getmembers, isfunction, isclass
import io
from itertools import chain
import math
import re
import unittest

from pyspark.errors import PySparkTypeError, PySparkValueError, SparkRuntimeException
from pyspark.sql import Row, Window, functions as F, types
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.column import Column
from pyspark.sql.functions.builtin import nullifzero, randstr, uniform, zeroifnull
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.testing.sqlutils import ReusedSQLTestCase, SQLTestUtils
from pyspark.testing.utils import have_numpy, assertDataFrameEqual


class FunctionsTestsMixin:
    def test_function_parity(self):
        # This test compares the available list of functions in pyspark.sql.functions with those
        # available in the Scala/Java DataFrame API in org.apache.spark.sql.functions.
        #
        # NOTE FOR DEVELOPERS:
        # If this test fails one of the following needs to happen
        # * If a function was added to org.apache.spark.sql.functions it either needs to be added to
        #     pyspark.sql.functions or added to the below expected_missing_in_py set.
        # * If a function was added to pyspark.sql.functions that was already in
        #     org.apache.spark.sql.functions then it needs to be removed from expected_missing_in_py
        #     below. If the function has a different name it needs to be added to py_equiv_jvm
        #     mapping.
        # * If it's not related to an added/removed function then likely the exclusion list
        #     jvm_excluded_fn needs to be updated.

        jvm_fn_set = {name for (name, value) in getmembers(self.sc._jvm.functions)}
        py_fn_set = {name for (name, value) in getmembers(F, isfunction) if name[0] != "_"}

        # Functions on the JVM side we do not expect to be available in python because they are
        # depreciated, irrelevant to python, or have equivalents.
        jvm_excluded_fn = [
            "callUDF",  # depreciated, use call_udf
            "typedlit",  # Scala only
            "typedLit",  # Scala only
            "monotonicallyIncreasingId",  # depreciated, use monotonically_increasing_id
            "not",  # equivalent to python ~expression
            "any",  # equivalent to python ~some
            "len",  # equivalent to python ~length
            "udaf",  # used for creating UDAF's which are not supported in PySpark
            "partitioning$",  # partitioning expressions for DSv2
        ]

        jvm_fn_set.difference_update(jvm_excluded_fn)

        # For functions that are named differently in pyspark this is the mapping of their
        # python name to the JVM equivalent
        py_equiv_jvm = {"create_map": "map"}
        for py_name, jvm_name in py_equiv_jvm.items():
            if py_name in py_fn_set:
                py_fn_set.remove(py_name)
                py_fn_set.add(jvm_name)

        missing_in_py = jvm_fn_set.difference(py_fn_set)

        # Functions that we expect to be missing in python until they are added to pyspark
        expected_missing_in_py = set()

        self.assertEqual(
            expected_missing_in_py, missing_in_py, "Missing functions in pyspark not as expected"
        )

    def test_wildcard_import(self):
        all_set = set(F.__all__)

        # {
        #     "abs",
        #     "acos",
        #     "acosh",
        #     "add_months",
        #     "aes_decrypt",
        #     "aes_encrypt",
        #     ...,
        # }
        fn_set = {
            name
            for (name, value) in getmembers(F, isfunction)
            if name[0] != "_" and value.__module__ != "typing"
        }

        deprecated_fn_list = [
            "approxCountDistinct",  # deprecated
            "bitwiseNOT",  # deprecated
            "countDistinct",  # deprecated
            "chr",  # name conflict with builtin function
            "random",  # name conflict with builtin function
            "shiftLeft",  # deprecated
            "shiftRight",  # deprecated
            "shiftRightUnsigned",  # deprecated
            "sumDistinct",  # deprecated
            "toDegrees",  # deprecated
            "toRadians",  # deprecated
            "uuid",  # name conflict with builtin module
        ]
        unregistered_fn_list = [
            "chr",  # name conflict with builtin function
            "random",  # name conflict with builtin function
            "uuid",  # name conflict with builtin module
        ]
        expected_fn_all_diff = set(deprecated_fn_list + unregistered_fn_list)
        self.assertEqual(expected_fn_all_diff, fn_set - all_set)

        # {
        #     "AnalyzeArgument",
        #     "AnalyzeResult",
        #     ...,
        #     "UserDefinedFunction",
        #     "UserDefinedTableFunction",
        # }
        clz_set = {
            name
            for (name, value) in getmembers(F, isclass)
            if name[0] != "_" and value.__module__ != "typing"
        }

        expected_clz_all_diff = {
            "ArrayType",  # should be imported from pyspark.sql.types
            "ByteType",  # should be imported from pyspark.sql.types
            "Column",  # should be imported from pyspark.sql
            "DataType",  # should be imported from pyspark.sql.types
            "NumericType",  # should be imported from pyspark.sql.types
            "PySparkTypeError",  # should be imported from pyspark.errors
            "PySparkValueError",  # should be imported from pyspark.errors
            "StringType",  # should be imported from pyspark.sql.types
            "StructType",  # should be imported from pyspark.sql.types
        }
        self.assertEqual(expected_clz_all_diff, clz_set - all_set)

        unknonw_set = all_set - (fn_set | clz_set)
        self.assertEqual(unknonw_set, set())

    def test_explode(self):
        d = [
            Row(a=1, intlist=[1, 2, 3], mapfield={"a": "b"}),
            Row(a=1, intlist=[], mapfield={}),
            Row(a=1, intlist=None, mapfield=None),
        ]
        data = self.spark.createDataFrame(d)

        result = data.select(F.explode(data.intlist).alias("a")).select("a").collect()
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[1][0], 2)
        self.assertEqual(result[2][0], 3)

        result = data.select(F.explode(data.mapfield).alias("a", "b")).select("a", "b").collect()
        self.assertEqual(result[0][0], "a")
        self.assertEqual(result[0][1], "b")

        result = [tuple(x) for x in data.select(F.posexplode_outer("intlist")).collect()]
        self.assertEqual(result, [(0, 1), (1, 2), (2, 3), (None, None), (None, None)])

        result = [tuple(x) for x in data.select(F.posexplode_outer("mapfield")).collect()]
        self.assertEqual(result, [(0, "a", "b"), (None, None, None), (None, None, None)])

        result = [x[0] for x in data.select(F.explode_outer("intlist")).collect()]
        self.assertEqual(result, [1, 2, 3, None, None])

        result = [tuple(x) for x in data.select(F.explode_outer("mapfield")).collect()]
        self.assertEqual(result, [("a", "b"), (None, None), (None, None)])

    def test_inline(self):
        d = [
            Row(structlist=[Row(b=1, c=2), Row(b=3, c=4)]),
            Row(structlist=[Row(b=None, c=5), None]),
            Row(structlist=[]),
        ]
        data = self.spark.createDataFrame(d)

        result = [tuple(x) for x in data.select(F.inline(data.structlist)).collect()]
        self.assertEqual(result, [(1, 2), (3, 4), (None, 5), (None, None)])

        result = [tuple(x) for x in data.select(F.inline_outer(data.structlist)).collect()]
        self.assertEqual(result, [(1, 2), (3, 4), (None, 5), (None, None), (None, None)])

    def test_basic_functions(self):
        rdd = self.sc.parallelize(['{"foo":"bar"}', '{"foo":"baz"}'])
        df = self.spark.read.json(rdd)
        df.count()
        df.collect()
        df.schema

        # cache and checkpoint
        self.assertFalse(df.is_cached)
        df.persist()
        df.unpersist(True)
        df.cache()
        self.assertTrue(df.is_cached)
        self.assertEqual(2, df.count())

        with self.tempView("temp"):
            df.createOrReplaceTempView("temp")
            df = self.spark.sql("select foo from temp")
            df.count()
            df.collect()

    def test_corr(self):
        df = self.spark.createDataFrame([Row(a=i, b=math.sqrt(i)) for i in range(10)])
        corr = df.stat.corr("a", "b")
        self.assertTrue(abs(corr - 0.95734012) < 1e-6)

    def test_sampleby(self):
        df = self.spark.createDataFrame([Row(a=i, b=(i % 3)) for i in range(100)])
        sampled = df.stat.sampleBy("b", fractions={0: 0.5, 1: 0.5}, seed=0)
        self.assertTrue(35 <= sampled.count() <= 36)

        with self.assertRaises(PySparkTypeError) as pe:
            df.sampleBy(10, fractions={0: 0.5, 1: 0.5})

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "col", "arg_type": "int"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            df.sampleBy("b", fractions=[0.5, 0.5])

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_DICT",
            messageParameters={"arg_name": "fractions", "arg_type": "list"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            df.sampleBy("b", fractions={None: 0.5, 1: 0.5})

        self.check_error(
            exception=pe.exception,
            errorClass="DISALLOWED_TYPE_FOR_CONTAINER",
            messageParameters={
                "arg_name": "fractions",
                "arg_type": "dict",
                "allowed_types": "float, int, str",
                "item_type": "NoneType",
            },
        )

    def test_cov(self):
        df = self.spark.createDataFrame([Row(a=i, b=2 * i) for i in range(10)])
        cov = df.stat.cov("a", "b")
        self.assertTrue(abs(cov - 55.0 / 3) < 1e-6)

        with self.assertRaises(PySparkTypeError) as pe:
            df.stat.cov(10, "b")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_STR",
            messageParameters={"arg_name": "col1", "arg_type": "int"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            df.stat.cov("a", True)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_STR",
            messageParameters={"arg_name": "col2", "arg_type": "bool"},
        )

    def test_crosstab(self):
        df = self.spark.createDataFrame([Row(a=i % 3, b=i % 2) for i in range(1, 7)])
        ct = df.stat.crosstab("a", "b").collect()
        ct = sorted(ct, key=lambda x: x[0])
        for i, row in enumerate(ct):
            self.assertEqual(row[0], str(i))
            self.assertTrue(row[1], 1)
            self.assertTrue(row[2], 1)

    def test_math_functions(self):
        df = self.spark.createDataFrame([Row(a=i, b=2 * i) for i in range(10)])

        SQLTestUtils.assert_close(
            [math.cos(i) for i in range(10)], df.select(F.cos(df.a)).collect()
        )
        SQLTestUtils.assert_close([math.cos(i) for i in range(10)], df.select(F.cos("a")).collect())
        SQLTestUtils.assert_close(
            [math.sin(i) for i in range(10)], df.select(F.sin(df.a)).collect()
        )
        SQLTestUtils.assert_close(
            [math.sin(i) for i in range(10)], df.select(F.sin(df["a"])).collect()
        )
        SQLTestUtils.assert_close(
            [math.pow(i, 2 * i) for i in range(10)], df.select(F.pow(df.a, df.b)).collect()
        )
        SQLTestUtils.assert_close(
            [math.pow(i, 2) for i in range(10)], df.select(F.pow(df.a, 2)).collect()
        )
        SQLTestUtils.assert_close(
            [math.pow(i, 2) for i in range(10)], df.select(F.pow(df.a, 2.0)).collect()
        )
        SQLTestUtils.assert_close(
            [math.hypot(i, 2 * i) for i in range(10)], df.select(F.hypot(df.a, df.b)).collect()
        )
        SQLTestUtils.assert_close(
            [math.hypot(i, 2 * i) for i in range(10)], df.select(F.hypot("a", "b")).collect()
        )
        SQLTestUtils.assert_close(
            [math.hypot(i, 2) for i in range(10)], df.select(F.hypot("a", 2)).collect()
        )
        SQLTestUtils.assert_close(
            [math.hypot(i, 2) for i in range(10)], df.select(F.hypot(df.a, 2)).collect()
        )

    def test_inverse_trig_functions(self):
        df = self.spark.createDataFrame([Row(a=i * 0.2, b=i * -0.2) for i in range(10)])

        def check(trig, inv, y_axis_symmetrical):
            SQLTestUtils.assert_close(
                [n * 0.2 for n in range(10)],
                df.select(inv(trig(df.a))).collect(),
            )
            if y_axis_symmetrical:
                SQLTestUtils.assert_close(
                    [n * 0.2 for n in range(10)],
                    df.select(inv(trig(df.b))).collect(),
                )
            else:
                SQLTestUtils.assert_close(
                    [n * -0.2 for n in range(10)],
                    df.select(inv(trig(df.b))).collect(),
                )

        check(F.cosh, F.acosh, y_axis_symmetrical=True)
        check(F.sinh, F.asinh, y_axis_symmetrical=False)
        check(F.tanh, F.atanh, y_axis_symmetrical=False)

    def test_reciprocal_trig_functions(self):
        # SPARK-36683: Tests for reciprocal trig functions (SEC, CSC and COT)
        lst = [
            0.0,
            math.pi / 6,
            math.pi / 4,
            math.pi / 3,
            math.pi / 2,
            math.pi,
            3 * math.pi / 2,
            2 * math.pi,
        ]

        df = self.spark.createDataFrame(lst, types.DoubleType())

        def to_reciprocal_trig(func):
            return [1.0 / func(i) if func(i) != 0 else math.inf for i in lst]

        SQLTestUtils.assert_close(
            to_reciprocal_trig(math.cos), df.select(F.sec(df.value)).collect()
        )
        SQLTestUtils.assert_close(
            to_reciprocal_trig(math.sin), df.select(F.csc(df.value)).collect()
        )
        SQLTestUtils.assert_close(
            to_reciprocal_trig(math.tan), df.select(F.cot(df.value)).collect()
        )

    def test_rand_functions(self):
        df = self.spark.createDataFrame([Row(key=i, value=str(i)) for i in range(100)])

        rnd = df.select("key", F.rand()).collect()
        for row in rnd:
            assert row[1] >= 0.0 and row[1] <= 1.0, "got: %s" % row[1]
        rndn = df.select("key", F.randn(5)).collect()
        for row in rndn:
            assert row[1] >= -4.0 and row[1] <= 4.0, "got: %s" % row[1]

        # If the specified seed is 0, we should use it.
        # https://issues.apache.org/jira/browse/SPARK-9691
        rnd1 = df.select("key", F.rand(0)).collect()
        rnd2 = df.select("key", F.rand(0)).collect()
        self.assertEqual(sorted(rnd1), sorted(rnd2))

        rndn1 = df.select("key", F.randn(0)).collect()
        rndn2 = df.select("key", F.randn(0)).collect()
        self.assertEqual(sorted(rndn1), sorted(rndn2))

    def test_time_diff(self):
        # SPARK-53111: test the time_diff function.
        df = self.spark.range(1).select(
            F.lit("hour").alias("unit"),
            F.lit(datetime.time(20, 30, 29)).alias("start"),
            F.lit(datetime.time(21, 30, 29)).alias("end"),
        )
        result = 1
        row_from_col = df.select(F.time_diff(df.unit, df.start, df.end)).first()
        self.assertEqual(row_from_col[0], result)
        row_from_name = df.select(F.time_diff("unit", "start", "end")).first()
        self.assertEqual(row_from_name[0], result)

    def test_time_trunc(self):
        # SPARK-53110: test the time_trunc function.
        df = self.spark.range(1).select(
            F.lit("minute").alias("unit"), F.lit(datetime.time(1, 2, 3)).alias("time")
        )
        result = datetime.time(1, 2, 0)
        row_from_col = df.select(F.time_trunc(df.unit, df.time)).first()
        self.assertIsInstance(row_from_col[0], datetime.time)
        self.assertEqual(row_from_col[0], result)
        row_from_name = df.select(F.time_trunc("unit", "time")).first()
        self.assertIsInstance(row_from_name[0], datetime.time)
        self.assertEqual(row_from_name[0], result)

    def test_try_parse_url(self):
        df = self.spark.createDataFrame(
            [("https://spark.apache.org/path?query=1", "QUERY", "query")],
            ["url", "part", "key"],
        )
        actual = df.select(F.try_parse_url(df.url, df.part, df.key))
        assertDataFrameEqual(actual, [Row("1")])
        df = self.spark.createDataFrame(
            [("inva lid://spark.apache.org/path?query=1", "QUERY", "query")],
            ["url", "part", "key"],
        )
        actual = df.select(F.try_parse_url(df.url, df.part, df.key))
        assertDataFrameEqual(actual, [Row(None)])

    def test_try_make_timestamp(self):
        """Comprehensive test cases for try_make_timestamp with various arguments."""

        # Common input dataframe setup for multiple test cases (with various arguments).
        df = self.spark.createDataFrame(
            [(2024, 5, 22, 10, 30, 0, "CET")],
            ["year", "month", "day", "hour", "minute", "second", "timezone"],
        )
        df_frac = self.spark.createDataFrame(
            [(2024, 5, 22, 10, 30, 45.123, "CET")],
            ["year", "month", "day", "hour", "minute", "second", "timezone"],
        )
        df_dt = self.spark.range(1).select(
            F.lit(datetime.date(2024, 5, 22)).alias("date"),
            F.lit(datetime.time(10, 30, 0)).alias("time"),
            F.lit("CET").alias("timezone"),
        )
        df_dt_frac = self.spark.range(1).select(
            F.lit(datetime.date(2024, 5, 22)).alias("date"),
            F.lit(datetime.time(10, 30, 45, 123000)).alias("time"),
            F.lit("CET").alias("timezone"),
        )
        # Expected results for comparison in different scenarios.
        result_no_tz = datetime.datetime(2024, 5, 22, 10, 30)
        result_with_tz = datetime.datetime(2024, 5, 22, 8, 30)
        result_frac_no_tz = datetime.datetime(2024, 5, 22, 10, 30, 45, 123000)
        result_frac_with_tz = datetime.datetime(2024, 5, 22, 8, 30, 45, 123000)

        # Test 1A: Basic 6 positional arguments (years, months, days, hours, mins, secs).
        actual = df.select(
            F.try_make_timestamp(df.year, df.month, df.day, df.hour, df.minute, df.second)
        )
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 1B: Basic 7 positional arguments (years, months, days, hours, mins, secs, timezone).
        actual = df.select(
            F.try_make_timestamp(
                df.year, df.month, df.day, df.hour, df.minute, df.second, df.timezone
            )
        )
        assertDataFrameEqual(actual, [Row(result_with_tz)])

        # Test 2A: Basic 6 keyword arguments (years, months, days, hours, mins, secs).
        actual = df.select(
            F.try_make_timestamp(
                years=df.year,
                months=df.month,
                days=df.day,
                hours=df.hour,
                mins=df.minute,
                secs=df.second,
            )
        )
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 2B: Basic 7 keyword arguments (years, months, days, hours, mins, secs, timezone).
        actual = df.select(
            F.try_make_timestamp(
                years=df.year,
                months=df.month,
                days=df.day,
                hours=df.hour,
                mins=df.minute,
                secs=df.second,
                timezone=df.timezone,
            )
        )
        assertDataFrameEqual(actual, [Row(result_with_tz)])

        # Test 3A: Alternative 2 keyword arguments (date, time).
        actual = df_dt.select(F.try_make_timestamp(date=df_dt.date, time=df_dt.time))
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 3B: Alternative 3 keyword arguments (date, time, timezone).
        actual = df_dt.select(
            F.try_make_timestamp(date=df_dt.date, time=df_dt.time, timezone=df_dt.timezone)
        )
        assertDataFrameEqual(actual, [Row(result_with_tz)])

        # Test 4A: Fractional seconds with positional arguments (without timezone).
        actual = df_frac.select(
            F.try_make_timestamp(
                df_frac.year,
                df_frac.month,
                df_frac.day,
                df_frac.hour,
                df_frac.minute,
                df_frac.second,
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac_no_tz)])

        # Test 4B: Fractional seconds with positional arguments (with timezone).
        actual = df_frac.select(
            F.try_make_timestamp(
                df_frac.year,
                df_frac.month,
                df_frac.day,
                df_frac.hour,
                df_frac.minute,
                df_frac.second,
                df_frac.timezone,
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac_with_tz)])

        # Test 5A: Fractional seconds with keyword arguments (without timezone).
        actual = df_frac.select(
            F.try_make_timestamp(
                years=df_frac.year,
                months=df_frac.month,
                days=df_frac.day,
                hours=df_frac.hour,
                mins=df_frac.minute,
                secs=df_frac.second,
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac_no_tz)])

        # Test 5B: Fractional seconds with keyword arguments (with timezone).
        actual = df_frac.select(
            F.try_make_timestamp(
                years=df_frac.year,
                months=df_frac.month,
                days=df_frac.day,
                hours=df_frac.hour,
                mins=df_frac.minute,
                secs=df_frac.second,
                timezone=df_frac.timezone,
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac_with_tz)])

        # Test 6A: Fractional seconds with date/time arguments (without timezone).
        actual = df_dt_frac.select(F.try_make_timestamp(date=df_dt_frac.date, time=df_dt_frac.time))
        assertDataFrameEqual(actual, [Row(result_frac_no_tz)])

        # Test 6B: Fractional seconds with date/time arguments (with timezone).
        actual = df_dt_frac.select(
            F.try_make_timestamp(
                date=df_dt_frac.date, time=df_dt_frac.time, timezone=df_dt_frac.timezone
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac_with_tz)])

        # Test 7: Edge case - February 29 in leap year.
        df_leap = self.spark.createDataFrame(
            [(2024, 2, 29, 0, 0, 0)], ["year", "month", "day", "hour", "minute", "second"]
        )
        expected_leap = datetime.datetime(2024, 2, 29, 0, 0, 0)
        actual = df_leap.select(
            F.try_make_timestamp(
                df_leap.year,
                df_leap.month,
                df_leap.day,
                df_leap.hour,
                df_leap.minute,
                df_leap.second,
            )
        )
        assertDataFrameEqual(actual, [Row(expected_leap)])

        # Test 8: Mixed positional and keyword (should work for valid combinations).
        actual = df.select(
            F.try_make_timestamp(
                df.year, df.month, df.day, hours=df.hour, mins=df.minute, secs=df.second
            )
        )
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 9A: Using literal values for positional arguments (without timezone).
        actual = self.spark.range(1).select(
            F.try_make_timestamp(F.lit(2024), F.lit(5), F.lit(22), F.lit(10), F.lit(30), F.lit(0))
        )
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 9B: Using literal values for positional arguments (with timezone).
        actual = self.spark.range(1).select(
            F.try_make_timestamp(
                F.lit(2024), F.lit(5), F.lit(22), F.lit(10), F.lit(30), F.lit(0), F.lit("CET")
            )
        )
        assertDataFrameEqual(actual, [Row(result_with_tz)])

        # Test 10A: Using literal values for date/time arguments (without timezone).
        actual = self.spark.range(1).select(
            F.try_make_timestamp(
                date=F.lit(datetime.date(2024, 5, 22)), time=F.lit(datetime.time(10, 30, 0))
            )
        )
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 10B: Using literal values for date/time arguments (with timezone).
        actual = self.spark.range(1).select(
            F.try_make_timestamp(
                date=F.lit(datetime.date(2024, 5, 22)),
                time=F.lit(datetime.time(10, 30, 0)),
                timezone=F.lit("CET"),
            )
        )
        assertDataFrameEqual(actual, [Row(result_with_tz)])

        # Error handling tests.

        # Test 11: Mixing timestamp and date/time keyword arguments - should raise Exception.
        with self.assertRaises(PySparkValueError) as context:
            df_dt.select(
                F.try_make_timestamp(years=df.year, date=df_dt.date, time=df_dt.time)
            ).collect()
        error_msg = str(context.exception)
        self.assertIn("CANNOT_SET_TOGETHER", error_msg)
        self.assertIn("years|months|days|hours|mins|secs and date|time", error_msg)

        with self.assertRaises(PySparkValueError) as context:
            df_dt.select(
                F.try_make_timestamp(hours=df.hour, time=df_dt.time, timezone=df_dt.timezone)
            ).collect()
        error_msg = str(context.exception)
        self.assertIn("CANNOT_SET_TOGETHER", error_msg)
        self.assertIn("years|months|days|hours|mins|secs and date|time", error_msg)

        # Test 12: Incomplete keyword arguments - should raise Exception for None values.
        with self.assertRaises(Exception):
            F.try_make_timestamp(years=df.year)
        with self.assertRaises(Exception):
            F.try_make_timestamp(secs=df.second)
        with self.assertRaises(Exception):
            F.try_make_timestamp(years=df.year, months=df.month, days=df.day)
        with self.assertRaises(Exception):
            F.try_make_timestamp(days=df.day, timezone=df.timezone)
        with self.assertRaises(Exception):
            F.try_make_timestamp(
                hours=df.hour, mins=df.minute, secs=df.second, timezone=df.timezone
            )
        with self.assertRaises(Exception):
            F.try_make_timestamp(date=df_dt.date)
        with self.assertRaises(Exception):
            F.try_make_timestamp(time=df_dt.time, timezone=df_dt.timezone)
        with self.assertRaises(Exception):
            F.try_make_timestamp(timezone=df.timezone)
        with self.assertRaises(Exception):
            F.try_make_timestamp(timezone=df_dt.timezone)

    def test_try_make_timestamp_ltz(self):
        # use local timezone here to avoid flakiness
        data = [(2024, 5, 22, 10, 30, 0, datetime.datetime.now().astimezone().tzinfo.__str__())]
        df = self.spark.createDataFrame(
            data, ["year", "month", "day", "hour", "minute", "second", "timezone"]
        )
        actual = df.select(
            F.try_make_timestamp_ltz(
                df.year, df.month, df.day, df.hour, df.minute, df.second, df.timezone
            )
        )
        assertDataFrameEqual(actual, [Row(datetime.datetime(2024, 5, 22, 10, 30, 0))])

        # use local timezone here to avoid flakiness
        data = [(2024, 13, 22, 10, 30, 0, datetime.datetime.now().astimezone().tzinfo.__str__())]
        df = self.spark.createDataFrame(
            data, ["year", "month", "day", "hour", "minute", "second", "timezone"]
        )
        actual = df.select(
            F.try_make_timestamp_ltz(
                df.year, df.month, df.day, df.hour, df.minute, df.second, df.timezone
            )
        )
        assertDataFrameEqual(actual, [Row(None)])

    def test_try_make_timestamp_ntz(self):
        """Test cases for try_make_timestamp_ntz with 6-parameter and date/time forms."""

        # Test 1: Valid 6 positional arguments
        data = [(2024, 5, 22, 10, 30, 0)]
        result = datetime.datetime(2024, 5, 22, 10, 30)
        df = self.spark.createDataFrame(data, ["year", "month", "day", "hour", "minute", "second"])
        actual = df.select(
            F.try_make_timestamp_ntz(df.year, df.month, df.day, df.hour, df.minute, df.second)
        )
        assertDataFrameEqual(actual, [Row(result)])

        # Test 2: Invalid input (month=13) - should return NULL
        data = [(2024, 13, 22, 10, 30, 0)]
        df = self.spark.createDataFrame(data, ["year", "month", "day", "hour", "minute", "second"])
        actual = df.select(
            F.try_make_timestamp_ntz(df.year, df.month, df.day, df.hour, df.minute, df.second)
        )
        assertDataFrameEqual(actual, [Row(None)])

        # Test 3: Date/time keyword arguments
        df = self.spark.range(1).select(
            F.lit(datetime.date(2024, 5, 22)).alias("date"),
            F.lit(datetime.time(10, 30, 0)).alias("time"),
        )
        actual = df.select(F.try_make_timestamp_ntz(date=df.date, time=df.time))
        assertDataFrameEqual(actual, [Row(result)])

        # Test 4: All 6 keyword arguments
        df_full = self.spark.createDataFrame(
            [(2024, 5, 22, 10, 30, 45)], ["year", "month", "day", "hour", "minute", "second"]
        )
        actual = df_full.select(
            F.try_make_timestamp_ntz(
                years=df_full.year,
                months=df_full.month,
                days=df_full.day,
                hours=df_full.hour,
                mins=df_full.minute,
                secs=df_full.second,
            )
        )
        expected = datetime.datetime(2024, 5, 22, 10, 30, 45)
        assertDataFrameEqual(actual, [Row(expected)])

        # Test 5: Only year provided - should raise Exception for missing required parameters
        with self.assertRaises(Exception):
            F.try_make_timestamp_ntz(years=df_full.year)

        # Test 6: Partial parameters - should raise Exception for missing required parameters
        with self.assertRaises(Exception):
            F.try_make_timestamp_ntz(years=df_full.year, months=df_full.month, days=df_full.day)

        # Test 7: Partial parameters - should raise Exception for missing required parameters
        with self.assertRaises(Exception):
            F.try_make_timestamp_ntz(
                years=df_full.year, months=df_full.month, days=df_full.day, hours=df_full.hour
            )

        # Test 8: Fractional seconds
        df_frac = self.spark.createDataFrame(
            [(2024, 5, 22, 10, 30, 45.123)], ["year", "month", "day", "hour", "minute", "second"]
        )
        actual = df_frac.select(
            F.try_make_timestamp_ntz(
                df_frac.year,
                df_frac.month,
                df_frac.day,
                df_frac.hour,
                df_frac.minute,
                df_frac.second,
            )
        )
        expected_frac = datetime.datetime(2024, 5, 22, 10, 30, 45, 123000)
        assertDataFrameEqual(actual, [Row(expected_frac)])

        # Test 9: Edge case - February 29 in leap year (full 6 parameters)
        df_leap = self.spark.createDataFrame(
            [(2024, 2, 29, 0, 0, 0)], ["year", "month", "day", "hour", "minute", "second"]
        )
        actual = df_leap.select(
            F.try_make_timestamp_ntz(
                df_leap.year,
                df_leap.month,
                df_leap.day,
                df_leap.hour,
                df_leap.minute,
                df_leap.second,
            )
        )
        expected_leap = datetime.datetime(2024, 2, 29, 0, 0, 0)
        assertDataFrameEqual(actual, [Row(expected_leap)])

        # Test 10: Edge case - February 29 in non-leap year (should return NULL)
        df_non_leap = self.spark.createDataFrame(
            [(2023, 2, 29, 0, 0, 0)], ["year", "month", "day", "hour", "minute", "second"]
        )
        actual = df_non_leap.select(
            F.try_make_timestamp_ntz(
                df_non_leap.year,
                df_non_leap.month,
                df_non_leap.day,
                df_non_leap.hour,
                df_non_leap.minute,
                df_non_leap.second,
            )
        )
        assertDataFrameEqual(actual, [Row(None)])

        # Test 11: Minimum valid values (full 6 parameters)
        df_min = self.spark.createDataFrame(
            [(1, 1, 1, 0, 0, 0)], ["year", "month", "day", "hour", "minute", "second"]
        )
        actual = df_min.select(
            F.try_make_timestamp_ntz(
                df_min.year, df_min.month, df_min.day, df_min.hour, df_min.minute, df_min.second
            )
        )
        expected_min = datetime.datetime(1, 1, 1, 0, 0, 0)
        assertDataFrameEqual(actual, [Row(expected_min)])

        # Test 12: Maximum valid hour/minute/second
        df_max_time = self.spark.createDataFrame(
            [(2024, 5, 22, 23, 59, 59)], ["year", "month", "day", "hour", "minute", "second"]
        )
        actual = df_max_time.select(
            F.try_make_timestamp_ntz(
                df_max_time.year,
                df_max_time.month,
                df_max_time.day,
                df_max_time.hour,
                df_max_time.minute,
                df_max_time.second,
            )
        )
        expected_max_time = datetime.datetime(2024, 5, 22, 23, 59, 59)
        assertDataFrameEqual(actual, [Row(expected_max_time)])

        # Test 13: Invalid hour (should return NULL)
        df_invalid_hour = self.spark.createDataFrame(
            [(2024, 5, 22, 25, 0, 0)], ["year", "month", "day", "hour", "minute", "second"]
        )
        actual = df_invalid_hour.select(
            F.try_make_timestamp_ntz(
                df_invalid_hour.year,
                df_invalid_hour.month,
                df_invalid_hour.day,
                df_invalid_hour.hour,
                df_invalid_hour.minute,
                df_invalid_hour.second,
            )
        )
        assertDataFrameEqual(actual, [Row(None)])

        # Test 14: Valid date/time combination with NULL date
        df = self.spark.range(1).select(
            F.lit(None).cast("date").alias("date"), F.lit(datetime.time(10, 30, 0)).alias("time")
        )
        actual = df.select(F.try_make_timestamp_ntz(date=df.date, time=df.time))
        assertDataFrameEqual(actual, [Row(None)])

        # Test 15: Valid date/time combination with NULL time
        df = self.spark.range(1).select(
            F.lit(datetime.date(2024, 5, 22)).alias("date"), F.lit(None).cast("time").alias("time")
        )
        actual = df.select(F.try_make_timestamp_ntz(date=df.date, time=df.time))
        assertDataFrameEqual(actual, [Row(None)])

        # Test 16: Mixed parameter usage should raise PySparkValueError
        with self.assertRaises(PySparkValueError) as context:
            F.try_make_timestamp_ntz(years=df_full.year, date=df_full.year)
        error_msg = str(context.exception)
        self.assertIn("CANNOT_SET_TOGETHER", error_msg)
        self.assertIn("years|months|days|hours|mins|secs and date|time", error_msg)

    def test_string_functions(self):
        string_functions = [
            "upper",
            "lower",
            "ascii",
            "base64",
            "unbase64",
            "ltrim",
            "rtrim",
            "trim",
        ]

        df = self.spark.createDataFrame([["nick"]], schema=["name"])
        with self.assertRaises(PySparkTypeError) as pe:
            F.col("name").substr(0, F.lit(1))

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_SAME_TYPE",
            messageParameters={
                "arg_name1": "startPos",
                "arg_name2": "length",
                "arg_type1": "int",
                "arg_type2": "Column",
            },
        )

        with self.assertRaises(PySparkTypeError) as pe:
            F.col("name").substr("", "")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_INT",
            messageParameters={
                "arg_name": "startPos",
                "arg_type": "str",
            },
        )

        for name in string_functions:
            assertDataFrameEqual(
                df.select(getattr(F, name)("name")),
                df.select(getattr(F, name)(F.col("name"))),
            )

    def test_collation(self):
        df = self.spark.createDataFrame([("a",), ("b",)], ["name"])
        actual = df.select(F.collation(F.collate("name", "UNICODE"))).distinct()
        assertDataFrameEqual([Row("SYSTEM.BUILTIN.UNICODE")], actual)

    def test_try_make_interval(self):
        df = self.spark.createDataFrame([(2147483647,)], ["num"])
        actual = df.select(F.isnull(F.try_make_interval("num")))
        assertDataFrameEqual([Row(True)], actual)

    def test_octet_length_function(self):
        # SPARK-36751: add octet length api for python
        df = self.spark.createDataFrame([("cat",), ("\U0001F408",)], ["cat"])
        actual = df.select(F.octet_length("cat"))
        assertDataFrameEqual([Row(3), Row(4)], actual)

    def test_bit_length_function(self):
        # SPARK-36751: add bit length api for python
        df = self.spark.createDataFrame([("cat",), ("\U0001F408",)], ["cat"])
        actual = df.select(F.bit_length("cat"))
        assertDataFrameEqual([Row(24), Row(32)], actual)

    def test_array_contains_function(self):
        df = self.spark.createDataFrame([(["1", "2", "3"],), ([],)], ["data"])
        actual = df.select(F.array_contains(df.data, "1").alias("b"))
        assertDataFrameEqual([Row(b=True), Row(b=False)], actual)

    def test_levenshtein_function(self):
        df = self.spark.createDataFrame([("kitten", "sitting")], ["l", "r"])
        actual_without_threshold = df.select(F.levenshtein(df.l, df.r).alias("b"))
        assertDataFrameEqual([Row(b=3)], actual_without_threshold)
        actual_with_threshold = df.select(F.levenshtein(df.l, df.r, 2).alias("b"))
        assertDataFrameEqual([Row(b=-1)], actual_with_threshold)

    def test_between_function(self):
        df = self.spark.createDataFrame(
            [Row(a=1, b=2, c=3), Row(a=2, b=1, c=3), Row(a=4, b=1, c=4)]
        )
        assertDataFrameEqual(
            [Row(a=2, b=1, c=3), Row(a=4, b=1, c=4)], df.filter(df.a.between(df.b, df.c))
        )

    def test_dayofweek(self):
        dt = datetime.datetime(2017, 11, 6)
        df = self.spark.createDataFrame([Row(date=dt)])
        row = df.select(F.dayofweek(df.date)).first()
        self.assertEqual(row[0], 2)

    def test_monthname(self):
        dt = datetime.datetime(2017, 11, 6)
        df = self.spark.createDataFrame([Row(date=dt)])
        row = df.select(F.monthname(df.date)).first()
        self.assertEqual(row[0], "Nov")

    def test_dayname(self):
        dt = datetime.datetime(2017, 11, 6)
        df = self.spark.createDataFrame([Row(date=dt)])
        row = df.select(F.dayname(df.date)).first()
        self.assertEqual(row[0], "Mon")

    def test_hour(self):
        # SPARK-52892: test the hour function with time.
        df = self.spark.range(1).select(F.lit(datetime.time(12, 34, 56)).alias("time"))
        row_from_col = df.select(F.hour(df.time)).first()
        self.assertEqual(row_from_col[0], 12)
        row_from_name = df.select(F.hour("time")).first()
        self.assertEqual(row_from_name[0], 12)

    def test_minute(self):
        # SPARK-52893: test the minute function with time.
        df = self.spark.range(1).select(F.lit(datetime.time(12, 34, 56)).alias("time"))
        row_from_col = df.select(F.minute(df.time)).first()
        self.assertEqual(row_from_col[0], 34)
        row_from_name = df.select(F.minute("time")).first()
        self.assertEqual(row_from_name[0], 34)

    def test_second(self):
        # SPARK-52894: test the second function with time.
        df = self.spark.range(1).select(F.lit(datetime.time(12, 34, 56)).alias("time"))
        row_from_col = df.select(F.second(df.time)).first()
        self.assertEqual(row_from_col[0], 56)
        row_from_name = df.select(F.second("time")).first()
        self.assertEqual(row_from_name[0], 56)

    # Test added for SPARK-37738; change Python API to accept both col & int as input
    def test_date_add_function(self):
        dt = datetime.date(2021, 12, 27)

        # Note; number var in Python gets converted to LongType column;
        # this is not supported by the function, so cast to Integer explicitly
        df = self.spark.createDataFrame([Row(date=dt, add=2)], "date date, add integer")

        self.assertTrue(
            all(
                df.select(
                    F.date_add(df.date, df.add) == datetime.date(2021, 12, 29),
                    F.date_add(df.date, "add") == datetime.date(2021, 12, 29),
                    F.date_add(df.date, 3) == datetime.date(2021, 12, 30),
                ).first()
            )
        )

    # Test added for SPARK-37738; change Python API to accept both col & int as input
    def test_date_sub_function(self):
        dt = datetime.date(2021, 12, 27)

        # Note; number var in Python gets converted to LongType column;
        # this is not supported by the function, so cast to Integer explicitly
        df = self.spark.createDataFrame([Row(date=dt, sub=2)], "date date, sub integer")

        self.assertTrue(
            all(
                df.select(
                    F.date_sub(df.date, df.sub) == datetime.date(2021, 12, 25),
                    F.date_sub(df.date, "sub") == datetime.date(2021, 12, 25),
                    F.date_sub(df.date, 3) == datetime.date(2021, 12, 24),
                ).first()
            )
        )

    # Test added for SPARK-37738; change Python API to accept both col & int as input
    def test_add_months_function(self):
        dt = datetime.date(2021, 12, 27)

        # Note; number in Python gets converted to LongType column;
        # this is not supported by the function, so cast to Integer explicitly
        df = self.spark.createDataFrame([Row(date=dt, add=2)], "date date, add integer")

        self.assertTrue(
            all(
                df.select(
                    F.add_months(df.date, df.add) == datetime.date(2022, 2, 27),
                    F.add_months(df.date, "add") == datetime.date(2022, 2, 27),
                    F.add_months(df.date, 3) == datetime.date(2022, 3, 27),
                ).first()
            )
        )

    def test_make_time(self):
        # SPARK-52888: test the make_time function.
        df = self.spark.createDataFrame([(1, 2, 3)], ["hour", "minute", "second"])
        result = datetime.time(1, 2, 3)
        row_from_col = df.select(F.make_time(df.hour, df.minute, df.second)).first()
        self.assertIsInstance(row_from_col[0], datetime.time)
        self.assertEqual(row_from_col[0], result)
        row_from_name = df.select(F.make_time("hour", "minute", "second")).first()
        self.assertIsInstance(row_from_name[0], datetime.time)
        self.assertEqual(row_from_name[0], result)

    def test_make_timestamp(self):
        """Comprehensive test cases for make_timestamp with various arguments and edge cases."""

        # Common input dataframe setup for multiple test cases (with various arguments).
        df = self.spark.createDataFrame(
            [(2024, 5, 22, 10, 30, 0, "CET")],
            ["year", "month", "day", "hour", "minute", "second", "timezone"],
        )
        df_frac = self.spark.createDataFrame(
            [(2024, 5, 22, 10, 30, 45.123, "CET")],
            ["year", "month", "day", "hour", "minute", "second", "timezone"],
        )
        df_dt = self.spark.range(1).select(
            F.lit(datetime.date(2024, 5, 22)).alias("date"),
            F.lit(datetime.time(10, 30, 0)).alias("time"),
            F.lit("CET").alias("timezone"),
        )
        df_dt_frac = self.spark.range(1).select(
            F.lit(datetime.date(2024, 5, 22)).alias("date"),
            F.lit(datetime.time(10, 30, 45, 123000)).alias("time"),
            F.lit("CET").alias("timezone"),
        )
        # Expected results for comparison in different scenarios.
        result_no_tz = datetime.datetime(2024, 5, 22, 10, 30)
        result_with_tz = datetime.datetime(2024, 5, 22, 8, 30)
        result_frac_no_tz = datetime.datetime(2024, 5, 22, 10, 30, 45, 123000)
        result_frac_with_tz = datetime.datetime(2024, 5, 22, 8, 30, 45, 123000)

        # Test 1A: Basic 6 positional arguments (years, months, days, hours, mins, secs).
        actual = df.select(
            F.make_timestamp(df.year, df.month, df.day, df.hour, df.minute, df.second)
        )
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 1B: Basic 7 positional arguments (years, months, days, hours, mins, secs, timezone).
        actual = df.select(
            F.make_timestamp(df.year, df.month, df.day, df.hour, df.minute, df.second, df.timezone)
        )
        assertDataFrameEqual(actual, [Row(result_with_tz)])

        # Test 2A: Basic 6 keyword arguments (years, months, days, hours, mins, secs).
        actual = df.select(
            F.make_timestamp(
                years=df.year,
                months=df.month,
                days=df.day,
                hours=df.hour,
                mins=df.minute,
                secs=df.second,
            )
        )
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 2B: Basic 7 keyword arguments (years, months, days, hours, mins, secs, timezone).
        actual = df.select(
            F.make_timestamp(
                years=df.year,
                months=df.month,
                days=df.day,
                hours=df.hour,
                mins=df.minute,
                secs=df.second,
                timezone=df.timezone,
            )
        )
        assertDataFrameEqual(actual, [Row(result_with_tz)])

        # Test 3A: Alternative 2 keyword arguments (date, time).
        actual = df_dt.select(F.make_timestamp(date=df_dt.date, time=df_dt.time))
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 3B: Alternative 3 keyword arguments (date, time, timezone).
        actual = df_dt.select(
            F.make_timestamp(date=df_dt.date, time=df_dt.time, timezone=df_dt.timezone)
        )
        assertDataFrameEqual(actual, [Row(result_with_tz)])

        # Test 4A: Fractional seconds with positional arguments (without timezone).
        actual = df_frac.select(
            F.make_timestamp(
                df_frac.year,
                df_frac.month,
                df_frac.day,
                df_frac.hour,
                df_frac.minute,
                df_frac.second,
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac_no_tz)])

        # Test 4B: Fractional seconds with positional arguments (with timezone).
        actual = df_frac.select(
            F.make_timestamp(
                df_frac.year,
                df_frac.month,
                df_frac.day,
                df_frac.hour,
                df_frac.minute,
                df_frac.second,
                df_frac.timezone,
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac_with_tz)])

        # Test 5A: Fractional seconds with keyword arguments (without timezone).
        actual = df_frac.select(
            F.make_timestamp(
                years=df_frac.year,
                months=df_frac.month,
                days=df_frac.day,
                hours=df_frac.hour,
                mins=df_frac.minute,
                secs=df_frac.second,
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac_no_tz)])

        # Test 5B: Fractional seconds with keyword arguments (with timezone).
        actual = df_frac.select(
            F.make_timestamp(
                years=df_frac.year,
                months=df_frac.month,
                days=df_frac.day,
                hours=df_frac.hour,
                mins=df_frac.minute,
                secs=df_frac.second,
                timezone=df_frac.timezone,
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac_with_tz)])

        # Test 6A: Fractional seconds with date/time arguments (without timezone).
        actual = df_dt_frac.select(F.make_timestamp(date=df_dt_frac.date, time=df_dt_frac.time))
        assertDataFrameEqual(actual, [Row(result_frac_no_tz)])

        # Test 6B: Fractional seconds with date/time arguments (with timezone).
        actual = df_dt_frac.select(
            F.make_timestamp(
                date=df_dt_frac.date, time=df_dt_frac.time, timezone=df_dt_frac.timezone
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac_with_tz)])

        # Test 7: Edge case - February 29 in leap year.
        df_leap = self.spark.createDataFrame(
            [(2024, 2, 29, 0, 0, 0)], ["year", "month", "day", "hour", "minute", "second"]
        )
        expected_leap = datetime.datetime(2024, 2, 29, 0, 0, 0)
        actual = df_leap.select(
            F.make_timestamp(
                df_leap.year,
                df_leap.month,
                df_leap.day,
                df_leap.hour,
                df_leap.minute,
                df_leap.second,
            )
        )
        assertDataFrameEqual(actual, [Row(expected_leap)])

        # Test 8: Mixed positional and keyword (should work for valid combinations).
        actual = df.select(
            F.make_timestamp(
                df.year, df.month, df.day, hours=df.hour, mins=df.minute, secs=df.second
            )
        )
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 9A: Using literal values for positional arguments (without timezone).
        actual = self.spark.range(1).select(
            F.make_timestamp(F.lit(2024), F.lit(5), F.lit(22), F.lit(10), F.lit(30), F.lit(0))
        )
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 9B: Using literal values for positional arguments (with timezone).
        actual = self.spark.range(1).select(
            F.make_timestamp(
                F.lit(2024), F.lit(5), F.lit(22), F.lit(10), F.lit(30), F.lit(0), F.lit("CET")
            )
        )
        assertDataFrameEqual(actual, [Row(result_with_tz)])

        # Test 10A: Using literal values for date/time arguments (without timezone).
        actual = self.spark.range(1).select(
            F.make_timestamp(
                date=F.lit(datetime.date(2024, 5, 22)), time=F.lit(datetime.time(10, 30, 0))
            )
        )
        assertDataFrameEqual(actual, [Row(result_no_tz)])

        # Test 10B: Using literal values for date/time arguments (with timezone).
        actual = self.spark.range(1).select(
            F.make_timestamp(
                date=F.lit(datetime.date(2024, 5, 22)),
                time=F.lit(datetime.time(10, 30, 0)),
                timezone=F.lit("CET"),
            )
        )
        assertDataFrameEqual(actual, [Row(result_with_tz)])

        # Error handling tests.

        # Test 11: Mixing timestamp and date/time keyword arguments - should raise Exception.
        with self.assertRaises(PySparkValueError) as context:
            df_dt.select(
                F.make_timestamp(years=df.year, date=df_dt.date, time=df_dt.time)
            ).collect()
        error_msg = str(context.exception)
        self.assertIn("CANNOT_SET_TOGETHER", error_msg)
        self.assertIn("years|months|days|hours|mins|secs and date|time", error_msg)

        with self.assertRaises(PySparkValueError) as context:
            df_dt.select(
                F.make_timestamp(hours=df.hour, time=df_dt.time, timezone=df_dt.timezone)
            ).collect()
        error_msg = str(context.exception)
        self.assertIn("CANNOT_SET_TOGETHER", error_msg)
        self.assertIn("years|months|days|hours|mins|secs and date|time", error_msg)

        # Test 12: Incomplete keyword arguments - should raise Exception for None values.
        with self.assertRaises(Exception):
            F.make_timestamp(years=df.year)
        with self.assertRaises(Exception):
            F.make_timestamp(secs=df.second)
        with self.assertRaises(Exception):
            F.make_timestamp(years=df.year, months=df.month, days=df.day)
        with self.assertRaises(Exception):
            F.make_timestamp(days=df.day, timezone=df.timezone)
        with self.assertRaises(Exception):
            F.make_timestamp(hours=df.hour, mins=df.minute, secs=df.second, timezone=df.timezone)
        with self.assertRaises(Exception):
            F.make_timestamp(date=df_dt.date)
        with self.assertRaises(Exception):
            F.make_timestamp(time=df_dt.time, timezone=df_dt.timezone)
        with self.assertRaises(Exception):
            F.make_timestamp(timezone=df.timezone)
        with self.assertRaises(Exception):
            F.make_timestamp(timezone=df_dt.timezone)

    def test_make_timestamp_ntz(self):
        """Comprehensive test cases for make_timestamp_ntz with various arguments and edge cases."""

        # Test 1: Basic 6 positional arguments
        data = [(2024, 5, 22, 10, 30, 0)]
        result = datetime.datetime(2024, 5, 22, 10, 30)
        df = self.spark.createDataFrame(data, ["year", "month", "day", "hour", "minute", "second"])

        actual = df.select(
            F.make_timestamp_ntz(df.year, df.month, df.day, df.hour, df.minute, df.second)
        )
        assertDataFrameEqual(actual, [Row(result)])

        # Test 2: All 6 keyword arguments
        actual = df.select(
            F.make_timestamp_ntz(
                years=df.year,
                months=df.month,
                days=df.day,
                hours=df.hour,
                mins=df.minute,
                secs=df.second,
            )
        )
        assertDataFrameEqual(actual, [Row(result)])

        # Test 3: Date/time keyword arguments
        df_dt = self.spark.range(1).select(
            F.lit(datetime.date(2024, 5, 22)).alias("date"),
            F.lit(datetime.time(10, 30, 0)).alias("time"),
        )
        actual = df_dt.select(F.make_timestamp_ntz(date=df_dt.date, time=df_dt.time))
        assertDataFrameEqual(actual, [Row(result)])

        # Test 4: Fractional seconds with positional arguments
        data_frac = [(2024, 5, 22, 10, 30, 45.123)]
        result_frac = datetime.datetime(2024, 5, 22, 10, 30, 45, 123000)
        df_frac = self.spark.createDataFrame(
            data_frac, ["year", "month", "day", "hour", "minute", "second"]
        )

        actual = df_frac.select(
            F.make_timestamp_ntz(
                df_frac.year,
                df_frac.month,
                df_frac.day,
                df_frac.hour,
                df_frac.minute,
                df_frac.second,
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac)])

        # Test 5: Fractional seconds with keyword arguments
        actual = df_frac.select(
            F.make_timestamp_ntz(
                years=df_frac.year,
                months=df_frac.month,
                days=df_frac.day,
                hours=df_frac.hour,
                mins=df_frac.minute,
                secs=df_frac.second,
            )
        )
        assertDataFrameEqual(actual, [Row(result_frac)])

        # Test 6: Fractional seconds with date/time arguments
        df_dt_frac = self.spark.range(1).select(
            F.lit(datetime.date(2024, 5, 22)).alias("date"),
            F.lit(datetime.time(10, 30, 45, 123000)).alias("time"),
        )
        actual = df_dt_frac.select(F.make_timestamp_ntz(date=df_dt_frac.date, time=df_dt_frac.time))
        assertDataFrameEqual(actual, [Row(result_frac)])

        # Test 7: Edge case - February 29 in leap year
        df_leap = self.spark.createDataFrame(
            [(2024, 2, 29, 0, 0, 0)], ["year", "month", "day", "hour", "minute", "second"]
        )
        expected_leap = datetime.datetime(2024, 2, 29, 0, 0, 0)
        actual = df_leap.select(
            F.make_timestamp_ntz(
                df_leap.year,
                df_leap.month,
                df_leap.day,
                df_leap.hour,
                df_leap.minute,
                df_leap.second,
            )
        )
        assertDataFrameEqual(actual, [Row(expected_leap)])

        # Test 8: Maximum valid time values
        df_max = self.spark.createDataFrame(
            [(2024, 12, 31, 23, 59, 59)], ["year", "month", "day", "hour", "minute", "second"]
        )
        expected_max = datetime.datetime(2024, 12, 31, 23, 59, 59)
        actual = df_max.select(
            F.make_timestamp_ntz(
                df_max.year, df_max.month, df_max.day, df_max.hour, df_max.minute, df_max.second
            )
        )
        assertDataFrameEqual(actual, [Row(expected_max)])

        # Test 9: Minimum valid values
        df_min = self.spark.createDataFrame(
            [(1, 1, 1, 0, 0, 0)], ["year", "month", "day", "hour", "minute", "second"]
        )
        expected_min = datetime.datetime(1, 1, 1, 0, 0, 0)
        actual = df_min.select(
            F.make_timestamp_ntz(
                df_min.year, df_min.month, df_min.day, df_min.hour, df_min.minute, df_min.second
            )
        )
        assertDataFrameEqual(actual, [Row(expected_min)])

        # Test 10: Mixed positional and keyword (should work for valid combinations)
        actual = df.select(
            F.make_timestamp_ntz(
                df.year, df.month, df.day, hours=df.hour, mins=df.minute, secs=df.second
            )
        )
        assertDataFrameEqual(actual, [Row(result)])

        # Test 11: Using literal values
        actual = self.spark.range(1).select(
            F.make_timestamp_ntz(F.lit(2024), F.lit(5), F.lit(22), F.lit(10), F.lit(30), F.lit(0))
        )
        assertDataFrameEqual(actual, [Row(result)])

        # Test 12: Using string column names
        actual = df.select(F.make_timestamp_ntz("year", "month", "day", "hour", "minute", "second"))
        assertDataFrameEqual(actual, [Row(result)])

        # Error handling tests

        # Test 13: Mixing timestamp and date/time keyword arguments
        with self.assertRaises(PySparkValueError) as context:
            df_dt.select(
                F.make_timestamp_ntz(years=df.year, date=df_dt.date, time=df_dt.time)
            ).collect()
        error_msg = str(context.exception)
        self.assertIn("CANNOT_SET_TOGETHER", error_msg)
        self.assertIn("years|months|days|hours|mins|secs and date|time", error_msg)

        # Test 14: Incomplete keyword arguments - should raise Exception for None values
        with self.assertRaises(Exception):
            F.make_timestamp_ntz(years=df.year, months=df.month, days=df.day)

        # Test 15: Only one keyword argument - should raise Exception for None values
        with self.assertRaises(Exception):
            F.make_timestamp_ntz(years=df.year)

        # Test 16: Only date without time - should raise Exception for None values
        with self.assertRaises(Exception):
            F.make_timestamp_ntz(date=df_dt.date)

    def test_make_date(self):
        # SPARK-36554: expose make_date expression
        df = self.spark.createDataFrame([(2020, 6, 26)], ["Y", "M", "D"])
        row_from_col = df.select(F.make_date(df.Y, df.M, df.D)).first()
        self.assertEqual(row_from_col[0], datetime.date(2020, 6, 26))
        row_from_name = df.select(F.make_date("Y", "M", "D")).first()
        self.assertEqual(row_from_name[0], datetime.date(2020, 6, 26))

    def test_expr(self):
        row = Row(a="length string", b=75)
        df = self.spark.createDataFrame([row])
        result = df.select(F.expr("length(a)")).collect()[0].asDict()
        self.assertEqual(13, result["length(a)"])

    # add test for SPARK-10577 (test broadcast join hint)
    def test_functions_broadcast(self):
        df1 = self.spark.createDataFrame([(1, "1"), (2, "2")], ("key", "value"))
        df2 = self.spark.createDataFrame([(1, "1"), (2, "2")], ("key", "value"))

        # equijoin - should be converted into broadcast join
        with io.StringIO() as buf, redirect_stdout(buf):
            df1.join(F.broadcast(df2), "key").explain(True)
            self.assertGreaterEqual(buf.getvalue().count("Broadcast"), 1)

        # no join key -- should not be a broadcast join
        with io.StringIO() as buf, redirect_stdout(buf):
            df1.crossJoin(F.broadcast(df2)).explain(True)
            self.assertGreaterEqual(buf.getvalue().count("Broadcast"), 1)

        # planner should not crash without a join
        F.broadcast(df1).explain(True)

    def test_first_last_ignorenulls(self):
        df = self.spark.range(0, 100)
        df2 = df.select(F.when(df.id % 3 == 0, None).otherwise(df.id).alias("id"))
        df3 = df2.select(
            F.first(df2.id, False).alias("a"),
            F.first(df2.id, True).alias("b"),
            F.last(df2.id, False).alias("c"),
            F.last(df2.id, True).alias("d"),
        )
        assertDataFrameEqual([Row(a=None, b=1, c=None, d=98)], df3)

    def test_approxQuantile(self):
        df = self.spark.createDataFrame([Row(a=i, b=i + 10) for i in range(10)])
        for f in ["a", "a"]:
            aq = df.stat.approxQuantile(f, [0.1, 0.5, 0.9], 0.1)
            self.assertTrue(isinstance(aq, list))
            self.assertEqual(len(aq), 3)
        self.assertTrue(all(isinstance(q, float) for q in aq))
        aqs = df.stat.approxQuantile(["a", "b"], [0.1, 0.5, 0.9], 0.1)
        self.assertTrue(isinstance(aqs, list))
        self.assertEqual(len(aqs), 2)
        self.assertTrue(isinstance(aqs[0], list))
        self.assertEqual(len(aqs[0]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqs[0]))
        self.assertTrue(isinstance(aqs[1], list))
        self.assertEqual(len(aqs[1]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqs[1]))
        aqt = df.stat.approxQuantile(("a", "b"), [0.1, 0.5, 0.9], 0.1)
        self.assertTrue(isinstance(aqt, list))
        self.assertEqual(len(aqt), 2)
        self.assertTrue(isinstance(aqt[0], list))
        self.assertEqual(len(aqt[0]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqt[0]))
        self.assertTrue(isinstance(aqt[1], list))
        self.assertEqual(len(aqt[1]), 3)
        self.assertTrue(all(isinstance(q, float) for q in aqt[1]))
        self.assertRaises(TypeError, lambda: df.stat.approxQuantile(123, [0.1, 0.9], 0.1))
        self.assertRaises(TypeError, lambda: df.stat.approxQuantile(("a", 123), [0.1, 0.9], 0.1))
        self.assertRaises(TypeError, lambda: df.stat.approxQuantile(["a", 123], [0.1, 0.9], 0.1))

    def test_sorting_functions_with_column(self):
        self.check_sorting_functions_with_column(Column)

    def check_sorting_functions_with_column(self, tpe):
        funs = [F.asc_nulls_first, F.asc_nulls_last, F.desc_nulls_first, F.desc_nulls_last]
        exprs = [F.col("x"), "x"]

        for fun in funs:
            for _expr in exprs:
                res = fun(_expr)
                self.assertIsInstance(res, tpe)
                self.assertIn(f"""'x {fun.__name__.replace("_", " ").upper()}'""", str(res))

        for _expr in exprs:
            res = F.asc(_expr)
            self.assertIsInstance(res, tpe)
            self.assertIn("""'x ASC NULLS FIRST'""", str(res))

        for _expr in exprs:
            res = F.desc(_expr)
            self.assertIsInstance(res, tpe)
            self.assertIn("""'x DESC NULLS LAST'""", str(res))

    def test_sort_with_nulls_order(self):
        df = self.spark.createDataFrame(
            [("Tom", 80), (None, 60), ("Alice", 50)], ["name", "height"]
        )
        assertDataFrameEqual(
            df.select(df.name).orderBy(F.asc_nulls_first("name")),
            [Row(name=None), Row(name="Alice"), Row(name="Tom")],
        )
        assertDataFrameEqual(
            df.select(df.name).orderBy(F.asc_nulls_last("name")),
            [Row(name="Alice"), Row(name="Tom"), Row(name=None)],
        )
        assertDataFrameEqual(
            df.select(df.name).orderBy(F.desc_nulls_first("name")),
            [Row(name=None), Row(name="Tom"), Row(name="Alice")],
        )
        assertDataFrameEqual(
            df.select(df.name).orderBy(F.desc_nulls_last("name")),
            [Row(name="Tom"), Row(name="Alice"), Row(name=None)],
        )

    def test_input_file_name_reset_for_rdd(self):
        rdd = self.sc.textFile("python/test_support/hello/hello.txt").map(lambda x: {"data": x})
        df = self.spark.createDataFrame(rdd, "data STRING")
        df.select(F.input_file_name().alias("file")).collect()

        non_file_df = self.spark.range(100).select(F.input_file_name())

        results = non_file_df.collect()
        self.assertTrue(len(results) == 100)

        # [SPARK-24605]: if everything was properly reset after the last job, this should return
        # empty string rather than the file read in the last job.
        for result in results:
            self.assertEqual(result[0], "")

    def test_slice(self):
        df = self.spark.createDataFrame(
            [
                (
                    [1, 2, 3],
                    2,
                    2,
                ),
                (
                    [4, 5],
                    2,
                    2,
                ),
            ],
            ["x", "index", "len"],
        )

        expected = [Row(sliced=[2, 3]), Row(sliced=[5])]
        assertDataFrameEqual(df.select(F.slice(df.x, 2, 2).alias("sliced")), expected)
        assertDataFrameEqual(df.select(F.slice(df.x, F.lit(2), F.lit(2)).alias("sliced")), expected)
        assertDataFrameEqual(df.select(F.slice("x", "index", "len").alias("sliced")), expected)

        assertDataFrameEqual(
            df.select(F.slice(df.x, F.size(df.x) - 1, F.lit(1)).alias("sliced")),
            [Row(sliced=[2]), Row(sliced=[4])],
        )
        assertDataFrameEqual(
            df.select(F.slice(df.x, F.lit(1), F.size(df.x) - 1).alias("sliced")),
            [Row(sliced=[1, 2]), Row(sliced=[4])],
        )

    def test_array_repeat(self):
        df = self.spark.range(1)
        df = df.withColumn("repeat_n", F.lit(3))

        expected = [Row(val=[0, 0, 0])]
        assertDataFrameEqual(df.select(F.array_repeat("id", 3).alias("val")), expected)
        assertDataFrameEqual(df.select(F.array_repeat("id", F.lit(3)).alias("val")), expected)
        assertDataFrameEqual(df.select(F.array_repeat("id", "repeat_n").alias("val")), expected)

    def test_input_file_name_udf(self):
        df = self.spark.read.text("python/test_support/hello/hello.txt")
        df = df.select(F.udf(lambda x: x)("value"), F.input_file_name().alias("file"))
        file_name = df.collect()[0].file
        self.assertTrue("python/test_support/hello/hello.txt" in file_name)

    def test_least(self):
        df = self.spark.createDataFrame([(1, 4, 3)], ["a", "b", "c"])

        expected = [Row(least=1)]
        assertDataFrameEqual(df.select(F.least(df.a, df.b, df.c).alias("least")), expected)
        assertDataFrameEqual(
            df.select(F.least(F.lit(3), F.lit(5), F.lit(1)).alias("least")), expected
        )
        assertDataFrameEqual(df.select(F.least("a", "b", "c").alias("least")), expected)

        with self.assertRaises(PySparkValueError) as pe:
            df.select(F.least(df.a).alias("least")).collect()

        self.check_error(
            exception=pe.exception,
            errorClass="WRONG_NUM_COLUMNS",
            messageParameters={"func_name": "least", "num_cols": "2"},
        )

    def test_overlay(self):
        actual = list(
            chain.from_iterable(
                [
                    re.findall("(overlay\\(.*\\))", str(x))
                    for x in [
                        F.overlay(F.col("foo"), F.col("bar"), 1),
                        F.overlay("x", "y", 3),
                        F.overlay(F.col("x"), F.col("y"), 1, 3),
                        F.overlay("x", "y", 2, 5),
                        F.overlay("x", "y", F.lit(11)),
                        F.overlay("x", "y", F.lit(2), F.lit(5)),
                    ]
                ]
            )
        )

        expected = [
            "overlay(foo, bar, 1, -1)",
            "overlay(x, y, 3, -1)",
            "overlay(x, y, 1, 3)",
            "overlay(x, y, 2, 5)",
            "overlay(x, y, 11, -1)",
            "overlay(x, y, 2, 5)",
        ]

        self.assertListEqual(actual, expected)

        df = self.spark.createDataFrame([("SPARK_SQL", "CORE", 7, 0)], ("x", "y", "pos", "len"))

        exp = [Row(ol="SPARK_CORESQL")]
        assertDataFrameEqual(df.select(F.overlay(df.x, df.y, 7, 0).alias("ol")), exp)
        assertDataFrameEqual(df.select(F.overlay(df.x, df.y, F.lit(7), F.lit(0)).alias("ol")), exp)
        assertDataFrameEqual(df.select(F.overlay("x", "y", "pos", "len").alias("ol")), exp)

        with self.assertRaises(PySparkTypeError) as pe:
            df.select(F.overlay(df.x, df.y, 7.5, 0).alias("ol")).collect()

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={"arg_name": "pos", "arg_type": "float"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            df.select(F.overlay(df.x, df.y, 7, 0.5).alias("ol")).collect()

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={"arg_name": "len", "arg_type": "float"},
        )

    def test_percentile(self):
        actual = list(
            chain.from_iterable(
                [
                    re.findall("(percentile\\(.*\\))", str(x))
                    for x in [
                        F.percentile(F.col("foo"), F.lit(0.5)),
                        F.percentile(F.col("bar"), 0.25, 2),
                        F.percentile(F.col("bar"), [0.25, 0.5, 0.75]),
                        F.percentile(F.col("foo"), (0.05, 0.95), 100),
                        F.percentile("foo", 0.5),
                        F.percentile("bar", [0.1, 0.9], F.lit(10)),
                    ]
                ]
            )
        )

        expected = [
            "percentile(foo, 0.5, 1)",
            "percentile(bar, 0.25, 2)",
            "percentile(bar, array(0.25, 0.5, 0.75), 1)",
            "percentile(foo, array(0.05, 0.95), 100)",
            "percentile(foo, 0.5, 1)",
            "percentile(bar, array(0.1, 0.9), 10)",
        ]

        self.assertListEqual(actual, expected)

    def test_median(self):
        actual = list(
            chain.from_iterable(
                [
                    re.findall("(median\\(.*\\))", str(x))
                    for x in [
                        F.median(F.col("foo")),
                    ]
                ]
            )
        )

        expected = [
            "median(foo)",
        ]

        self.assertListEqual(actual, expected)

    def test_percentile_approx(self):
        actual = list(
            chain.from_iterable(
                [
                    re.findall("(percentile_approx\\(.*\\))", str(x))
                    for x in [
                        F.percentile_approx(F.col("foo"), F.lit(0.5)),
                        F.percentile_approx(F.col("bar"), 0.25, 42),
                        F.percentile_approx(F.col("bar"), [0.25, 0.5, 0.75]),
                        F.percentile_approx(F.col("foo"), (0.05, 0.95), 100),
                        F.percentile_approx("foo", 0.5),
                        F.percentile_approx("bar", [0.1, 0.9], F.lit(10)),
                    ]
                ]
            )
        )

        expected = [
            "percentile_approx(foo, 0.5, 10000)",
            "percentile_approx(bar, 0.25, 42)",
            "percentile_approx(bar, array(0.25, 0.5, 0.75), 10000)",
            "percentile_approx(foo, array(0.05, 0.95), 100)",
            "percentile_approx(foo, 0.5, 10000)",
            "percentile_approx(bar, array(0.1, 0.9), 10)",
        ]

        self.assertListEqual(actual, expected)

    def test_nth_value(self):
        df = self.spark.createDataFrame(
            [
                ("a", 0, None),
                ("a", 1, "x"),
                ("a", 2, "y"),
                ("a", 3, "z"),
                ("a", 4, None),
                ("b", 1, None),
                ("b", 2, None),
            ],
            schema=("key", "order", "value"),
        )
        w = Window.partitionBy("key").orderBy("order")

        rs = df.select(
            df.key,
            df.order,
            F.nth_value("value", 2).over(w),
            F.nth_value("value", 2, False).over(w),
            F.nth_value("value", 2, True).over(w),
        ).collect()

        expected = [
            ("a", 0, None, None, None),
            ("a", 1, "x", "x", None),
            ("a", 2, "x", "x", "y"),
            ("a", 3, "x", "x", "y"),
            ("a", 4, "x", "x", "y"),
            ("b", 1, None, None, None),
            ("b", 2, None, None, None),
        ]

        for r, ex in zip(sorted(rs), sorted(expected)):
            self.assertEqual(tuple(r), ex[: len(r)])

    def test_higher_order_function_failures(self):
        # Should fail with varargs
        with self.assertRaises(PySparkValueError) as pe:
            F.transform(F.col("foo"), lambda *x: F.lit(1))

        self.check_error(
            exception=pe.exception,
            errorClass="UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION",
            messageParameters={"func_name": "<lambda>"},
        )

        # Should fail with kwargs
        with self.assertRaises(PySparkValueError) as pe:
            F.transform(F.col("foo"), lambda **x: F.lit(1))

        self.check_error(
            exception=pe.exception,
            errorClass="UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION",
            messageParameters={"func_name": "<lambda>"},
        )

        # Should fail with nullary function
        with self.assertRaises(PySparkValueError) as pe:
            F.transform(F.col("foo"), lambda: F.lit(1))

        self.check_error(
            exception=pe.exception,
            errorClass="WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION",
            messageParameters={"func_name": "<lambda>", "num_args": "0"},
        )

        # Should fail with quaternary function
        with self.assertRaises(PySparkValueError) as pe:
            F.transform(F.col("foo"), lambda x1, x2, x3, x4: F.lit(1))

        self.check_error(
            exception=pe.exception,
            errorClass="WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION",
            messageParameters={"func_name": "<lambda>", "num_args": "4"},
        )

        # Should fail if function doesn't return Column
        with self.assertRaises(PySparkValueError) as pe:
            F.transform(F.col("foo"), lambda x: 1)

        self.check_error(
            exception=pe.exception,
            errorClass="HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN",
            messageParameters={"func_name": "<lambda>", "return_type": "int"},
        )

    def test_nested_higher_order_function(self):
        # SPARK-35382: lambda vars must be resolved properly in nested higher order functions
        df = self.spark.sql("SELECT array(1, 2, 3) as numbers, array('a', 'b', 'c') as letters")

        actual = df.select(
            F.flatten(
                F.transform(
                    "numbers",
                    lambda number: F.transform(
                        "letters", lambda letter: F.struct(number.alias("n"), letter.alias("l"))
                    ),
                )
            )
        ).first()[0]

        expected = [
            (1, "a"),
            (1, "b"),
            (1, "c"),
            (2, "a"),
            (2, "b"),
            (2, "c"),
            (3, "a"),
            (3, "b"),
            (3, "c"),
        ]

        self.assertEqual(actual, expected)

    def test_window_functions(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        w = Window.partitionBy("value").orderBy("key")

        sel = df.select(
            df.value,
            df.key,
            F.max("key").over(w.rowsBetween(0, 1)),
            F.min("key").over(w.rowsBetween(0, 1)),
            F.count("key").over(w.rowsBetween(float("-inf"), float("inf"))),
            F.row_number().over(w),
            F.rank().over(w),
            F.dense_rank().over(w),
            F.ntile(2).over(w),
        )
        rs = sorted(sel.collect())
        expected = [
            ("1", 1, 1, 1, 1, 1, 1, 1, 1),
            ("2", 1, 1, 1, 3, 1, 1, 1, 1),
            ("2", 1, 2, 1, 3, 2, 1, 1, 1),
            ("2", 2, 2, 2, 3, 3, 3, 2, 2),
        ]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

    def test_window_functions_without_partitionBy(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])
        w = Window.orderBy("key", df.value)

        sel = df.select(
            df.value,
            df.key,
            F.max("key").over(w.rowsBetween(0, 1)),
            F.min("key").over(w.rowsBetween(0, 1)),
            F.count("key").over(w.rowsBetween(float("-inf"), float("inf"))),
            F.row_number().over(w),
            F.rank().over(w),
            F.dense_rank().over(w),
            F.ntile(2).over(w),
        )
        rs = sorted(sel.collect())
        expected = [
            ("1", 1, 1, 1, 4, 1, 1, 1, 1),
            ("2", 1, 1, 1, 4, 2, 2, 2, 1),
            ("2", 1, 2, 1, 4, 3, 2, 2, 2),
            ("2", 2, 2, 2, 4, 4, 4, 3, 2),
        ]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

    def test_window_functions_cumulative_sum(self):
        df = self.spark.createDataFrame([("one", 1), ("two", 2)], ["key", "value"])

        # Test cumulative sum
        sel = df.select(
            df.key, F.sum(df.value).over(Window.rowsBetween(Window.unboundedPreceding, 0))
        )
        rs = sorted(sel.collect())
        expected = [("one", 1), ("two", 3)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

        # Test boundary values less than JVM's Long.MinValue and make sure we don't overflow
        sel = df.select(
            df.key, F.sum(df.value).over(Window.rowsBetween(Window.unboundedPreceding - 1, 0))
        )
        rs = sorted(sel.collect())
        expected = [("one", 1), ("two", 3)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

        # Test boundary values greater than JVM's Long.MaxValue and make sure we don't overflow
        frame_end = Window.unboundedFollowing + 1
        sel = df.select(
            df.key, F.sum(df.value).over(Window.rowsBetween(Window.currentRow, frame_end))
        )
        rs = sorted(sel.collect())
        expected = [("one", 3), ("two", 2)]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

    def test_window_functions_moving_average(self):
        data = [
            (datetime.datetime(2023, 1, 1), 20),
            (datetime.datetime(2023, 1, 2), 22),
            (datetime.datetime(2023, 1, 3), 21),
            (datetime.datetime(2023, 1, 4), 23),
            (datetime.datetime(2023, 1, 5), 24),
            (datetime.datetime(2023, 1, 6), 26),
        ]
        df = self.spark.createDataFrame(data, ["date", "temperature"])

        def to_sec(i):
            return i * 86400

        w = Window.orderBy(F.col("date").cast("timestamp").cast("long")).rangeBetween(-to_sec(3), 0)
        res = df.withColumn("3_day_avg_temp", F.avg("temperature").over(w))
        rs = sorted(res.collect())
        expected = [
            (datetime.datetime(2023, 1, 1, 0, 0), 20, 20.0),
            (datetime.datetime(2023, 1, 2, 0, 0), 22, 21.0),
            (datetime.datetime(2023, 1, 3, 0, 0), 21, 21.0),
            (datetime.datetime(2023, 1, 4, 0, 0), 23, 21.5),
            (datetime.datetime(2023, 1, 5, 0, 0), 24, 22.5),
            (datetime.datetime(2023, 1, 6, 0, 0), 26, 23.5),
        ]
        for r, ex in zip(rs, expected):
            self.assertEqual(tuple(r), ex[: len(r)])

    def test_window_time(self):
        df = self.spark.createDataFrame(
            [(datetime.datetime(2016, 3, 11, 9, 0, 7), 1)], ["date", "val"]
        )

        w = df.groupBy(F.window("date", "5 seconds", "5 seconds")).agg(F.sum("val").alias("sum"))
        r = w.select(
            w.window.end.cast("string").alias("end"),
            F.window_time(w.window).cast("string").alias("window_time"),
            "sum",
        ).collect()
        self.assertEqual(
            r[0], Row(end="2016-03-11 09:00:10", window_time="2016-03-11 09:00:09.999999", sum=1)
        )

    def test_collect_functions(self):
        df = self.spark.createDataFrame([(1, "1"), (2, "2"), (1, "2"), (1, "2")], ["key", "value"])

        self.assertEqual(sorted(df.select(F.collect_set(df.key).alias("r")).collect()[0].r), [1, 2])
        self.assertEqual(
            sorted(df.select(F.collect_list(df.key).alias("r")).collect()[0].r), [1, 1, 1, 2]
        )
        self.assertEqual(
            sorted(df.select(F.collect_set(df.value).alias("r")).collect()[0].r), ["1", "2"]
        )
        self.assertEqual(
            sorted(df.select(F.collect_list(df.value).alias("r")).collect()[0].r),
            ["1", "2", "2", "2"],
        )

    def test_listagg_functions(self):
        df = self.spark.createDataFrame(
            [(1, "1"), (2, "2"), (None, None), (1, "2")], ["key", "value"]
        )
        df_with_bytes = self.spark.createDataFrame(
            [(b"\x01",), (b"\x02",), (None,), (b"\x03",), (b"\x02",)], ["bytes"]
        )
        df_with_nulls = self.spark.createDataFrame(
            [(None,), (None,), (None,), (None,), (None,)],
            StructType([StructField("nulls", StringType(), True)]),
        )
        # listagg and string_agg are aliases
        for listagg_ref in [F.listagg, F.string_agg]:
            self.assertEqual(df.select(listagg_ref(df.key).alias("r")).collect()[0].r, "121")
            self.assertEqual(df.select(listagg_ref(df.value).alias("r")).collect()[0].r, "122")
            self.assertEqual(
                df.select(listagg_ref(df.value, ",").alias("r")).collect()[0].r, "1,2,2"
            )
            self.assertEqual(
                df_with_bytes.select(listagg_ref(df_with_bytes.bytes, b"\x42").alias("r"))
                .collect()[0]
                .r,
                b"\x01\x42\x02\x42\x03\x42\x02",
            )
            self.assertEqual(
                df_with_nulls.select(listagg_ref(df_with_nulls.nulls).alias("r")).collect()[0].r,
                None,
            )

    def test_listagg_distinct_functions(self):
        df = self.spark.createDataFrame(
            [(1, "1"), (2, "2"), (None, None), (1, "2")], ["key", "value"]
        )
        df_with_bytes = self.spark.createDataFrame(
            [(b"\x01",), (b"\x02",), (None,), (b"\x03",), (b"\x02",)], ["bytes"]
        )
        df_with_nulls = self.spark.createDataFrame(
            [(None,), (None,), (None,), (None,), (None,)],
            StructType([StructField("nulls", StringType(), True)]),
        )
        # listagg_distinct and string_agg_distinct are aliases
        for listagg_distinct_ref in [F.listagg_distinct, F.string_agg_distinct]:
            self.assertEqual(
                df.select(listagg_distinct_ref(df.key).alias("r")).collect()[0].r, "12"
            )
            self.assertEqual(
                df.select(listagg_distinct_ref(df.value).alias("r")).collect()[0].r, "12"
            )
            self.assertEqual(
                df.select(listagg_distinct_ref(df.value, ",").alias("r")).collect()[0].r, "1,2"
            )
            self.assertEqual(
                df_with_bytes.select(listagg_distinct_ref(df_with_bytes.bytes, b"\x42").alias("r"))
                .collect()[0]
                .r,
                b"\x01\x42\x02\x42\x03",
            )
            self.assertEqual(
                df_with_nulls.select(listagg_distinct_ref(df_with_nulls.nulls).alias("r"))
                .collect()[0]
                .r,
                None,
            )

    def test_kll_sketch_agg_bigint(self):
        """Test kll_sketch_agg_bigint function"""
        df = self.spark.createDataFrame([1, 2, 3, 4, 5], "INT")

        # Test with default k
        sketch = df.agg(F.kll_sketch_agg_bigint("value")).first()[0]
        self.assertIsNotNone(sketch)
        self.assertIsInstance(sketch, (bytes, bytearray))

        # Test with explicit k
        sketch_k = df.agg(F.kll_sketch_agg_bigint("value", 400)).first()[0]
        self.assertIsNotNone(sketch_k)

    def test_kll_sketch_agg_float(self):
        """Test kll_sketch_agg_float function"""
        df = self.spark.createDataFrame([1.0, 2.0, 3.0, 4.0, 5.0], "FLOAT")

        sketch = df.agg(F.kll_sketch_agg_float("value")).first()[0]
        self.assertIsNotNone(sketch)
        self.assertIsInstance(sketch, (bytes, bytearray))

    def test_kll_sketch_agg_double(self):
        """Test kll_sketch_agg_double function"""
        df = self.spark.createDataFrame([1.0, 2.0, 3.0, 4.0, 5.0], "DOUBLE")

        sketch = df.agg(F.kll_sketch_agg_double("value")).first()[0]
        self.assertIsNotNone(sketch)
        self.assertIsInstance(sketch, (bytes, bytearray))

    def test_kll_sketch_to_string_bigint(self):
        """Test kll_sketch_to_string_bigint function"""
        df = self.spark.createDataFrame([1, 2, 3, 4, 5], "INT")
        sketch_df = df.agg(F.kll_sketch_agg_bigint("value").alias("sketch"))

        result = sketch_df.select(F.kll_sketch_to_string_bigint("sketch")).first()[0]
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        self.assertIn("Kll", result)

    def test_kll_sketch_get_n_bigint(self):
        """Test kll_sketch_get_n_bigint function"""
        df = self.spark.createDataFrame([1, 2, 3, 4, 5], "INT")
        sketch_df = df.agg(F.kll_sketch_agg_bigint("value").alias("sketch"))

        n = sketch_df.select(F.kll_sketch_get_n_bigint("sketch")).first()[0]
        self.assertEqual(n, 5)

    def test_kll_sketch_merge_bigint(self):
        """Test kll_sketch_merge_bigint function"""
        df = self.spark.createDataFrame([1, 2, 3], "INT")
        sketch_df = df.agg(F.kll_sketch_agg_bigint("value").alias("sketch"))

        merged = sketch_df.select(F.kll_sketch_merge_bigint("sketch", "sketch")).first()[0]
        self.assertIsNotNone(merged)
        self.assertIsInstance(merged, (bytes, bytearray))

    def test_kll_sketch_get_quantile_bigint(self):
        """Test kll_sketch_get_quantile_bigint function"""
        df = self.spark.createDataFrame([1, 2, 3, 4, 5], "INT")
        sketch_df = df.agg(F.kll_sketch_agg_bigint("value").alias("sketch"))

        quantile = sketch_df.select(F.kll_sketch_get_quantile_bigint("sketch", F.lit(0.5))).first()[
            0
        ]
        self.assertIsNotNone(quantile)
        self.assertGreaterEqual(quantile, 1)
        self.assertLessEqual(quantile, 5)

    def test_kll_sketch_get_quantile_bigint_array(self):
        """Test kll_sketch_get_quantile_bigint with array of ranks"""
        df = self.spark.createDataFrame([1, 2, 3, 4, 5], "INT")
        sketch_df = df.agg(F.kll_sketch_agg_bigint("value").alias("sketch"))

        quantiles = sketch_df.select(
            F.kll_sketch_get_quantile_bigint(
                "sketch", F.array(F.lit(0.25), F.lit(0.5), F.lit(0.75))
            )
        ).first()[0]
        self.assertIsNotNone(quantiles)
        self.assertEqual(len(quantiles), 3)

    def test_kll_sketch_get_rank_bigint(self):
        """Test kll_sketch_get_rank_bigint function"""
        df = self.spark.createDataFrame([1, 2, 3, 4, 5], "INT")
        sketch_df = df.agg(F.kll_sketch_agg_bigint("value").alias("sketch"))

        rank = sketch_df.select(F.kll_sketch_get_rank_bigint("sketch", F.lit(3))).first()[0]
        self.assertIsNotNone(rank)
        self.assertGreaterEqual(rank, 0.0)
        self.assertLessEqual(rank, 1.0)

    def test_kll_sketch_float_variants(self):
        """Test all float variant functions"""
        df = self.spark.createDataFrame([1.0, 2.0, 3.0, 4.0, 5.0], "FLOAT")
        sketch_df = df.agg(F.kll_sketch_agg_float("value").alias("sketch"))

        # Test to_string
        string_result = sketch_df.select(F.kll_sketch_to_string_float("sketch")).first()[0]
        self.assertIn("Kll", string_result)

        # Test get_n
        n = sketch_df.select(F.kll_sketch_get_n_float("sketch")).first()[0]
        self.assertEqual(n, 5)

        # Test merge
        merged = sketch_df.select(F.kll_sketch_merge_float("sketch", "sketch")).first()[0]
        self.assertIsNotNone(merged)

        # Test get_quantile
        quantile = sketch_df.select(F.kll_sketch_get_quantile_float("sketch", F.lit(0.5))).first()[
            0
        ]
        self.assertIsNotNone(quantile)

        # Test get_rank
        rank = sketch_df.select(F.kll_sketch_get_rank_float("sketch", F.lit(3.0))).first()[0]
        self.assertGreaterEqual(rank, 0.0)
        self.assertLessEqual(rank, 1.0)

    def test_kll_sketch_double_variants(self):
        """Test all double variant functions"""
        df = self.spark.createDataFrame([1.0, 2.0, 3.0, 4.0, 5.0], "DOUBLE")
        sketch_df = df.agg(F.kll_sketch_agg_double("value").alias("sketch"))

        # Test to_string
        string_result = sketch_df.select(F.kll_sketch_to_string_double("sketch")).first()[0]
        self.assertIn("Kll", string_result)

        # Test get_n
        n = sketch_df.select(F.kll_sketch_get_n_double("sketch")).first()[0]
        self.assertEqual(n, 5)

        # Test merge
        merged = sketch_df.select(F.kll_sketch_merge_double("sketch", "sketch")).first()[0]
        self.assertIsNotNone(merged)

        # Test get_quantile
        quantile = sketch_df.select(F.kll_sketch_get_quantile_double("sketch", F.lit(0.5))).first()[
            0
        ]
        self.assertIsNotNone(quantile)

        # Test get_rank
        rank = sketch_df.select(F.kll_sketch_get_rank_double("sketch", F.lit(3.0))).first()[0]
        self.assertGreaterEqual(rank, 0.0)
        self.assertLessEqual(rank, 1.0)

    def test_kll_sketch_with_nulls(self):
        """Test KLL sketch with null values"""
        df = self.spark.createDataFrame([(1,), (None,), (3,), (4,), (None,)], ["value"])
        sketch_df = df.agg(F.kll_sketch_agg_bigint("value").alias("sketch"))

        n = sketch_df.select(F.kll_sketch_get_n_bigint("sketch")).first()[0]
        # Should only count non-null values
        self.assertEqual(n, 3)

    def test_datetime_functions(self):
        df = self.spark.range(1).selectExpr("'2017-01-22' as dateCol")
        parse_result = df.select(F.to_date(F.col("dateCol"))).first()
        self.assertEqual(datetime.date(2017, 1, 22), parse_result["to_date(dateCol)"])

    def test_try_datetime_functions(self):
        df = self.spark.range(1).selectExpr("'2017-01-22' as dateCol")
        parse_result = df.select(F.try_to_date(F.col("dateCol")).alias("tryToDateCol")).first()
        self.assertEqual(datetime.date(2017, 1, 22), parse_result["tryToDateCol"])

    def test_assert_true(self):
        self.check_assert_true(SparkRuntimeException)

    def check_assert_true(self, tpe):
        df = self.spark.range(3)

        assertDataFrameEqual(
            df.select(F.assert_true(df.id < 3)).toDF("val"),
            [Row(val=None), Row(val=None), Row(val=None)],
        )

        with self.assertRaisesRegex(tpe, r"\[USER_RAISED_EXCEPTION\] too big"):
            df.select(F.assert_true(df.id < 2, "too big")).toDF("val").collect()

        with self.assertRaisesRegex(tpe, r"\[USER_RAISED_EXCEPTION\] 2000000.0"):
            df.select(F.assert_true(df.id < 2, df.id * 1e6)).toDF("val").collect()

        with self.assertRaises(PySparkTypeError) as pe:
            df.select(F.assert_true(df.id < 2, 5))

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "errMsg", "arg_type": "int"},
        )

    def test_raise_error(self):
        self.check_raise_error(SparkRuntimeException)

    def check_raise_error(self, tpe):
        df = self.spark.createDataFrame([Row(id="foobar")])

        with self.assertRaisesRegex(tpe, "foobar"):
            df.select(F.raise_error(df.id)).collect()

        with self.assertRaisesRegex(tpe, "barfoo"):
            df.select(F.raise_error("barfoo")).collect()

        with self.assertRaises(PySparkTypeError) as pe:
            df.select(F.raise_error(None))

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "errMsg", "arg_type": "NoneType"},
        )

    def test_sum_distinct(self):
        self.spark.range(10).select(
            F.assert_true(F.sum_distinct(F.col("id")) == F.sumDistinct(F.col("id")))
        ).collect()

    def test_shiftleft(self):
        self.spark.range(10).select(
            F.assert_true(F.shiftLeft(F.col("id"), 2) == F.shiftleft(F.col("id"), 2))
        ).collect()

    def test_shiftright(self):
        self.spark.range(10).select(
            F.assert_true(F.shiftRight(F.col("id"), 2) == F.shiftright(F.col("id"), 2))
        ).collect()

    def test_shiftrightunsigned(self):
        self.spark.range(10).select(
            F.assert_true(
                F.shiftRightUnsigned(F.col("id"), 2) == F.shiftrightunsigned(F.col("id"), 2)
            )
        ).collect()

    def test_lit_time(self):
        t = datetime.time(12, 34, 56)
        actual = self.spark.range(1).select(F.lit(t)).first()[0]
        self.assertEqual(actual, t)

    def test_lit_day_time_interval(self):
        td = datetime.timedelta(days=1, hours=12, milliseconds=123)
        actual = self.spark.range(1).select(F.lit(td)).first()[0]
        self.assertEqual(actual, td)

    def test_lit_list(self):
        # SPARK-40271: added list type supporting
        test_list = [1, 2, 3]
        expected = [1, 2, 3]
        actual = self.spark.range(1).select(F.lit(test_list)).first()[0]
        self.assertEqual(actual, expected)

        test_list = [[1, 2, 3], [3, 4]]
        expected = [[1, 2, 3], [3, 4]]
        actual = self.spark.range(1).select(F.lit(test_list)).first()[0]
        self.assertEqual(actual, expected)

        with self.sql_conf({"spark.sql.ansi.enabled": False}):
            test_list = ["a", 1, None, 1.0]
            expected = ["a", "1", None, "1.0"]
            actual = self.spark.range(1).select(F.lit(test_list)).first()[0]
            self.assertEqual(actual, expected)

            test_list = [["a", 1, None, 1.0], [1, None, "b"]]
            expected = [["a", "1", None, "1.0"], ["1", None, "b"]]
            actual = self.spark.range(1).select(F.lit(test_list)).first()[0]
            self.assertEqual(actual, expected)

        df = self.spark.range(10)
        with self.assertRaises(PySparkValueError) as pe:
            F.lit([df.id, df.id])

        self.check_error(
            exception=pe.exception,
            errorClass="COLUMN_IN_LIST",
            messageParameters={"func_name": "lit"},
        )

    # Test added for SPARK-39832; change Python API to accept both col & str as input
    def test_regexp_replace(self):
        df = self.spark.createDataFrame(
            [("100-200", r"(\d+)", "--")], ["str", "pattern", "replacement"]
        )
        self.assertTrue(
            all(
                df.select(
                    F.regexp_replace("str", r"(\d+)", "--") == "-----",
                    F.regexp_replace("str", F.col("pattern"), F.col("replacement")) == "-----",
                ).first()
            )
        )

    @unittest.skipIf(not have_numpy, "NumPy not installed")
    def test_lit_np_scalar(self):
        import numpy as np

        dtype_to_spark_dtypes = [
            (np.int8, [("1", "tinyint")]),
            (np.int16, [("1", "smallint")]),
            (np.int32, [("1", "int")]),
            (np.int64, [("1", "bigint")]),
            (np.float32, [("1.0", "float")]),
            (np.float64, [("1.0", "double")]),
            (np.bool_, [("true", "boolean")]),
        ]
        for dtype, spark_dtypes in dtype_to_spark_dtypes:
            with self.subTest(dtype):
                self.assertEqual(self.spark.range(1).select(F.lit(dtype(1))).dtypes, spark_dtypes)

    @unittest.skipIf(not have_numpy, "NumPy not installed")
    def test_np_scalar_input(self):
        import numpy as np

        df = self.spark.createDataFrame([([1, 2, 3],), ([],)], ["data"])
        for dtype in [np.int8, np.int16, np.int32, np.int64]:
            res = df.select(F.array_contains(df.data, dtype(1)).alias("b"))
            assertDataFrameEqual([Row(b=True), Row(b=False)], res)
            res = df.select(F.array_position(df.data, dtype(1)).alias("c"))
            assertDataFrameEqual([Row(c=1), Row(c=0)], res)

        df = self.spark.createDataFrame([([1.0, 2.0, 3.0],), ([],)], ["data"])
        for dtype in [np.float32, np.float64]:
            res = df.select(F.array_contains(df.data, dtype(1)).alias("b"))
            assertDataFrameEqual([Row(b=True), Row(b=False)], res)
            res = df.select(F.array_position(df.data, dtype(1)).alias("c"))
            assertDataFrameEqual([Row(c=1), Row(c=0)], res)

    @unittest.skipIf(not have_numpy, "NumPy not installed")
    def test_ndarray_input(self):
        import numpy as np

        arr_dtype_to_spark_dtypes = [
            ("int8", [("b", "array<tinyint>")]),
            ("int16", [("b", "array<smallint>")]),
            ("int32", [("b", "array<int>")]),
            ("int64", [("b", "array<bigint>")]),
            ("float32", [("b", "array<float>")]),
            ("float64", [("b", "array<double>")]),
        ]
        for t, expected_spark_dtypes in arr_dtype_to_spark_dtypes:
            arr = np.array([1, 2]).astype(t)
            self.assertEqual(
                expected_spark_dtypes, self.spark.range(1).select(F.lit(arr).alias("b")).dtypes
            )
        arr = np.array([1, 2]).astype(np.uint)
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.range(1).select(F.lit(arr).alias("b"))

        self.check_error(
            exception=pe.exception,
            errorClass="UNSUPPORTED_NUMPY_ARRAY_SCALAR",
            messageParameters={
                "dtype": "uint64",
            },
        )

    @unittest.skipIf(not have_numpy, "NumPy not installed")
    def test_bool_ndarray(self):
        import numpy as np

        for arr in [
            np.array([], np.bool_),
            np.array([True, False], np.bool_),
            np.array([1, 0, 3], np.bool_),
        ]:
            self.assertEqual(
                [("a", "array<boolean>")],
                self.spark.range(1).select(F.lit(arr).alias("a")).dtypes,
            )

    @unittest.skipIf(not have_numpy, "NumPy not installed")
    def test_str_ndarray(self):
        import numpy as np

        for arr in [
            np.array([], np.str_),
            np.array(["a"], np.str_),
            np.array([1, 2, 3], np.str_),
        ]:
            self.assertEqual(
                [("a", "array<string>")],
                self.spark.range(1).select(F.lit(arr).alias("a")).dtypes,
            )

    @unittest.skipIf(not have_numpy, "NumPy not installed")
    def test_empty_ndarray(self):
        import numpy as np

        arr_dtype_to_spark_dtypes = [
            ("int8", [("b", "array<tinyint>")]),
            ("int16", [("b", "array<smallint>")]),
            ("int32", [("b", "array<int>")]),
            ("int64", [("b", "array<bigint>")]),
            ("float32", [("b", "array<float>")]),
            ("float64", [("b", "array<double>")]),
        ]
        for t, expected_spark_dtypes in arr_dtype_to_spark_dtypes:
            arr = np.array([]).astype(t)
            self.assertEqual(
                expected_spark_dtypes, self.spark.range(1).select(F.lit(arr).alias("b")).dtypes
            )

    def test_binary_math_function(self):
        funcs, expected = zip(
            *[(F.atan2, 0.13664), (F.hypot, 8.07527), (F.pow, 2.14359), (F.pmod, 1.1)]
        )
        df = self.spark.range(1).select(*(func(1.1, 8) for func in funcs))
        for a, e in zip(df.first(), expected):
            self.assertAlmostEqual(a, e, 5)

    def test_map_functions(self):
        # SPARK-38496: Check basic functionality of all "map" type related functions
        expected = {"a": 1, "b": 2}
        expected2 = {"c": 3, "d": 4}
        df = self.spark.createDataFrame(
            [(list(expected.keys()), list(expected.values()))], ["k", "v"]
        )
        actual = (
            df.select(
                F.expr("map('c', 3, 'd', 4) as dict2"),
                F.map_from_arrays(df.k, df.v).alias("dict"),
                "*",
            )
            .select(
                F.map_contains_key("dict", "a").alias("one"),
                F.map_contains_key("dict", "d").alias("not_exists"),
                F.map_keys("dict").alias("keys"),
                F.map_values("dict").alias("values"),
                F.map_entries("dict").alias("items"),
                "*",
            )
            .select(
                F.map_concat("dict", "dict2").alias("merged"),
                F.map_from_entries(F.arrays_zip("keys", "values")).alias("from_items"),
                "*",
            )
            .first()
        )
        self.assertEqual(expected, actual["dict"])
        self.assertTrue(actual["one"])
        self.assertFalse(actual["not_exists"])
        self.assertEqual(list(expected.keys()), actual["keys"])
        self.assertEqual(list(expected.values()), actual["values"])
        self.assertEqual(expected, dict(actual["items"]))
        self.assertEqual({**expected, **expected2}, dict(actual["merged"]))
        self.assertEqual(expected, actual["from_items"])

    def test_parse_json(self):
        df = self.spark.createDataFrame([{"json": """{ "a" : 1 }"""}])
        actual = df.select(
            F.to_json(F.parse_json(df.json)).alias("var"),
            F.to_json(F.parse_json(F.lit("""{"b": [{"c": "str2"}]}"""))).alias("var_lit"),
        ).first()

        self.assertEqual("""{"a":1}""", actual["var"])
        self.assertEqual("""{"b":[{"c":"str2"}]}""", actual["var_lit"])

    def test_variant_expressions(self):
        df = self.spark.createDataFrame(
            [Row(json="""{ "a" : 1 }""", path="$.a"), Row(json="""{ "b" : 2 }""", path="$.b")]
        )
        v = F.parse_json(df.json)

        def check(resultDf, expected):
            self.assertEqual([r[0] for r in resultDf.collect()], expected)

        check(df.select(F.is_variant_null(v)), [False, False])
        check(df.select(F.schema_of_variant(v)), ["OBJECT<a: BIGINT>", "OBJECT<b: BIGINT>"])
        check(df.select(F.schema_of_variant_agg(v)), ["OBJECT<a: BIGINT, b: BIGINT>"])

        check(df.select(F.variant_get(v, "$.a", "int")), [1, None])
        check(df.select(F.variant_get(v, "$.b", "int")), [None, 2])
        check(df.select(F.variant_get(v, "$.a", "double")), [1.0, None])

        # non-literal variant_get
        check(df.select(F.variant_get(v, df.path, "int")), [1, 2])
        check(df.select(F.try_variant_get(v, df.path, "binary")), [None, None])

        with self.assertRaises(SparkRuntimeException) as ex:
            df.select(F.variant_get(v, "$.a", "binary")).collect()

        self.check_error(
            exception=ex.exception,
            errorClass="INVALID_VARIANT_CAST",
            messageParameters={"value": "1", "dataType": '"BINARY"'},
        )

        check(df.select(F.try_variant_get(v, "$.a", "int")), [1, None])
        check(df.select(F.try_variant_get(v, "$.b", "int")), [None, 2])
        check(df.select(F.try_variant_get(v, "$.a", "double")), [1.0, None])
        check(df.select(F.try_variant_get(v, "$.a", "binary")), [None, None])

    def test_schema_of_json(self):
        with self.assertRaises(PySparkTypeError) as pe:
            F.schema_of_json(1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "json", "arg_type": "int"},
        )

    def test_try_parse_json(self):
        df = self.spark.createDataFrame([{"json": """{ "a" : 1 }"""}, {"json": """{ a : 1 }"""}])
        actual = df.select(
            F.to_json(F.try_parse_json(df.json)).alias("var"),
        ).collect()
        self.assertEqual("""{"a":1}""", actual[0]["var"])
        self.assertEqual(None, actual[1]["var"])

    def test_try_to_time(self):
        # SPARK-52891: test the try_to_time function.
        df = self.spark.createDataFrame([("10:30:00", "HH:mm:ss")], ["time", "format"])
        result = datetime.time(10, 30, 0)
        # Test without format.
        row_from_col_no_format = df.select(F.try_to_time(df.time)).first()
        self.assertIsInstance(row_from_col_no_format[0], datetime.time)
        self.assertEqual(row_from_col_no_format[0], result)
        row_from_name_no_format = df.select(F.try_to_time("time")).first()
        self.assertIsInstance(row_from_name_no_format[0], datetime.time)
        self.assertEqual(row_from_name_no_format[0], result)
        # Test with format.
        row_from_col_with_format = df.select(F.try_to_time(df.time, df.format)).first()
        self.assertIsInstance(row_from_col_with_format[0], datetime.time)
        self.assertEqual(row_from_col_with_format[0], result)
        row_from_name_with_format = df.select(F.try_to_time("time", "format")).first()
        self.assertIsInstance(row_from_name_with_format[0], datetime.time)
        self.assertEqual(row_from_name_with_format[0], result)
        # Test with malformed time.
        df = self.spark.createDataFrame([("malformed", "HH:mm:ss")], ["time", "format"])
        row_from_col_no_format_malformed = df.select(F.try_to_time(df.time)).first()
        self.assertIsNone(row_from_col_no_format_malformed[0])
        row_from_name_no_format_malformed = df.select(F.try_to_time("time")).first()
        self.assertIsNone(row_from_name_no_format_malformed[0])
        row_from_col_with_format_malformed = df.select(F.try_to_time(df.time, df.format)).first()
        self.assertIsNone(row_from_col_with_format_malformed[0])
        row_from_name_with_format_malformed = df.select(F.try_to_time("time", "format")).first()
        self.assertIsNone(row_from_name_with_format_malformed[0])

    def test_to_variant_object(self):
        df = self.spark.createDataFrame([(1, {"a": 1})], "i int, v struct<a int>")
        actual = df.select(
            F.to_json(F.to_variant_object(df.v)).alias("var"),
        ).collect()
        self.assertEqual("""{"a":1}""", actual[0]["var"])

    def test_schema_of_csv(self):
        with self.assertRaises(PySparkTypeError) as pe:
            F.schema_of_csv(1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "csv", "arg_type": "int"},
        )

    def test_from_csv(self):
        df = self.spark.range(10)
        with self.assertRaises(PySparkTypeError) as pe:
            F.from_csv(df.id, 1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "schema", "arg_type": "int"},
        )

    def test_schema_of_xml(self):
        with self.assertRaises(PySparkTypeError) as pe:
            F.schema_of_xml(1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "xml", "arg_type": "int"},
        )

    def test_from_xml(self):
        df = self.spark.range(10)
        with self.assertRaises(PySparkTypeError) as pe:
            F.from_xml(df.id, 1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR_OR_STRUCT",
            messageParameters={"arg_name": "schema", "arg_type": "int"},
        )

    def test_greatest(self):
        df = self.spark.range(10)
        with self.assertRaises(PySparkValueError) as pe:
            F.greatest(df.id)

        self.check_error(
            exception=pe.exception,
            errorClass="WRONG_NUM_COLUMNS",
            messageParameters={"func_name": "greatest", "num_cols": "2"},
        )

    def test_when(self):
        with self.assertRaises(PySparkTypeError) as pe:
            F.when("id", 1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN",
            messageParameters={"arg_name": "condition", "arg_type": "str"},
        )

    def test_window(self):
        with self.assertRaises(PySparkTypeError) as pe:
            F.window("date", 5)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_STR",
            messageParameters={"arg_name": "windowDuration", "arg_type": "int"},
        )

    def test_session_window(self):
        with self.assertRaises(PySparkTypeError) as pe:
            F.session_window("date", 5)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "gapDuration", "arg_type": "int"},
        )

    def test_current_user(self):
        df = self.spark.range(1).select(F.current_user())
        self.assertIsInstance(df.first()[0], str)
        self.assertEqual(df.schema.names[0], "current_user()")
        df = self.spark.range(1).select(F.user())
        self.assertEqual(df.schema.names[0], "user()")
        df = self.spark.range(1).select(F.session_user())
        self.assertEqual(df.schema.names[0], "session_user()")

    def test_bucket(self):
        with self.assertRaises(PySparkTypeError) as pe:
            F.bucket("5", "id")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_COLUMN_OR_INT",
            messageParameters={"arg_name": "numBuckets", "arg_type": "str"},
        )

    def test_to_time(self):
        # SPARK-52890: test the to_time function.
        df = self.spark.createDataFrame([("10:30:00", "HH:mm:ss")], ["time", "format"])
        result = datetime.time(10, 30, 0)
        # Test without format.
        row_from_col_no_format = df.select(F.to_time(df.time)).first()
        self.assertIsInstance(row_from_col_no_format[0], datetime.time)
        self.assertEqual(row_from_col_no_format[0], result)
        row_from_name_no_format = df.select(F.to_time("time")).first()
        self.assertIsInstance(row_from_name_no_format[0], datetime.time)
        self.assertEqual(row_from_name_no_format[0], result)
        # Test with format.
        row_from_col_with_format = df.select(F.to_time(df.time, df.format)).first()
        self.assertIsInstance(row_from_col_with_format[0], datetime.time)
        self.assertEqual(row_from_col_with_format[0], result)
        row_from_name_with_format = df.select(F.to_time("time", "format")).first()
        self.assertIsInstance(row_from_name_with_format[0], datetime.time)
        self.assertEqual(row_from_name_with_format[0], result)

    def test_to_timestamp_ltz(self):
        df = self.spark.createDataFrame([("2016-12-31",)], ["e"])
        df = df.select(F.to_timestamp_ltz(df.e, F.lit("yyyy-MM-dd")).alias("r"))
        self.assertIsInstance(df.first()[0], datetime.datetime)

        df = self.spark.createDataFrame([("2016-12-31",)], ["e"])
        df = df.select(F.to_timestamp_ltz(df.e).alias("r"))
        self.assertIsInstance(df.first()[0], datetime.datetime)

    def test_to_timestamp_ntz(self):
        df = self.spark.createDataFrame([("2016-12-31",)], ["e"])
        df = df.select(F.to_timestamp_ntz(df.e).alias("r"))
        self.assertIsInstance(df.first()[0], datetime.datetime)

    def test_convert_timezone(self):
        df = self.spark.createDataFrame([("2015-04-08",)], ["dt"])
        df = df.select(
            F.convert_timezone(F.lit("America/Los_Angeles"), F.lit("Asia/Hong_Kong"), "dt")
        )
        self.assertIsInstance(df.first()[0], datetime.datetime)

    def test_map_concat(self):
        df = self.spark.sql("SELECT map(1, 'a', 2, 'b') as map1, map(3, 'c') as map2")
        self.assertEqual(
            df.select(F.map_concat(["map1", "map2"]).alias("map3")).first()[0],
            {1: "a", 2: "b", 3: "c"},
        )

    def test_version(self):
        self.assertIsInstance(self.spark.range(1).select(F.version()).first()[0], str)

    # SPARK-45216: Fix non-deterministic seeded Dataset APIs
    def test_non_deterministic_with_seed(self):
        df = self.spark.createDataFrame([([*range(0, 10, 1)],)], ["a"])

        r = F.rand()
        r2 = F.randn()
        r3 = F.shuffle("a")
        res = df.select(r, r, r2, r2, r3, r3).collect()
        for i in range(3):
            self.assertEqual(res[0][i * 2], res[0][i * 2 + 1])

    def test_current_time(self):
        # SPARK-52889: test the current_time function without precision.
        df = self.spark.range(1).select(F.current_time())
        self.assertIsInstance(df.first()[0], datetime.time)
        self.assertEqual(df.schema.names[0], "current_time(6)")
        # SPARK-52889: test the current_time function with precision.
        df = self.spark.range(1).select(F.current_time(3))
        self.assertIsInstance(df.first()[0], datetime.time)
        self.assertEqual(df.schema.names[0], "current_time(3)")

    def test_current_timestamp(self):
        df = self.spark.range(1).select(F.current_timestamp())
        self.assertIsInstance(df.first()[0], datetime.datetime)
        self.assertEqual(df.schema.names[0], "current_timestamp()")
        df = self.spark.range(1).select(F.now())
        self.assertIsInstance(df.first()[0], datetime.datetime)
        self.assertEqual(df.schema.names[0], "now()")

    def test_json_tuple_empty_fields(self):
        df = self.spark.createDataFrame(
            [
                ("1", """{"f1": "value1", "f2": "value2"}"""),
                ("2", """{"f1": "value12"}"""),
            ],
            ("key", "jstring"),
        )
        self.assertRaisesRegex(
            PySparkValueError,
            "At least one field must be specified",
            lambda: df.select(F.json_tuple(df.jstring)),
        )

    def test_avro_type_check(self):
        parameters = ["data", "jsonFormatSchema", "options"]
        expected_type = ["pyspark.sql.Column or str", "str", "dict, optional"]
        dummyDF = self.spark.createDataFrame([Row(a=i, b=i) for i in range(5)])

        # test from_avro type checks for each parameter
        wrong_type_value = 1
        with self.assertRaises(PySparkTypeError) as pe1:
            dummyDF.select(from_avro(wrong_type_value, "jsonSchema", None))
        with self.assertRaises(PySparkTypeError) as pe2:
            dummyDF.select(from_avro("value", wrong_type_value, None))
        with self.assertRaises(PySparkTypeError) as pe3:
            dummyDF.select(from_avro("value", "jsonSchema", wrong_type_value))
        from_avro_pes = [pe1, pe2, pe3]
        for i in range(3):
            self.check_error(
                exception=from_avro_pes[i].exception,
                errorClass="INVALID_TYPE",
                messageParameters={"arg_name": parameters[i], "arg_type": expected_type[i]},
            )

        # test to_avro type checks for each parameter
        with self.assertRaises(PySparkTypeError) as pe4:
            dummyDF.select(to_avro(wrong_type_value, "jsonSchema"))
        with self.assertRaises(PySparkTypeError) as pe5:
            dummyDF.select(to_avro("value", wrong_type_value))
        to_avro_pes = [pe4, pe5]
        for i in range(2):
            self.check_error(
                exception=to_avro_pes[i].exception,
                errorClass="INVALID_TYPE",
                messageParameters={"arg_name": parameters[i], "arg_type": expected_type[i]},
            )

    def test_enum_literals(self):
        class IntEnum(Enum):
            X = 1
            Y = 2
            Z = 3

        id = F.col("id")
        b = F.col("b")

        cols, expected = list(
            zip(
                (F.lit(IntEnum.X), 1),
                (F.lit([IntEnum.X, IntEnum.Y]), [1, 2]),
                (F.rand(IntEnum.X), 0.9531453492357947),
                (F.randn(IntEnum.X), -1.1081822375859998),
                (F.when(b, IntEnum.X), 1),
            )
        )

        result = (
            self.spark.range(1, 2)
            .select(id, id.astype("string").alias("s"), id.astype("boolean").alias("b"))
            .select(*cols)
            .first()
        )

        for r, c, e in zip(result, cols, expected):
            self.assertEqual(r, e, str(c))

    def test_nullifzero_zeroifnull(self):
        df = self.spark.createDataFrame([(0,), (1,)], ["a"])
        result = df.select(nullifzero(df.a).alias("r"))
        assertDataFrameEqual([Row(r=None), Row(r=1)], result)

        df = self.spark.createDataFrame([(None,), (1,)], ["a"])
        result = df.select(zeroifnull(df.a).alias("r"))
        assertDataFrameEqual([Row(r=0), Row(r=1)], result)

    def test_randstr_uniform(self):
        df = self.spark.createDataFrame([(0,)], ["a"])
        result = df.select(randstr(F.lit(5), F.lit(0)).alias("x")).selectExpr("length(x)")
        assertDataFrameEqual([Row(5)], result)
        # The random seed is optional.
        result = df.select(randstr(F.lit(5)).alias("x")).selectExpr("length(x)")
        assertDataFrameEqual([Row(5)], result)

        df = self.spark.createDataFrame([(0,)], ["a"])
        result = df.select(uniform(F.lit(10), F.lit(20), F.lit(0)).alias("x")).selectExpr("x > 5")
        assertDataFrameEqual([Row(True)], result)
        # The random seed is optional.
        result = df.select(uniform(F.lit(10), F.lit(20)).alias("x")).selectExpr("x > 5")
        assertDataFrameEqual([Row(True)], result)

    def test_string_validation(self):
        df = self.spark.createDataFrame([("abc",)], ["a"])
        # test is_valid_utf8
        result_is_valid_utf8 = df.select(F.is_valid_utf8(df.a).alias("r"))
        assertDataFrameEqual([Row(r=True)], result_is_valid_utf8)
        # test make_valid_utf8
        result_make_valid_utf8 = df.select(F.make_valid_utf8(df.a).alias("r"))
        assertDataFrameEqual([Row(r="abc")], result_make_valid_utf8)
        # test validate_utf8
        result_validate_utf8 = df.select(F.validate_utf8(df.a).alias("r"))
        assertDataFrameEqual([Row(r="abc")], result_validate_utf8)
        # test try_validate_utf8
        result_try_validate_utf8 = df.select(F.try_validate_utf8(df.a).alias("r"))
        assertDataFrameEqual([Row(r="abc")], result_try_validate_utf8)

    # Geospatial ST Functions

    def test_st_asbinary(self):
        df = self.spark.createDataFrame(
            [(bytes.fromhex("0101000000000000000000F03F0000000000000040"),)],
            ["wkb"],
        )
        results = df.select(
            F.hex(F.st_asbinary(F.st_geogfromwkb("wkb"))),
            F.hex(F.st_asbinary(F.st_geomfromwkb("wkb"))),
        ).collect()
        expected = Row(
            "0101000000000000000000F03F0000000000000040",
            "0101000000000000000000F03F0000000000000040",
        )
        self.assertEqual(results, [expected])

    def test_st_setsrid(self):
        df = self.spark.createDataFrame(
            [(bytes.fromhex("0101000000000000000000F03F0000000000000040"), 4326)],
            ["wkb", "srid"],
        )
        results = df.select(
            F.st_srid(F.st_setsrid(F.st_geogfromwkb("wkb"), "srid")),
            F.st_srid(F.st_setsrid(F.st_geomfromwkb("wkb"), "srid")),
            F.st_srid(F.st_setsrid(F.st_geogfromwkb("wkb"), 4326)),
            F.st_srid(F.st_setsrid(F.st_geomfromwkb("wkb"), 4326)),
        ).collect()
        expected = Row(
            4326,
            4326,
            4326,
            4326,
        )
        self.assertEqual(results, [expected])

    def test_st_srid(self):
        df = self.spark.createDataFrame(
            [(bytes.fromhex("0101000000000000000000F03F0000000000000040"),)],
            ["wkb"],
        )
        results = df.select(
            F.st_srid(F.st_geogfromwkb("wkb")),
            F.st_srid(F.st_geomfromwkb("wkb")),
        ).collect()
        expected = Row(
            4326,
            0,
        )
        self.assertEqual(results, [expected])


class FunctionsTests(ReusedSQLTestCase, FunctionsTestsMixin):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_functions import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
