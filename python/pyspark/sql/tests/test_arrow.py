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
import os
import threading
import calendar
import time
import unittest
from typing import cast
from collections import namedtuple

from pyspark import SparkConf
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import rand, udf, assert_true, lit
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    DateType,
    TimestampType,
    TimestampNTZType,
    BinaryType,
    StructField,
    ArrayType,
    MapType,
    NullType,
    DayTimeIntervalType,
)
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
    ExamplePoint,
    ExamplePointUDT,
)
from pyspark.errors import ArithmeticException, PySparkTypeError, UnsupportedOperationException
from pyspark.loose_version import LooseVersion
from pyspark.util import is_remote_only
from pyspark.loose_version import LooseVersion

if have_pandas:
    import pandas as pd
    from pandas.testing import assert_frame_equal

if have_pyarrow:
    import pyarrow as pa  # noqa: F401


class ArrowTestsMixin:
    @classmethod
    def setUpClass(cls):
        from datetime import date, datetime
        from decimal import Decimal

        super().setUpClass()
        cls.warnings_lock = threading.Lock()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.spark.conf.set("spark.sql.session.timeZone", tz)

        # Test fallback
        cls.spark.conf.set("spark.sql.execution.arrow.enabled", "false")
        assert cls.spark.conf.get("spark.sql.execution.arrow.pyspark.enabled") == "false"
        cls.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        assert cls.spark.conf.get("spark.sql.execution.arrow.pyspark.enabled") == "true"

        cls.spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "true")
        assert cls.spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled") == "true"
        cls.spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "false")
        assert cls.spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled") == "false"

        # Enable Arrow optimization in this tests.
        cls.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Disable fallback by default to easily detect the failures.
        cls.spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")

        cls.schema_wo_null = StructType(
            [
                StructField("1_str_t", StringType(), True),
                StructField("2_int_t", IntegerType(), True),
                StructField("3_long_t", LongType(), True),
                StructField("4_float_t", FloatType(), True),
                StructField("5_double_t", DoubleType(), True),
                StructField("6_decimal_t", DecimalType(38, 18), True),
                StructField("7_date_t", DateType(), True),
                StructField("8_timestamp_t", TimestampType(), True),
                StructField("9_binary_t", BinaryType(), True),
            ]
        )
        cls.schema = cls.schema_wo_null.add("10_null_t", NullType(), True)
        cls.data_wo_null = [
            (
                "a",
                1,
                10,
                0.2,
                2.0,
                Decimal("2.0"),
                date(1969, 1, 1),
                datetime(1969, 1, 1, 1, 1, 1),
                bytearray(b"a"),
            ),
            (
                "b",
                2,
                20,
                0.4,
                4.0,
                Decimal("4.0"),
                date(2012, 2, 2),
                datetime(2012, 2, 2, 2, 2, 2),
                bytearray(b"bb"),
            ),
            (
                "c",
                3,
                30,
                0.8,
                6.0,
                Decimal("6.0"),
                date(2100, 3, 3),
                datetime(2100, 3, 3, 3, 3, 3),
                bytearray(b"ccc"),
            ),
            (
                "d",
                4,
                40,
                1.0,
                8.0,
                Decimal("8.0"),
                date(2262, 4, 12),
                datetime(2262, 3, 3, 3, 3, 3),
                bytearray(b"dddd"),
            ),
        ]
        cls.data = [tuple(list(d) + [None]) for d in cls.data_wo_null]

        cls.schema_nested_timestamp = (
            StructType()
            .add("ts", TimestampType())
            .add("ts_ntz", TimestampNTZType())
            .add(
                "struct", StructType().add("ts", TimestampType()).add("ts_ntz", TimestampNTZType())
            )
            .add("array", ArrayType(TimestampType()))
            .add("array_ntz", ArrayType(TimestampNTZType()))
            .add("map", MapType(StringType(), TimestampType()))
            .add("map_ntz", MapType(StringType(), TimestampNTZType()))
        )
        cls.data_nested_timestamp = [
            Row(
                datetime(2023, 1, 1, 0, 0, 0),
                datetime(2023, 1, 1, 0, 0, 0),
                Row(
                    datetime(2023, 1, 1, 0, 0, 0),
                    datetime(2023, 1, 1, 0, 0, 0),
                ),
                [datetime(2023, 1, 1, 0, 0, 0)],
                [datetime(2023, 1, 1, 0, 0, 0)],
                dict(ts=datetime(2023, 1, 1, 0, 0, 0)),
                dict(ts_ntz=datetime(2023, 1, 1, 0, 0, 0)),
            )
        ]
        cls.data_nested_timestamp_expected_ny = Row(
            ts=datetime(2022, 12, 31, 21, 0, 0),
            ts_ntz=datetime(2023, 1, 1, 0, 0, 0),
            struct=Row(
                ts=datetime(2022, 12, 31, 21, 0, 0),
                ts_ntz=datetime(2023, 1, 1, 0, 0, 0),
            ),
            array=[datetime(2022, 12, 31, 21, 0, 0)],
            array_ntz=[datetime(2023, 1, 1, 0, 0, 0)],
            map=dict(ts=datetime(2022, 12, 31, 21, 0, 0)),
            map_ntz=dict(ts_ntz=datetime(2023, 1, 1, 0, 0, 0)),
        )

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()
        super().tearDownClass()

    def create_pandas_data_frame(self):
        import numpy as np

        data_dict = {}
        for j, name in enumerate(self.schema.names):
            data_dict[name] = [self.data[i][j] for i in range(len(self.data))]
        # need to convert these to numpy types first
        data_dict["2_int_t"] = np.int32(data_dict["2_int_t"])
        data_dict["4_float_t"] = np.float32(data_dict["4_float_t"])
        return pd.DataFrame(data=data_dict)

    def create_arrow_table(self):
        import pyarrow as pa

        data_dict = {}
        for j, name in enumerate(self.schema.names):
            data_dict[name] = [self.data[i][j] for i in range(len(self.data))]
        t = pa.Table.from_pydict(data_dict)
        # convert these to Arrow types
        new_schema = t.schema.set(
            t.schema.get_field_index("2_int_t"), pa.field("2_int_t", pa.int32())
        )
        new_schema = new_schema.set(
            new_schema.get_field_index("4_float_t"), pa.field("4_float_t", pa.float32())
        )
        new_schema = new_schema.set(
            new_schema.get_field_index("6_decimal_t"),
            pa.field("6_decimal_t", pa.decimal128(38, 18)),
        )
        t = t.cast(new_schema)
        return t

    @property
    def create_np_arrs(self):
        import numpy as np

        int_dtypes = ["int8", "int16", "int32", "int64"]
        float_dtypes = ["float32", "float64"]
        return (
            [np.array([1, 2]).astype(t) for t in int_dtypes]
            + [np.array([0.1, 0.2]).astype(t) for t in float_dtypes]
            + [np.array([[1], [2]]).astype(t) for t in int_dtypes]
            + [np.array([[0.1], [0.2]]).astype(t) for t in float_dtypes]
            + [np.array([[1, 1, 1], [2, 2, 2]]).astype(t) for t in int_dtypes]
            + [np.array([[0.1, 0.1, 0.1], [0.2, 0.2, 0.2]]).astype(t) for t in float_dtypes]
        )

    def test_toPandas_empty_df_arrow_enabled(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_toPandas_empty_df_arrow_enabled(arrow_enabled)

    def check_toPandas_empty_df_arrow_enabled(self, arrow_enabled):
        # SPARK-30537 test that toPandas() on an empty dataframe has the correct dtypes
        # when arrow is enabled
        from datetime import date
        from decimal import Decimal

        schema = StructType(
            [
                StructField("a", StringType(), True),
                StructField("a", IntegerType(), True),
                StructField("c", TimestampType(), True),
                StructField("d", NullType(), True),
                StructField("e", LongType(), True),
                StructField("f", FloatType(), True),
                StructField("g", DateType(), True),
                StructField("h", BinaryType(), True),
                StructField("i", DecimalType(38, 18), True),
                StructField("k", TimestampNTZType(), True),
                StructField("L", DayTimeIntervalType(0, 3), True),
            ]
        )
        df = self.spark.createDataFrame([], schema=schema)
        non_empty_df = self.spark.createDataFrame(
            [
                (
                    "a",
                    1,
                    datetime.datetime(1969, 1, 1, 1, 1, 1),
                    None,
                    10,
                    0.2,
                    date(1969, 1, 1),
                    bytearray(b"a"),
                    Decimal("2.0"),
                    datetime.datetime(1969, 1, 1, 1, 1, 1),
                    datetime.timedelta(microseconds=123),
                )
            ],
            schema=schema,
        )

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
            pdf = df.toPandas()
            pdf_non_empty = non_empty_df.toPandas()
        self.assertTrue(pdf.dtypes.equals(pdf_non_empty.dtypes))

    def test_null_conversion(self):
        df_null = self.spark.createDataFrame(
            [tuple([None for _ in range(len(self.data_wo_null[0]))])] + self.data_wo_null
        )
        pdf = df_null.toPandas()
        null_counts = pdf.isnull().sum().tolist()
        self.assertTrue(all([c == 1 for c in null_counts]))

    def _toPandas_arrow_toggle(self, df):
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            pdf = df.toPandas()

        pdf_arrow = df.toPandas()

        return pdf, pdf_arrow

    def test_toPandas_arrow_toggle(self):
        df = self.spark.createDataFrame(self.data, schema=self.schema)
        pdf, pdf_arrow = self._toPandas_arrow_toggle(df)
        expected = self.create_pandas_data_frame()
        assert_frame_equal(expected, pdf)
        assert_frame_equal(expected, pdf_arrow)

    def test_create_data_frame_to_pandas_timestamp_ntz(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_create_data_frame_to_pandas_timestamp_ntz(arrow_enabled)

    def check_create_data_frame_to_pandas_timestamp_ntz(self, arrow_enabled):
        # SPARK-36626: Test TimestampNTZ in createDataFrame and toPandas
        with self.sql_conf({"spark.sql.session.timeZone": "America/Los_Angeles"}):
            origin = pd.DataFrame({"a": [datetime.datetime(2012, 2, 2, 2, 2, 2)]})
            df = self.spark.createDataFrame(
                origin, schema=StructType([StructField("a", TimestampNTZType(), True)])
            )
            df.selectExpr("assert_true('2012-02-02 02:02:02' == CAST(a AS STRING))").collect()

            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                pdf = df.toPandas()
            assert_frame_equal(origin, pdf)

    def test_create_data_frame_to_arrow_timestamp_ntz(self):
        with self.sql_conf({"spark.sql.session.timeZone": "America/Los_Angeles"}):
            origin = pa.table({"a": [datetime.datetime(2012, 2, 2, 2, 2, 2)]})
            df = self.spark.createDataFrame(
                origin, schema=StructType([StructField("a", TimestampNTZType(), True)])
            )
            df.selectExpr("assert_true('2012-02-02 02:02:02' == CAST(a AS STRING))").collect()

            t = df.toArrow()
            self.assertTrue(origin.equals(t))

    def test_create_data_frame_to_pandas_day_time_internal(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_create_data_frame_to_pandas_day_time_internal(arrow_enabled)

    def check_create_data_frame_to_pandas_day_time_internal(self, arrow_enabled):
        # SPARK-37279: Test DayTimeInterval in createDataFrame and toPandas
        origin = pd.DataFrame({"a": [datetime.timedelta(microseconds=123)]})
        df = self.spark.createDataFrame(origin)
        df.select(
            assert_true(lit("INTERVAL '0 00:00:00.000123' DAY TO SECOND") == df.a.cast("string"))
        ).collect()

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
            pdf = df.toPandas()
        assert_frame_equal(origin, pdf)

    def test_create_data_frame_to_arrow_day_time_internal(self):
        origin = pa.table({"a": [datetime.timedelta(microseconds=123)]})
        df = self.spark.createDataFrame(origin)
        df.select(
            assert_true(lit("INTERVAL '0 00:00:00.000123' DAY TO SECOND") == df.a.cast("string"))
        ).collect()

        t = df.toArrow()
        self.assertTrue(origin.equals(t))

    def test_toPandas_respect_session_timezone(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_toPandas_respect_session_timezone(arrow_enabled)

    def check_toPandas_respect_session_timezone(self, arrow_enabled):
        df = self.spark.createDataFrame(self.data, schema=self.schema)

        timezone = "America/Los_Angeles"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                pdf_la = df.toPandas()

        timezone = "America/New_York"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                pdf_ny = df.toPandas()

            self.assertFalse(pdf_ny.equals(pdf_la))

            from pyspark.sql.pandas.types import _check_series_convert_timestamps_local_tz

            pdf_la_corrected = pdf_la.copy()
            for field in self.schema:
                if isinstance(field.dataType, TimestampType):
                    pdf_la_corrected[field.name] = _check_series_convert_timestamps_local_tz(
                        pdf_la_corrected[field.name], timezone
                    )
            assert_frame_equal(pdf_ny, pdf_la_corrected)

    def test_toArrow_keep_utc_timezone(self):
        df = self.spark.createDataFrame(self.data, schema=self.schema)

        timezone = "America/Los_Angeles"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            t_la = df.toArrow()

        timezone = "America/New_York"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            t_ny = df.toArrow()

            self.assertTrue(t_ny.equals(t_la))
            self.assertEqual(t_la["8_timestamp_t"].type.tz, "UTC")
            self.assertEqual(t_ny["8_timestamp_t"].type.tz, "UTC")

    def test_pandas_round_trip(self):
        pdf = self.create_pandas_data_frame()
        df = self.spark.createDataFrame(self.data, schema=self.schema)
        pdf_arrow = df.toPandas()
        assert_frame_equal(pdf_arrow, pdf)

    def test_arrow_round_trip(self):
        import pyarrow.compute as pc

        t_in = self.create_arrow_table()

        # Convert timezone-naive local timestamp column in input table to UTC
        # to enable comparison to UTC timestamp column in output table
        timezone = self.spark.conf.get("spark.sql.session.timeZone")
        t_in = t_in.set_column(
            t_in.schema.get_field_index("8_timestamp_t"),
            "8_timestamp_t",
            pc.assume_timezone(t_in["8_timestamp_t"], timezone),
        )
        t_in = t_in.cast(
            t_in.schema.set(
                t_in.schema.get_field_index("8_timestamp_t"),
                pa.field("8_timestamp_t", pa.timestamp("us", tz="UTC")),
            )
        )

        df = self.spark.createDataFrame(self.data, schema=self.schema)
        t_out = df.toArrow()

        self.assertTrue(t_out.equals(t_in))

    def test_pandas_self_destruct(self):
        import pyarrow as pa

        rows = 2**10
        cols = 4
        expected_bytes = rows * cols * 8
        df = self.spark.range(0, rows).select(*[rand() for _ in range(cols)])
        # Test the self_destruct behavior by testing _collect_as_arrow directly
        allocation_before = pa.total_allocated_bytes()
        batches = df._collect_as_arrow(split_batches=True)
        table = pa.Table.from_batches(batches)
        del batches
        pdf_split = table.to_pandas(self_destruct=True, split_blocks=True, use_threads=False)
        allocation_after = pa.total_allocated_bytes()
        difference = allocation_after - allocation_before
        # Should be around 1x the data size (table should not hold on to any memory)
        self.assertGreaterEqual(difference, 0.9 * expected_bytes)
        self.assertLessEqual(difference, 1.1 * expected_bytes)

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.selfDestruct.enabled": False}):
            no_self_destruct_pdf = df.toPandas()
            # Note while memory usage is 2x data size here (both table and pdf hold on to
            # memory), in this case Arrow still only tracks 1x worth of memory (since the
            # batches are not allocated by Arrow in this case), so we can't make any
            # assertions here

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.selfDestruct.enabled": True}):
            self_destruct_pdf = df.toPandas()

        assert_frame_equal(pdf_split, no_self_destruct_pdf)
        assert_frame_equal(pdf_split, self_destruct_pdf)

    def test_filtered_frame(self):
        df = self.spark.range(3).toDF("i")
        pdf = df.filter("i < 0").toPandas()
        self.assertEqual(len(pdf.columns), 1)
        self.assertEqual(pdf.columns[0], "i")
        self.assertTrue(pdf.empty)

    def test_no_partition_frame(self):
        schema = StructType([StructField("field1", StringType(), True)])
        df = self.spark.createDataFrame(self.sc.emptyRDD(), schema)
        pdf = df.toPandas()
        self.assertEqual(len(pdf.columns), 1)
        self.assertEqual(pdf.columns[0], "field1")
        self.assertTrue(pdf.empty)

    def test_propagates_spark_exception(self):
        with self.quiet():
            self.check_propagates_spark_exception()

    def check_propagates_spark_exception(self):
        df = self.spark.range(3).toDF("i")

        def raise_exception():
            raise RuntimeError("My error")

        exception_udf = udf(raise_exception, IntegerType())
        df = df.withColumn("error", exception_udf())

        with self.assertRaisesRegex(Exception, "My error"):
            df.toPandas()

    def test_createDataFrame_arrow_pandas(self):
        table = self.create_arrow_table()
        pdf = self.create_pandas_data_frame()
        df_arrow = self.spark.createDataFrame(table)
        df_pandas = self.spark.createDataFrame(pdf)
        self.assertEqual(df_arrow.collect(), df_pandas.collect())

    def _createDataFrame_toggle(self, data, schema=None):
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            df_no_arrow = self.spark.createDataFrame(data, schema=schema)

        df_arrow = self.spark.createDataFrame(data, schema=schema)

        return df_no_arrow, df_arrow

    def test_createDataFrame_toggle(self):
        pdf = self.create_pandas_data_frame()
        df_no_arrow, df_arrow = self._createDataFrame_toggle(pdf, schema=self.schema)
        self.assertEqual(df_no_arrow.collect(), df_arrow.collect())

    def test_createDataFrame_pandas_respect_session_timezone(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_createDataFrame_pandas_respect_session_timezone(arrow_enabled)

    def check_createDataFrame_pandas_respect_session_timezone(self, arrow_enabled):
        from datetime import timedelta

        pdf = self.create_pandas_data_frame()
        timezone = "America/Los_Angeles"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                df_la = self.spark.createDataFrame(pdf, schema=self.schema)
            result_la = df_la.collect()

        timezone = "America/New_York"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                df_ny = self.spark.createDataFrame(pdf, schema=self.schema)
            result_ny = df_ny.collect()

            self.assertNotEqual(result_ny, result_la)

            # Correct result_la by adjusting 3 hours difference between Los Angeles and New York
            result_la_corrected = [
                Row(
                    **{
                        k: v - timedelta(hours=3) if k == "8_timestamp_t" else v
                        for k, v in row.asDict().items()
                    }
                )
                for row in result_la
            ]
            self.assertEqual(result_ny, result_la_corrected)

    def test_createDataFrame_arrow_respect_session_timezone(self):
        from datetime import timedelta

        t = self.create_arrow_table()
        timezone = "America/Los_Angeles"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            df_la = self.spark.createDataFrame(t, schema=self.schema)
            result_la = df_la.collect()

        timezone = "America/New_York"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            df_ny = self.spark.createDataFrame(t, schema=self.schema)
            result_ny = df_ny.collect()

            self.assertNotEqual(result_ny, result_la)

            # Correct result_la by adjusting 3 hours difference between Los Angeles and New York
            result_la_corrected = [
                Row(
                    **{
                        k: v - timedelta(hours=3) if k == "8_timestamp_t" else v
                        for k, v in row.asDict().items()
                    }
                )
                for row in result_la
            ]
            self.assertEqual(result_ny, result_la_corrected)

    def test_createDataFrame_pandas_with_schema(self):
        pdf = self.create_pandas_data_frame()
        df = self.spark.createDataFrame(pdf, schema=self.schema)
        self.assertEqual(self.schema, df.schema)
        pdf_arrow = df.toPandas()
        assert_frame_equal(pdf_arrow, pdf)

    def test_createDataFrame_pandas_with_incorrect_schema(self):
        with self.quiet():
            self.check_createDataFrame_pandas_with_incorrect_schema()

    def check_createDataFrame_pandas_with_incorrect_schema(self):
        pdf = self.create_pandas_data_frame()
        fields = list(self.schema)
        fields[5], fields[6] = fields[6], fields[5]  # swap decimal with date
        wrong_schema = StructType(fields)
        with self.sql_conf({"spark.sql.execution.pandas.convertToArrowArraySafely": False}):
            with self.assertRaises(Exception) as context:
                self.spark.createDataFrame(pdf, schema=wrong_schema)

            # the exception provides us with the column that is incorrect
            exception = context.exception
            self.assertTrue(hasattr(exception, "args"))
            self.assertEqual(len(exception.args), 1)
            self.assertRegex(
                exception.args[0],
                "with name '7_date_t' " "to Arrow Array \\(decimal128\\(38, 18\\)\\)",
            )

            # the inner exception provides us with the incorrect types
            exception = exception.__context__
            self.assertTrue(hasattr(exception, "args"))
            self.assertEqual(len(exception.args), 1)
            self.assertRegex(exception.args[0], "[D|d]ecimal.*got.*date")

    def test_createDataFrame_arrow_with_incorrect_schema(self):
        t = self.create_arrow_table()
        fields = list(self.schema)
        fields[5], fields[6] = fields[6], fields[5]  # swap decimal with date
        wrong_schema = StructType(fields)
        with self.assertRaises(Exception):
            self.spark.createDataFrame(t, schema=wrong_schema)

    def test_createDataFrame_pandas_with_names(self):
        pdf = self.create_pandas_data_frame()
        new_names = list(map(str, range(len(self.schema.fieldNames()))))
        # Test that schema as a list of column names gets applied
        df = self.spark.createDataFrame(pdf, schema=list(new_names))
        self.assertEqual(df.schema.fieldNames(), new_names)
        # Test that schema as tuple of column names gets applied
        df = self.spark.createDataFrame(pdf, schema=tuple(new_names))
        self.assertEqual(df.schema.fieldNames(), new_names)

    def test_createDataFrame_arrow_with_names(self):
        t = self.create_arrow_table()
        new_names = list(map(str, range(len(self.schema.fieldNames()))))
        # Test that schema as a list of column names gets applied
        df = self.spark.createDataFrame(t, schema=list(new_names))
        self.assertEqual(df.schema.fieldNames(), new_names)
        # Test that schema as tuple of column names gets applied
        df = self.spark.createDataFrame(t, schema=tuple(new_names))
        self.assertEqual(df.schema.fieldNames(), new_names)

    def test_createDataFrame_pandas_column_name_encoding(self):
        pdf = pd.DataFrame({"a": [1]})
        columns = self.spark.createDataFrame(pdf).columns
        self.assertTrue(isinstance(columns[0], str))
        self.assertEqual(columns[0], "a")
        columns = self.spark.createDataFrame(pdf, ["b"]).columns
        self.assertTrue(isinstance(columns[0], str))
        self.assertEqual(columns[0], "b")

    def test_createDataFrame_arrow_column_name_encoding(self):
        t = pa.table({"a": [1]})
        columns = self.spark.createDataFrame(t).columns
        self.assertTrue(isinstance(columns[0], str))
        self.assertEqual(columns[0], "a")
        columns = self.spark.createDataFrame(t, ["b"]).columns
        self.assertTrue(isinstance(columns[0], str))
        self.assertEqual(columns[0], "b")

    def test_createDataFrame_with_single_data_type(self):
        with self.quiet():
            self.check_createDataFrame_with_single_data_type()

    def check_createDataFrame_with_single_data_type(self):
        for schema in ["int", IntegerType()]:
            with self.subTest(schema=schema):
                with self.assertRaises(PySparkTypeError) as pe:
                    self.spark.createDataFrame(pd.DataFrame({"a": [1]}), schema=schema).collect()

                self.check_error(
                    exception=pe.exception,
                    errorClass="UNSUPPORTED_DATA_TYPE_FOR_ARROW",
                    messageParameters={"data_type": "IntegerType()"},
                )

    def test_createDataFrame_does_not_modify_input(self):
        # Some series get converted for Spark to consume, this makes sure input is unchanged
        pdf = self.create_pandas_data_frame()
        # Use a nanosecond value to make sure it is not truncated
        pdf.iloc[0, 7] = pd.Timestamp(1)
        # Integers with nulls will get NaNs filled with 0 and will be casted
        pdf.iloc[1, 1] = None
        pdf_copy = pdf.copy(deep=True)
        self.spark.createDataFrame(pdf, schema=self.schema)
        self.assertTrue(pdf.equals(pdf_copy))

    def test_createDataFrame_arrow_truncate_timestamp(self):
        t_in = pa.Table.from_arrays(
            [pa.array([1234567890123456789], type=pa.timestamp("ns", tz="UTC"))], names=["ts"]
        )
        df = self.spark.createDataFrame(t_in)
        t_out = df.toArrow()
        expected = pa.Table.from_arrays(
            [pa.array([1234567890123456], type=pa.timestamp("us", tz="UTC"))], names=["ts"]
        )
        self.assertTrue(t_out.equals(expected))

    def test_schema_conversion_roundtrip(self):
        from pyspark.sql.pandas.types import from_arrow_schema, to_arrow_schema

        arrow_schema = to_arrow_schema(self.schema)
        schema_rt = from_arrow_schema(arrow_schema, prefer_timestamp_ntz=True)
        self.assertEqual(self.schema, schema_rt)

    def test_createDataFrame_with_ndarray(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_createDataFrame_with_ndarray(arrow_enabled)

    def check_createDataFrame_with_ndarray(self, arrow_enabled):
        import numpy as np

        dtypes = ["tinyint", "smallint", "int", "bigint", "float", "double"]
        expected_dtypes = (
            [[("value", t)] for t in dtypes]
            + [[("value", t)] for t in dtypes]
            + [[("_1", t), ("_2", t), ("_3", t)] for t in dtypes]
        )
        arrs = self.create_np_arrs

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
            for arr, dtypes in zip(arrs, expected_dtypes):
                df = self.spark.createDataFrame(arr)
                self.assertEqual(df.dtypes, dtypes)
                np.array_equal(np.array(df.collect()), arr)

            with self.assertRaisesRegex(
                ValueError, "NumPy array input should be of 1 or 2 dimensions"
            ):
                self.spark.createDataFrame(np.array(0))

    def test_createDataFrame_pandas_with_array_type(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_createDataFrame_pandas_with_array_type(arrow_enabled)

    def check_createDataFrame_pandas_with_array_type(self, arrow_enabled):
        pdf = pd.DataFrame({"a": [[1, 2], [3, 4]], "b": [["x", "y"], ["y", "z"]]})
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
            df = self.spark.createDataFrame(pdf)
        result = df.collect()
        expected = [tuple(list(e) for e in rec) for rec in pdf.to_records(index=False)]
        for r in range(len(expected)):
            for e in range(len(expected[r])):
                self.assertTrue(expected[r][e] == result[r][e])

    def test_createDataFrame_arrow_with_array_type_nulls(self):
        t = pa.table({"a": [[1, 2], None, [3, 4]], "b": [["x", "y"], ["y", "z"], None]})
        df = self.spark.createDataFrame(t)
        result = df.collect()
        expected = [
            tuple(list(e) if e is not None else None for e in rec)
            for rec in t.to_pandas().to_records(index=False)
        ]
        for r in range(len(expected)):
            for e in range(len(expected[r])):
                self.assertTrue(expected[r][e] == result[r][e])

    def test_toPandas_with_array_type(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_toPandas_with_array_type(arrow_enabled)

    def check_toPandas_with_array_type(self, arrow_enabled):
        expected = [([1, 2], ["x", "y"]), ([3, 4], ["y", "z"])]
        array_schema = StructType(
            [StructField("a", ArrayType(IntegerType())), StructField("b", ArrayType(StringType()))]
        )
        df = self.spark.createDataFrame(expected, schema=array_schema)
        pdf = df.toPandas()
        result = [tuple(list(e) for e in rec) for rec in pdf.to_records(index=False)]
        for r in range(len(expected)):
            for e in range(len(expected[r])):
                self.assertTrue(expected[r][e] == result[r][e])

    def test_toArrow_with_array_type_nulls(self):
        expected = [([1, 2], ["x", "y"]), (None, ["y", "z"]), ([3, 4], None)]
        array_schema = StructType(
            [StructField("a", ArrayType(IntegerType())), StructField("b", ArrayType(StringType()))]
        )
        df = self.spark.createDataFrame(expected, schema=array_schema)
        t = df.toArrow()
        result = [
            tuple(None if e is None else list(e) for e in rec)
            for rec in t.to_pandas().to_records(index=False)
        ]
        for r in range(len(expected)):
            for e in range(len(expected[r])):
                self.assertTrue(expected[r][e] == result[r][e])

    def test_createDataFrame_pandas_with_map_type(self):
        with self.quiet():
            for arrow_enabled in [True, False]:
                with self.subTest(arrow_enabled=arrow_enabled):
                    self.check_createDataFrame_pandas_with_map_type(arrow_enabled)

    def check_createDataFrame_pandas_with_map_type(self, arrow_enabled):
        map_data = [{"a": 1}, {"b": 2, "c": 3}, {}, None, {"d": None}]

        pdf = pd.DataFrame({"id": [0, 1, 2, 3, 4], "m": map_data})
        for schema in (
            "id long, m map<string, long>",
            StructType().add("id", LongType()).add("m", MapType(StringType(), LongType())),
        ):
            with self.subTest(schema=schema):
                with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                    df = self.spark.createDataFrame(pdf, schema=schema)

                    result = df.collect()

                    for row in result:
                        i, m = row
                        self.assertEqual(m, map_data[i])

    def test_createDataFrame_arrow_with_map_type(self):
        map_data = [{"a": 1}, {"b": 2, "c": 3}, {}, {}, {"d": None}]

        t = pa.table(
            {"id": [0, 1, 2, 3, 4], "m": map_data},
            schema=pa.schema([("id", pa.int64()), ("m", pa.map_(pa.string(), pa.int64()))]),
        )
        for schema in (
            "id long, m map<string, long>",
            StructType().add("id", LongType()).add("m", MapType(StringType(), LongType())),
        ):
            with self.subTest(schema=schema):
                df = self.spark.createDataFrame(t, schema=schema)

                result = df.collect()

                for row in result:
                    i, m = row
                    self.assertEqual(m, map_data[i])

    def test_createDataFrame_arrow_with_map_type_nulls(self):
        map_data = [{"a": 1}, {"b": 2, "c": 3}, {}, None, {"d": None}]

        t = pa.table(
            {"id": [0, 1, 2, 3, 4], "m": map_data},
            schema=pa.schema([("id", pa.int64()), ("m", pa.map_(pa.string(), pa.int64()))]),
        )
        for schema in (
            "id long, m map<string, long>",
            StructType().add("id", LongType()).add("m", MapType(StringType(), LongType())),
        ):
            with self.subTest(schema=schema):
                df = self.spark.createDataFrame(t, schema=schema)

                result = df.collect()

                for row in result:
                    i, m = row
                    self.assertEqual(m, map_data[i])

    def test_createDataFrame_pandas_with_struct_type(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_createDataFrame_pandas_with_struct_type(arrow_enabled)

    def check_createDataFrame_pandas_with_struct_type(self, arrow_enabled):
        pdf = pd.DataFrame(
            {"a": [Row(1, "a"), Row(2, "b")], "b": [{"s": 3, "t": "x"}, {"s": 4, "t": "y"}]}
        )
        for schema in (
            "a struct<x int, y string>, b struct<s int, t string>",
            StructType()
            .add("a", StructType().add("x", LongType()).add("y", StringType()))
            .add("b", StructType().add("s", LongType()).add("t", StringType())),
        ):
            with self.subTest(schema=schema):
                with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                    df = self.spark.createDataFrame(pdf, schema)
                result = df.collect()
                expected = [(rec[0], Row(**rec[1])) for rec in pdf.to_records(index=False)]
                for r in range(len(expected)):
                    for e in range(len(expected[r])):
                        self.assertTrue(
                            expected[r][e] == result[r][e], f"{expected[r][e]} == {result[r][e]}"
                        )

    def test_createDataFrame_pandas_with_struct_type(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_createDataFrame_pandas_with_struct_type(arrow_enabled)

    def test_createDataFrame_arrow_with_struct_type_nulls(self):
        t = pa.table(
            {
                "a": [{"x": 1, "y": "a"}, None, {"x": None, "y": "b"}],
                "b": [{"s": 3, "t": None}, {"s": 4, "t": "y"}, None],
            },
        )
        for schema in (
            "a struct<x int, y string>, b struct<s int, t string>",
            StructType()
            .add("a", StructType().add("x", LongType()).add("y", StringType()))
            .add("b", StructType().add("s", LongType()).add("t", StringType())),
        ):
            with self.subTest(schema=schema):
                df = self.spark.createDataFrame(t, schema)
                result = df.collect()
                expected = [
                    (
                        Row(
                            a=None if rec[0] is None else (Row(**rec[0])),
                            b=None if rec[1] is None else Row(**rec[1]),
                        )
                    )
                    for rec in t.to_pandas().to_records(index=False)
                ]
                for r in range(len(expected)):
                    for e in range(len(expected[r])):
                        self.assertTrue(
                            expected[r][e] == result[r][e], f"{expected[r][e]} == {result[r][e]}"
                        )

    def test_createDataFrame_with_string_dtype(self):
        # SPARK-34521: spark.createDataFrame does not support Pandas StringDtype extension type
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": True}):
            data = [["abc"], ["def"], [None], ["ghi"], [None]]
            pandas_df = pd.DataFrame(data, columns=["col"], dtype="string")
            schema = StructType([StructField("col", StringType(), True)])
            df = self.spark.createDataFrame(pandas_df, schema=schema)

            # dtypes won't match. Pandas has two different ways to store string columns:
            # using ndarray (when dtype isn't specified) or using a StringArray when dtype="string".
            # When calling dataframe#toPandas() it will use the ndarray version.
            # Changing that to use a StringArray would be backwards incompatible.
            assert_frame_equal(pandas_df, df.toPandas(), check_dtype=False)

    def test_createDataFrame_with_int64(self):
        # SPARK-34521: spark.createDataFrame does not support Pandas StringDtype extension type
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": True}):
            pandas_df = pd.DataFrame({"col": [1, 2, 3, None]}, dtype="Int64")
            df = self.spark.createDataFrame(pandas_df)
            assert_frame_equal(pandas_df, df.toPandas(), check_dtype=False)

    def test_toPandas_with_map_type(self):
        with self.quiet():
            for arrow_enabled in [True, False]:
                with self.subTest(arrow_enabled=arrow_enabled):
                    self.check_toPandas_with_map_type(arrow_enabled)

    def check_toPandas_with_map_type(self, arrow_enabled):
        origin = pd.DataFrame(
            {"id": [0, 1, 2, 3], "m": [{}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 2, "c": 3}]}
        )

        for schema in [
            "id long, m map<string, long>",
            StructType().add("id", LongType()).add("m", MapType(StringType(), LongType())),
        ]:
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
                df = self.spark.createDataFrame(origin, schema=schema)

            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                pdf = df.toPandas()
                assert_frame_equal(origin, pdf)

    def test_toArrow_with_map_type(self):
        origin = pa.table(
            {"id": [0, 1, 2, 3], "m": [{}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 2, "c": 3}]},
            schema=pa.schema(
                [pa.field("id", pa.int64()), pa.field("m", pa.map_(pa.string(), pa.int64()), True)]
            ),
        )
        for schema in [
            "id long, m map<string, long>",
            StructType().add("id", LongType()).add("m", MapType(StringType(), LongType())),
        ]:
            df = self.spark.createDataFrame(origin, schema=schema)

            t = df.toArrow()
            self.assertTrue(origin.equals(t))

    def test_toPandas_with_map_type_nulls(self):
        with self.quiet():
            for arrow_enabled in [True, False]:
                with self.subTest(arrow_enabled=arrow_enabled):
                    self.check_toPandas_with_map_type_nulls(arrow_enabled)

    def check_toPandas_with_map_type_nulls(self, arrow_enabled):
        origin = pd.DataFrame(
            {"id": [0, 1, 2, 3, 4], "m": [{"a": 1}, {"b": 2, "c": 3}, {}, None, {"d": None}]}
        )

        for schema in [
            "id long, m map<string, long>",
            StructType().add("id", LongType()).add("m", MapType(StringType(), LongType())),
        ]:
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
                df = self.spark.createDataFrame(origin, schema=schema)

            with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
                pdf = df.toPandas()
                assert_frame_equal(origin, pdf)

    def test_toArrow_with_map_type_nulls(self):
        map_data = [{"a": 1}, {"b": 2, "c": 3}, {}, None, {"d": None}]

        origin = pa.table(
            {"id": [0, 1, 2, 3, 4], "m": map_data},
            schema=pa.schema(
                [pa.field("id", pa.int64()), pa.field("m", pa.map_(pa.string(), pa.int64()), True)]
            ),
        )
        for schema in [
            "id long, m map<string, long>",
            StructType().add("id", LongType()).add("m", MapType(StringType(), LongType())),
        ]:
            df = self.spark.createDataFrame(origin, schema=schema)
            pdf = df.toArrow().to_pandas()
            assert_frame_equal(origin.to_pandas(), pdf)

    def test_createDataFrame_pandas_with_int_col_names(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_createDataFrame_pandas_with_int_col_names(arrow_enabled)

    def check_createDataFrame_pandas_with_int_col_names(self, arrow_enabled):
        import numpy as np

        pdf = pd.DataFrame(np.random.rand(4, 2))
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
            df = self.spark.createDataFrame(pdf)
        pdf_col_names = [str(c) for c in pdf.columns]
        self.assertEqual(pdf_col_names, df.columns)

    def test_createDataFrame_arrow_with_int_col_names(self):
        import numpy as np

        t = pa.table(pd.DataFrame(np.random.rand(4, 2)))
        df = self.spark.createDataFrame(t)
        self.assertEqual(t.schema.names, df.columns)

    # Regression test for SPARK-23314
    def test_timestamp_dst(self):
        # Daylight saving time for Los Angeles for 2015 is Sun, Nov 1 at 2:00 am
        dt = [
            datetime.datetime(2015, 11, 1, 0, 30),
            datetime.datetime(2015, 11, 1, 1, 30),
            datetime.datetime(2015, 11, 1, 2, 30),
        ]
        pdf = pd.DataFrame({"time": dt})

        df_from_python = self.spark.createDataFrame(dt, "timestamp").toDF("time")
        df_from_pandas = self.spark.createDataFrame(pdf)

        assert_frame_equal(pdf, df_from_python.toPandas())
        assert_frame_equal(pdf, df_from_pandas.toPandas())

    # Regression test for SPARK-28003
    def test_timestamp_nat(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_timestamp_nat(arrow_enabled)

    def check_timestamp_nat(self, arrow_enabled):
        dt = [pd.NaT, pd.Timestamp("2019-06-11"), None] * 100
        pdf = pd.DataFrame({"time": dt})
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
            df = self.spark.createDataFrame(pdf)

        assert_frame_equal(pdf, df.toPandas())

    def test_toPandas_batch_order(self):
        def delay_first_part(partition_index, iterator):
            if partition_index == 0:
                time.sleep(0.1)
            return iterator

        # Collects Arrow RecordBatches out of order in driver JVM then re-orders in Python
        def run_test(num_records, num_parts, max_records, use_delay=False):
            df = self.spark.range(num_records, numPartitions=num_parts).toDF("a")
            if use_delay:
                df = df.rdd.mapPartitionsWithIndex(delay_first_part).toDF()
            with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": max_records}):
                pdf, pdf_arrow = self._toPandas_arrow_toggle(df)
                assert_frame_equal(pdf, pdf_arrow)

        cases = [
            (1024, 512, 2),  # Use large num partitions for more likely collecting out of order
            (64, 8, 2, True),  # Use delay in first partition to force collecting out of order
            (64, 64, 1),  # Test single batch per partition
            (64, 1, 64),  # Test single partition, single batch
            (64, 1, 8),  # Test single partition, multiple batches
            (30, 7, 2),  # Test different sized partitions
        ]

        for case in cases:
            run_test(*case)

    def test_createDataFrame_with_category_type(self):
        pdf = pd.DataFrame({"A": ["a", "b", "c", "a"]})
        pdf["B"] = pdf["A"].astype("category")
        category_first_element = dict(enumerate(pdf["B"].cat.categories))[0]

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": True}):
            arrow_df = self.spark.createDataFrame(pdf)
            arrow_type = arrow_df.dtypes[1][1]
            result_arrow = arrow_df.toPandas()
            arrow_first_category_element = result_arrow["B"][0]

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            df = self.spark.createDataFrame(pdf)
            spark_type = df.dtypes[1][1]
            result_spark = df.toPandas()
            spark_first_category_element = result_spark["B"][0]

        assert_frame_equal(result_spark, result_arrow)

        # ensure original category elements are string
        self.assertIsInstance(category_first_element, str)
        # spark data frame and arrow execution mode enabled data frame type must match pandas
        self.assertEqual(spark_type, "string")
        self.assertEqual(arrow_type, "string")
        self.assertIsInstance(arrow_first_category_element, str)
        self.assertIsInstance(spark_first_category_element, str)

    def test_createDataFrame_with_dictionary_type_nulls(self):
        import pyarrow.compute as pc

        t = pa.table({"A": ["a", "b", "c", None, "a"]})
        t = t.add_column(1, "B", pc.dictionary_encode(t["A"]))
        category_first_element = sorted(t["B"].combine_chunks().dictionary.to_pylist())[0]

        df = self.spark.createDataFrame(t)
        type = df.dtypes[1][1]
        result = df.toArrow()
        result_first_category_element = result["B"][0].as_py()

        # ensure original category elements are string
        self.assertIsInstance(category_first_element, str)
        self.assertEqual(type, "string")
        self.assertIsInstance(result_first_category_element, str)

    def test_createDataFrame_with_float_index(self):
        # SPARK-32098: float index should not produce duplicated or truncated Spark DataFrame
        self.assertEqual(
            self.spark.createDataFrame(pd.DataFrame({"a": [1, 2, 3]}, index=[2.0, 3.0, 4.0]))
            .distinct()
            .count(),
            3,
        )

    def test_no_partition_toPandas(self):
        # SPARK-32301: toPandas should work from a Spark DataFrame with no partitions
        # Forward-ported from SPARK-32300.
        pdf = self.spark.sparkContext.emptyRDD().toDF("col1 int").toPandas()
        self.assertEqual(len(pdf), 0)
        self.assertEqual(list(pdf.columns), ["col1"])

    def test_createDataFrame_empty_partition(self):
        pdf = pd.DataFrame({"c1": [1], "c2": ["string"]})
        df = self.spark.createDataFrame(pdf)
        self.assertEqual([Row(c1=1, c2="string")], df.collect())
        if not is_remote_only():
            self.assertGreater(self._legacy_sc.defaultParallelism, len(pdf))

    def test_toPandas_error(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_toPandas_error(arrow_enabled)

    def check_toPandas_error(self, arrow_enabled):
        with self.sql_conf(
            {
                "spark.sql.ansi.enabled": True,
                "spark.sql.execution.arrow.pyspark.enabled": arrow_enabled,
            }
        ):
            with self.assertRaises(ArithmeticException):
                self.spark.sql("select 1/0").toPandas()

    def test_toArrow_error(self):
        with self.sql_conf(
            {
                "spark.sql.ansi.enabled": True,
            }
        ):
            with self.assertRaises(ArithmeticException):
                self.spark.sql("select 1/0").toArrow()

    def test_toPandas_duplicate_field_names(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_toPandas_duplicate_field_names(arrow_enabled)

    def check_toPandas_duplicate_field_names(self, arrow_enabled):
        data = [Row(Row("a", 1), Row(2, 3, "b", 4, "c")), Row(Row("x", 6), Row(7, 8, "y", 9, "z"))]
        schema = (
            StructType()
            .add("struct", StructType().add("x", StringType()).add("x", IntegerType()))
            .add(
                "struct",
                StructType()
                .add("a", IntegerType())
                .add("x", IntegerType())
                .add("x", StringType())
                .add("y", IntegerType())
                .add("y", StringType()),
            )
        )
        for struct_in_pandas in ["legacy", "row", "dict"]:
            df = self.spark.createDataFrame(data, schema=schema)

            with self.subTest(struct_in_pandas=struct_in_pandas):
                with self.sql_conf(
                    {
                        "spark.sql.execution.arrow.pyspark.enabled": arrow_enabled,
                        "spark.sql.execution.pandas.structHandlingMode": struct_in_pandas,
                    }
                ):
                    if arrow_enabled and struct_in_pandas == "legacy":
                        with self.assertRaisesRegex(
                            UnsupportedOperationException, "DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT"
                        ):
                            df.toPandas()
                    else:
                        if struct_in_pandas == "dict":
                            expected = pd.DataFrame(
                                [
                                    [
                                        {"x_0": "a", "x_1": 1},
                                        {"a": 2, "x_0": 3, "x_1": "b", "y_0": 4, "y_1": "c"},
                                    ],
                                    [
                                        {"x_0": "x", "x_1": 6},
                                        {"a": 7, "x_0": 8, "x_1": "y", "y_0": 9, "y_1": "z"},
                                    ],
                                ],
                                columns=schema.names,
                            )
                        else:
                            expected = pd.DataFrame.from_records(data, columns=schema.names)
                        assert_frame_equal(df.toPandas(), expected)

    def test_toArrow_duplicate_field_names(self):
        data = [[1, 1], [2, 2]]
        names = ["a", "a"]
        df = self.spark.createDataFrame(data, names)

        expected = pa.table(
            [[1, 2], [1, 2]],
            schema=pa.schema([pa.field("a", pa.int64()), pa.field("a", pa.int64())]),
        )

        self.assertTrue(df.toArrow().equals(expected))

        data = [Row(Row("a", 1), Row(2, 3, "b", 4, "c")), Row(Row("x", 6), Row(7, 8, "y", 9, "z"))]
        schema = (
            StructType()
            .add("struct", StructType().add("x", StringType()).add("x", IntegerType()))
            .add(
                "struct",
                StructType()
                .add("a", IntegerType())
                .add("x", IntegerType())
                .add("x", StringType())
                .add("y", IntegerType())
                .add("y", StringType()),
            )
        )
        df = self.spark.createDataFrame(data, schema=schema)

        with self.assertRaisesRegex(
            UnsupportedOperationException, "DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT"
        ):
            df.toArrow()

    def test_createDataFrame_pandas_duplicate_field_names(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_createDataFrame_pandas_duplicate_field_names(arrow_enabled)

    def check_createDataFrame_pandas_duplicate_field_names(self, arrow_enabled):
        schema = (
            StructType()
            .add("struct", StructType().add("x", StringType()).add("x", IntegerType()))
            .add(
                "struct",
                StructType()
                .add("a", IntegerType())
                .add("x", IntegerType())
                .add("x", StringType())
                .add("y", IntegerType())
                .add("y", StringType()),
            )
        )

        data = [Row(Row("a", 1), Row(2, 3, "b", 4, "c")), Row(Row("x", 6), Row(7, 8, "y", 9, "z"))]
        pdf = pd.DataFrame.from_records(data, columns=schema.names)

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
            df = self.spark.createDataFrame(pdf, schema)

        self.assertEqual(df.collect(), data)

    def test_createDataFrame_arrow_duplicate_field_names(self):
        t = pa.table(
            [[1, 2], [1, 2]],
            schema=pa.schema([pa.field("a", pa.int64()), pa.field("a", pa.int64())]),
        )
        schema = StructType().add("a", LongType()).add("a", LongType())

        df = self.spark.createDataFrame(t)

        self.assertTrue(df.toArrow().equals(t))

        df = self.spark.createDataFrame(t, schema=schema)

        self.assertTrue(df.toArrow().equals(t))

        t = pa.table(
            [
                pa.StructArray.from_arrays(
                    [
                        pa.array(["a", "x"], type=pa.string()),
                        pa.array([1, 6], type=pa.int32()),
                    ],
                    names=["x", "x"],
                ),
                pa.StructArray.from_arrays(
                    [
                        pa.array([2, 7], type=pa.int32()),
                        pa.array([3, 8], type=pa.int32()),
                        pa.array(["b", "y"], type=pa.string()),
                        pa.array([4, 9], type=pa.int32()),
                        pa.array(["c", "z"], type=pa.string()),
                    ],
                    names=["a", "x", "x", "y", "y"],
                ),
            ],
            names=["struct", "struct"],
        )
        schema = (
            StructType()
            .add("struct", StructType().add("x", StringType()).add("x", IntegerType()))
            .add(
                "struct",
                StructType()
                .add("a", IntegerType())
                .add("x", IntegerType())
                .add("x", StringType())
                .add("y", IntegerType())
                .add("y", StringType()),
            )
        )
        with self.assertRaisesRegex(
            UnsupportedOperationException, "DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT"
        ):
            self.spark.createDataFrame(t)

        with self.assertRaisesRegex(
            UnsupportedOperationException, "DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT"
        ):
            self.spark.createDataFrame(t, schema)

    def test_toPandas_empty_columns(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_toPandas_empty_columns(arrow_enabled)

    def check_toPandas_empty_columns(self, arrow_enabled):
        df = self.spark.range(2).select([])

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
            assert_frame_equal(df.toPandas(), pd.DataFrame(columns=[], index=range(2)))

    def test_toArrow_empty_columns(self):
        df = self.spark.range(2).select([])

        self.assertTrue(df.toArrow().equals(pa.table([])))

    def test_toPandas_empty_rows(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_toPandas_empty_rows(arrow_enabled)

    def check_toPandas_empty_rows(self, arrow_enabled):
        df = self.spark.range(2).limit(0)

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
            assert_frame_equal(df.toPandas(), pd.DataFrame({"id": pd.Series([], dtype="int64")}))

    def test_toArrow_empty_rows(self):
        df = self.spark.range(2).limit(0)

        self.assertTrue(
            df.toArrow().equals(
                pa.Table.from_arrays([[]], schema=pa.schema([pa.field("id", pa.int64(), False)]))
            )
        )

    def test_createDataFrame_pandas_nested_timestamp(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_createDataFrame_pandas_nested_timestamp(arrow_enabled)

    def check_createDataFrame_pandas_nested_timestamp(self, arrow_enabled):
        schema = self.schema_nested_timestamp
        data = self.data_nested_timestamp
        pdf = pd.DataFrame.from_records(data, columns=schema.names)

        with self.sql_conf(
            {
                "spark.sql.session.timeZone": "America/New_York",
                "spark.sql.execution.arrow.pyspark.enabled": arrow_enabled,
            }
        ):
            df = self.spark.createDataFrame(pdf, schema)

        expected = self.data_nested_timestamp_expected_ny

        self.assertEqual(df.first(), expected)

    def test_createDataFrame_arrow_nested_timestamp(self):
        from pyspark.sql.pandas.types import to_arrow_schema

        schema = self.schema_nested_timestamp
        data = self.data_nested_timestamp
        pdf = pd.DataFrame.from_records(data, columns=schema.names)
        arrow_schema = to_arrow_schema(schema, timestamp_utc=False)
        t = pa.Table.from_pandas(pdf, arrow_schema)

        with self.sql_conf({"spark.sql.session.timeZone": "America/New_York"}):
            df = self.spark.createDataFrame(t, schema)

        expected = self.data_nested_timestamp_expected_ny

        self.assertEqual(df.first(), expected)

    def test_toPandas_timestmap_tzinfo(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_toPandas_timestmap_tzinfo(arrow_enabled)

    def check_toPandas_timestmap_tzinfo(self, arrow_enabled):
        # SPARK-47202: Test timestamp with tzinfo in toPandas and createDataFrame
        from zoneinfo import ZoneInfo

        ts_tzinfo = datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("America/Los_Angeles"))
        data = pd.DataFrame({"a": [ts_tzinfo]})
        df = self.spark.createDataFrame(data)

        with self.sql_conf(
            {
                "spark.sql.execution.arrow.pyspark.enabled": arrow_enabled,
            }
        ):
            pdf = df.toPandas()

        expected = pd.DataFrame(
            # Spark unsets tzinfo and converts them to localtimes.
            {"a": [datetime.datetime.fromtimestamp(calendar.timegm(ts_tzinfo.utctimetuple()))]}
        )

        assert_frame_equal(pdf, expected)

    def test_toPandas_nested_timestamp(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_toPandas_nested_timestamp(arrow_enabled)

    def check_toPandas_nested_timestamp(self, arrow_enabled):
        schema = self.schema_nested_timestamp
        data = self.data_nested_timestamp
        df = self.spark.createDataFrame(data, schema)

        with self.sql_conf(
            {
                "spark.sql.session.timeZone": "America/New_York",
                "spark.sql.execution.arrow.pyspark.enabled": arrow_enabled,
                "spark.sql.execution.pandas.structHandlingMode": "row",
            }
        ):
            pdf = df.toPandas()

        expected = pd.DataFrame(
            {
                "ts": [datetime.datetime(2023, 1, 1, 3, 0, 0)],
                "ts_ntz": [datetime.datetime(2023, 1, 1, 0, 0, 0)],
                "struct": [
                    Row(
                        datetime.datetime(2023, 1, 1, 3, 0, 0),
                        datetime.datetime(2023, 1, 1, 0, 0, 0),
                    )
                ],
                "array": [[datetime.datetime(2023, 1, 1, 3, 0, 0)]],
                "array_ntz": [[datetime.datetime(2023, 1, 1, 0, 0, 0)]],
                "map": [dict(ts=datetime.datetime(2023, 1, 1, 3, 0, 0))],
                "map_ntz": [dict(ts_ntz=datetime.datetime(2023, 1, 1, 0, 0, 0))],
            }
        )

        assert_frame_equal(pdf, expected)

    def test_toArrow_nested_timestamp(self):
        schema = self.schema_nested_timestamp
        data = self.data_nested_timestamp
        df = self.spark.createDataFrame(data, schema)

        t = df.toArrow()

        from pyspark.sql.pandas.types import to_arrow_schema

        arrow_schema = to_arrow_schema(schema)
        expected = pa.Table.from_pydict(
            {
                "ts": [datetime.datetime(2023, 1, 1, 8, 0, 0)],
                "ts_ntz": [datetime.datetime(2023, 1, 1, 0, 0, 0)],
                "struct": [
                    Row(
                        datetime.datetime(2023, 1, 1, 8, 0, 0),
                        datetime.datetime(2023, 1, 1, 0, 0, 0),
                    )
                ],
                "array": [[datetime.datetime(2023, 1, 1, 8, 0, 0)]],
                "array_ntz": [[datetime.datetime(2023, 1, 1, 0, 0, 0)]],
                "map": [dict(ts=datetime.datetime(2023, 1, 1, 8, 0, 0))],
                "map_ntz": [dict(ts_ntz=datetime.datetime(2023, 1, 1, 0, 0, 0))],
            },
            schema=arrow_schema,
        )

        self.assertTrue(t.equals(expected))

    def test_arrow_map_timestamp_nulls_round_trip(self):
        origin_schema = pa.schema([("map", pa.map_(pa.string(), pa.timestamp("us", tz="UTC")))])
        origin = pa.table(
            [[dict(ts=datetime.datetime(2023, 1, 1, 8, 0, 0)), None]],
            schema=origin_schema,
        )
        df = self.spark.createDataFrame(origin)
        t = df.toArrow()

        # SPARK-48302: PyArrow versions before 17.0.0 replaced nulls with empty lists when
        # reconstructing MapArray columns to localize timestamps
        if LooseVersion(pa.__version__) >= LooseVersion("17.0.0"):
            expected = origin
        else:
            expected = pa.table(
                [[dict(ts=datetime.datetime(2023, 1, 1, 8, 0, 0)), []]],
                schema=origin_schema,
            )

        self.assertTrue(t.equals(expected))

    def test_createDataFrame_udt(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_createDataFrame_udt(arrow_enabled)

    def check_createDataFrame_udt(self, arrow_enabled):
        schema = (
            StructType()
            .add("point", ExamplePointUDT())
            .add("struct", StructType().add("point", ExamplePointUDT()))
            .add("array", ArrayType(ExamplePointUDT()))
            .add("map", MapType(StringType(), ExamplePointUDT()))
        )
        data = [
            Row(
                ExamplePoint(1.0, 2.0),
                Row(ExamplePoint(3.0, 4.0)),
                [ExamplePoint(5.0, 6.0)],
                dict(point=ExamplePoint(7.0, 8.0)),
            )
        ]
        pdf = pd.DataFrame.from_records(data, columns=schema.names)

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": arrow_enabled}):
            df = self.spark.createDataFrame(pdf, schema)

        self.assertEqual(df.collect(), data)

    def test_toPandas_udt(self):
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_toPandas_udt(arrow_enabled)

    def check_toPandas_udt(self, arrow_enabled):
        schema = (
            StructType()
            .add("point", ExamplePointUDT())
            .add("struct", StructType().add("point", ExamplePointUDT()))
            .add("array", ArrayType(ExamplePointUDT()))
            .add("map", MapType(StringType(), ExamplePointUDT()))
        )
        data = [
            Row(
                ExamplePoint(1.0, 2.0),
                Row(ExamplePoint(3.0, 4.0)),
                [ExamplePoint(5.0, 6.0)],
                dict(point=ExamplePoint(7.0, 8.0)),
            )
        ]
        df = self.spark.createDataFrame(data, schema)

        with self.sql_conf(
            {
                "spark.sql.execution.arrow.pyspark.enabled": arrow_enabled,
                "spark.sql.execution.pandas.structHandlingMode": "row",
            }
        ):
            pdf = df.toPandas()

        expected = pd.DataFrame.from_records(data, columns=schema.names)

        assert_frame_equal(pdf, expected)

    def test_create_dataframe_namedtuples(self):
        # SPARK-44980: Inherited namedtuples in createDataFrame
        for arrow_enabled in [True, False]:
            with self.subTest(arrow_enabled=arrow_enabled):
                self.check_create_dataframe_namedtuples(arrow_enabled)

    def check_create_dataframe_namedtuples(self, arrow_enabled):
        MyTuple = namedtuple("MyTuple", ["a", "b", "c"])

        class MyInheritedTuple(MyTuple):
            pass

        with self.sql_conf(
            {
                "spark.sql.execution.arrow.pyspark.enabled": arrow_enabled,
            }
        ):
            df = self.spark.createDataFrame([MyInheritedTuple(1, 2, 3)])
            self.assertEqual(df.first(), Row(a=1, b=2, c=3))

            df = self.spark.createDataFrame([MyInheritedTuple(1, 2, MyInheritedTuple(1, 2, 3))])
            self.assertEqual(df.first(), Row(a=1, b=2, c=Row(a=1, b=2, c=3)))

    def test_negative_and_zero_batch_size(self):
        # SPARK-47068: Negative and zero value should work as unlimited batch size.
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 0}):
            pdf = pd.DataFrame({"a": [123]})
            assert_frame_equal(pdf, self.spark.createDataFrame(pdf).toPandas())

        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": -1}):
            pdf = pd.DataFrame({"a": [123]})
            assert_frame_equal(pdf, self.spark.createDataFrame(pdf).toPandas())

    def test_createDataFrame_arrow_large_string(self):
        a = pa.array(["a"] * 5, type=pa.large_string())
        t = pa.table([a], ["ls"])
        df = self.spark.createDataFrame(t)
        self.assertIsInstance(df.schema["ls"].dataType, StringType)

    def test_createDataFrame_arrow_large_binary(self):
        a = pa.array(["a"] * 5, type=pa.large_binary())
        t = pa.table([a], ["lb"])
        df = self.spark.createDataFrame(t)
        self.assertIsInstance(df.schema["lb"].dataType, BinaryType)

    def test_createDataFrame_arrow_large_list(self):
        a = pa.array([[-1, 3]] * 5, type=pa.large_list(pa.int32()))
        t = pa.table([a], ["ll"])
        df = self.spark.createDataFrame(t)
        self.assertIsInstance(df.schema["ll"].dataType, ArrayType)

    def test_createDataFrame_arrow_large_list_int64_offset(self):
        # Check for expected failure if the large list contains an index >= 2^31
        a = pa.LargeListArray.from_arrays(
            [0, 2**31], pa.NullArray.from_buffers(pa.null(), 2**31, [None])
        )
        t = pa.table([a], ["ll"])
        with self.assertRaises(Exception):
            self.spark.createDataFrame(t)

    def test_createDataFrame_arrow_fixed_size_binary(self):
        a = pa.array(["a"] * 5, type=pa.binary(1))
        t = pa.table([a], ["fsb"])
        df = self.spark.createDataFrame(t)
        self.assertIsInstance(df.schema["fsb"].dataType, BinaryType)

    def test_createDataFrame_arrow_fixed_size_list(self):
        a = pa.array([[-1, 3]] * 5, type=pa.list_(pa.int32(), 2))
        t = pa.table([a], ["fsl"])
        if LooseVersion(pa.__version__) < LooseVersion("14.0.0"):
            # PyArrow versions before 14.0.0 do not support casting FixedSizeListArray to ListArray
            with self.assertRaises(PySparkTypeError):
                df = self.spark.createDataFrame(t)
        else:
            df = self.spark.createDataFrame(t)
            self.assertIsInstance(df.schema["fsl"].dataType, ArrayType)


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class ArrowTests(ArrowTestsMixin, ReusedSQLTestCase):
    pass


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class MaxResultArrowTests(unittest.TestCase):
    # These tests are separate as 'spark.driver.maxResultSize' configuration
    # is a static configuration to Spark context.

    @classmethod
    def setUpClass(cls):
        from pyspark import SparkContext

        cls.spark = SparkSession(
            SparkContext(
                "local[4]", cls.__name__, conf=SparkConf().set("spark.driver.maxResultSize", "10k")
            )
        )

        # Explicitly enable Arrow and disable fallback.
        cls.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        cls.spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_exception_by_max_results(self):
        with self.assertRaisesRegex(Exception, "is bigger than"):
            self.spark.range(0, 10000, 1, 100).toPandas()


class EncryptionArrowTests(ArrowTests):
    @classmethod
    def conf(cls):
        return super(EncryptionArrowTests, cls).conf().set("spark.io.encryption.enabled", "true")


class RDDBasedArrowTests(ArrowTests):
    @classmethod
    def conf(cls):
        return (
            super(RDDBasedArrowTests, cls)
            .conf()
            .set("spark.sql.execution.arrow.localRelationThreshold", "0")
            # to test multiple partitions
            .set("spark.sql.execution.arrow.maxRecordsPerBatch", "2")
        )


if __name__ == "__main__":
    from pyspark.sql.tests.test_arrow import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
