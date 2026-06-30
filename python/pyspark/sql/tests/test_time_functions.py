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

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    TimeType,
)
from pyspark.errors import DateTimeException, IllegalArgumentException
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


class TimeFunctionsTestsMixin:
    """Tests for TIME data type functions in PySpark."""

    def test_create_dataframe_roundtrip(self):
        """Test that datetime.time values survive createDataFrame -> collect round-trip."""
        data = [
            (datetime.time(0, 0, 0),),
            (datetime.time(12, 30, 45),),
            (datetime.time(23, 59, 59, 999999),),
            (None,),
        ]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)

        result = df.collect()
        self.assertEqual(result[0].t, datetime.time(0, 0, 0))
        self.assertEqual(result[1].t, datetime.time(12, 30, 45))
        self.assertEqual(result[2].t, datetime.time(23, 59, 59, 999999))
        self.assertIsNone(result[3].t)

    def test_create_dataframe_infer_schema(self):
        """Test that schema inference recognizes datetime.time as TimeType."""
        data = [(datetime.time(10, 30),), (datetime.time(14, 0),)]
        df = self.spark.createDataFrame(data, ["t"])
        self.assertIsInstance(df.schema["t"].dataType, TimeType)

    def test_make_time(self):
        """Test make_time function with various inputs."""
        df = self.spark.createDataFrame([(10, 30, 45), (23, 59, 59)], ["h", "m", "s"])

        result = df.select(F.make_time("h", "m", "s")).collect()
        self.assertEqual(result[0][0], datetime.time(10, 30, 45))
        self.assertEqual(result[1][0], datetime.time(23, 59, 59))

        # selectExpr parity
        result_expr = df.selectExpr("make_time(h, m, s)").collect()
        self.assertEqual(result[0][0], result_expr[0][0])
        self.assertEqual(result[1][0], result_expr[1][0])

    def test_make_time_null_handling(self):
        """Test make_time returns NULL when any input is NULL."""
        df = self.spark.createDataFrame(
            [(None, 30, 45), (10, None, 45), (10, 30, None)],
            "h: int, m: int, s: int",
        )
        result = df.select(F.make_time("h", "m", "s")).collect()
        for row in result:
            self.assertIsNone(row[0])

    def test_hour(self):
        """Test hour extraction from TIME column."""
        data = [(datetime.time(10, 30, 45),), (datetime.time(23, 0, 0),)]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)

        result = df.select(F.hour("t")).collect()
        self.assertEqual(result[0][0], 10)
        self.assertEqual(result[1][0], 23)

        # selectExpr parity
        result_expr = df.selectExpr("hour(t)").collect()
        self.assertEqual(result[0][0], result_expr[0][0])

    def test_minute(self):
        """Test minute extraction from TIME column."""
        data = [(datetime.time(10, 30, 45),), (datetime.time(23, 59, 0),)]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)

        result = df.select(F.minute("t")).collect()
        self.assertEqual(result[0][0], 30)
        self.assertEqual(result[1][0], 59)

        # selectExpr parity
        result_expr = df.selectExpr("minute(t)").collect()
        self.assertEqual(result[0][0], result_expr[0][0])

    def test_second(self):
        """Test second extraction from TIME column."""
        data = [(datetime.time(10, 30, 45),), (datetime.time(23, 59, 59),)]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)

        result = df.select(F.second("t")).collect()
        self.assertEqual(result[0][0], 45)
        self.assertEqual(result[1][0], 59)

        # selectExpr parity
        result_expr = df.selectExpr("second(t)").collect()
        self.assertEqual(result[0][0], result_expr[0][0])

    def test_to_time(self):
        """Test to_time string parsing without format."""
        df = self.spark.createDataFrame([("10:30:45",), ("23:59:59",)], ["s"])
        result = df.select(F.to_time("s")).collect()
        self.assertEqual(result[0][0], datetime.time(10, 30, 45))
        self.assertEqual(result[1][0], datetime.time(23, 59, 59))

    def test_to_time_with_format(self):
        """Test to_time string parsing with custom format."""
        df = self.spark.createDataFrame([("10.30.45",), ("23.59.59",)], ["s"])
        result = df.select(F.to_time("s", F.lit("HH.mm.ss"))).collect()
        self.assertEqual(result[0][0], datetime.time(10, 30, 45))
        self.assertEqual(result[1][0], datetime.time(23, 59, 59))

    def test_try_to_time(self):
        """Test try_to_time returns NULL on invalid input instead of raising."""
        df = self.spark.createDataFrame([("10:30:45",), ("not_a_time",), (None,)], ["s"])
        result = df.select(F.try_to_time("s")).collect()
        self.assertEqual(result[0][0], datetime.time(10, 30, 45))
        self.assertIsNone(result[1][0])
        self.assertIsNone(result[2][0])

    def test_try_to_time_with_format(self):
        """Test try_to_time with custom format returns NULL on mismatch."""
        df = self.spark.createDataFrame([("10.30.45",), ("10:30:45",)], ["s"])
        result = df.select(F.try_to_time("s", F.lit("HH.mm.ss"))).collect()
        self.assertEqual(result[0][0], datetime.time(10, 30, 45))
        self.assertIsNone(result[1][0])

    def test_time_diff(self):
        """Test time_diff function with various units."""
        data = [(datetime.time(10, 0, 0), datetime.time(12, 30, 0))]
        schema = StructType(
            [
                StructField("t1", TimeType()),
                StructField("t2", TimeType()),
            ]
        )
        df = self.spark.createDataFrame(data, schema=schema)

        # HOUR unit
        result = df.select(F.time_diff(F.lit("HOUR"), "t1", "t2")).collect()
        self.assertEqual(result[0][0], 2)

        # selectExpr parity
        result_expr = df.selectExpr("time_diff('HOUR', t1, t2)").collect()
        self.assertEqual(result[0][0], result_expr[0][0])

    def test_time_diff_null_handling(self):
        """Test time_diff returns NULL when inputs are NULL."""
        data = [(None, datetime.time(12, 0, 0)), (datetime.time(10, 0, 0), None)]
        schema = StructType(
            [
                StructField("t1", TimeType()),
                StructField("t2", TimeType()),
            ]
        )
        df = self.spark.createDataFrame(data, schema=schema)
        result = df.select(F.time_diff(F.lit("HOUR"), "t1", "t2")).collect()
        self.assertIsNone(result[0][0])
        self.assertIsNone(result[1][0])

    def test_time_trunc(self):
        """Test time_trunc function with various units."""
        data = [(datetime.time(10, 30, 45, 123456),)]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)

        # Truncate to HOUR
        result = df.select(F.time_trunc(F.lit("HOUR"), "t")).collect()
        self.assertEqual(result[0][0], datetime.time(10, 0, 0))

        # Truncate to MINUTE
        result = df.select(F.time_trunc(F.lit("MINUTE"), "t")).collect()
        self.assertEqual(result[0][0], datetime.time(10, 30, 0))

        # selectExpr parity
        result_expr = df.selectExpr("time_trunc('HOUR', t)").collect()
        self.assertEqual(result_expr[0][0], datetime.time(10, 0, 0))

    def test_time_trunc_null_handling(self):
        """Test time_trunc returns NULL when input is NULL."""
        data = [(None,)]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)
        result = df.select(F.time_trunc(F.lit("HOUR"), "t")).collect()
        self.assertIsNone(result[0][0])

    def test_current_time(self):
        """Test current_time returns a valid TIME value."""
        result = self.spark.sql("SELECT current_time()").collect()
        self.assertIsInstance(result[0][0], datetime.time)

    def test_current_time_with_precision(self):
        """Test current_time with specified precision."""
        for precision in range(0, 7):
            result = self.spark.sql(f"SELECT current_time({precision})").collect()
            self.assertIsInstance(result[0][0], datetime.time)

    def test_lit_time(self):
        """Test creating a literal TIME column."""
        t = datetime.time(10, 30, 45)
        df = self.spark.range(1).select(F.lit(t).alias("t"))
        self.assertIsInstance(df.schema["t"].dataType, TimeType)
        result = df.collect()
        self.assertEqual(result[0][0], t)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        pandas_requirement_message or pyarrow_requirement_message,
    )
    def test_arrow_roundtrip(self):
        """Test that TIME values survive Arrow-based createDataFrame -> toPandas round-trip."""
        data = [
            (datetime.time(0, 0, 0),),
            (datetime.time(12, 30, 45, 500000),),
            (datetime.time(23, 59, 59, 999999),),
        ]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)

        pdf = df.toPandas()
        self.assertEqual(pdf["t"][0], datetime.time(0, 0, 0))
        self.assertEqual(pdf["t"][1], datetime.time(12, 30, 45, 500000))
        self.assertEqual(pdf["t"][2], datetime.time(23, 59, 59, 999999))

        # Round-trip back
        df2 = self.spark.createDataFrame(pdf, schema=schema)
        result = df2.collect()
        self.assertEqual(result[0][0], datetime.time(0, 0, 0))
        self.assertEqual(result[1][0], datetime.time(12, 30, 45, 500000))
        self.assertEqual(result[2][0], datetime.time(23, 59, 59, 999999))

    def test_time_from_seconds(self):
        """Test time_from_seconds function."""
        df = self.spark.createDataFrame([(0,), (3600,), (45045,)], ["s"])
        result = df.select(F.time_from_seconds("s")).collect()
        self.assertEqual(result[0][0], datetime.time(0, 0, 0))
        self.assertEqual(result[1][0], datetime.time(1, 0, 0))
        self.assertEqual(result[2][0], datetime.time(12, 30, 45))

    def test_time_to_seconds(self):
        """Test time_to_seconds function."""
        data = [(datetime.time(0, 0, 0),), (datetime.time(1, 0, 0),), (datetime.time(12, 30, 45),)]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)
        result = df.select(F.time_to_seconds("t")).collect()
        self.assertEqual(result[0][0], 0)
        self.assertEqual(result[1][0], 3600)
        self.assertEqual(result[2][0], 45045)

    def test_time_from_millis(self):
        """Test time_from_millis function."""
        df = self.spark.createDataFrame([(0,), (3600000,), (45045500,)], ["ms"])
        result = df.select(F.time_from_millis("ms")).collect()
        self.assertEqual(result[0][0], datetime.time(0, 0, 0))
        self.assertEqual(result[1][0], datetime.time(1, 0, 0))
        self.assertEqual(result[2][0], datetime.time(12, 30, 45, 500000))

    def test_time_to_millis(self):
        """Test time_to_millis function."""
        data = [
            (datetime.time(0, 0, 0),),
            (datetime.time(1, 0, 0),),
            (datetime.time(12, 30, 45, 500000),),
        ]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)
        result = df.select(F.time_to_millis("t")).collect()
        self.assertEqual(result[0][0], 0)
        self.assertEqual(result[1][0], 3600000)
        self.assertEqual(result[2][0], 45045500)

    def test_time_from_micros(self):
        """Test time_from_micros function."""
        df = self.spark.createDataFrame([(0,), (3600000000,), (45045500000,)], ["us"])
        result = df.select(F.time_from_micros("us")).collect()
        self.assertEqual(result[0][0], datetime.time(0, 0, 0))
        self.assertEqual(result[1][0], datetime.time(1, 0, 0))
        self.assertEqual(result[2][0], datetime.time(12, 30, 45, 500000))

    def test_time_to_micros(self):
        """Test time_to_micros function."""
        data = [
            (datetime.time(0, 0, 0),),
            (datetime.time(1, 0, 0),),
            (datetime.time(12, 30, 45, 500000),),
        ]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)
        result = df.select(F.time_to_micros("t")).collect()
        self.assertEqual(result[0][0], 0)
        self.assertEqual(result[1][0], 3600000000)
        self.assertEqual(result[2][0], 45045500000)

    def test_multi_row_operations(self):
        """Test TIME operations on multi-row DataFrames."""
        data = [
            (datetime.time(8, 0, 0),),
            (datetime.time(12, 0, 0),),
            (datetime.time(17, 30, 0),),
            (datetime.time(23, 59, 59),),
        ]
        schema = StructType([StructField("t", TimeType())])
        df = self.spark.createDataFrame(data, schema=schema)

        # Extract hour for all rows
        result = df.select(F.hour("t")).collect()
        self.assertEqual([r[0] for r in result], [8, 12, 17, 23])

        # Filter
        filtered = df.filter(F.hour("t") >= 12).collect()
        self.assertEqual(len(filtered), 3)

    def test_to_time_error_on_invalid_input(self):
        """Test to_time raises error on unparseable input."""
        df = self.spark.range(1).select(F.lit("invalid_time").alias("s"))
        with self.assertRaises(DateTimeException) as ctx:
            df.select(F.to_time("s")).collect()
        self.assertIn("CANNOT_PARSE_TIME", ctx.exception.getErrorClass())

    def test_time_diff_error_on_invalid_unit(self):
        """Test time_diff raises error on invalid unit."""
        df = self.spark.range(1).select(
            F.lit("invalid_unit").alias("unit"),
            F.lit(datetime.time(10, 0, 0)).alias("t1"),
            F.lit(datetime.time(12, 0, 0)).alias("t2"),
        )
        with self.assertRaises(IllegalArgumentException) as ctx:
            df.select(F.time_diff("unit", "t1", "t2")).collect()
        self.assertIn(
            "INVALID_PARAMETER_VALUE.TIME_UNIT",
            ctx.exception.getErrorClass(),
        )

    def test_time_trunc_error_on_invalid_unit(self):
        """Test time_trunc raises error on invalid unit."""
        df = self.spark.range(1).select(
            F.lit("invalid_unit").alias("unit"),
            F.lit(datetime.time(10, 30, 45)).alias("t"),
        )
        with self.assertRaises(IllegalArgumentException) as ctx:
            df.select(F.time_trunc("unit", "t")).collect()
        self.assertIn(
            "INVALID_PARAMETER_VALUE.TIME_UNIT",
            ctx.exception.getErrorClass(),
        )

    def test_time_diff_multiple_units(self):
        """Test time_diff with SECOND and MICROSECOND units."""
        df = self.spark.range(1).select(
            F.lit(datetime.time(10, 0, 0)).alias("t1"),
            F.lit(datetime.time(10, 0, 30)).alias("t2"),
        )

        # SECOND unit
        result = df.select(F.time_diff(F.lit("second"), "t1", "t2")).collect()
        self.assertEqual(result[0][0], 30)

        # MICROSECOND unit
        result = df.select(F.time_diff(F.lit("microsecond"), "t1", "t2")).collect()
        self.assertEqual(result[0][0], 30000000)

    def test_time_trunc_second(self):
        """Test time_trunc to SECOND level."""
        df = self.spark.range(1).select(F.lit(datetime.time(10, 30, 45, 123456)).alias("t"))

        result = df.select(F.time_trunc(F.lit("second"), "t")).collect()
        self.assertEqual(result[0][0], datetime.time(10, 30, 45))


class TimeFunctionsTests(TimeFunctionsTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
