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

import decimal
import datetime

from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase
from pyspark.sql.connect.column import (
    Column,
    LiteralExpression,
    JVM_BYTE_MIN,
    JVM_BYTE_MAX,
    JVM_SHORT_MIN,
    JVM_SHORT_MAX,
    JVM_INT_MIN,
    JVM_INT_MAX,
    JVM_LONG_MIN,
    JVM_LONG_MAX,
)

from pyspark.sql.types import (
    StructField,
    StructType,
    ArrayType,
    MapType,
    NullType,
    DateType,
    TimestampType,
    TimestampNTZType,
    ByteType,
    BinaryType,
    ShortType,
    IntegerType,
    FloatType,
    DayTimeIntervalType,
    StringType,
    DoubleType,
    LongType,
    DecimalType,
    BooleanType,
)
from pyspark.testing.connectutils import should_test_connect

if should_test_connect:
    import pandas as pd
    from pyspark.sql.connect.functions import lit


class SparkConnectTests(SparkConnectSQLTestCase):
    def test_column_operator(self):
        # SPARK-41351: Column needs to support !=
        df = self.connect.range(10)
        self.assertEqual(9, len(df.filter(df.id != lit(1)).collect()))

    def test_columns(self):
        # SPARK-41036: test `columns` API for python client.
        df = self.connect.read.table(self.tbl_name)
        df2 = self.spark.read.table(self.tbl_name)
        self.assertEqual(["id", "name"], df.columns)

        self.assert_eq(
            df.filter(df.name.rlike("20")).toPandas(), df2.filter(df2.name.rlike("20")).toPandas()
        )
        self.assert_eq(
            df.filter(df.name.like("20")).toPandas(), df2.filter(df2.name.like("20")).toPandas()
        )
        self.assert_eq(
            df.filter(df.name.ilike("20")).toPandas(), df2.filter(df2.name.ilike("20")).toPandas()
        )
        self.assert_eq(
            df.filter(df.name.contains("20")).toPandas(),
            df2.filter(df2.name.contains("20")).toPandas(),
        )
        self.assert_eq(
            df.filter(df.name.startswith("2")).toPandas(),
            df2.filter(df2.name.startswith("2")).toPandas(),
        )
        self.assert_eq(
            df.filter(df.name.endswith("0")).toPandas(),
            df2.filter(df2.name.endswith("0")).toPandas(),
        )
        self.assert_eq(
            df.select(df.name.substr(0, 1).alias("col")).toPandas(),
            df2.select(df2.name.substr(0, 1).alias("col")).toPandas(),
        )
        df3 = self.connect.sql("SELECT cast(null as int) as name")
        df4 = self.spark.sql("SELECT cast(null as int) as name")
        self.assert_eq(
            df3.filter(df3.name.isNull()).toPandas(),
            df4.filter(df4.name.isNull()).toPandas(),
        )
        self.assert_eq(
            df3.filter(df3.name.isNotNull()).toPandas(),
            df4.filter(df4.name.isNotNull()).toPandas(),
        )

    def test_datetime(self):
        query = """
            SELECT * FROM VALUES
            (TIMESTAMP('2022-12-22 15:50:00'), DATE('2022-12-25'), 1.1),
            (TIMESTAMP('2022-12-22 18:50:00'), NULL, 2.2),
            (TIMESTAMP('2022-12-23 15:50:00'), DATE('2022-12-24'), 3.3),
            (NULL, DATE('2022-12-22'), NULL)
            AS tab(a, b, c)
            """
        # +-------------------+----------+----+
        # |                  a|         b|   c|
        # +-------------------+----------+----+
        # |2022-12-22 15:50:00|2022-12-25| 1.1|
        # |2022-12-22 18:50:00|      null| 2.2|
        # |2022-12-23 15:50:00|2022-12-24| 3.3|
        # |               null|2022-12-22|null|
        # +-------------------+----------+----+

        cdf = self.spark.sql(query)
        sdf = self.connect.sql(query)

        # datetime.date
        self.assert_eq(
            cdf.select(cdf.a < datetime.date(2022, 12, 23)).toPandas(),
            sdf.select(sdf.a < datetime.date(2022, 12, 23)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.a != datetime.date(2022, 12, 23)).toPandas(),
            sdf.select(sdf.a != datetime.date(2022, 12, 23)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.a == datetime.date(2022, 12, 22)).toPandas(),
            sdf.select(sdf.a == datetime.date(2022, 12, 22)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.b < datetime.date(2022, 12, 23)).toPandas(),
            sdf.select(sdf.b < datetime.date(2022, 12, 23)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.b >= datetime.date(2022, 12, 23)).toPandas(),
            sdf.select(sdf.b >= datetime.date(2022, 12, 23)).toPandas(),
        )

        # datetime.datetime
        self.assert_eq(
            cdf.select(cdf.a < datetime.datetime(2022, 12, 22, 17, 0, 0)).toPandas(),
            sdf.select(sdf.a < datetime.datetime(2022, 12, 22, 17, 0, 0)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.a > datetime.datetime(2022, 12, 22, 17, 0, 0)).toPandas(),
            sdf.select(sdf.a > datetime.datetime(2022, 12, 22, 17, 0, 0)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.b >= datetime.datetime(2022, 12, 23, 17, 0, 0)).toPandas(),
            sdf.select(sdf.b >= datetime.datetime(2022, 12, 23, 17, 0, 0)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.b < datetime.datetime(2022, 12, 23, 17, 0, 0)).toPandas(),
            sdf.select(sdf.b < datetime.datetime(2022, 12, 23, 17, 0, 0)).toPandas(),
        )

    def test_simple_binary_expressions(self):
        """Test complex expression"""
        df = self.connect.read.table(self.tbl_name)
        pdf = df.select(df.id).where(df.id % lit(30) == lit(0)).sort(df.id.asc()).toPandas()
        self.assertEqual(len(pdf.index), 4)

        res = pd.DataFrame(data={"id": [0, 30, 60, 90]})
        self.assert_(pdf.equals(res), f"{pdf.to_string()} != {res.to_string()}")

    def test_literal_with_acceptable_type(self):
        for value, dataType in [
            (b"binary\0\0asas", BinaryType()),
            (True, BooleanType()),
            (False, BooleanType()),
            (0, ByteType()),
            (JVM_BYTE_MIN, ByteType()),
            (JVM_BYTE_MAX, ByteType()),
            (0, ShortType()),
            (JVM_SHORT_MIN, ShortType()),
            (JVM_SHORT_MAX, ShortType()),
            (0, IntegerType()),
            (JVM_INT_MIN, IntegerType()),
            (JVM_INT_MAX, IntegerType()),
            (0, LongType()),
            (JVM_LONG_MIN, LongType()),
            (JVM_LONG_MAX, LongType()),
            (0.0, FloatType()),
            (1.234567, FloatType()),
            (float("nan"), FloatType()),
            (float("inf"), FloatType()),
            (float("-inf"), FloatType()),
            (0.0, DoubleType()),
            (1.234567, DoubleType()),
            (float("nan"), DoubleType()),
            (float("inf"), DoubleType()),
            (float("-inf"), DoubleType()),
            (decimal.Decimal(0.0), DecimalType()),
            (decimal.Decimal(1.234567), DecimalType()),
            ("sss", StringType()),
            (datetime.date(2022, 12, 13), DateType()),
            (datetime.datetime.now(), DateType()),
            (datetime.datetime.now(), TimestampType()),
            (datetime.datetime.now(), TimestampNTZType()),
            (datetime.timedelta(1, 2, 3), DayTimeIntervalType()),
        ]:
            lit = LiteralExpression(value=value, dataType=dataType)
            self.assertEqual(dataType, lit._dataType)

    def test_literal_with_unsupported_type(self):
        for value, dataType in [
            (b"binary\0\0asas", BooleanType()),
            (True, StringType()),
            (False, DoubleType()),
            (JVM_BYTE_MIN - 1, ByteType()),
            (JVM_BYTE_MAX + 1, ByteType()),
            (JVM_SHORT_MIN - 1, ShortType()),
            (JVM_SHORT_MAX + 1, ShortType()),
            (JVM_INT_MIN - 1, IntegerType()),
            (JVM_INT_MAX + 1, IntegerType()),
            (JVM_LONG_MIN - 1, LongType()),
            (JVM_LONG_MAX + 1, LongType()),
            (0.1, DecimalType()),
            (datetime.date(2022, 12, 13), TimestampType()),
            (datetime.timedelta(1, 2, 3), DateType()),
            ([1, 2, 3], ArrayType(IntegerType())),
            ({1: 2}, MapType(IntegerType(), IntegerType())),
            (
                {"a": "xyz", "b": 1},
                StructType([StructField("a", StringType()), StructField("b", IntegerType())]),
            ),
        ]:
            with self.assertRaises(AssertionError):
                LiteralExpression(value=value, dataType=dataType)

    def test_literal_null(self):
        for dataType in [
            NullType(),
            BinaryType(),
            BooleanType(),
            ByteType(),
            ShortType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            DecimalType(),
            DateType(),
            TimestampType(),
            TimestampNTZType(),
            DayTimeIntervalType(),
        ]:
            lit_null = LiteralExpression(value=None, dataType=dataType)
            self.assertTrue(lit_null._value is None)
            self.assertEqual(dataType, lit_null._dataType)

            cdf = self.connect.range(0, 1).select(Column(lit_null))
            self.assertEqual(dataType, cdf.schema.fields[0].dataType)

        for value, dataType in [
            ("123", NullType()),
            (123, NullType()),
            (None, ArrayType(IntegerType())),
            (None, MapType(IntegerType(), IntegerType())),
            (None, StructType([StructField("a", StringType())])),
        ]:
            with self.assertRaises(AssertionError):
                LiteralExpression(value=value, dataType=dataType)

    def test_literal_integers(self):
        cdf = self.connect.range(0, 1)
        sdf = self.spark.range(0, 1)

        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF

        cdf1 = cdf.select(
            CF.lit(0),
            CF.lit(1),
            CF.lit(-1),
            CF.lit(JVM_INT_MAX),
            CF.lit(JVM_INT_MIN),
            CF.lit(JVM_INT_MAX + 1),
            CF.lit(JVM_INT_MIN - 1),
            CF.lit(JVM_LONG_MAX),
            CF.lit(JVM_LONG_MIN),
            CF.lit(JVM_LONG_MAX - 1),
            CF.lit(JVM_LONG_MIN + 1),
        )

        sdf1 = sdf.select(
            SF.lit(0),
            SF.lit(1),
            SF.lit(-1),
            SF.lit(JVM_INT_MAX),
            SF.lit(JVM_INT_MIN),
            SF.lit(JVM_INT_MAX + 1),
            SF.lit(JVM_INT_MIN - 1),
            SF.lit(JVM_LONG_MAX),
            SF.lit(JVM_LONG_MIN),
            SF.lit(JVM_LONG_MAX - 1),
            SF.lit(JVM_LONG_MIN + 1),
        )

        self.assertEqual(cdf1.schema, sdf1.schema)
        self.assert_eq(cdf1.toPandas(), sdf1.toPandas())

        with self.assertRaisesRegex(
            ValueError,
            "integer 9223372036854775808 out of bounds",
        ):
            cdf.select(CF.lit(JVM_LONG_MAX + 1)).show()

        with self.assertRaisesRegex(
            ValueError,
            "integer -9223372036854775809 out of bounds",
        ):
            cdf.select(CF.lit(JVM_LONG_MIN - 1)).show()

    def test_cast(self):
        # SPARK-41412: test basic Column.cast
        df = self.connect.read.table(self.tbl_name)
        df2 = self.spark.read.table(self.tbl_name)

        self.assert_eq(
            df.select(df.id.cast("string")).toPandas(), df2.select(df2.id.cast("string")).toPandas()
        )
        self.assert_eq(
            df.select(df.id.astype("string")).toPandas(),
            df2.select(df2.id.astype("string")).toPandas(),
        )

        for x in [
            StringType(),
            ShortType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            ByteType(),
            DecimalType(10, 2),
            BooleanType(),
            DayTimeIntervalType(),
        ]:
            self.assert_eq(
                df.select(df.id.cast(x)).toPandas(), df2.select(df2.id.cast(x)).toPandas()
            )

    def test_isin(self):
        # SPARK-41526: test Column.isin
        query = """
            SELECT * FROM VALUES
            (1, 1, 0, NULL), (2, NULL, 1, 2.0), (3, 3, 4, 3.5)
            AS tab(a, b, c, d)
            """
        # +---+----+---+----+
        # |  a|   b|  c|   d|
        # +---+----+---+----+
        # |  1|   1|  0|null|
        # |  2|null|  1| 2.0|
        # |  3|   3|  4| 3.5|
        # +---+----+---+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # test literals
        self.assert_eq(
            cdf.select(cdf.b.isin(1, 2, 3)).toPandas(),
            sdf.select(sdf.b.isin(1, 2, 3)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.b.isin([1, 2, 3])).toPandas(),
            sdf.select(sdf.b.isin([1, 2, 3])).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.b.isin(set([1, 2, 3]))).toPandas(),
            sdf.select(sdf.b.isin(set([1, 2, 3]))).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.d.isin([1.0, None, 3.5])).toPandas(),
            sdf.select(sdf.d.isin([1.0, None, 3.5])).toPandas(),
        )

        # test columns
        self.assert_eq(
            cdf.select(cdf.a.isin(cdf.b)).toPandas(),
            sdf.select(sdf.a.isin(sdf.b)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.a.isin(cdf.b, cdf.c)).toPandas(),
            sdf.select(sdf.a.isin(sdf.b, sdf.c)).toPandas(),
        )

        # test columns mixed with literals
        self.assert_eq(
            cdf.select(cdf.a.isin(cdf.b, 4, 5, 6)).toPandas(),
            sdf.select(sdf.a.isin(sdf.b, 4, 5, 6)).toPandas(),
        )

    def test_unsupported_functions(self):
        # SPARK-41225: Disable unsupported functions.
        c = self.connect.range(1).id
        for f in (
            "getItem",
            "between",
            "getField",
            "withField",
            "dropFields",
        ):
            with self.assertRaises(NotImplementedError):
                getattr(c, f)()

        with self.assertRaises(NotImplementedError):
            c["a"]

        with self.assertRaises(TypeError):
            for x in c:
                pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_connect_column import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
