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

from pyspark.sql.types import (
    Row,
    StructField,
    StructType,
    MapType,
    NullType,
    DateType,
    TimeType,
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
from pyspark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.connectutils import should_test_connect, ReusedMixedTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils

if should_test_connect:
    import pandas as pd
    from pyspark.sql import functions as SF
    from pyspark.sql.connect import functions as CF
    from pyspark.sql.connect.column import Column
    from pyspark.sql.connect.expressions import DistributedSequenceID, LiteralExpression
    from pyspark.util import (
        JVM_BYTE_MIN,
        JVM_BYTE_MAX,
        JVM_SHORT_MIN,
        JVM_SHORT_MAX,
        JVM_INT_MIN,
        JVM_INT_MAX,
        JVM_LONG_MIN,
        JVM_LONG_MAX,
    )
    from pyspark.errors.exceptions.connect import SparkConnectException


class SparkConnectColumnTests(ReusedMixedTestCase, PandasOnSparkTestUtils):
    def test_column_operator(self):
        # SPARK-41351: Column needs to support !=
        df = self.connect.range(10)
        self.assertEqual(9, len(df.filter(df.id != CF.lit(1)).collect()))

    def test_columns(self):
        # SPARK-41036: test `columns` API for python client.
        query = "SELECT id, CAST(id AS STRING) AS name FROM RANGE(100)"
        df = self.connect.sql(query)
        df2 = self.spark.sql(query)
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
        self.assert_eq(
            df.select(df.name.substr(0, 1).name("col")).toPandas(),
            df2.select(df2.name.substr(0, 1).name("col")).toPandas(),
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

        # check error
        with self.assertRaises(PySparkTypeError) as pe:
            df.name.substr(df.id, 10)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_SAME_TYPE",
            messageParameters={
                "arg_name1": "startPos",
                "arg_name2": "length",
                "arg_type1": "Column",
                "arg_type2": "int",
            },
        )

        with self.assertRaises(PySparkTypeError) as pe:
            df.name.substr(10.5, 10.5)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "Column or int",
                "arg_name": "startPos",
                "arg_type": "float",
            },
        )

    def test_select_column_replaced_by_withcolumn(self):
        from pyspark.errors.exceptions.connect import AnalysisException

        # Selecting the original DataFrame's column after `withColumn` replaces it
        # with a cast: the plan-id-tagged reference must resolve to the overwritten
        # alias rather than the inner column. With non-strict DataFrame column
        # resolution, the analyzer falls back to name-based resolution for the
        # tagged attribute and the query succeeds.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": False}):
            df = self.connect.sql("SELECT 123 AS c")
            df.withColumn("c", CF.col("c").cast("string")).select(df["c"]).collect()

        # Under strict DataFrame column resolution (the default), the tagged
        # reference cannot be resolved: the resolved attribute from the original
        # plan is filtered out at the shadowing `withColumn` Project, and
        # name-based fallback is disabled, so analysis fails with
        # CANNOT_RESOLVE_DATAFRAME_COLUMN.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": True}):
            df = self.connect.sql("SELECT 123 AS c")
            with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
                df.withColumn("c", CF.col("c").cast("string")).select(df["c"]).collect()

    def test_column_with_null(self):
        # SPARK-41751: test isNull, isNotNull, eqNullSafe

        query = """
            SELECT * FROM VALUES
            (1, 1, NULL), (2, NULL, NULL), (3, 3, 1)
            AS tab(a, b, c)
            """

        # +---+----+----+
        # |  a|   b|   c|
        # +---+----+----+
        # |  1|   1|NULL|
        # |  2|NULL|NULL|
        # |  3|   3|   1|
        # +---+----+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # test isNull
        self.assert_eq(
            cdf.select(cdf.a.isNull(), cdf["b"].isNull(), CF.col("c").isNull()).toPandas(),
            sdf.select(sdf.a.isNull(), sdf["b"].isNull(), SF.col("c").isNull()).toPandas(),
        )

        # test isNotNull
        self.assert_eq(
            cdf.select(cdf.a.isNotNull(), cdf["b"].isNotNull(), CF.col("c").isNotNull()).toPandas(),
            sdf.select(sdf.a.isNotNull(), sdf["b"].isNotNull(), SF.col("c").isNotNull()).toPandas(),
        )

        # test eqNullSafe
        self.assert_eq(
            cdf.select(cdf.a.eqNullSafe(cdf.b), cdf["b"].eqNullSafe(CF.col("c"))).toPandas(),
            sdf.select(sdf.a.eqNullSafe(sdf.b), sdf["b"].eqNullSafe(SF.col("c"))).toPandas(),
        )

    def test_invalid_ops(self):
        query = """
            SELECT * FROM VALUES
            (1, 1, 0, NULL), (2, NULL, 1, 2.0), (3, 3, 4, 3.5)
            AS tab(a, b, c, d)
            """
        cdf = self.connect.sql(query)

        with self.assertRaisesRegex(
            ValueError,
            "Cannot apply 'in' operator against a column",
        ):
            1 in cdf.a

        with self.assertRaisesRegex(
            ValueError,
            "Cannot convert column into bool",
        ):
            cdf.a > 2 and cdf.b < 1

        with self.assertRaisesRegex(
            ValueError,
            "Cannot convert column into bool",
        ):
            cdf.a > 2 or cdf.b < 1

        with self.assertRaisesRegex(
            ValueError,
            "Cannot convert column into bool",
        ):
            not (cdf.a > 2)

        with self.assertRaisesRegex(
            TypeError,
            "Column is not iterable",
        ):
            for x in cdf.a:
                pass

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
        # |2022-12-22 18:50:00|      NULL| 2.2|
        # |2022-12-23 15:50:00|2022-12-24| 3.3|
        # |               NULL|2022-12-22|NULL|
        # +-------------------+----------+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

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

    def test_decimal(self):
        # SPARK-41701: test decimal
        query = """
            SELECT * FROM VALUES
            (1, 1, 0, NULL), (2, NULL, 1, 2.0), (3, 3, 4, 3.5)
            AS tab(a, b, c, d)
            """
        # +---+----+---+----+
        # |  a|   b|  c|   d|
        # +---+----+---+----+
        # |  1|   1|  0|NULL|
        # |  2|NULL|  1| 2.0|
        # |  3|   3|  4| 3.5|
        # +---+----+---+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        self.assert_eq(
            cdf.select(cdf.a < decimal.Decimal(3)).toPandas(),
            sdf.select(sdf.a < decimal.Decimal(3)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.a != decimal.Decimal(2)).toPandas(),
            sdf.select(sdf.a != decimal.Decimal(2)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.a == decimal.Decimal(2)).toPandas(),
            sdf.select(sdf.a == decimal.Decimal(2)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.b < decimal.Decimal(2.5)).toPandas(),
            sdf.select(sdf.b < decimal.Decimal(2.5)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.d >= decimal.Decimal(3.0)).toPandas(),
            sdf.select(sdf.d >= decimal.Decimal(3.0)).toPandas(),
        )

    def test_none(self):
        # SPARK-41783: test none

        query = """
            SELECT * FROM VALUES
            (1, 1, NULL), (2, NULL, 1), (NULL, 3, 4)
            AS tab(a, b, c)
            """

        # +----+----+----+
        # |   a|   b|   c|
        # +----+----+----+
        # |   1|   1|NULL|
        # |   2|NULL|   1|
        # |NULL|   3|   4|
        # +----+----+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        self.assert_eq(
            cdf.select(cdf.b > None, CF.col("c") >= None).toPandas(),
            sdf.select(sdf.b > None, SF.col("c") >= None).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.b < None, CF.col("c") <= None).toPandas(),
            sdf.select(sdf.b < None, SF.col("c") <= None).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.b.eqNullSafe(None), CF.col("c").eqNullSafe(None)).toPandas(),
            sdf.select(sdf.b.eqNullSafe(None), SF.col("c").eqNullSafe(None)).toPandas(),
        )

    def test_simple_binary_expressions(self):
        """Test complex expression"""
        query = "SELECT id, CAST(id AS STRING) AS name FROM RANGE(100)"
        cdf = self.connect.sql(query)
        pdf = (
            cdf.select(cdf.id).where(cdf.id % CF.lit(30) == CF.lit(0)).sort(cdf.id.asc()).toPandas()
        )
        self.assertEqual(len(pdf.index), 4)

        res = pd.DataFrame(data={"id": [0, 30, 60, 90]})
        self.assertTrue(pdf.equals(res), f"{pdf.to_string()} != {res.to_string()}")

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
            (datetime.time(1, 0, 0), TimeType()),
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
            TimeType(),
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
            (None, MapType(IntegerType(), IntegerType())),
            (None, StructType([StructField("a", StringType())])),
        ]:
            with self.assertRaises(AssertionError):
                LiteralExpression(value=value, dataType=dataType)

    def test_literal_integers(self):
        cdf = self.connect.range(0, 1)
        sdf = self.spark.range(0, 1)

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

        # negative test for incorrect type
        with self.assertRaises(PySparkValueError) as pe:
            cdf.select(CF.lit(JVM_LONG_MAX + 1)).show()

        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_BETWEEN",
            messageParameters={"arg_name": "value", "min": "-9223372036854775808", "max": "32767"},
        )

        with self.assertRaises(PySparkValueError) as pe:
            cdf.select(CF.lit(JVM_LONG_MIN - 1)).show()

        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_BETWEEN",
            messageParameters={"arg_name": "value", "min": "-9223372036854775808", "max": "32767"},
        )

    def test_cast(self):
        # SPARK-41412: test basic Column.cast
        query = "SELECT id, CAST(id AS STRING) AS name FROM RANGE(100)"
        df = self.connect.sql(query)
        df2 = self.spark.sql(query)

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

        with self.assertRaises(PySparkTypeError) as pe:
            df.id.cast(10)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "DataType or str",
                "arg_name": "dataType",
                "arg_type": "int",
            },
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
        # |  1|   1|  0|NULL|
        # |  2|NULL|  1| 2.0|
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

    def test_between(self):
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
        # |2022-12-22 18:50:00|      NULL| 2.2|
        # |2022-12-23 15:50:00|2022-12-24| 3.3|
        # |               NULL|2022-12-22|NULL|
        # +-------------------+----------+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        self.assert_eq(
            cdf.select(cdf.c.between(0, 2)).toPandas(),
            sdf.select(sdf.c.between(0, 2)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.c.between(1.1, 2.2)).toPandas(),
            sdf.select(sdf.c.between(1.1, 2.2)).toPandas(),
        )

        self.assert_eq(
            cdf.select(cdf.c.between(decimal.Decimal(0), decimal.Decimal(2))).toPandas(),
            sdf.select(sdf.c.between(decimal.Decimal(0), decimal.Decimal(2))).toPandas(),
        )

        self.assert_eq(
            cdf.select(
                cdf.a.between(
                    datetime.datetime(2022, 12, 22, 17, 0, 0),
                    datetime.datetime(2022, 12, 23, 6, 0, 0),
                )
            ).toPandas(),
            sdf.select(
                sdf.a.between(
                    datetime.datetime(2022, 12, 22, 17, 0, 0),
                    datetime.datetime(2022, 12, 23, 6, 0, 0),
                )
            ).toPandas(),
        )
        self.assert_eq(
            cdf.select(
                cdf.b.between(datetime.date(2022, 12, 23), datetime.date(2022, 12, 24))
            ).toPandas(),
            sdf.select(
                sdf.b.between(datetime.date(2022, 12, 23), datetime.date(2022, 12, 24))
            ).toPandas(),
        )

    def test_column_bitwise_ops(self):
        # SPARK-41751: test bitwiseAND, bitwiseOR, bitwiseXOR
        query = """
            SELECT * FROM VALUES
            (1, 1, 0), (2, NULL, 1), (3, 3, 4)
            AS tab(a, b, c)
            """

        # +---+----+---+
        # |  a|   b|  c|
        # +---+----+---+
        # |  1|   1|  0|
        # |  2|NULL|  1|
        # |  3|   3|  4|
        # +---+----+---+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # test bitwiseAND
        self.assert_eq(
            cdf.select(cdf.a.bitwiseAND(cdf.b), cdf["a"].bitwiseAND(CF.col("c"))).toPandas(),
            sdf.select(sdf.a.bitwiseAND(sdf.b), sdf["a"].bitwiseAND(SF.col("c"))).toPandas(),
        )

        # test bitwiseOR
        self.assert_eq(
            cdf.select(cdf.a.bitwiseOR(cdf.b), cdf["a"].bitwiseOR(CF.col("c"))).toPandas(),
            sdf.select(sdf.a.bitwiseOR(sdf.b), sdf["a"].bitwiseOR(SF.col("c"))).toPandas(),
        )

        # test bitwiseXOR
        self.assert_eq(
            cdf.select(cdf.a.bitwiseXOR(cdf.b), cdf["a"].bitwiseXOR(CF.col("c"))).toPandas(),
            sdf.select(sdf.a.bitwiseXOR(sdf.b), sdf["a"].bitwiseXOR(SF.col("c"))).toPandas(),
        )

    def test_column_accessor(self):
        query = """
            SELECT STRUCT(a, b, c) AS x, y, z, c FROM VALUES
            (float(1.0), double(1.0), '2022', MAP('b', '123', 'a', 'kk'), ARRAY(1, 2, 3)),
            (float(2.0), double(2.0), '2018', MAP('a', 'xy'), ARRAY(-1, -2, -3)),
            (float(3.0), double(3.0), NULL, MAP('a', 'ab'), ARRAY(-1, 0, 1))
            AS tab(a, b, c, y, z)
            """

        # +----------------+-------------------+------------+----+
        # |               x|                  y|           z|   c|
        # +----------------+-------------------+------------+----+
        # |{1.0, 1.0, 2022}|{b -> 123, a -> kk}|   [1, 2, 3]|2022|
        # |{2.0, 2.0, 2018}|          {a -> xy}|[-1, -2, -3]|2018|
        # |{3.0, 3.0, null}|          {a -> ab}|  [-1, 0, 1]|NULL|
        # +----------------+-------------------+------------+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # test struct
        self.assert_eq(
            cdf.select(cdf.x.a, cdf.x["b"], cdf["x"].c).toPandas(),
            sdf.select(sdf.x.a, sdf.x["b"], sdf["x"].c).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.col("x").a, cdf.x.b, CF.col("x")["c"]).toPandas(),
            sdf.select(SF.col("x").a, sdf.x.b, SF.col("x")["c"]).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.x.getItem("a"), cdf.x.getItem("b"), cdf["x"].getField("c")).toPandas(),
            sdf.select(sdf.x.getItem("a"), sdf.x.getItem("b"), sdf["x"].getField("c")).toPandas(),
        )

        # test map
        self.assert_eq(
            cdf.select(cdf.y.a, cdf.y["b"], cdf["y"].c).toPandas(),
            sdf.select(sdf.y.a, sdf.y["b"], sdf["y"].c).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.col("y").a, cdf.y.b, CF.col("y")["c"]).toPandas(),
            sdf.select(SF.col("y").a, sdf.y.b, SF.col("y")["c"]).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.y.getItem("a"), cdf.y.getItem("b"), cdf["y"].getField("c")).toPandas(),
            sdf.select(sdf.y.getItem("a"), sdf.y.getItem("b"), sdf["y"].getField("c")).toPandas(),
        )

        # test array
        self.assert_eq(
            cdf.select(cdf.z[0], cdf.z[1], cdf["z"][2]).toPandas(),
            sdf.select(sdf.z[0], sdf.z[1], sdf["z"][2]).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.col("z")[0], CF.get(cdf.z, 10), CF.get(CF.col("z"), -10)).toPandas(),
            sdf.select(SF.col("z")[0], SF.get(sdf.z, 10), SF.get(SF.col("z"), -10)).toPandas(),
        )
        self.assert_eq(
            cdf.select(cdf.z.getItem(0), cdf.z.getItem(1), cdf["z"].getField(2)).toPandas(),
            sdf.select(sdf.z.getItem(0), sdf.z.getItem(1), sdf["z"].getField(2)).toPandas(),
        )

        # test string with slice
        self.assert_eq(
            cdf.select(cdf.c[0:1], cdf["c"][2:10]).toPandas(),
            sdf.select(sdf.c[0:1], sdf["c"][2:10]).toPandas(),
        )

    def test_column_arithmetic_ops(self):
        # SPARK-41761: test arithmetic ops
        query = """
            SELECT * FROM VALUES
            (1, 1, 0, NULL), (2, NULL, 1, 2.0), (3, 3, 4, 3.5)
            AS tab(a, b, c, d)
            """
        # +---+----+---+----+
        # |  a|   b|  c|   d|
        # +---+----+---+----+
        # |  1|   1|  0|NULL|
        # |  2|NULL|  1| 2.0|
        # |  3|   3|  4| 3.5|
        # +---+----+---+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        self.assert_eq(
            cdf.select(
                cdf.a + cdf["b"] - 1, cdf.a - cdf["b"] * cdf["c"] / 2, cdf.d / cdf.b / 3
            ).toPandas(),
            sdf.select(
                sdf.a + sdf["b"] - 1, sdf.a - sdf["b"] * sdf["c"] / 2, sdf.d / sdf.b / 3
            ).toPandas(),
        )

        self.assert_eq(
            cdf.select((-cdf.a)).toPandas(),
            sdf.select((-sdf.a)).toPandas(),
        )

        self.assert_eq(
            cdf.select(3 - cdf.a + cdf["b"] * cdf["c"] - cdf.d / cdf.b).toPandas(),
            sdf.select(3 - sdf.a + sdf["b"] * sdf["c"] - sdf.d / sdf.b).toPandas(),
        )

        self.assert_eq(
            cdf.select(cdf.a % cdf["b"], cdf["a"] % 2, CF.try_mod(CF.lit(12), cdf.c)).toPandas(),
            sdf.select(sdf.a % sdf["b"], sdf["a"] % 2, SF.try_mod(SF.lit(12), sdf.c)).toPandas(),
        )

        self.assert_eq(
            cdf.select(cdf.a ** cdf["b"], cdf.d**2, 2**cdf.c).toPandas(),
            sdf.select(sdf.a ** sdf["b"], sdf.d**2, 2**sdf.c).toPandas(),
        )

    def test_column_field_ops(self):
        # SPARK-41767: test withField, dropFields
        query = """
            SELECT STRUCT(a, b, c, d) AS x, e FROM VALUES
            (float(1.0), double(1.0), '2022', 1, 0),
            (float(2.0), double(2.0), '2018', NULL, 2),
            (float(3.0), double(3.0), NULL, 3, NULL)
            AS tab(a, b, c, d, e)
            """

        # +----------------------+----+
        # |                     x|   e|
        # +----------------------+----+
        # |   {1.0, 1.0, 2022, 1}|   0|
        # |{2.0, 2.0, 2018, null}|   2|
        # |   {3.0, 3.0, null, 3}|NULL|
        # +----------------------+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # add field
        self.compare_by_show(
            cdf.select(cdf.x.withField("z", cdf.e)),
            sdf.select(sdf.x.withField("z", sdf.e)),
            truncate=100,
        )
        self.compare_by_show(
            cdf.select(cdf.x.withField("z", CF.col("e"))),
            sdf.select(sdf.x.withField("z", SF.col("e"))),
            truncate=100,
        )
        self.compare_by_show(
            cdf.select(cdf.x.withField("z", CF.lit("xyz"))),
            sdf.select(sdf.x.withField("z", SF.lit("xyz"))),
            truncate=100,
        )

        # replace field
        self.compare_by_show(
            cdf.select(cdf.x.withField("a", cdf.e)),
            sdf.select(sdf.x.withField("a", sdf.e)),
            truncate=100,
        )
        self.compare_by_show(
            cdf.select(cdf.x.withField("a", CF.col("e"))),
            sdf.select(sdf.x.withField("a", SF.col("e"))),
            truncate=100,
        )
        self.compare_by_show(
            cdf.select(cdf.x.withField("a", CF.lit("xyz"))),
            sdf.select(sdf.x.withField("a", SF.lit("xyz"))),
            truncate=100,
        )

        # drop field
        self.compare_by_show(
            cdf.select(cdf.x.dropFields("a")),
            sdf.select(sdf.x.dropFields("a")),
            truncate=100,
        )
        self.compare_by_show(
            cdf.select(cdf.x.dropFields("z")),
            sdf.select(sdf.x.dropFields("z")),
            truncate=100,
        )
        self.compare_by_show(
            cdf.select(cdf.x.dropFields("a", "b", "z")),
            sdf.select(sdf.x.dropFields("a", "b", "z")),
            truncate=100,
        )

        # check error
        # invalid column: not a struct column
        with self.assertRaises(SparkConnectException):
            cdf.select(cdf.e.withField("a", CF.lit(1))).show()

        # invalid column: not a struct column
        with self.assertRaises(SparkConnectException):
            cdf.select(cdf.e.dropFields("a")).show()

        # cannot drop all fields in struct
        with self.assertRaises(SparkConnectException):
            cdf.select(cdf.x.dropFields("a", "b", "c", "d")).show()

        with self.assertRaises(PySparkTypeError) as pe:
            cdf.select(cdf.x.withField(CF.col("a"), cdf.e)).show()

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "str",
                "arg_name": "fieldName",
                "arg_type": "Column",
            },
        )

        with self.assertRaises(PySparkTypeError) as pe:
            cdf.select(cdf.x.withField("a", 2)).show()

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={"expected_type": "Column", "arg_name": "col", "arg_type": "int"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            cdf.select(cdf.x.dropFields("a", 1, 2)).show()

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={"expected_type": "str", "arg_name": "fieldName", "arg_type": "int"},
        )

        with self.assertRaises(PySparkValueError) as pe:
            cdf.select(cdf.x.dropFields()).show()

        self.check_error(
            exception=pe.exception,
            errorClass="CANNOT_BE_EMPTY",
            messageParameters={"item": "dropFields"},
        )

    def test_column_string_ops(self):
        # SPARK-41764: test string ops
        query = """
            SELECT * FROM VALUES
            (1, 'abcdef', 'ghij', 'hello world', 'a'),
            (2, 'abcd', 'efghij', 'how are you', 'd')
            AS tab(a, b, c, d, e)
            """

        # +---+------+------+-----------+---+
        # |  a|     b|     c|          d|  e|
        # +---+------+------+-----------+---+
        # |  1|abcdef|  ghij|hello world|  a|
        # |  2|  abcd|efghij|how are you|  d|
        # +---+------+------+-----------+---+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        self.assert_eq(
            cdf.select(
                cdf.b.startswith("a"), cdf["c"].startswith("g"), cdf["b"].startswith(cdf.e)
            ).toPandas(),
            sdf.select(
                sdf.b.startswith("a"), sdf["c"].startswith("g"), sdf["b"].startswith(sdf.e)
            ).toPandas(),
        )

        self.assert_eq(
            cdf.select(
                cdf.b.endswith("a"), cdf["c"].endswith("j"), cdf["b"].endswith(cdf.e)
            ).toPandas(),
            sdf.select(
                sdf.b.endswith("a"), sdf["c"].endswith("j"), sdf["b"].endswith(sdf.e)
            ).toPandas(),
        )

        self.assert_eq(
            cdf.select(
                cdf.b.contains("a"), cdf["c"].contains("j"), cdf["b"].contains(cdf.e)
            ).toPandas(),
            sdf.select(
                sdf.b.contains("a"), sdf["c"].contains("j"), sdf["b"].contains(sdf.e)
            ).toPandas(),
        )

    def test_with_field_column_name(self):
        data = [Row(a=Row(b=1, c=2))]

        cdf = self.connect.createDataFrame(data)
        cdf1 = cdf.withColumn("a", cdf["a"].withField("b", CF.lit(3))).select("a.b")

        sdf = self.spark.createDataFrame(data)
        sdf1 = sdf.withColumn("a", sdf["a"].withField("b", SF.lit(3))).select("a.b")

        assertDataFrameEqual(cdf1, sdf1)

    def test_distributed_sequence_id(self):
        cdf = self.connect.range(10)
        expected = self.connect.range(0, 10).selectExpr("id as index", "id")
        self.assertEqual(
            cdf.select(Column(DistributedSequenceID()).alias("index"), "*").collect(),
            expected.collect(),
        )

    def test_lambda_str_representation(self):
        from pyspark.sql.connect.expressions import UnresolvedNamedLambdaVariable

        # forcely clear the internal increasing id,
        # otherwise the string representation varies with this id
        UnresolvedNamedLambdaVariable._nextVarNameId = 0

        c = CF.array_sort(
            "data",
            lambda x, y: CF.when(x.isNull() | y.isNull(), CF.lit(0)).otherwise(
                CF.length(y) - CF.length(x)
            ),
        )

        self.assertEqual(
            str(c),
            (
                """Column<'array_sort(data, LambdaFunction(CASE WHEN or(isNull(x_0), """
                """isNull(y_1)) THEN 0 ELSE -(length(y_1), length(x_0)) END, x_0, y_1))'>"""
            ),
        )

    def test_cast_default_column_name(self):
        cdf = self.connect.range(1).select(
            CF.lit(b"123").cast("STRING"),
            CF.lit(123).cast("STRING"),
            CF.lit(123).cast("LONG"),
            CF.lit(123).cast("DOUBLE"),
        )
        sdf = self.spark.range(1).select(
            SF.lit(b"123").cast("STRING"),
            SF.lit(123).cast("STRING"),
            SF.lit(123).cast("LONG"),
            SF.lit(123).cast("DOUBLE"),
        )
        self.assertEqual(cdf.columns, sdf.columns)

    def test_transform(self):
        # Test with built-in functions
        cdf = self.connect.createDataFrame([("  hello  ",), ("  world  ",)], ["text"])
        sdf = self.spark.createDataFrame([("  hello  ",), ("  world  ",)], ["text"])

        self.assert_eq(
            cdf.select(cdf.text.transform(CF.trim).transform(CF.upper)).toPandas(),
            sdf.select(sdf.text.transform(SF.trim).transform(SF.upper)).toPandas(),
        )

        # Test with lambda functions
        cdf = self.connect.createDataFrame([(10,), (20,), (30,)], ["value"])
        sdf = self.spark.createDataFrame([(10,), (20,), (30,)], ["value"])

        self.assert_eq(
            cdf.select(
                cdf.value.transform(lambda c: c + 5)
                .transform(lambda c: c * 2)
                .transform(lambda c: c - 10)
            ).toPandas(),
            sdf.select(
                sdf.value.transform(lambda c: c + 5)
                .transform(lambda c: c * 2)
                .transform(lambda c: c - 10)
            ).toPandas(),
        )


class SparkConnectColumnResolutionTests(ReusedMixedTestCase):
    """Connect-only tests pinning known Connect/Classic divergences in DataFrame
    column resolution.

    For each pattern, the test runs the equivalent program against:

      * Spark Classic (``self.spark``) - baseline that anchors the Classic
        behavior.
      * Spark Connect strict mode (default,
        ``spark.sql.analyzer.strictDataFrameColumnResolution=true``) - plan-id-
        based resolution only. Tagged ``df["c"]`` references whose ancestor's
        attribute is gone fail with ``CANNOT_RESOLVE_DATAFRAME_COLUMN``.
      * Spark Connect lenient mode
        (``spark.sql.analyzer.strictDataFrameColumnResolution=false``) - if
        plan-id-based resolution fails the analyzer also tries name-based
        resolution against the current child output.
    """

    def test_resolve_after_chained_withcolumn_shadow(self):
        # Two consecutive withColumn calls each shadow `c` with a new
        # attribute carrying the same name; the original `c` is no longer in
        # the projection.
        # Classic: fails.
        sdf = self.spark.sql("SELECT 1 AS c")
        with self.assertRaises(AnalysisException):
            sdf.withColumn("c", SF.col("c").cast("string")).withColumn(
                "c", SF.col("c").cast("int")
            ).select(sdf["c"]).collect()

        # Connect strict: same root cause, different error class.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": True}):
            cdf = self.connect.sql("SELECT 1 AS c")
            with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
                cdf.withColumn("c", CF.col("c").cast("string")).withColumn(
                    "c", CF.col("c").cast("int")
                ).select(cdf["c"]).collect()

        # Connect lenient: succeeds via name-based fallback.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": False}):
            cdf = self.connect.sql("SELECT 1 AS c")
            cdf.withColumn("c", CF.col("c").cast("string")).withColumn(
                "c", CF.col("c").cast("int")
            ).select(cdf["c"]).collect()

    def test_resolve_after_select_alias_shadow(self):
        # Same shadowing shape as withColumn but expressed through a select
        # with alias.
        # Classic: fails.
        sdf = self.spark.sql("SELECT 1 AS c")
        with self.assertRaises(AnalysisException):
            sdf.select(sdf["c"].cast("string").alias("c")).select(sdf["c"]).collect()

        # Connect strict: fails.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": True}):
            cdf = self.connect.sql("SELECT 1 AS c")
            with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
                cdf.select(cdf["c"].cast("string").alias("c")).select(cdf["c"]).collect()

        # Connect lenient: succeeds via name-based fallback.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": False}):
            cdf = self.connect.sql("SELECT 1 AS c")
            cdf.select(cdf["c"].cast("string").alias("c")).select(cdf["c"]).collect()

    def test_resolve_after_withcolumnrenamed(self):
        # withColumnRenamed drops the original `c` attribute and projects it
        # as `c2`. The tagged cdf["c"] cannot resolve under any mode because
        # neither the original attribute nor a column named `c` is in the
        # current child output.
        # Classic: fails.
        sdf = self.spark.sql("SELECT 1 AS c")
        with self.assertRaises(AnalysisException):
            sdf.withColumnRenamed("c", "c2").select(sdf["c"]).collect()

        # Connect: fails in both modes.
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf = self.connect.sql("SELECT 1 AS c")
                with self.assertRaises(AnalysisException):
                    cdf.withColumnRenamed("c", "c2").select(cdf["c"]).collect()

    def test_resolve_after_drop(self):
        # drop("c") removes the column entirely. Tagged cdf["c"] cannot resolve
        # under any mode.
        # Classic: fails.
        sdf = self.spark.sql("SELECT 1 AS c, 2 AS d")
        with self.assertRaises(AnalysisException):
            sdf.drop("c").select(sdf["c"]).collect()

        # Connect: fails in both modes.
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf = self.connect.sql("SELECT 1 AS c, 2 AS d")
                with self.assertRaises(AnalysisException):
                    cdf.drop("c").select(cdf["c"]).collect()

    def test_resolve_through_filter(self):
        # filter is a pass-through operator: the child Project's attributes
        # flow through unchanged, so the tagged reference resolves in both
        # worlds.
        expected = [1, 2]

        # Classic: succeeds.
        sdf = self.spark.sql("SELECT 1 AS c UNION ALL SELECT 2 AS c")
        srows = sdf.filter(sdf["c"] > 0).select(sdf["c"]).collect()
        self.assertEqual(sorted(r.c for r in srows), expected)

        # Connect: succeeds in both modes.
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf = self.connect.sql("SELECT 1 AS c UNION ALL SELECT 2 AS c")
                rows = cdf.filter(cdf["c"] > 0).select(cdf["c"]).collect()
                self.assertEqual(sorted(r.c for r in rows), expected)

    def test_resolve_through_sort(self):
        # sort is also a pass-through operator.
        expected = [1, 2]

        # Classic: succeeds.
        sdf = self.spark.sql("SELECT 2 AS c UNION ALL SELECT 1 AS c")
        srows = sdf.sort(sdf["c"]).select(sdf["c"]).collect()
        self.assertEqual([r.c for r in srows], expected)

        # Connect: succeeds in both modes.
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf = self.connect.sql("SELECT 2 AS c UNION ALL SELECT 1 AS c")
                rows = cdf.sort(cdf["c"]).select(cdf["c"]).collect()
                self.assertEqual([r.c for r in rows], expected)

    def test_resolve_through_distinct(self):
        # distinct preserves attribute identity from the perspective of
        # column resolution.
        expected = [1]

        # Classic: succeeds.
        sdf = self.spark.sql("SELECT 1 AS c UNION ALL SELECT 1 AS c")
        srows = sdf.distinct().select(sdf["c"]).collect()
        self.assertEqual([r.c for r in srows], expected)

        # Connect: succeeds in both modes.
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf = self.connect.sql("SELECT 1 AS c UNION ALL SELECT 1 AS c")
                rows = cdf.distinct().select(cdf["c"]).collect()
                self.assertEqual([r.c for r in rows], expected)

    def test_resolve_after_groupby_count(self):
        # groupBy("c").count() preserves the grouping key's attribute id in
        # both Classic and Connect, so the tagged reference resolves in all
        # modes.
        query = "SELECT 1 AS c UNION ALL SELECT 1 AS c UNION ALL SELECT 2 AS c"
        expected = [1, 2]

        # Classic: succeeds.
        sdf = self.spark.sql(query)
        srows = sdf.groupBy("c").count().select(sdf["c"]).collect()
        self.assertEqual(sorted(r.c for r in srows), expected)

        # Connect: succeeds in both modes.
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf = self.connect.sql(query)
                rows = cdf.groupBy("c").count().select(cdf["c"]).collect()
                self.assertEqual(sorted(r.c for r in rows), expected)

    def test_resolve_after_agg_alias_shadow(self):
        # An aggregate output named `c` via alias() collides by name with
        # the source `c`. The tagged cdf["c"] still references the source
        # attribute that has been aggregated away.
        # Classic: fails.
        sdf = self.spark.sql("SELECT 1 AS x")
        with self.assertRaises(AnalysisException):
            sdf.groupBy().agg(SF.sum("x").alias("c")).select(sdf["c"]).collect()

        # Connect strict: fails.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": True}):
            cdf = self.connect.sql("SELECT 1 AS x")
            with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
                cdf.groupBy().agg(CF.sum("x").alias("c")).select(cdf["c"]).collect()

        # Connect lenient: succeeds via name-based fallback.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": False}):
            cdf = self.connect.sql("SELECT 1 AS x")
            cdf.groupBy().agg(CF.sum("x").alias("c")).select(cdf["c"]).collect()

    def test_resolve_after_pivot(self):
        # pivot preserves the grouping key's attribute id in both Classic
        # and Connect, so the tagged reference resolves in all modes.
        query = "SELECT 1 AS c, 'a' AS k, 10 AS v UNION ALL SELECT 2 AS c, 'b' AS k, 20 AS v"
        expected = [1, 2]

        # Classic: succeeds.
        sdf = self.spark.sql(query)
        srows = sdf.groupBy("c").pivot("k").sum("v").select(sdf["c"]).collect()
        self.assertEqual(sorted(r.c for r in srows), expected)

        # Connect: succeeds in both modes.
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf = self.connect.sql(query)
                rows = cdf.groupBy("c").pivot("k").sum("v").select(cdf["c"]).collect()
                self.assertEqual(sorted(r.c for r in rows), expected)

    def test_resolve_after_union(self):
        # Union emits new attribute ids. Classic still resolves the tagged
        # left-side reference by attribute id propagation, but Connect fails
        # in both modes: plan-id-based resolution does not find the tagged
        # ancestor in the union output, and name-based fallback is not
        # triggered for set-op outputs.
        # Classic: succeeds.
        sdf1 = self.spark.sql("SELECT 1 AS c")
        sdf2 = self.spark.sql("SELECT 2 AS c")
        srows = sdf1.union(sdf2).select(sdf1["c"]).collect()
        self.assertEqual(sorted(r.c for r in srows), [1, 2])

        # Connect: fails in both modes.
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf1 = self.connect.sql("SELECT 1 AS c")
                cdf2 = self.connect.sql("SELECT 2 AS c")
                with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
                    cdf1.union(cdf2).select(cdf1["c"]).collect()

    def test_resolve_after_intersect(self):
        # intersect, like union, emits new attribute ids. Classic resolves
        # the tagged reference by attribute id propagation; Connect also
        # resolves it successfully (the intersect output retains the
        # propagated id), in both modes.
        expected = [2]

        # Classic: succeeds.
        sdf1 = self.spark.sql("SELECT 1 AS c UNION ALL SELECT 2 AS c")
        sdf2 = self.spark.sql("SELECT 2 AS c UNION ALL SELECT 3 AS c")
        srows = sdf1.intersect(sdf2).select(sdf1["c"]).collect()
        self.assertEqual([r.c for r in srows], expected)

        # Connect: succeeds in both modes.
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf1 = self.connect.sql("SELECT 1 AS c UNION ALL SELECT 2 AS c")
                cdf2 = self.connect.sql("SELECT 2 AS c UNION ALL SELECT 3 AS c")
                rows = cdf1.intersect(cdf2).select(cdf1["c"]).collect()
                self.assertEqual([r.c for r in rows], expected)

    def test_resolve_self_join_alias(self):
        # In a self-join, both sides originate from the same plan-id-tagged
        # ancestor. The tagged cdf["c"] is ambiguous because two output
        # attributes match by name.
        # Classic: fails (ambiguous reference).
        sdf = self.spark.sql("SELECT 1 AS c UNION ALL SELECT 2 AS c")
        a, b = sdf.alias("a"), sdf.alias("b")
        with self.assertRaises(AnalysisException):
            a.join(b, a["c"] == b["c"]).select(sdf["c"]).collect()

        # Connect: fails in both modes.
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf = self.connect.sql("SELECT 1 AS c UNION ALL SELECT 2 AS c")
                a, b = cdf.alias("a"), cdf.alias("b")
                with self.assertRaises(AnalysisException):
                    a.join(b, a["c"] == b["c"]).select(cdf["c"]).collect()

    def test_resolve_after_subquery_view(self):
        # Persisting the original DataFrame as a temp view and reading it
        # back via spark.table() produces a new plan. Classic resolves the
        # tagged reference; Connect also resolves it in both modes.
        import uuid

        expected = [1]

        # Classic: succeeds.
        view = f"v_{uuid.uuid4().hex}"
        sdf = self.spark.sql("SELECT 1 AS c")
        sdf.createOrReplaceTempView(view)
        try:
            srows = self.spark.table(view).select(sdf["c"]).collect()
            self.assertEqual([r.c for r in srows], expected)
        finally:
            self.spark.sql(f"DROP VIEW IF EXISTS {view}")

        # Connect: succeeds in both modes.
        for strict in (True, False):
            view = f"v_{uuid.uuid4().hex}"
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                cdf = self.connect.sql("SELECT 1 AS c")
                cdf.createOrReplaceTempView(view)
                try:
                    rows = self.connect.table(view).select(cdf["c"]).collect()
                    self.assertEqual([r.c for r in rows], expected)
                finally:
                    self.connect.sql(f"DROP VIEW IF EXISTS {view}")

    # --- Mixed-surface layered DataFrame programs ---------------------------
    #
    # These tests chain multiple DataFrame transformations - semi-joins
    # (for SQL EXISTS/IN), window functions, cube aggregations, UDFs and
    # struct field access - into 4-5 layer pipelines. Each program builds
    # the layered base entirely through the DataFrame API, then layers a
    # shadowing operation on top with a tagged ``cdf["c"]`` reference at the
    # outermost select. The goal is to catch regressions in plan-id
    # propagation across Connect's analyzer rules that single-operator
    # tests miss when rules interact.

    def test_layered_semijoin_groupby_window_shadow(self):
        # 4-layer DataFrame pipeline: filter -> semi-join -> groupBy/agg
        # -> windows. Then a tagged ``layered["category"]`` reference after
        # a groupBy shadow.
        from pyspark.sql.connect.window import Window as CWindow
        from pyspark.sql.window import Window as SWindow

        events_data = [
            (1, 1, "Books", 100.0, 2, True),
            (2, 1, "Books", 50.0, 3, True),
            (3, 2, "Electronics", 200.0, 1, True),
            (4, 2, "Electronics", 300.0, 2, True),
            (5, 3, "Home", 80.0, 4, True),
            (6, 4, "Books", 60.0, 1, False),
        ]
        users_data = [(1, 25), (2, 30), (3, 22), (4, 18)]
        events_cols = ["id", "user_id", "category", "amount", "quantity", "is_active"]
        users_cols = ["id", "age"]

        def build_layered(spark, F, Window):
            events = spark.createDataFrame(events_data, events_cols)
            users = spark.createDataFrame(users_data, users_cols)

            # Layer 1: filter + semi-join (DataFrame-API equivalent of
            # WHERE is_active AND EXISTS (user with age > 20)).
            active = events.where(events["is_active"]).join(
                users.where(users["age"] > 20),
                events["user_id"] == users["id"],
                "left_semi",
            )
            # Layer 2: groupBy + agg, then post-agg filter (HAVING equivalent).
            totals = (
                active.groupBy("category")
                .agg(
                    F.sum(active["amount"] * active["quantity"] * F.lit(0.1)).alias("total_amt"),
                    F.sum(active["amount"]).alias("amount_sum"),
                )
                .where(F.col("amount_sum") > 50)
                .select("category", "total_amt")
            )
            # Layer 3: window functions on top of the aggregate.
            running = Window.orderBy("total_amt").rowsBetween(-1, 1)
            ranking = Window.orderBy(F.col("total_amt").desc())
            windowed = totals.select(
                "category",
                "total_amt",
                F.avg(F.col("total_amt")).over(running).alias("running_avg"),
                F.rank().over(ranking).alias("rank_num"),
            )
            # Layer 4: outer filter.
            return windowed.where(F.col("rank_num") <= 5)

        expected_categories = ["Books", "Electronics", "Home"]

        # Classic: groupBy propagates the "category" attribute id through
        # the aggregate, so the tagged reference still resolves.
        slayered = build_layered(self.spark, SF, SWindow)
        srows = slayered.groupBy("category").count().select(slayered["category"]).collect()
        self.assertEqual(sorted(r.category for r in srows), expected_categories)

        # Connect: succeeds in both modes (groupBy attribute id propagates
        # through Connect's aggregate as well).
        for strict in (True, False):
            with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": strict}):
                clayered = build_layered(self.connect, CF, CWindow)
                rows = clayered.groupBy("category").count().select(clayered["category"]).collect()
                self.assertEqual(sorted(r.category for r in rows), expected_categories)

    def test_layered_struct_semijoin_cube_ntile_shadow(self):
        # 5-layer DataFrame pipeline: filter -> semi-join -> struct field
        # access -> cube aggregation -> window NTILE. Then withColumn
        # shadow + tagged select.
        from pyspark.sql.connect.window import Window as CWindow
        from pyspark.sql.window import Window as SWindow

        events_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("category", StringType()),
                StructField("status", StringType()),
                StructField("amount", IntegerType()),
                StructField("quantity", IntegerType()),
                StructField(
                    "detail",
                    StructType(
                        [
                            StructField("name", StringType()),
                            StructField("nested", StructType([StructField("x", IntegerType())])),
                        ]
                    ),
                ),
            ]
        )
        events_data = [
            (1, "Books", "A", 100, 5, ("alpha", (1,))),
            (2, "Electronics", "B", 200, 3, ("beta", (2,))),
            (3, "Books", "A", 50, 7, ("alpha", (1,))),
            (4, "Electronics", "B", 300, 4, ("beta", (2,))),
            (5, "Home", "C", 80, 2, ("gamma", (3,))),
        ]
        categories_data = [("Books", 1), ("Electronics", 2), ("Home", 3), ("Toys", 5)]
        categories_cols = ["name", "priority"]

        def build_layered(spark, F, Window):
            events = spark.createDataFrame(events_data, events_schema)
            categories = spark.createDataFrame(categories_data, categories_cols)

            # Layer 1: filter + semi-join (DataFrame-API equivalent of
            # WHERE quantity > 1 AND category IN (SELECT ...)).
            filtered = events.where(events["quantity"] > 1).join(
                categories.where(categories["priority"] <= 3),
                events["category"] == categories["name"],
                "left_semi",
            )
            # Layer 2: project with struct field access.
            base = filtered.select(
                filtered["id"],
                filtered["category"],
                filtered["status"],
                filtered["amount"],
                filtered["detail"]["name"].alias("detail_name"),
                filtered["detail"]["nested"]["x"].alias("nx"),
            )
            # Layer 3: cube aggregation (mixed grouping levels - similar
            # surface area to SQL GROUPING SETS without an exact equivalent
            # in the DataFrame API).
            grouped = (
                base.cube("category", "status", "detail_name")
                .agg(F.sum(F.col("amount")).alias("total"), F.count(F.lit(1)).alias("cnt"))
                .where(F.col("category").isNotNull() & F.col("status").isNotNull())
            )
            # Layer 4: NTILE window.
            tiled = grouped.withColumn(
                "tile", F.ntile(2).over(Window.orderBy(F.col("total").desc()))
            )
            # Layer 5: outer filter.
            return tiled.where(F.col("tile") <= 2)

        # Classic: withColumn shadow of "category" drops the original
        # attribute - the tagged reference cannot resolve.
        slayered = build_layered(self.spark, SF, SWindow)
        with self.assertRaises(AnalysisException):
            slayered.withColumn("category", SF.col("category").cast("string")).select(
                slayered["category"], "total"
            ).collect()

        # Connect strict: fails.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": True}):
            clayered = build_layered(self.connect, CF, CWindow)
            with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
                clayered.withColumn("category", CF.col("category").cast("string")).select(
                    clayered["category"], "total"
                ).collect()

        # Connect lenient: succeeds via name-based fallback.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": False}):
            clayered = build_layered(self.connect, CF, CWindow)
            rows = (
                clayered.withColumn("category", CF.col("category").cast("string"))
                .select(clayered["category"], "total")
                .collect()
            )
            self.assertGreater(len(rows), 0)

    def test_layered_window_window_udf_shadow(self):
        # 4-layer DataFrame pipeline: filter -> running-total window ->
        # per-partition max window -> UDF wrap. Then withColumn shadow +
        # tagged select.
        from pyspark.sql.connect.window import Window as CWindow
        from pyspark.sql.window import Window as SWindow

        data = [
            (1, "A", 100),
            (2, "A", 200),
            (3, "B", 150),
            (4, "B", 250),
            (5, "C", 50),
        ]
        cols = ["id", "category", "amount"]

        def build_layered(spark, F, Window):
            df = spark.createDataFrame(data, cols)
            # Layer 1: filter (replaces WHERE EXISTS amount > 0).
            filtered = df.where(df["amount"] > 0)
            # Layer 2: running total window.
            run_w = Window.partitionBy("category").orderBy("id")
            with_run = filtered.withColumn("run_amt", F.sum(F.col("amount")).over(run_w))
            # Layer 3: per-category max window (replaces correlated subquery
            # for cat_max).
            cat_w = Window.partitionBy("category")
            return with_run.withColumn("cat_max", F.max(F.col("amount")).over(cat_w))

        # Classic: withColumn shadow of "category" breaks the tagged reference.
        slayered = build_layered(self.spark, SF, SWindow)
        sdouble = SF.udf(lambda x: x * 2 if x is not None else None, IntegerType())
        with self.assertRaises(AnalysisException):
            slayered.withColumn("amount", sdouble(SF.col("amount"))).withColumn(
                "category", SF.col("category").cast("string")
            ).select(slayered["category"], "amount", "run_amt", "cat_max").collect()

        # Connect strict: fails.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": True}):
            clayered = build_layered(self.connect, CF, CWindow)
            cdouble = CF.udf(lambda x: x * 2 if x is not None else None, IntegerType())
            with self.assertRaisesRegex(AnalysisException, "CANNOT_RESOLVE_DATAFRAME_COLUMN"):
                clayered.withColumn("amount", cdouble(CF.col("amount"))).withColumn(
                    "category", CF.col("category").cast("string")
                ).select(clayered["category"], "amount", "run_amt", "cat_max").collect()

        # Connect lenient: succeeds via name-based fallback.
        with self.connect_conf({"spark.sql.analyzer.strictDataFrameColumnResolution": False}):
            clayered = build_layered(self.connect, CF, CWindow)
            cdouble = CF.udf(lambda x: x * 2 if x is not None else None, IntegerType())
            rows = (
                clayered.withColumn("amount", cdouble(CF.col("amount")))
                .withColumn("category", CF.col("category").cast("string"))
                .select(clayered["category"], "amount", "run_amt", "cat_max")
                .collect()
            )
            self.assertEqual(len(rows), 5)
            self.assertEqual(sorted({r.category for r in rows}), ["A", "B", "C"])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
