/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import java.sql.{Date, Timestamp}
import java.time.{Duration, Period}
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCoercionSuite
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectList, CollectSet}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.{DAY, HOUR, MINUTE, SECOND}
import org.apache.spark.sql.types.YearMonthIntervalType.{MONTH, YEAR}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test suite for data type casting expression [[Cast]] with ANSI mode disabled.
 */
class CastWithAnsiOffSuite extends CastSuiteBase {

  override def evalMode: EvalMode.Value = EvalMode.LEGACY

  test("null cast #2") {
    import DataTypeTestUtils._

    checkNullCast(DateType, BooleanType)
    checkNullCast(TimestampType, BooleanType)
    checkNullCast(BooleanType, TimestampType)
    numericTypes.foreach(dt => checkNullCast(dt, TimestampType))
    numericTypes.foreach(dt => checkNullCast(TimestampType, dt))
    numericTypes.foreach(dt => checkNullCast(DateType, dt))
  }

  test("cast from long #2") {
    checkEvaluation(cast(123L, DecimalType(3, 1)), null)
    checkEvaluation(cast(123L, DecimalType(2, 0)), null)
  }

  test("cast from int #2") {
    checkEvaluation(cast(cast(1000, TimestampType), LongType), 1000.toLong)
    checkEvaluation(cast(cast(-1200, TimestampType), LongType), -1200.toLong)

    checkEvaluation(cast(123, DecimalType(3, 1)), null)
    checkEvaluation(cast(123, DecimalType(2, 0)), null)
  }

  test("cast string to date #2") {
    checkEvaluation(cast(Literal("2015-03-18X"), DateType), null)
    checkEvaluation(cast(Literal("2015/03/18"), DateType), null)
    checkEvaluation(cast(Literal("2015.03.18"), DateType), null)
    checkEvaluation(cast(Literal("20150318"), DateType), null)
    checkEvaluation(cast(Literal("2015-031-8"), DateType), null)
  }

  test("casting to fixed-precision decimals") {
    assert(cast(123, DecimalType.USER_DEFAULT).nullable === false)
    assert(cast(10.03f, DecimalType.SYSTEM_DEFAULT).nullable)
    assert(cast(10.03, DecimalType.SYSTEM_DEFAULT).nullable)
    assert(cast(Decimal(10.03), DecimalType.SYSTEM_DEFAULT).nullable === false)

    assert(cast(123, DecimalType(2, 1)).nullable)
    assert(cast(10.03f, DecimalType(2, 1)).nullable)
    assert(cast(10.03, DecimalType(2, 1)).nullable)
    assert(cast(Decimal(10.03), DecimalType(2, 1)).nullable)

    assert(cast(123, DecimalType.IntDecimal).nullable === false)
    assert(cast(10.03f, DecimalType.FloatDecimal).nullable)
    assert(cast(10.03, DecimalType.DoubleDecimal).nullable)
    assert(cast(Decimal(10.03), DecimalType(4, 2)).nullable === false)
    assert(cast(Decimal(10.03), DecimalType(5, 3)).nullable === false)

    assert(cast(Decimal(10.03), DecimalType(3, 1)).nullable)
    assert(cast(Decimal(10.03), DecimalType(4, 1)).nullable === false)
    assert(cast(Decimal(9.95), DecimalType(2, 1)).nullable)
    assert(cast(Decimal(9.95), DecimalType(3, 1)).nullable === false)

    assert(cast(true, DecimalType.SYSTEM_DEFAULT).nullable === false)
    assert(cast(true, DecimalType(1, 1)).nullable)

    checkEvaluation(cast(10.03, DecimalType.SYSTEM_DEFAULT), Decimal(10.03))
    checkEvaluation(cast(10.03, DecimalType(4, 2)), Decimal(10.03))
    checkEvaluation(cast(10.03, DecimalType(3, 1)), Decimal(10.0))
    checkEvaluation(cast(10.03, DecimalType(2, 0)), Decimal(10))
    checkEvaluation(cast(10.03, DecimalType(1, 0)), null)
    checkEvaluation(cast(10.03, DecimalType(2, 1)), null)
    checkEvaluation(cast(10.03, DecimalType(3, 2)), null)
    checkEvaluation(cast(Decimal(10.03), DecimalType(3, 1)), Decimal(10.0))
    checkEvaluation(cast(Decimal(10.03), DecimalType(3, 2)), null)

    checkEvaluation(cast(10.05, DecimalType.SYSTEM_DEFAULT), Decimal(10.05))
    checkEvaluation(cast(10.05, DecimalType(4, 2)), Decimal(10.05))
    checkEvaluation(cast(10.05, DecimalType(3, 1)), Decimal(10.1))
    checkEvaluation(cast(10.05, DecimalType(2, 0)), Decimal(10))
    checkEvaluation(cast(10.05, DecimalType(1, 0)), null)
    checkEvaluation(cast(10.05, DecimalType(2, 1)), null)
    checkEvaluation(cast(10.05, DecimalType(3, 2)), null)
    checkEvaluation(cast(Decimal(10.05), DecimalType(3, 1)), Decimal(10.1))
    checkEvaluation(cast(Decimal(10.05), DecimalType(3, 2)), null)

    checkEvaluation(cast(9.95, DecimalType(3, 2)), Decimal(9.95))
    checkEvaluation(cast(9.95, DecimalType(3, 1)), Decimal(10.0))
    checkEvaluation(cast(9.95, DecimalType(2, 0)), Decimal(10))
    checkEvaluation(cast(9.95, DecimalType(2, 1)), null)
    checkEvaluation(cast(9.95, DecimalType(1, 0)), null)
    checkEvaluation(cast(Decimal(9.95), DecimalType(3, 1)), Decimal(10.0))
    checkEvaluation(cast(Decimal(9.95), DecimalType(1, 0)), null)

    checkEvaluation(cast(-9.95, DecimalType(3, 2)), Decimal(-9.95))
    checkEvaluation(cast(-9.95, DecimalType(3, 1)), Decimal(-10.0))
    checkEvaluation(cast(-9.95, DecimalType(2, 0)), Decimal(-10))
    checkEvaluation(cast(-9.95, DecimalType(2, 1)), null)
    checkEvaluation(cast(-9.95, DecimalType(1, 0)), null)
    checkEvaluation(cast(Decimal(-9.95), DecimalType(3, 1)), Decimal(-10.0))
    checkEvaluation(cast(Decimal(-9.95), DecimalType(1, 0)), null)

    checkEvaluation(cast(Decimal("1003"), DecimalType.SYSTEM_DEFAULT), Decimal(1003))
    checkEvaluation(cast(Decimal("1003"), DecimalType(4, 0)), Decimal(1003))
    checkEvaluation(cast(Decimal("1003"), DecimalType(3, 0)), null)

    checkEvaluation(cast(Decimal("995"), DecimalType(3, 0)), Decimal(995))

    checkEvaluation(cast(Double.NaN, DecimalType.SYSTEM_DEFAULT), null)
    checkEvaluation(cast(1.0 / 0.0, DecimalType.SYSTEM_DEFAULT), null)
    checkEvaluation(cast(Float.NaN, DecimalType.SYSTEM_DEFAULT), null)
    checkEvaluation(cast(1.0f / 0.0f, DecimalType.SYSTEM_DEFAULT), null)

    checkEvaluation(cast(Double.NaN, DecimalType(2, 1)), null)
    checkEvaluation(cast(1.0 / 0.0, DecimalType(2, 1)), null)
    checkEvaluation(cast(Float.NaN, DecimalType(2, 1)), null)
    checkEvaluation(cast(1.0f / 0.0f, DecimalType(2, 1)), null)

    checkEvaluation(cast(true, DecimalType(2, 1)), Decimal(1))
    checkEvaluation(cast(true, DecimalType(1, 1)), null)

    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      assert(cast(Decimal("1003"), DecimalType(3, -1)).nullable)
      assert(cast(Decimal("1003"), DecimalType(4, -1)).nullable === false)
      assert(cast(Decimal("995"), DecimalType(2, -1)).nullable)
      assert(cast(Decimal("995"), DecimalType(3, -1)).nullable === false)

      checkEvaluation(cast(Decimal("1003"), DecimalType(3, -1)), Decimal(1000))
      checkEvaluation(cast(Decimal("1003"), DecimalType(2, -2)), Decimal(1000))
      checkEvaluation(cast(Decimal("1003"), DecimalType(1, -2)), null)
      checkEvaluation(cast(Decimal("1003"), DecimalType(2, -1)), null)

      checkEvaluation(cast(Decimal("995"), DecimalType(3, -1)), Decimal(1000))
      checkEvaluation(cast(Decimal("995"), DecimalType(2, -2)), Decimal(1000))
      checkEvaluation(cast(Decimal("995"), DecimalType(2, -1)), null)
      checkEvaluation(cast(Decimal("995"), DecimalType(1, -2)), null)
    }
  }

  test("SPARK-28470: Cast should honor nullOnOverflow property") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(Cast(Literal("134.12"), DecimalType(3, 2)), null)
      checkEvaluation(
        Cast(Literal(Timestamp.valueOf("2019-07-25 22:04:36")), DecimalType(3, 2)), null)
      checkEvaluation(Cast(Literal(BigDecimal(134.12)), DecimalType(3, 2)), null)
      checkEvaluation(Cast(Literal(134.12), DecimalType(3, 2)), null)
    }
  }

  test("collect_list/collect_set can cast to ArrayType not containsNull") {
    val list = CollectList(Literal(1))
    assert(Cast.canCast(list.dataType, ArrayType(IntegerType, false)))
    val set = CollectSet(Literal(1))
    assert(Cast.canCast(set.dataType, ArrayType(StringType, false)))
  }

  test("NullTypes should be able to cast to any complex types") {
    assert(Cast.canCast(ArrayType(NullType, true), ArrayType(IntegerType, true)))
    assert(Cast.canCast(ArrayType(NullType, false), ArrayType(IntegerType, true)))

    assert(Cast.canCast(
      MapType(NullType, NullType, true), MapType(IntegerType, IntegerType, true)))
    assert(Cast.canCast(
      MapType(NullType, NullType, false), MapType(IntegerType, IntegerType, true)))

    assert(Cast.canCast(
      StructType(StructField("a", NullType, true) :: Nil),
      StructType(StructField("a", IntegerType, true) :: Nil)))
    assert(Cast.canCast(
      StructType(StructField("a", NullType, false) :: Nil),
      StructType(StructField("a", IntegerType, true) :: Nil)))
  }

  test("cast string to boolean II") {
    checkEvaluation(cast("abc", BooleanType), null)
    checkEvaluation(cast("", BooleanType), null)
  }

  test("cast from array II") {
    val array = Literal.create(Seq("123", "true", "f", null),
      ArrayType(StringType, containsNull = true))
    val array_notNull = Literal.create(Seq("123", "true", "f"),
      ArrayType(StringType, containsNull = false))

    {
      val ret = cast(array, ArrayType(BooleanType, containsNull = true))
      assert(ret.resolved)
      checkEvaluation(ret, Seq(null, true, false, null))
    }

    {
      val ret = cast(array_notNull, ArrayType(BooleanType, containsNull = true))
      assert(ret.resolved)
      checkEvaluation(ret, Seq(null, true, false))
    }

    {
      val ret = cast(array_notNull, ArrayType(BooleanType, containsNull = false))
      assert(ret.resolved === false)
    }
  }

  test("cast from map II") {
    val map = Literal.create(
      Map("a" -> "123", "b" -> "true", "c" -> "f", "d" -> null),
      MapType(StringType, StringType, valueContainsNull = true))
    val map_notNull = Literal.create(
      Map("a" -> "123", "b" -> "true", "c" -> "f"),
      MapType(StringType, StringType, valueContainsNull = false))

    {
      val ret = cast(map, MapType(StringType, BooleanType, valueContainsNull = true))
      assert(ret.resolved)
      checkEvaluation(ret, Map("a" -> null, "b" -> true, "c" -> false, "d" -> null))
    }

    {
      val ret = cast(map_notNull, MapType(StringType, BooleanType, valueContainsNull = true))
      assert(ret.resolved)
      checkEvaluation(ret, Map("a" -> null, "b" -> true, "c" -> false))
    }

    {
      val ret = cast(map, MapType(IntegerType, StringType, valueContainsNull = true))
      assert(ret.resolved === false)
    }

    {
      val ret = cast(map_notNull, MapType(StringType, BooleanType, valueContainsNull = false))
      assert(ret.resolved === false)
    }

    {
      val ret = cast(map_notNull, MapType(IntegerType, StringType, valueContainsNull = true))
      assert(ret.resolved === false)
    }
  }

  test("cast from struct II") {
    checkNullCast(
      StructType(Seq(
        StructField("a", StringType),
        StructField("b", IntegerType))),
      StructType(Seq(
        StructField("a", StringType),
        StructField("b", StringType))))

    val struct = Literal.create(
      InternalRow(
        UTF8String.fromString("123"),
        UTF8String.fromString("true"),
        UTF8String.fromString("f"),
        null),
      StructType(Seq(
        StructField("a", StringType, nullable = true),
        StructField("b", StringType, nullable = true),
        StructField("c", StringType, nullable = true),
        StructField("d", StringType, nullable = true))))
    val struct_notNull = Literal.create(
      InternalRow(
        UTF8String.fromString("123"),
        UTF8String.fromString("true"),
        UTF8String.fromString("f")),
      StructType(Seq(
        StructField("a", StringType, nullable = false),
        StructField("b", StringType, nullable = false),
        StructField("c", StringType, nullable = false))))

    {
      val ret = cast(struct, StructType(Seq(
        StructField("a", BooleanType, nullable = true),
        StructField("b", BooleanType, nullable = true),
        StructField("c", BooleanType, nullable = true),
        StructField("d", BooleanType, nullable = true))))
      assert(ret.resolved)
      checkEvaluation(ret, InternalRow(null, true, false, null))
    }

    {
      val ret = cast(struct_notNull, StructType(Seq(
        StructField("a", BooleanType, nullable = true),
        StructField("b", BooleanType, nullable = true),
        StructField("c", BooleanType, nullable = true))))
      assert(ret.resolved)
      checkEvaluation(ret, InternalRow(null, true, false))
    }

    {
      val ret = cast(struct_notNull, StructType(Seq(
        StructField("a", BooleanType, nullable = true),
        StructField("b", BooleanType, nullable = true),
        StructField("c", BooleanType, nullable = false))))
      assert(ret.resolved === false)
    }
  }

  test("complex casting") {
    val complex = Literal.create(
      Row(
        Seq("123", "true", "f"),
        Map("a" -> "123", "b" -> "true", "c" -> "f"),
        Row(0)),
      StructType(Seq(
        StructField("a",
          ArrayType(StringType, containsNull = false), nullable = true),
        StructField("m",
          MapType(StringType, StringType, valueContainsNull = false), nullable = true),
        StructField("s",
          StructType(Seq(
            StructField("i", IntegerType, nullable = true)))))))

    val ret = cast(complex, StructType(Seq(
      StructField("a",
        ArrayType(IntegerType, containsNull = true), nullable = true),
      StructField("m",
        MapType(StringType, BooleanType, valueContainsNull = false), nullable = true),
      StructField("s",
        StructType(Seq(
          StructField("l", LongType, nullable = true)))))))

    assert(ret.resolved === false)
  }

  test("SPARK-31227: Non-nullable null type should not coerce to nullable type") {
    TypeCoercionSuite.allTypes.foreach { t =>
      assert(Cast.canCast(ArrayType(NullType, false), ArrayType(t, false)))

      assert(Cast.canCast(
        MapType(NullType, NullType, false), MapType(t, t, false)))

      assert(Cast.canCast(
        StructType(StructField("a", NullType, false) :: Nil),
        StructType(StructField("a", t, false) :: Nil)))
    }
  }

  test("Cast should output null for invalid strings when ANSI is not enabled.") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(cast("abdef", DecimalType.USER_DEFAULT), null)
      checkEvaluation(cast("2012-12-11", DoubleType), null)

      // cast to array
      val array = Literal.create(Seq("123", "true", "f", null),
        ArrayType(StringType, containsNull = true))
      val array_notNull = Literal.create(Seq("123", "true", "f"),
        ArrayType(StringType, containsNull = false))

      {
        val ret = cast(array, ArrayType(IntegerType, containsNull = true))
        assert(ret.resolved)
        checkEvaluation(ret, Seq(123, null, null, null))
      }
      {
        val ret = cast(array, ArrayType(IntegerType, containsNull = false))
        assert(ret.resolved === false)
      }
      {
        val ret = cast(array_notNull, ArrayType(IntegerType, containsNull = true))
        assert(ret.resolved)
        checkEvaluation(ret, Seq(123, null, null))
      }
      {
        val ret = cast(array_notNull, ArrayType(IntegerType, containsNull = false))
        assert(ret.resolved === false)
      }

      // cast from map
      val map = Literal.create(
        Map("a" -> "123", "b" -> "true", "c" -> "f", "d" -> null),
        MapType(StringType, StringType, valueContainsNull = true))
      val map_notNull = Literal.create(
        Map("a" -> "123", "b" -> "true", "c" -> "f"),
        MapType(StringType, StringType, valueContainsNull = false))

      {
        val ret = cast(map, MapType(StringType, IntegerType, valueContainsNull = true))
        assert(ret.resolved)
        checkEvaluation(ret, Map("a" -> 123, "b" -> null, "c" -> null, "d" -> null))
      }
      {
        val ret = cast(map, MapType(StringType, IntegerType, valueContainsNull = false))
        assert(ret.resolved === false)
      }
      {
        val ret = cast(map_notNull, MapType(StringType, IntegerType, valueContainsNull = true))
        assert(ret.resolved)
        checkEvaluation(ret, Map("a" -> 123, "b" -> null, "c" -> null))
      }
      {
        val ret = cast(map_notNull, MapType(StringType, IntegerType, valueContainsNull = false))
        assert(ret.resolved === false)
      }

      // cast from struct
      val struct = Literal.create(
        InternalRow(
          UTF8String.fromString("123"),
          UTF8String.fromString("true"),
          UTF8String.fromString("f"),
          null),
        StructType(Seq(
          StructField("a", StringType, nullable = true),
          StructField("b", StringType, nullable = true),
          StructField("c", StringType, nullable = true),
          StructField("d", StringType, nullable = true))))
      val struct_notNull = Literal.create(
        InternalRow(
          UTF8String.fromString("123"),
          UTF8String.fromString("true"),
          UTF8String.fromString("f")),
        StructType(Seq(
          StructField("a", StringType, nullable = false),
          StructField("b", StringType, nullable = false),
          StructField("c", StringType, nullable = false))))

      {
        val ret = cast(struct, StructType(Seq(
          StructField("a", IntegerType, nullable = true),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = true),
          StructField("d", IntegerType, nullable = true))))
        assert(ret.resolved)
        checkEvaluation(ret, InternalRow(123, null, null, null))
      }
      {
        val ret = cast(struct, StructType(Seq(
          StructField("a", IntegerType, nullable = true),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false),
          StructField("d", IntegerType, nullable = true))))
        assert(ret.resolved === false)
      }
      {
        val ret = cast(struct_notNull, StructType(Seq(
          StructField("a", IntegerType, nullable = true),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = true))))
        assert(ret.resolved)
        checkEvaluation(ret, InternalRow(123, null, null))
      }
      {
        val ret = cast(struct_notNull, StructType(Seq(
          StructField("a", IntegerType, nullable = true),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false))))
        assert(ret.resolved === false)
      }

      // Invalid literals when casted to double and float results in null.
      Seq(DoubleType, FloatType).foreach { dataType =>
        checkEvaluation(cast("badvalue", dataType), null)
      }
    }
  }

  test("cast from date") {
    val d = Date.valueOf("1970-01-01")
    checkEvaluation(cast(d, ShortType), null)
    checkEvaluation(cast(d, IntegerType), null)
    checkEvaluation(cast(d, LongType), null)
    checkEvaluation(cast(d, FloatType), null)
    checkEvaluation(cast(d, DoubleType), null)
    checkEvaluation(cast(d, DecimalType.SYSTEM_DEFAULT), null)
    checkEvaluation(cast(d, DecimalType(10, 2)), null)
    checkEvaluation(cast(d, StringType), "1970-01-01")

    checkEvaluation(
      cast(cast(d, TimestampType, UTC_OPT), StringType, UTC_OPT),
      "1970-01-01 00:00:00")
  }

  test("cast from timestamp II") {
    checkEvaluation(cast(Double.NaN, TimestampType), null)
    checkEvaluation(cast(1.0 / 0.0, TimestampType), null)
    checkEvaluation(cast(Float.NaN, TimestampType), null)
    checkEvaluation(cast(1.0f / 0.0f, TimestampType), null)
    checkEvaluation(cast(Literal(Long.MaxValue), TimestampType), Long.MaxValue)
    checkEvaluation(cast(Literal(Long.MinValue), TimestampType), Long.MinValue)
  }

  test("cast a timestamp before the epoch 1970-01-01 00:00:00Z") {
    withDefaultTimeZone(UTC) {
      val negativeTs = Timestamp.valueOf("1900-05-05 18:34:56.1")
      assert(negativeTs.getTime < 0)
      val expectedSecs = Math.floorDiv(negativeTs.getTime, MILLIS_PER_SECOND)
      checkEvaluation(cast(negativeTs, ByteType), expectedSecs.toByte)
      checkEvaluation(cast(negativeTs, ShortType), expectedSecs.toShort)
      checkEvaluation(cast(negativeTs, IntegerType), expectedSecs.toInt)
      checkEvaluation(cast(negativeTs, LongType), expectedSecs)
    }
  }

  test("SPARK-32828: cast from a derived user-defined type to a base type") {
    val v = Literal.create(Row(1), new ExampleSubTypeUDT())
    checkEvaluation(cast(v, new ExampleBaseTypeUDT), Row(1))
  }

  test("Fast fail for cast string type to decimal type") {
    checkEvaluation(cast("12345678901234567890123456789012345678", DecimalType(38, 0)),
      Decimal("12345678901234567890123456789012345678"))
    checkEvaluation(cast("123456789012345678901234567890123456789", DecimalType(38, 0)), null)
    checkEvaluation(cast("12345678901234567890123456789012345678", DecimalType(38, 1)), null)

    checkEvaluation(cast("0.00000000000000000000000000000000000001", DecimalType(38, 0)),
      Decimal("0"))
    checkEvaluation(cast("0.00000000000000000000000000000000000000000001", DecimalType(38, 0)),
      Decimal("0"))
    checkEvaluation(cast("0.00000000000000000000000000000000000001", DecimalType(38, 18)),
      Decimal("0E-18"))
    checkEvaluation(cast("6E-120", DecimalType(38, 0)),
      Decimal("0"))

    checkEvaluation(cast("6E+37", DecimalType(38, 0)),
      Decimal("60000000000000000000000000000000000000"))
    checkEvaluation(cast("6E+38", DecimalType(38, 0)), null)
    checkEvaluation(cast("6E+37", DecimalType(38, 1)), null)

    checkEvaluation(cast("abcd", DecimalType(38, 1)), null)
  }

  test("data type casting II") {
    checkEvaluation(
      cast(cast(cast(cast(cast(cast("5", ByteType), TimestampType),
        DecimalType.SYSTEM_DEFAULT), LongType), StringType), ShortType),
        5.toShort)
      checkEvaluation(
        cast(cast(cast(cast(cast(cast("5", TimestampType, UTC_OPT), ByteType),
          DecimalType.SYSTEM_DEFAULT), LongType), StringType), ShortType),
        null)
      checkEvaluation(cast(cast(cast(cast(cast(cast("5", DecimalType.SYSTEM_DEFAULT),
        ByteType), TimestampType), LongType), StringType), ShortType),
        5.toShort)
  }

  test("Cast from double II") {
    checkEvaluation(cast(cast(1.toDouble, TimestampType), DoubleType), 1.toDouble)
  }

  test("SPARK-34727: cast from float II") {
    checkCast(16777215.0f, java.time.Instant.ofEpochSecond(16777215))
  }

  test("SPARK-34744: Improve error message for casting cause overflow error") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val e1 = intercept[ArithmeticException] {
        Cast(Literal(Byte.MaxValue + 1), ByteType).eval()
      }.getMessage
      assert(e1.contains("The value 128 of the type \"INT\" cannot be cast to \"TINYINT\""))
      val e2 = intercept[ArithmeticException] {
        Cast(Literal(Short.MaxValue + 1), ShortType).eval()
      }.getMessage
      assert(e2.contains("The value 32768 of the type \"INT\" cannot be cast to \"SMALLINT\""))
      val e3 = intercept[ArithmeticException] {
        Cast(Literal(Int.MaxValue + 1L), IntegerType).eval()
      }.getMessage
      assert(e3.contains("The value 2147483648L of the type \"BIGINT\" cannot be cast to \"INT\""))
    }
  }

  test("SPARK-35720: cast invalid string input to timestamp without time zone") {
    Seq("00:00:00",
      "a",
      "123",
      "a2021-06-17",
      "2021-06-17abc",
      "2021-06-17 00:00:00ABC").foreach { invalidInput =>
      checkEvaluation(cast(invalidInput, TimestampNTZType), null)
    }
  }

  test("SPARK-36286: invalid string cast to timestamp") {
    checkEvaluation(cast(Literal("2015-03-18T"), TimestampType), null)
  }

  test("SPARK-39749: cast Decimal to string") {
    val input = Literal.create(Decimal(0.000000123), DecimalType(9, 9))
    checkEvaluation(cast(input, StringType), "1.23E-7")
  }

  test("SPARK-42176: cast boolean to timestamp") {
    checkEvaluation(cast(true, TimestampType), 1L)
    checkEvaluation(cast(false, TimestampType), 0L)
  }

  private def castOverflowErrMsg(targetType: DataType): String = {
    s"""cannot be cast to "${targetType.sql}" due to an overflow."""
  }

  test("SPARK-36924: Cast DayTimeIntervalType to IntegralType") {
    DataTypeTestUtils.dayTimeIntervalTypes.foreach { dt =>
      val v1 = Literal.create(Duration.ZERO, dt)
      checkEvaluation(cast(v1, ByteType), 0.toByte)
      checkEvaluation(cast(v1, ShortType), 0.toShort)
      checkEvaluation(cast(v1, IntegerType), 0)
      checkEvaluation(cast(v1, LongType), 0L)

      val num = SECONDS_PER_DAY + SECONDS_PER_HOUR + SECONDS_PER_MINUTE + 1
      val v2 = Literal.create(Duration.ofSeconds(num), dt)
      dt.endField match {
        case DAY =>
          checkEvaluation(cast(v2, ByteType), 1.toByte)
          checkEvaluation(cast(v2, ShortType), 1.toShort)
          checkEvaluation(cast(v2, IntegerType), 1)
          checkEvaluation(cast(v2, LongType), 1.toLong)
        case HOUR =>
          checkEvaluation(cast(v2, ByteType), 25.toByte)
          checkEvaluation(cast(v2, ShortType), 25.toShort)
          checkEvaluation(cast(v2, IntegerType), 25)
          checkEvaluation(cast(v2, LongType), 25L)
        case MINUTE =>
          checkExceptionInExpression[ArithmeticException](cast(v2, ByteType),
            castOverflowErrMsg(ByteType))
          checkEvaluation(cast(v2, ShortType), (MINUTES_PER_HOUR * 25 + 1).toShort)
          checkEvaluation(cast(v2, IntegerType), (MINUTES_PER_HOUR * 25 + 1).toInt)
          checkEvaluation(cast(v2, LongType), MINUTES_PER_HOUR * 25 + 1)
        case SECOND =>
          checkExceptionInExpression[ArithmeticException](cast(v2, ByteType),
            castOverflowErrMsg(ByteType))
          checkExceptionInExpression[ArithmeticException](cast(v2, ShortType),
            castOverflowErrMsg(ShortType))
          checkEvaluation(cast(v2, IntegerType), num.toInt)
          checkEvaluation(cast(v2, LongType), num)
      }

      val v3 = Literal.create(Duration.of(Long.MaxValue, ChronoUnit.MICROS), dt)
      dt.endField match {
        case DAY =>
          checkExceptionInExpression[ArithmeticException](cast(v3, ByteType),
            castOverflowErrMsg(ByteType))
          checkExceptionInExpression[ArithmeticException](cast(v3, ShortType),
            castOverflowErrMsg(ShortType))
          checkEvaluation(cast(v3, IntegerType), (Long.MaxValue / MICROS_PER_DAY).toInt)
          checkEvaluation(cast(v3, LongType), Long.MaxValue / MICROS_PER_DAY)
        case HOUR =>
          checkExceptionInExpression[ArithmeticException](cast(v3, ByteType),
            castOverflowErrMsg(ByteType))
          checkExceptionInExpression[ArithmeticException](cast(v3, ShortType),
            castOverflowErrMsg(ShortType))
          checkExceptionInExpression[ArithmeticException](cast(v3, IntegerType),
            castOverflowErrMsg(IntegerType))
          checkEvaluation(cast(v3, LongType), Long.MaxValue / MICROS_PER_HOUR)
        case MINUTE =>
          checkExceptionInExpression[ArithmeticException](cast(v3, ByteType),
            castOverflowErrMsg(ByteType))
          checkExceptionInExpression[ArithmeticException](cast(v3, ShortType),
            castOverflowErrMsg(ShortType))
          checkExceptionInExpression[ArithmeticException](cast(v3, IntegerType),
            castOverflowErrMsg(IntegerType))
          checkEvaluation(cast(v3, LongType), Long.MaxValue / MICROS_PER_MINUTE)
        case SECOND =>
          checkExceptionInExpression[ArithmeticException](cast(v3, ByteType),
            castOverflowErrMsg(ByteType))
          checkExceptionInExpression[ArithmeticException](cast(v3, ShortType),
            castOverflowErrMsg(ShortType))
          checkExceptionInExpression[ArithmeticException](cast(v3, IntegerType),
            castOverflowErrMsg(IntegerType))
          checkEvaluation(cast(v3, LongType), Long.MaxValue / MICROS_PER_SECOND)
      }

      val v4 = Literal.create(Duration.of(Long.MinValue, ChronoUnit.MICROS), dt)
      dt.endField match {
        case DAY =>
          checkExceptionInExpression[ArithmeticException](cast(v4, ByteType),
            castOverflowErrMsg(ByteType))
          checkExceptionInExpression[ArithmeticException](cast(v4, ShortType),
            castOverflowErrMsg(ShortType))
          checkEvaluation(cast(v4, IntegerType), (Long.MinValue / MICROS_PER_DAY).toInt)
          checkEvaluation(cast(v4, LongType), Long.MinValue / MICROS_PER_DAY)
        case HOUR =>
          checkExceptionInExpression[ArithmeticException](cast(v4, ByteType),
            castOverflowErrMsg(ByteType))
          checkExceptionInExpression[ArithmeticException](cast(v4, ShortType),
            castOverflowErrMsg(ShortType))
          checkExceptionInExpression[ArithmeticException](cast(v4, IntegerType),
            castOverflowErrMsg(IntegerType))
          checkEvaluation(cast(v4, LongType), Long.MinValue / MICROS_PER_HOUR)
        case MINUTE =>
          checkExceptionInExpression[ArithmeticException](cast(v4, ByteType),
            castOverflowErrMsg(ByteType))
          checkExceptionInExpression[ArithmeticException](cast(v4, ShortType),
            castOverflowErrMsg(ShortType))
          checkExceptionInExpression[ArithmeticException](cast(v4, IntegerType),
            castOverflowErrMsg(IntegerType))
          checkEvaluation(cast(v4, LongType), Long.MinValue / MICROS_PER_MINUTE)
        case SECOND =>
          checkExceptionInExpression[ArithmeticException](cast(v4, ByteType),
            castOverflowErrMsg(ByteType))
          checkExceptionInExpression[ArithmeticException](cast(v4, ShortType),
            castOverflowErrMsg(ShortType))
          checkExceptionInExpression[ArithmeticException](cast(v4, IntegerType),
            castOverflowErrMsg(IntegerType))
          checkEvaluation(cast(v4, LongType), Long.MinValue / MICROS_PER_SECOND)
      }
    }
  }

  test("SPARK-36924: Cast IntegralType to DayTimeIntervalType") {
    Seq(
      (0, ByteType, 0L, 0L, 0L, 0L),
      (0, ShortType, 0L, 0L, 0L, 0L),
      (0, IntegerType, 0L, 0L, 0L, 0L),
      (0, LongType, 0L, 0L, 0L, 0L),
      (1, ByteType, MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND),
      (1, ShortType, MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND),
      (1, IntegerType, MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND),
      (1, LongType, MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND),
      (Byte.MaxValue, ByteType, Byte.MaxValue * MICROS_PER_DAY, Byte.MaxValue * MICROS_PER_HOUR,
        Byte.MaxValue * MICROS_PER_MINUTE, Byte.MaxValue * MICROS_PER_SECOND),
      (Byte.MinValue, ByteType, Byte.MinValue * MICROS_PER_DAY, Byte.MinValue * MICROS_PER_HOUR,
        Byte.MinValue * MICROS_PER_MINUTE, Byte.MinValue * MICROS_PER_SECOND),
      (Short.MaxValue, ShortType, Short.MaxValue * MICROS_PER_DAY, Short.MaxValue * MICROS_PER_HOUR,
        Short.MaxValue * MICROS_PER_MINUTE, Short.MaxValue * MICROS_PER_SECOND),
      (Short.MinValue, ShortType, Short.MinValue * MICROS_PER_DAY, Short.MinValue * MICROS_PER_HOUR,
        Short.MinValue * MICROS_PER_MINUTE, Short.MinValue * MICROS_PER_SECOND)
    ).foreach { case (v, dt, r1, r2, r3, r4) =>
      checkEvaluation(cast(
        cast(v, dt), DayTimeIntervalType(DAY)), r1)
      checkEvaluation(cast(
        cast(v, dt), DayTimeIntervalType(HOUR)), r2)
      checkEvaluation(cast(
        cast(v, dt), DayTimeIntervalType(MINUTE)), r3)
      checkEvaluation(cast(
        cast(v, dt), DayTimeIntervalType(SECOND)), r4)
    }

    Seq(
      (Int.MaxValue,
        Math.multiplyExact(Int.MaxValue.toLong, MICROS_PER_HOUR),
        Math.multiplyExact(Int.MaxValue.toLong, MICROS_PER_MINUTE),
        Math.multiplyExact(Int.MaxValue.toLong, MICROS_PER_SECOND)),
      (Int.MinValue,
        Math.multiplyExact(Int.MinValue.toLong, MICROS_PER_HOUR),
        Math.multiplyExact(Int.MinValue.toLong, MICROS_PER_MINUTE),
        Math.multiplyExact(Int.MinValue.toLong, MICROS_PER_SECOND))
    ).foreach { case (v, r1, r2, r3) =>
      checkEvaluation(cast(v, DayTimeIntervalType(HOUR)), r1)
      checkEvaluation(cast(v, DayTimeIntervalType(MINUTE)), r2)
      checkEvaluation(cast(v, DayTimeIntervalType(SECOND)), r3)
    }

    Seq(
      (Int.MaxValue, DayTimeIntervalType(DAY)),
      (Int.MinValue, DayTimeIntervalType(DAY))
    ).foreach {
      case (v, toType) =>
        checkExceptionInExpression[ArithmeticException](cast(v, toType),
          castOverflowErrMsg(toType))
    }

    Seq(
      (Long.MaxValue, DayTimeIntervalType(DAY)),
      (Long.MinValue, DayTimeIntervalType(DAY)),
      (Long.MaxValue, DayTimeIntervalType(HOUR)),
      (Long.MinValue, DayTimeIntervalType(HOUR)),
      (Long.MaxValue, DayTimeIntervalType(MINUTE)),
      (Long.MinValue, DayTimeIntervalType(MINUTE)),
      (Long.MaxValue, DayTimeIntervalType(SECOND)),
      (Long.MinValue, DayTimeIntervalType(SECOND))
    ).foreach {
      case (v, toType) =>
        checkExceptionInExpression[ArithmeticException](cast(v, toType),
          castOverflowErrMsg(toType))
    }
  }

  test("SPARK-36924: Cast YearMonthIntervalType to IntegralType") {
    Seq(
      (Period.ofYears(0), YearMonthIntervalType(YEAR), 0.toByte, 0.toShort, 0, 0L),
      (Period.ofYears(1), YearMonthIntervalType(YEAR), 1.toByte, 1.toShort, 1, 1L),
      (Period.ofYears(0), YearMonthIntervalType(YEAR, MONTH), 0.toByte, 0.toShort, 0, 0L),
      (Period.ofMonths(1), YearMonthIntervalType(YEAR, MONTH), 1.toByte, 1.toShort, 1, 1L),
      (Period.ofMonths(0), YearMonthIntervalType(MONTH), 0.toByte, 0.toShort, 0, 0L),
      (Period.ofMonths(1), YearMonthIntervalType(MONTH), 1.toByte, 1.toShort, 1, 1L)
    ).foreach { case (v, dt, r1, r2, r3, r4) =>
      val value = Literal.create(v, dt)
      checkEvaluation(cast(value, ByteType), r1)
      checkEvaluation(cast(value, ShortType), r2)
      checkEvaluation(cast(value, IntegerType), r3)
      checkEvaluation(cast(value, LongType), r4)
    }

    Seq(
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(YEAR), ByteType),
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(YEAR), ShortType),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(YEAR), ByteType),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(YEAR), ShortType),
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(YEAR, MONTH), ByteType),
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(YEAR, MONTH), ShortType),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(YEAR, MONTH), ByteType),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(YEAR, MONTH), ShortType),
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(MONTH), ByteType),
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(MONTH), ShortType),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(MONTH), ByteType),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(MONTH), ShortType)
    ).foreach {
      case (v, dt, toType) =>
        val value = Literal.create(v, dt)
        checkExceptionInExpression[ArithmeticException](cast(value, toType),
          castOverflowErrMsg(toType))
    }

    Seq(
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(YEAR), IntegerType, Int.MaxValue / 12),
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(YEAR), LongType, Int.MaxValue /12L),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(YEAR), IntegerType, Int.MinValue / 12),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(YEAR), LongType, Int.MinValue /12L),
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(YEAR, MONTH),
        IntegerType, Int.MaxValue),
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(YEAR, MONTH),
        LongType, Int.MaxValue.toLong),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(YEAR, MONTH),
        IntegerType, Int.MinValue),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(YEAR, MONTH),
        LongType, Int.MinValue.toLong),
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(MONTH), IntegerType, Int.MaxValue),
      (Period.ofMonths(Int.MaxValue), YearMonthIntervalType(MONTH), LongType, Int.MaxValue.toLong),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(MONTH), IntegerType, Int.MinValue),
      (Period.ofMonths(Int.MinValue), YearMonthIntervalType(MONTH), LongType, Int.MinValue.toLong)
    ).foreach {
      case (v, dt, toType, expect) =>
        val value = Literal.create(v, dt)
        checkEvaluation(cast(value, toType), expect)
    }
  }

  test("SPARK-36924: Cast IntegralType to YearMonthIntervalType") {
    Seq(
      (0, 0, 0, ByteType),
      (0, 0, 0, ShortType),
      (0, 0, 0, IntegerType),
      (0, 0, 0, LongType),
      (1, 12, 1, ByteType),
      (Byte.MaxValue, Byte.MaxValue * 12, Byte.MaxValue.toInt, ByteType),
      (Byte.MinValue, Byte.MinValue * 12, Byte.MinValue.toInt, ByteType),
      (1, 12, 1, ShortType),
      (Short.MaxValue, Short.MaxValue * 12, Short.MaxValue.toInt, ShortType),
      (Short.MinValue, Short.MinValue * 12, Short.MinValue.toInt, ShortType),
      (1, 12, 1, IntegerType),
      (1, 12, 1, LongType)
    ).foreach { case (v, r1, r2, dt) =>
      checkEvaluation(cast(
        cast(v, dt), YearMonthIntervalType(YEAR)), r1)
      checkEvaluation(cast(
        cast(v, dt), YearMonthIntervalType(MONTH)), r2)
    }

    Seq(Int.MaxValue, Int.MinValue).foreach { v =>
      checkEvaluation(cast(v, YearMonthIntervalType(MONTH)), v)
    }

    Seq(
      (Int.MaxValue, YearMonthIntervalType(YEAR)),
      (Int.MinValue, YearMonthIntervalType(YEAR))
    ).foreach {
      case (v, toType) =>
        checkExceptionInExpression[ArithmeticException](cast(v, toType),
          castOverflowErrMsg(toType))
    }

    Seq(
      (Long.MaxValue, YearMonthIntervalType(YEAR)),
      (Long.MinValue, YearMonthIntervalType(YEAR)),
      (Long.MaxValue, YearMonthIntervalType(MONTH)),
      (Long.MinValue, YearMonthIntervalType(MONTH))
    ).foreach {
      case (v, toType) =>
        checkExceptionInExpression[ArithmeticException](cast(v, toType),
          castOverflowErrMsg(toType))
    }
  }
}
