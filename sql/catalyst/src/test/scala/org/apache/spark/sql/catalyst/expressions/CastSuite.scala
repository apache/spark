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
import java.time.DateTimeException
import java.util.{Calendar, TimeZone}

import scala.collection.parallel.immutable.ParVector

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.numericPrecedence
import org.apache.spark.sql.catalyst.analysis.TypeCoercionSuite
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectList, CollectSet}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class CastSuiteBase extends SparkFunSuite with ExpressionEvalHelper {

  protected def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase

  // expected cannot be null
  protected def checkCast(v: Any, expected: Any): Unit = {
    checkEvaluation(cast(v, Literal(expected).dataType), expected)
  }

  protected def checkNullCast(from: DataType, to: DataType): Unit = {
    checkEvaluation(cast(Literal.create(null, from), to, UTC_OPT), null)
  }

  test("null cast") {
    import DataTypeTestUtils._

    atomicTypes.zip(atomicTypes).foreach { case (from, to) =>
      checkNullCast(from, to)
    }

    atomicTypes.foreach(dt => checkNullCast(NullType, dt))
    atomicTypes.foreach(dt => checkNullCast(dt, StringType))
    checkNullCast(StringType, BinaryType)
    checkNullCast(StringType, BooleanType)
    numericTypes.foreach(dt => checkNullCast(dt, BooleanType))

    checkNullCast(StringType, TimestampType)
    checkNullCast(DateType, TimestampType)

    checkNullCast(StringType, DateType)
    checkNullCast(TimestampType, DateType)

    checkNullCast(StringType, CalendarIntervalType)
    numericTypes.foreach(dt => checkNullCast(StringType, dt))
    numericTypes.foreach(dt => checkNullCast(BooleanType, dt))
    for (from <- numericTypes; to <- numericTypes) checkNullCast(from, to)
  }

  test("cast string to date") {
    var c = Calendar.getInstance()
    c.set(2015, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    checkEvaluation(Cast(Literal("2015"), DateType), new Date(c.getTimeInMillis))
    c = Calendar.getInstance()
    c.set(2015, 2, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    checkEvaluation(Cast(Literal("2015-03"), DateType), new Date(c.getTimeInMillis))
    c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    checkEvaluation(Cast(Literal("2015-03-18"), DateType), new Date(c.getTimeInMillis))
    checkEvaluation(Cast(Literal("2015-03-18 "), DateType), new Date(c.getTimeInMillis))
    checkEvaluation(Cast(Literal("2015-03-18 123142"), DateType), new Date(c.getTimeInMillis))
    checkEvaluation(Cast(Literal("2015-03-18T123123"), DateType), new Date(c.getTimeInMillis))
    checkEvaluation(Cast(Literal("2015-03-18T"), DateType), new Date(c.getTimeInMillis))

    checkEvaluation(Cast(Literal("2015-03-18X"), DateType), null)
    checkEvaluation(Cast(Literal("2015/03/18"), DateType), null)
    checkEvaluation(Cast(Literal("2015.03.18"), DateType), null)
    checkEvaluation(Cast(Literal("20150318"), DateType), null)
    checkEvaluation(Cast(Literal("2015-031-8"), DateType), null)
  }

  test("cast string to timestamp") {
    new ParVector(ALL_TIMEZONES.toVector).foreach { zid =>
      def checkCastStringToTimestamp(str: String, expected: Timestamp): Unit = {
        checkEvaluation(cast(Literal(str), TimestampType, Option(zid.getId)), expected)
      }

      val tz = TimeZone.getTimeZone(zid)
      var c = Calendar.getInstance(tz)
      c.set(2015, 0, 1, 0, 0, 0)
      c.set(Calendar.MILLISECOND, 0)
      checkCastStringToTimestamp("2015", new Timestamp(c.getTimeInMillis))
      c = Calendar.getInstance(tz)
      c.set(2015, 2, 1, 0, 0, 0)
      c.set(Calendar.MILLISECOND, 0)
      checkCastStringToTimestamp("2015-03", new Timestamp(c.getTimeInMillis))
      c = Calendar.getInstance(tz)
      c.set(2015, 2, 18, 0, 0, 0)
      c.set(Calendar.MILLISECOND, 0)
      checkCastStringToTimestamp("2015-03-18", new Timestamp(c.getTimeInMillis))
      checkCastStringToTimestamp("2015-03-18 ", new Timestamp(c.getTimeInMillis))
      checkCastStringToTimestamp("2015-03-18T", new Timestamp(c.getTimeInMillis))

      c = Calendar.getInstance(tz)
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkCastStringToTimestamp("2015-03-18 12:03:17", new Timestamp(c.getTimeInMillis))
      checkCastStringToTimestamp("2015-03-18T12:03:17", new Timestamp(c.getTimeInMillis))

      // If the string value includes timezone string, it represents the timestamp string
      // in the timezone regardless of the timeZoneId parameter.
      c = Calendar.getInstance(TimeZone.getTimeZone(UTC))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkCastStringToTimestamp("2015-03-18T12:03:17Z", new Timestamp(c.getTimeInMillis))
      checkCastStringToTimestamp("2015-03-18 12:03:17Z", new Timestamp(c.getTimeInMillis))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT-01:00"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkCastStringToTimestamp("2015-03-18T12:03:17-1:0", new Timestamp(c.getTimeInMillis))
      checkCastStringToTimestamp("2015-03-18T12:03:17-01:00", new Timestamp(c.getTimeInMillis))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkCastStringToTimestamp("2015-03-18T12:03:17+07:30", new Timestamp(c.getTimeInMillis))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:03"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkCastStringToTimestamp("2015-03-18T12:03:17+7:3", new Timestamp(c.getTimeInMillis))

      // tests for the string including milliseconds.
      c = Calendar.getInstance(tz)
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 123)
      checkCastStringToTimestamp("2015-03-18 12:03:17.123", new Timestamp(c.getTimeInMillis))
      checkCastStringToTimestamp("2015-03-18T12:03:17.123", new Timestamp(c.getTimeInMillis))

      // If the string value includes timezone string, it represents the timestamp string
      // in the timezone regardless of the timeZoneId parameter.
      c = Calendar.getInstance(TimeZone.getTimeZone(UTC))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 456)
      checkCastStringToTimestamp("2015-03-18T12:03:17.456Z", new Timestamp(c.getTimeInMillis))
      checkCastStringToTimestamp("2015-03-18 12:03:17.456Z", new Timestamp(c.getTimeInMillis))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT-01:00"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 123)
      checkCastStringToTimestamp("2015-03-18T12:03:17.123-1:0", new Timestamp(c.getTimeInMillis))
      checkCastStringToTimestamp("2015-03-18T12:03:17.123-01:00", new Timestamp(c.getTimeInMillis))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 123)
      checkCastStringToTimestamp("2015-03-18T12:03:17.123+07:30", new Timestamp(c.getTimeInMillis))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:03"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 123)
      checkCastStringToTimestamp("2015-03-18T12:03:17.123+7:3", new Timestamp(c.getTimeInMillis))
    }
  }

  test("cast from boolean") {
    checkEvaluation(cast(true, IntegerType), 1)
    checkEvaluation(cast(false, IntegerType), 0)
    checkEvaluation(cast(true, StringType), "true")
    checkEvaluation(cast(false, StringType), "false")
    checkEvaluation(cast(cast(1, BooleanType), IntegerType), 1)
    checkEvaluation(cast(cast(0, BooleanType), IntegerType), 0)
  }

  test("cast from int") {
    checkCast(0, false)
    checkCast(1, true)
    checkCast(-5, true)
    checkCast(1, 1.toByte)
    checkCast(1, 1.toShort)
    checkCast(1, 1)
    checkCast(1, 1.toLong)
    checkCast(1, 1.0f)
    checkCast(1, 1.0)
    checkCast(123, "123")

    checkEvaluation(cast(123, DecimalType.USER_DEFAULT), Decimal(123))
    checkEvaluation(cast(123, DecimalType(3, 0)), Decimal(123))
    checkEvaluation(cast(1, LongType), 1.toLong)
  }

  test("cast from long") {
    checkCast(0L, false)
    checkCast(1L, true)
    checkCast(-5L, true)
    checkCast(1L, 1.toByte)
    checkCast(1L, 1.toShort)
    checkCast(1L, 1)
    checkCast(1L, 1.toLong)
    checkCast(1L, 1.0f)
    checkCast(1L, 1.0)
    checkCast(123L, "123")

    checkEvaluation(cast(123L, DecimalType.USER_DEFAULT), Decimal(123))
    checkEvaluation(cast(123L, DecimalType(3, 0)), Decimal(123))
  }

  test("cast from float") {
    checkCast(0.0f, false)
    checkCast(0.5f, true)
    checkCast(-5.0f, true)
    checkCast(1.5f, 1.toByte)
    checkCast(1.5f, 1.toShort)
    checkCast(1.5f, 1)
    checkCast(1.5f, 1.toLong)
    checkCast(1.5f, 1.5)
    checkCast(1.5f, "1.5")
  }

  test("cast from double") {
    checkCast(0.0, false)
    checkCast(0.5, true)
    checkCast(-5.0, true)
    checkCast(1.5, 1.toByte)
    checkCast(1.5, 1.toShort)
    checkCast(1.5, 1)
    checkCast(1.5, 1.toLong)
    checkCast(1.5, 1.5f)
    checkCast(1.5, "1.5")
  }

  test("cast from string") {
    assert(cast("abcdef", StringType).nullable === false)
    assert(cast("abcdef", BinaryType).nullable === false)
    assert(cast("abcdef", BooleanType).nullable)
    assert(cast("abcdef", TimestampType).nullable)
    assert(cast("abcdef", LongType).nullable)
    assert(cast("abcdef", IntegerType).nullable)
    assert(cast("abcdef", ShortType).nullable)
    assert(cast("abcdef", ByteType).nullable)
    assert(cast("abcdef", DecimalType.USER_DEFAULT).nullable)
    assert(cast("abcdef", DecimalType(4, 2)).nullable)
    assert(cast("abcdef", DoubleType).nullable)
    assert(cast("abcdef", FloatType).nullable)
  }

  test("data type casting") {
    val sd = "1970-01-01"
    val d = Date.valueOf(sd)
    val zts = sd + " 00:00:00"
    val sts = sd + " 00:00:02"
    val nts = sts + ".1"
    val ts = withDefaultTimeZone(UTC)(Timestamp.valueOf(nts))

    for (tz <- ALL_TIMEZONES) {
      val timeZoneId = Option(tz.getId)
      var c = Calendar.getInstance(TimeZoneUTC)
      c.set(2015, 2, 8, 2, 30, 0)
      checkEvaluation(
        cast(cast(new Timestamp(c.getTimeInMillis), StringType, timeZoneId),
          TimestampType, timeZoneId),
        millisToMicros(c.getTimeInMillis))
      c = Calendar.getInstance(TimeZoneUTC)
      c.set(2015, 10, 1, 2, 30, 0)
      checkEvaluation(
        cast(cast(new Timestamp(c.getTimeInMillis), StringType, timeZoneId),
          TimestampType, timeZoneId),
        millisToMicros(c.getTimeInMillis))
    }

    checkEvaluation(cast("abdef", StringType), "abdef")
    checkEvaluation(cast("12.65", DecimalType.SYSTEM_DEFAULT), Decimal(12.65))

    checkEvaluation(cast(cast(sd, DateType), StringType), sd)
    checkEvaluation(cast(cast(d, StringType), DateType), 0)
    checkEvaluation(cast(cast(nts, TimestampType, UTC_OPT), StringType, UTC_OPT), nts)
    checkEvaluation(
      cast(cast(ts, StringType, UTC_OPT), TimestampType, UTC_OPT),
      fromJavaTimestamp(ts))

    // all convert to string type to check
    checkEvaluation(
      cast(cast(cast(nts, TimestampType, UTC_OPT), DateType, UTC_OPT), StringType),
      sd)
    checkEvaluation(
      cast(cast(cast(ts, DateType, UTC_OPT), TimestampType, UTC_OPT), StringType, UTC_OPT),
      zts)

    checkEvaluation(cast(cast("abdef", BinaryType), StringType), "abdef")

    checkEvaluation(cast(cast(cast(cast(
      cast(cast("5", ByteType), ShortType), IntegerType), FloatType), DoubleType), LongType),
      5.toLong)

    checkEvaluation(cast("23", DoubleType), 23d)
    checkEvaluation(cast("23", IntegerType), 23)
    checkEvaluation(cast("23", FloatType), 23f)
    checkEvaluation(cast("23", DecimalType.USER_DEFAULT), Decimal(23))
    checkEvaluation(cast("23", ByteType), 23.toByte)
    checkEvaluation(cast("23", ShortType), 23.toShort)
    checkEvaluation(cast(123, IntegerType), 123)

    checkEvaluation(cast(Literal.create(null, IntegerType), ShortType), null)
  }

  test("cast and add") {
    checkEvaluation(Add(Literal(23d), cast(true, DoubleType)), 24d)
    checkEvaluation(Add(Literal(23), cast(true, IntegerType)), 24)
    checkEvaluation(Add(Literal(23f), cast(true, FloatType)), 24f)
    checkEvaluation(Add(Literal(Decimal(23)), cast(true, DecimalType.USER_DEFAULT)), Decimal(24))
    checkEvaluation(Add(Literal(23.toByte), cast(true, ByteType)), 24.toByte)
    checkEvaluation(Add(Literal(23.toShort), cast(true, ShortType)), 24.toShort)
  }

  test("from decimal") {
    checkCast(Decimal(0.0), false)
    checkCast(Decimal(0.5), true)
    checkCast(Decimal(-5.0), true)
    checkCast(Decimal(1.5), 1.toByte)
    checkCast(Decimal(1.5), 1.toShort)
    checkCast(Decimal(1.5), 1)
    checkCast(Decimal(1.5), 1.toLong)
    checkCast(Decimal(1.5), 1.5f)
    checkCast(Decimal(1.5), 1.5)
    checkCast(Decimal(1.5), "1.5")
  }

  test("cast from array") {
    val array = Literal.create(Seq("123", "true", "f", null),
      ArrayType(StringType, containsNull = true))
    val array_notNull = Literal.create(Seq("123", "true", "f"),
      ArrayType(StringType, containsNull = false))

    checkNullCast(ArrayType(StringType), ArrayType(IntegerType))

    {
      val ret = cast(array, ArrayType(BooleanType, containsNull = true))
      assert(ret.resolved)
      checkEvaluation(ret, Seq(null, true, false, null))
    }

    {
      val array = Literal.create(Seq.empty, ArrayType(NullType, containsNull = false))
      val ret = cast(array, ArrayType(IntegerType, containsNull = false))
      assert(ret.resolved)
      checkEvaluation(ret, Seq.empty)
    }

    {
      val ret = cast(array, ArrayType(BooleanType, containsNull = false))
      assert(ret.resolved === false)
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

    {
      val ret = cast(array, IntegerType)
      assert(ret.resolved === false)
    }
  }

  test("cast from map") {
    val map = Literal.create(
      Map("a" -> "123", "b" -> "true", "c" -> "f", "d" -> null),
      MapType(StringType, StringType, valueContainsNull = true))
    val map_notNull = Literal.create(
      Map("a" -> "123", "b" -> "true", "c" -> "f"),
      MapType(StringType, StringType, valueContainsNull = false))

    checkNullCast(MapType(StringType, IntegerType), MapType(StringType, StringType))

    {
      val ret = cast(map, MapType(StringType, BooleanType, valueContainsNull = true))
      assert(ret.resolved)
      checkEvaluation(ret, Map("a" -> null, "b" -> true, "c" -> false, "d" -> null))
    }
    {
      val ret = cast(map, MapType(StringType, BooleanType, valueContainsNull = false))
      assert(ret.resolved === false)
    }
    {
      val ret = cast(map, MapType(IntegerType, StringType, valueContainsNull = true))
      assert(ret.resolved === false)
    }
    {
      val ret = cast(map_notNull, MapType(StringType, BooleanType, valueContainsNull = true))
      assert(ret.resolved)
      checkEvaluation(ret, Map("a" -> null, "b" -> true, "c" -> false))
    }
    {
      val ret = cast(map_notNull, MapType(StringType, BooleanType, valueContainsNull = false))
      assert(ret.resolved === false)
    }
    {
      val ret = cast(map_notNull, MapType(IntegerType, StringType, valueContainsNull = true))
      assert(ret.resolved === false)
    }

    {
      val ret = cast(map, IntegerType)
      assert(ret.resolved === false)
    }
  }

  test("cast from struct") {
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
      val ret = cast(struct, StructType(Seq(
        StructField("a", BooleanType, nullable = true),
        StructField("b", BooleanType, nullable = true),
        StructField("c", BooleanType, nullable = false),
        StructField("d", BooleanType, nullable = true))))
      assert(ret.resolved === false)
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

    {
      val ret = cast(struct, StructType(Seq(
        StructField("a", StringType, nullable = true),
        StructField("b", StringType, nullable = true),
        StructField("c", StringType, nullable = true))))
      assert(ret.resolved === false)
    }
    {
      val ret = cast(struct, IntegerType)
      assert(ret.resolved === false)
    }
  }

  test("cast struct with a timestamp field") {
    val originalSchema = new StructType().add("tsField", TimestampType, nullable = false)
    // nine out of ten times I'm casting a struct, it's to normalize its fields nullability
    val targetSchema = new StructType().add("tsField", TimestampType, nullable = true)

    val inp = Literal.create(InternalRow(0L), originalSchema)
    val expected = InternalRow(0L)
    checkEvaluation(cast(inp, targetSchema), expected)
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

  test("cast between string and interval") {
    import org.apache.spark.unsafe.types.CalendarInterval

    checkEvaluation(Cast(Literal(""), CalendarIntervalType), null)
    checkEvaluation(Cast(Literal("interval -3 month 1 day 7 hours"), CalendarIntervalType),
      new CalendarInterval(-3, 1, 7 * MICROS_PER_HOUR))
    checkEvaluation(Cast(Literal.create(
      new CalendarInterval(15, 9, -3 * MICROS_PER_HOUR), CalendarIntervalType),
      StringType),
      "1 years 3 months 9 days -3 hours")
    checkEvaluation(Cast(Literal("INTERVAL 1 Second 1 microsecond"), CalendarIntervalType),
      new CalendarInterval(0, 0, 1000001))
    checkEvaluation(Cast(Literal("1 MONTH 1 Microsecond"), CalendarIntervalType),
      new CalendarInterval(1, 0, 1))
  }

  test("cast string to boolean") {
    checkCast("t", true)
    checkCast("true", true)
    checkCast("tRUe", true)
    checkCast("y", true)
    checkCast("yes", true)
    checkCast("1", true)
    checkCast("f", false)
    checkCast("false", false)
    checkCast("FAlsE", false)
    checkCast("n", false)
    checkCast("no", false)
    checkCast("0", false)

    checkEvaluation(cast("abc", BooleanType), null)
    checkEvaluation(cast("", BooleanType), null)
  }

  protected def checkInvalidCastFromNumericType(to: DataType): Unit = {
    assert(cast(1.toByte, to).checkInputDataTypes().isFailure)
    assert(cast(1.toShort, to).checkInputDataTypes().isFailure)
    assert(cast(1, to).checkInputDataTypes().isFailure)
    assert(cast(1L, to).checkInputDataTypes().isFailure)
    assert(cast(1.0.toFloat, to).checkInputDataTypes().isFailure)
    assert(cast(1.0, to).checkInputDataTypes().isFailure)
  }

  test("SPARK-16729 type checking for casting to date type") {
    assert(cast("1234", DateType).checkInputDataTypes().isSuccess)
    assert(cast(new Timestamp(1), DateType).checkInputDataTypes().isSuccess)
    assert(cast(false, DateType).checkInputDataTypes().isFailure)
    checkInvalidCastFromNumericType(DateType)
  }

  test("SPARK-20302 cast with same structure") {
    val from = new StructType()
      .add("a", IntegerType)
      .add("b", new StructType().add("b1", LongType))

    val to = new StructType()
      .add("a1", IntegerType)
      .add("b1", new StructType().add("b11", LongType))

    val input = Row(10, Row(12L))

    checkEvaluation(cast(Literal.create(input, from), to), input)
  }

  test("SPARK-22500: cast for struct should not generate codes beyond 64KB") {
    val N = 25

    val fromInner = new StructType(
      (1 to N).map(i => StructField(s"s$i", DoubleType)).toArray)
    val toInner = new StructType(
      (1 to N).map(i => StructField(s"i$i", IntegerType)).toArray)
    val inputInner = Row.fromSeq((1 to N).map(i => i + 0.5))
    val outputInner = Row.fromSeq((1 to N))
    val fromOuter = new StructType(
      (1 to N).map(i => StructField(s"s$i", fromInner)).toArray)
    val toOuter = new StructType(
      (1 to N).map(i => StructField(s"s$i", toInner)).toArray)
    val inputOuter = Row.fromSeq((1 to N).map(_ => inputInner))
    val outputOuter = Row.fromSeq((1 to N).map(_ => outputInner))
    checkEvaluation(cast(Literal.create(inputOuter, fromOuter), toOuter), outputOuter)
  }

  test("SPARK-22570: Cast should not create a lot of global variables") {
    val ctx = new CodegenContext
    cast("1", IntegerType).genCode(ctx)
    cast("2", LongType).genCode(ctx)
    assert(ctx.inlinedMutableStates.length == 0)
  }

  test("up-cast") {
    def isCastSafe(from: NumericType, to: NumericType): Boolean = (from, to) match {
      case (_, dt: DecimalType) => dt.isWiderThan(from)
      case (dt: DecimalType, _) => dt.isTighterThan(to)
      case _ => numericPrecedence.indexOf(from) <= numericPrecedence.indexOf(to)
    }

    def makeComplexTypes(dt: NumericType, nullable: Boolean): Seq[DataType] = {
      Seq(
        new StructType().add("a", dt, nullable).add("b", dt, nullable),
        ArrayType(dt, nullable),
        MapType(dt, dt, nullable),
        ArrayType(new StructType().add("a", dt, nullable), nullable),
        new StructType().add("a", ArrayType(dt, nullable), nullable)
      )
    }

    import DataTypeTestUtils._
    numericTypes.foreach { from =>
      val (safeTargetTypes, unsafeTargetTypes) = numericTypes.partition(to => isCastSafe(from, to))

      safeTargetTypes.foreach { to =>
        assert(Cast.canUpCast(from, to), s"It should be possible to up-cast $from to $to")

        // If the nullability is compatible, we can up-cast complex types too.
        Seq(true -> true, false -> false, false -> true).foreach { case (fn, tn) =>
          makeComplexTypes(from, fn).zip(makeComplexTypes(to, tn)).foreach {
            case (complexFromType, complexToType) =>
              assert(Cast.canUpCast(complexFromType, complexToType))
          }
        }

        makeComplexTypes(from, true).zip(makeComplexTypes(to, false)).foreach {
          case (complexFromType, complexToType) =>
            assert(!Cast.canUpCast(complexFromType, complexToType))
        }
      }

      unsafeTargetTypes.foreach { to =>
        assert(!Cast.canUpCast(from, to), s"It shouldn't be possible to up-cast $from to $to")
        makeComplexTypes(from, true).zip(makeComplexTypes(to, true)).foreach {
          case (complexFromType, complexToType) =>
            assert(!Cast.canUpCast(complexFromType, complexToType))
        }
      }
    }
    numericTypes.foreach { dt =>
      makeComplexTypes(dt, true).foreach { complexType =>
        assert(!Cast.canUpCast(complexType, StringType))
      }
    }

    atomicTypes.foreach { atomicType =>
      assert(Cast.canUpCast(NullType, atomicType))
    }
  }

  test("SPARK-27671: cast from nested null type in struct") {
    import DataTypeTestUtils._

    atomicTypes.foreach { atomicType =>
      val struct = Literal.create(
        InternalRow(null),
        StructType(Seq(StructField("a", NullType, nullable = true))))

      val ret = cast(struct, StructType(Seq(
        StructField("a", atomicType, nullable = true))))
      assert(ret.resolved)
      checkEvaluation(ret, InternalRow(null))
    }
  }

  test("Process Infinity, -Infinity, NaN in case insensitive manner") {
    Seq("inf", "+inf", "infinity", "+infiNity", " infinity ").foreach { value =>
      checkEvaluation(cast(value, FloatType), Float.PositiveInfinity)
    }
    Seq("-infinity", "-infiniTy", "  -infinity  ", "  -inf  ").foreach { value =>
      checkEvaluation(cast(value, FloatType), Float.NegativeInfinity)
    }
    Seq("inf", "+inf", "infinity", "+infiNity", " infinity ").foreach { value =>
      checkEvaluation(cast(value, DoubleType), Double.PositiveInfinity)
    }
    Seq("-infinity", "-infiniTy", "  -infinity  ", "  -inf  ").foreach { value =>
      checkEvaluation(cast(value, DoubleType), Double.NegativeInfinity)
    }
    Seq("nan", "nAn", " nan ").foreach { value =>
      checkEvaluation(cast(value, FloatType), Float.NaN)
    }
    Seq("nan", "nAn", " nan ").foreach { value =>
      checkEvaluation(cast(value, DoubleType), Double.NaN)
    }
  }
}

abstract class AnsiCastSuiteBase extends CastSuiteBase {

  private def testIntMaxAndMin(dt: DataType): Unit = {
    assert(Seq(IntegerType, ShortType, ByteType).contains(dt))
    Seq(Int.MaxValue + 1L, Int.MinValue - 1L).foreach { value =>
      checkExceptionInExpression[ArithmeticException](cast(value, dt), "overflow")
      checkExceptionInExpression[ArithmeticException](cast(Decimal(value.toString), dt), "overflow")
      checkExceptionInExpression[ArithmeticException](
        cast(Literal(value * 1.5f, FloatType), dt), "overflow")
      checkExceptionInExpression[ArithmeticException](
        cast(Literal(value * 1.0, DoubleType), dt), "overflow")
    }
  }

  private def testLongMaxAndMin(dt: DataType): Unit = {
    assert(Seq(LongType, IntegerType).contains(dt))
    Seq(Decimal(Long.MaxValue) + Decimal(1), Decimal(Long.MinValue) - Decimal(1)).foreach { value =>
      checkExceptionInExpression[ArithmeticException](
        cast(value, dt), "overflow")
      checkExceptionInExpression[ArithmeticException](
        cast((value * Decimal(1.1)).toFloat, dt), "overflow")
      checkExceptionInExpression[ArithmeticException](
        cast((value * Decimal(1.1)).toDouble, dt), "overflow")
    }
  }

  test("ANSI mode: Throw exception on casting out-of-range value to byte type") {
    testIntMaxAndMin(ByteType)
    Seq(Byte.MaxValue + 1, Byte.MinValue - 1).foreach { value =>
      checkExceptionInExpression[ArithmeticException](cast(value, ByteType), "overflow")
      checkExceptionInExpression[ArithmeticException](
        cast(Literal(value.toFloat, FloatType), ByteType), "overflow")
      checkExceptionInExpression[ArithmeticException](
        cast(Literal(value.toDouble, DoubleType), ByteType), "overflow")
    }

    Seq(Byte.MaxValue, 0.toByte, Byte.MinValue).foreach { value =>
      checkEvaluation(cast(value, ByteType), value)
      checkEvaluation(cast(value.toString, ByteType), value)
      checkEvaluation(cast(Decimal(value.toString), ByteType), value)
      checkEvaluation(cast(Literal(value.toFloat, FloatType), ByteType), value)
      checkEvaluation(cast(Literal(value.toDouble, DoubleType), ByteType), value)
    }
  }

  test("ANSI mode: Throw exception on casting out-of-range value to short type") {
    testIntMaxAndMin(ShortType)
    Seq(Short.MaxValue + 1, Short.MinValue - 1).foreach { value =>
      checkExceptionInExpression[ArithmeticException](cast(value, ShortType), "overflow")
      checkExceptionInExpression[ArithmeticException](
        cast(Literal(value.toFloat, FloatType), ShortType), "overflow")
      checkExceptionInExpression[ArithmeticException](
        cast(Literal(value.toDouble, DoubleType), ShortType), "overflow")
    }

    Seq(Short.MaxValue, 0.toShort, Short.MinValue).foreach { value =>
      checkEvaluation(cast(value, ShortType), value)
      checkEvaluation(cast(value.toString, ShortType), value)
      checkEvaluation(cast(Decimal(value.toString), ShortType), value)
      checkEvaluation(cast(Literal(value.toFloat, FloatType), ShortType), value)
      checkEvaluation(cast(Literal(value.toDouble, DoubleType), ShortType), value)
    }
  }

  test("ANSI mode: Throw exception on casting out-of-range value to int type") {
    testIntMaxAndMin(IntegerType)
    testLongMaxAndMin(IntegerType)

    Seq(Int.MaxValue, 0, Int.MinValue).foreach { value =>
      checkEvaluation(cast(value, IntegerType), value)
      checkEvaluation(cast(value.toString, IntegerType), value)
      checkEvaluation(cast(Decimal(value.toString), IntegerType), value)
      checkEvaluation(cast(Literal(value * 1.0, DoubleType), IntegerType), value)
    }
    checkEvaluation(cast(Int.MaxValue + 0.9D, IntegerType), Int.MaxValue)
    checkEvaluation(cast(Int.MinValue - 0.9D, IntegerType), Int.MinValue)
  }

  test("ANSI mode: Throw exception on casting out-of-range value to long type") {
    testLongMaxAndMin(LongType)

    Seq(Long.MaxValue, 0, Long.MinValue).foreach { value =>
      checkEvaluation(cast(value, LongType), value)
      checkEvaluation(cast(value.toString, LongType), value)
      checkEvaluation(cast(Decimal(value.toString), LongType), value)
    }
    checkEvaluation(cast(Long.MaxValue + 0.9F, LongType), Long.MaxValue)
    checkEvaluation(cast(Long.MinValue - 0.9F, LongType), Long.MinValue)
    checkEvaluation(cast(Long.MaxValue + 0.9D, LongType), Long.MaxValue)
    checkEvaluation(cast(Long.MinValue - 0.9D, LongType), Long.MinValue)
  }

  test("ANSI mode: Throw exception on casting out-of-range value to decimal type") {
    checkExceptionInExpression[ArithmeticException](
      cast(Literal("134.12"), DecimalType(3, 2)), "cannot be represented")
    checkExceptionInExpression[ArithmeticException](
      cast(Literal(BigDecimal(134.12)), DecimalType(3, 2)), "cannot be represented")
    checkExceptionInExpression[ArithmeticException](
      cast(Literal(134.12), DecimalType(3, 2)), "cannot be represented")
  }

  protected def setConfigurationHint: String

  private def verifyCastFailure(c: CastBase, optionalExpectedMsg: Option[String] = None): Unit = {
    val typeCheckResult = c.checkInputDataTypes()
    assert(typeCheckResult.isFailure)
    assert(typeCheckResult.isInstanceOf[TypeCheckFailure])
    val message = typeCheckResult.asInstanceOf[TypeCheckFailure].message

    if (optionalExpectedMsg.isDefined) {
      assert(message.contains(optionalExpectedMsg.get))
    } else {
      assert(message.contains("with ANSI mode on"))
      assert(message.contains(setConfigurationHint))
    }
  }

  test("ANSI mode: disallow type conversions between Numeric types and Timestamp type") {
    import DataTypeTestUtils.numericTypes
    checkInvalidCastFromNumericType(TimestampType)
    var errorMsg =
      "you can use functions TIMESTAMP_SECONDS/TIMESTAMP_MILLIS/TIMESTAMP_MICROS instead"
    verifyCastFailure(cast(Literal(0L), TimestampType), Some(errorMsg))

    val timestampLiteral = Literal(1L, TimestampType)
    errorMsg = "you can use functions UNIX_SECONDS/UNIX_MILLIS/UNIX_MICROS instead."
    numericTypes.foreach { numericType =>
      verifyCastFailure(cast(timestampLiteral, numericType), Some(errorMsg))
    }
  }

  test("ANSI mode: disallow type conversions between Numeric types and Date type") {
    import DataTypeTestUtils.numericTypes
    checkInvalidCastFromNumericType(DateType)
    var errorMsg = "you can use function DATE_FROM_UNIX_DATE instead"
    verifyCastFailure(cast(Literal(0L), DateType), Some(errorMsg))
    val dateLiteral = Literal(1, DateType)
    errorMsg = "you can use function UNIX_DATE instead"
    numericTypes.foreach { numericType =>
      verifyCastFailure(cast(dateLiteral, numericType), Some(errorMsg))
    }
  }

  test("ANSI mode: disallow type conversions between Numeric types and Binary type") {
    import DataTypeTestUtils.numericTypes
    checkInvalidCastFromNumericType(BinaryType)
    val binaryLiteral = Literal(new Array[Byte](1.toByte), BinaryType)
    numericTypes.foreach { numericType =>
      assert(cast(binaryLiteral, numericType).checkInputDataTypes().isFailure)
    }
  }

  test("ANSI mode: disallow type conversions between Datatime types and Boolean types") {
    val timestampLiteral = Literal(1L, TimestampType)
    assert(cast(timestampLiteral, BooleanType).checkInputDataTypes().isFailure)
    val dateLiteral = Literal(1, DateType)
    assert(cast(dateLiteral, BooleanType).checkInputDataTypes().isFailure)

    val booleanLiteral = Literal(true, BooleanType)
    assert(cast(booleanLiteral, TimestampType).checkInputDataTypes().isFailure)
    assert(cast(booleanLiteral, DateType).checkInputDataTypes().isFailure)
  }

  test("ANSI mode: disallow casting complex types as String type") {
    verifyCastFailure(cast(Literal.create(Array(1, 2, 3, 4, 5)), StringType))
    verifyCastFailure(cast(Literal.create(Map(1 -> "a")), StringType))
    verifyCastFailure(cast(Literal.create((1, "a", 0.1)), StringType))
  }

  test("cast from invalid string to numeric should throw NumberFormatException") {
    // cast to IntegerType
    Seq(IntegerType, ShortType, ByteType, LongType).foreach { dataType =>
      val array = Literal.create(Seq("123", "true", "f", null),
        ArrayType(StringType, containsNull = true))
      checkExceptionInExpression[NumberFormatException](
        cast(array, ArrayType(dataType, containsNull = true)),
        "invalid input syntax for type numeric: true")
      checkExceptionInExpression[NumberFormatException](
        cast("string", dataType), "invalid input syntax for type numeric: string")
      checkExceptionInExpression[NumberFormatException](
        cast("123-string", dataType), "invalid input syntax for type numeric: 123-string")
      checkExceptionInExpression[NumberFormatException](
        cast("2020-07-19", dataType), "invalid input syntax for type numeric: 2020-07-19")
      checkExceptionInExpression[NumberFormatException](
        cast("1.23", dataType), "invalid input syntax for type numeric: 1.23")
    }

    Seq(DoubleType, FloatType, DecimalType.USER_DEFAULT).foreach { dataType =>
      checkExceptionInExpression[NumberFormatException](
        cast("string", dataType), "invalid input syntax for type numeric: string")
      checkExceptionInExpression[NumberFormatException](
        cast("123.000.00", dataType), "invalid input syntax for type numeric: 123.000.00")
      checkExceptionInExpression[NumberFormatException](
        cast("abc.com", dataType), "invalid input syntax for type numeric: abc.com")
    }
  }

  test("Fast fail for cast string type to decimal type in ansi mode") {
    checkEvaluation(cast("12345678901234567890123456789012345678", DecimalType(38, 0)),
      Decimal("12345678901234567890123456789012345678"))
    checkExceptionInExpression[ArithmeticException](
      cast("123456789012345678901234567890123456789", DecimalType(38, 0)),
      "out of decimal type range")
    checkExceptionInExpression[ArithmeticException](
      cast("12345678901234567890123456789012345678", DecimalType(38, 1)),
      "cannot be represented as Decimal(38, 1)")

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
    checkExceptionInExpression[ArithmeticException](
      cast("6E+38", DecimalType(38, 0)),
      "out of decimal type range")
    checkExceptionInExpression[ArithmeticException](
      cast("6E+37", DecimalType(38, 1)),
      "cannot be represented as Decimal(38, 1)")

    checkExceptionInExpression[NumberFormatException](
      cast("abcd", DecimalType(38, 1)),
      "invalid input syntax for type numeric")
  }

  test("ANSI mode: cast string to timestamp with parse error") {
    ALL_TIMEZONES.foreach { zid =>
      def checkCastWithParseError(str: String): Unit = {
        checkExceptionInExpression[DateTimeException](
          cast(Literal(str), TimestampType, Option(zid.getId)),
          s"Cannot cast $str to TimestampType.")
      }

      checkCastWithParseError("123")
      checkCastWithParseError("2015-03-18 123142")
      checkCastWithParseError("2015-03-18T123123")
      checkCastWithParseError("2015-03-18X")
      checkCastWithParseError("2015/03/18")
      checkCastWithParseError("2015.03.18")
      checkCastWithParseError("20150318")
      checkCastWithParseError("2015-031-8")
      checkCastWithParseError("2015-03-18T12:03:17-0:70")
      checkCastWithParseError("abdef")
    }
  }

  test("SPARK-26218: Fix the corner case of codegen when casting float to Integer") {
    checkExceptionInExpression[ArithmeticException](
      cast(cast(Literal("2147483648"), FloatType), IntegerType), "overflow")
  }
}

/**
 * Test suite for data type casting expression [[Cast]].
 */
class CastSuite extends CastSuiteBase {

  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase = {
    v match {
      case lit: Expression => Cast(lit, targetType, timeZoneId)
      case _ => Cast(Literal(v), targetType, timeZoneId)
    }
  }

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

  test("cast from timestamp") {
    val millis = 15 * 1000 + 3
    val seconds = millis * 1000 + 3
    val ts = new Timestamp(millis)
    val tss = new Timestamp(seconds)
    checkEvaluation(cast(ts, ShortType), 15.toShort)
    checkEvaluation(cast(ts, IntegerType), 15)
    checkEvaluation(cast(ts, LongType), 15.toLong)
    checkEvaluation(cast(ts, FloatType), 15.003f)
    checkEvaluation(cast(ts, DoubleType), 15.003)

    checkEvaluation(cast(cast(tss, ShortType), TimestampType),
      fromJavaTimestamp(ts) * MILLIS_PER_SECOND)
    checkEvaluation(cast(cast(tss, IntegerType), TimestampType),
      fromJavaTimestamp(ts) * MILLIS_PER_SECOND)
    checkEvaluation(cast(cast(tss, LongType), TimestampType),
      fromJavaTimestamp(ts) * MILLIS_PER_SECOND)
    checkEvaluation(
      cast(cast(millis.toFloat / MILLIS_PER_SECOND, TimestampType), FloatType),
      millis.toFloat / MILLIS_PER_SECOND)
    checkEvaluation(
      cast(cast(millis.toDouble / MILLIS_PER_SECOND, TimestampType), DoubleType),
      millis.toDouble / MILLIS_PER_SECOND)
    checkEvaluation(
      cast(cast(Decimal(1), TimestampType), DecimalType.SYSTEM_DEFAULT),
      Decimal(1))

    // A test for higher precision than millis
    checkEvaluation(cast(cast(0.000001, TimestampType), DoubleType), 0.000001)

    checkEvaluation(cast(Double.NaN, TimestampType), null)
    checkEvaluation(cast(1.0 / 0.0, TimestampType), null)
    checkEvaluation(cast(Float.NaN, TimestampType), null)
    checkEvaluation(cast(1.0f / 0.0f, TimestampType), null)
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

  test("SPARK-22825 Cast array to string") {
    val ret1 = cast(Literal.create(Array(1, 2, 3, 4, 5)), StringType)
    checkEvaluation(ret1, "[1, 2, 3, 4, 5]")
    val ret2 = cast(Literal.create(Array("ab", "cde", "f")), StringType)
    checkEvaluation(ret2, "[ab, cde, f]")
    Seq(false, true).foreach { omitNull =>
      withSQLConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING.key -> omitNull.toString) {
        val ret3 = cast(Literal.create(Array("ab", null, "c")), StringType)
        checkEvaluation(ret3, s"[ab,${if (omitNull) "" else " null"}, c]")
      }
    }
    val ret4 =
      cast(Literal.create(Array("ab".getBytes, "cde".getBytes, "f".getBytes)), StringType)
    checkEvaluation(ret4, "[ab, cde, f]")
    val ret5 = cast(
      Literal.create(Array("2014-12-03", "2014-12-04", "2014-12-06").map(Date.valueOf)),
      StringType)
    checkEvaluation(ret5, "[2014-12-03, 2014-12-04, 2014-12-06]")
    val ret6 = cast(
      Literal.create(Array("2014-12-03 13:01:00", "2014-12-04 15:05:00")
        .map(Timestamp.valueOf)),
      StringType)
    checkEvaluation(ret6, "[2014-12-03 13:01:00, 2014-12-04 15:05:00]")
    val ret7 = cast(Literal.create(Array(Array(1, 2, 3), Array(4, 5))), StringType)
    checkEvaluation(ret7, "[[1, 2, 3], [4, 5]]")
    val ret8 = cast(
      Literal.create(Array(Array(Array("a"), Array("b", "c")), Array(Array("d")))),
      StringType)
    checkEvaluation(ret8, "[[[a], [b, c]], [[d]]]")
  }

  test("SPARK-33291: Cast array with null elements to string") {
    Seq(false, true).foreach { omitNull =>
      withSQLConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING.key -> omitNull.toString) {
        val ret1 = cast(Literal.create(Array(null, null)), StringType)
        checkEvaluation(
          ret1,
          s"[${if (omitNull) "" else "null"},${if (omitNull) "" else " null"}]")
      }
    }
  }

  test("SPARK-22973 Cast map to string") {
    Seq(
      false -> ("{", "}"),
      true -> ("[", "]")).foreach { case (legacyCast, (lb, rb)) =>
      withSQLConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING.key -> legacyCast.toString) {
        val ret1 = cast(Literal.create(Map(1 -> "a", 2 -> "b", 3 -> "c")), StringType)
        checkEvaluation(ret1, s"${lb}1 -> a, 2 -> b, 3 -> c$rb")
        val ret2 = cast(
          Literal.create(Map("1" -> "a".getBytes, "2" -> null, "3" -> "c".getBytes)),
          StringType)
        checkEvaluation(ret2, s"${lb}1 -> a, 2 ->${if (legacyCast) "" else " null"}, 3 -> c$rb")
        val ret3 = cast(
          Literal.create(Map(
            1 -> Date.valueOf("2014-12-03"),
            2 -> Date.valueOf("2014-12-04"),
            3 -> Date.valueOf("2014-12-05"))),
          StringType)
        checkEvaluation(ret3, s"${lb}1 -> 2014-12-03, 2 -> 2014-12-04, 3 -> 2014-12-05$rb")
        val ret4 = cast(
          Literal.create(Map(
            1 -> Timestamp.valueOf("2014-12-03 13:01:00"),
            2 -> Timestamp.valueOf("2014-12-04 15:05:00"))),
          StringType)
        checkEvaluation(ret4, s"${lb}1 -> 2014-12-03 13:01:00, 2 -> 2014-12-04 15:05:00$rb")
        val ret5 = cast(
          Literal.create(Map(
            1 -> Array(1, 2, 3),
            2 -> Array(4, 5, 6))),
          StringType)
        checkEvaluation(ret5, s"${lb}1 -> [1, 2, 3], 2 -> [4, 5, 6]$rb")
      }
    }
  }

  test("SPARK-22981 Cast struct to string") {
    Seq(
      false -> ("{", "}"),
      true -> ("[", "]")).foreach { case (legacyCast, (lb, rb)) =>
      withSQLConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING.key -> legacyCast.toString) {
        val ret1 = cast(Literal.create((1, "a", 0.1)), StringType)
        checkEvaluation(ret1, s"${lb}1, a, 0.1$rb")
        val ret2 = cast(Literal.create(Tuple3[Int, String, String](1, null, "a")), StringType)
        checkEvaluation(ret2, s"${lb}1,${if (legacyCast) "" else " null"}, a$rb")
        val ret3 = cast(Literal.create(
          (Date.valueOf("2014-12-03"), Timestamp.valueOf("2014-12-03 15:05:00"))), StringType)
        checkEvaluation(ret3, s"${lb}2014-12-03, 2014-12-03 15:05:00$rb")
        val ret4 = cast(Literal.create(((1, "a"), 5, 0.1)), StringType)
        checkEvaluation(ret4, s"$lb${lb}1, a$rb, 5, 0.1$rb")
        val ret5 = cast(Literal.create((Seq(1, 2, 3), "a", 0.1)), StringType)
        checkEvaluation(ret5, s"$lb[1, 2, 3], a, 0.1$rb")
        val ret6 = cast(Literal.create((1, Map(1 -> "a", 2 -> "b", 3 -> "c"))), StringType)
        checkEvaluation(ret6, s"${lb}1, ${lb}1 -> a, 2 -> b, 3 -> c$rb$rb")
      }
    }
  }

  test("SPARK-33291: Cast struct with null elements to string") {
    Seq(
      false -> ("{", "}"),
      true -> ("[", "]")).foreach { case (legacyCast, (lb, rb)) =>
      withSQLConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING.key -> legacyCast.toString) {
        val ret1 = cast(Literal.create(Tuple2[String, String](null, null)), StringType)
        checkEvaluation(
          ret1,
          s"$lb${if (legacyCast) "" else "null"},${if (legacyCast) "" else " null"}$rb")
      }
    }
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
}

/**
 * Test suite for data type casting expression [[Cast]] with ANSI mode disabled.
 */
class CastSuiteWithAnsiModeOn extends AnsiCastSuiteBase {
  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConf(SQLConf.ANSI_ENABLED, true)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.unsetConf(SQLConf.ANSI_ENABLED)
  }

  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase = {
    v match {
      case lit: Expression => Cast(lit, targetType, timeZoneId)
      case _ => Cast(Literal(v), targetType, timeZoneId)
    }
  }

  override def setConfigurationHint: String =
    s"set ${SQLConf.ANSI_ENABLED.key} as false"
}

/**
 * Test suite for data type casting expression [[AnsiCast]] with ANSI mode enabled.
 */
class AnsiCastSuiteWithAnsiModeOn extends AnsiCastSuiteBase {
  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConf(SQLConf.ANSI_ENABLED, true)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.unsetConf(SQLConf.ANSI_ENABLED)
  }

  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase = {
    v match {
      case lit: Expression => AnsiCast(lit, targetType, timeZoneId)
      case _ => AnsiCast(Literal(v), targetType, timeZoneId)
    }
  }

  override def setConfigurationHint: String =
    s"set ${SQLConf.STORE_ASSIGNMENT_POLICY.key} as" +
      s" ${SQLConf.StoreAssignmentPolicy.LEGACY.toString}"
}

/**
 * Test suite for data type casting expression [[AnsiCast]] with ANSI mode disabled.
 */
class AnsiCastSuiteWithAnsiModeOff extends AnsiCastSuiteBase {
  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConf(SQLConf.ANSI_ENABLED, false)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.unsetConf(SQLConf.ANSI_ENABLED)
  }

  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase = {
    v match {
      case lit: Expression => AnsiCast(lit, targetType, timeZoneId)
      case _ => AnsiCast(Literal(v), targetType, timeZoneId)
    }
  }

  override def setConfigurationHint: String =
    s"set ${SQLConf.STORE_ASSIGNMENT_POLICY.key} as" +
      s" ${SQLConf.StoreAssignmentPolicy.LEGACY.toString}"
}
