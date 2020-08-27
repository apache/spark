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
import java.util.{Calendar, Locale, TimeZone}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.numericPrecedence
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneGMT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test suite for data type casting expression [[Cast]].
 */
class CastSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): Cast = {
    v match {
      case lit: Expression => Cast(lit, targetType, timeZoneId)
      case _ => Cast(Literal(v), targetType, timeZoneId)
    }
  }

  // expected cannot be null
  private def checkCast(v: Any, expected: Any): Unit = {
    checkEvaluation(cast(v, Literal(expected).dataType), expected)
  }

  private def checkNullCast(from: DataType, to: DataType): Unit = {
    checkEvaluation(cast(Literal.create(null, from), to, Option("GMT")), null)
  }

  test("null cast") {
    import DataTypeTestUtils._

    // follow [[org.apache.spark.sql.catalyst.expressions.Cast.canCast]] logic
    // to ensure we test every possible cast situation here
    atomicTypes.zip(atomicTypes).foreach { case (from, to) =>
      checkNullCast(from, to)
    }

    atomicTypes.foreach(dt => checkNullCast(NullType, dt))
    atomicTypes.foreach(dt => checkNullCast(dt, StringType))
    checkNullCast(StringType, BinaryType)
    checkNullCast(StringType, BooleanType)
    checkNullCast(DateType, BooleanType)
    checkNullCast(TimestampType, BooleanType)
    numericTypes.foreach(dt => checkNullCast(dt, BooleanType))

    checkNullCast(StringType, TimestampType)
    checkNullCast(BooleanType, TimestampType)
    checkNullCast(DateType, TimestampType)
    numericTypes.foreach(dt => checkNullCast(dt, TimestampType))

    checkNullCast(StringType, DateType)
    checkNullCast(TimestampType, DateType)

    checkNullCast(StringType, CalendarIntervalType)
    numericTypes.foreach(dt => checkNullCast(StringType, dt))
    numericTypes.foreach(dt => checkNullCast(BooleanType, dt))
    numericTypes.foreach(dt => checkNullCast(DateType, dt))
    numericTypes.foreach(dt => checkNullCast(TimestampType, dt))
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
    for (tz <- ALL_TIMEZONES) {
      def checkCastStringToTimestamp(str: String, expected: Timestamp): Unit = {
        checkEvaluation(cast(Literal(str), TimestampType, Option(tz.getID)), expected)
      }

      checkCastStringToTimestamp("123", null)

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
      c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
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
      c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
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

      checkCastStringToTimestamp("2015-03-18 123142", null)
      checkCastStringToTimestamp("2015-03-18T123123", null)
      checkCastStringToTimestamp("2015-03-18X", null)
      checkCastStringToTimestamp("2015/03/18", null)
      checkCastStringToTimestamp("2015.03.18", null)
      checkCastStringToTimestamp("20150318", null)
      checkCastStringToTimestamp("2015-031-8", null)
      checkCastStringToTimestamp("2015-03-18T12:03:17-0:70", null)
    }
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
    checkEvaluation(cast(123, DecimalType(3, 1)), null)
    checkEvaluation(cast(123, DecimalType(2, 0)), null)
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
    checkEvaluation(cast(123L, DecimalType(3, 1)), null)

    checkEvaluation(cast(123L, DecimalType(2, 0)), null)
  }

  test("cast from boolean") {
    checkEvaluation(cast(true, IntegerType), 1)
    checkEvaluation(cast(false, IntegerType), 0)
    checkEvaluation(cast(true, StringType), "true")
    checkEvaluation(cast(false, StringType), "false")
    checkEvaluation(cast(cast(1, BooleanType), IntegerType), 1)
    checkEvaluation(cast(cast(0, BooleanType), IntegerType), 0)
  }

  test("cast from int 2") {
    checkEvaluation(cast(1, LongType), 1.toLong)
    checkEvaluation(cast(cast(1000, TimestampType), LongType), 1000.toLong)
    checkEvaluation(cast(cast(-1200, TimestampType), LongType), -1200.toLong)

    checkEvaluation(cast(123, DecimalType.USER_DEFAULT), Decimal(123))
    checkEvaluation(cast(123, DecimalType(3, 0)), Decimal(123))
    checkEvaluation(cast(123, DecimalType(3, 1)), null)
    checkEvaluation(cast(123, DecimalType(2, 0)), null)
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

    checkEvaluation(cast(cast(1.toDouble, TimestampType), DoubleType), 1.toDouble)
  }

  test("cast from string") {
    assert(cast("abcdef", StringType).nullable === false)
    assert(cast("abcdef", BinaryType).nullable === false)
    assert(cast("abcdef", BooleanType).nullable === true)
    assert(cast("abcdef", TimestampType).nullable === true)
    assert(cast("abcdef", LongType).nullable === true)
    assert(cast("abcdef", IntegerType).nullable === true)
    assert(cast("abcdef", ShortType).nullable === true)
    assert(cast("abcdef", ByteType).nullable === true)
    assert(cast("abcdef", DecimalType.USER_DEFAULT).nullable === true)
    assert(cast("abcdef", DecimalType(4, 2)).nullable === true)
    assert(cast("abcdef", DoubleType).nullable === true)
    assert(cast("abcdef", FloatType).nullable === true)
  }

  test("data type casting") {
    val sd = "1970-01-01"
    val d = Date.valueOf(sd)
    val zts = sd + " 00:00:00"
    val sts = sd + " 00:00:02"
    val nts = sts + ".1"
    val ts = withDefaultTimeZone(TimeZoneGMT)(Timestamp.valueOf(nts))

    for (tz <- ALL_TIMEZONES) {
      val timeZoneId = Option(tz.getID)
      var c = Calendar.getInstance(TimeZoneGMT)
      c.set(2015, 2, 8, 2, 30, 0)
      checkEvaluation(
        cast(cast(new Timestamp(c.getTimeInMillis), StringType, timeZoneId),
          TimestampType, timeZoneId),
        c.getTimeInMillis * 1000)
      c = Calendar.getInstance(TimeZoneGMT)
      c.set(2015, 10, 1, 2, 30, 0)
      checkEvaluation(
        cast(cast(new Timestamp(c.getTimeInMillis), StringType, timeZoneId),
          TimestampType, timeZoneId),
        c.getTimeInMillis * 1000)
    }

    val gmtId = Option("GMT")

    checkEvaluation(cast("abdef", StringType), "abdef")
    checkEvaluation(cast("abdef", DecimalType.USER_DEFAULT), null)
    checkEvaluation(cast("abdef", TimestampType, gmtId), null)
    checkEvaluation(cast("12.65", DecimalType.SYSTEM_DEFAULT), Decimal(12.65))

    checkEvaluation(cast(cast(sd, DateType), StringType), sd)
    checkEvaluation(cast(cast(d, StringType), DateType), 0)
    checkEvaluation(cast(cast(nts, TimestampType, gmtId), StringType, gmtId), nts)
    checkEvaluation(
      cast(cast(ts, StringType, gmtId), TimestampType, gmtId),
      DateTimeUtils.fromJavaTimestamp(ts))

    // all convert to string type to check
    checkEvaluation(cast(cast(cast(nts, TimestampType, gmtId), DateType, gmtId), StringType), sd)
    checkEvaluation(
      cast(cast(cast(ts, DateType, gmtId), TimestampType, gmtId), StringType, gmtId),
      zts)

    checkEvaluation(cast(cast("abdef", BinaryType), StringType), "abdef")

    checkEvaluation(cast(cast(cast(cast(
      cast(cast("5", ByteType), ShortType), IntegerType), FloatType), DoubleType), LongType),
      5.toLong)
    checkEvaluation(
      cast(cast(cast(cast(cast(cast("5", ByteType), TimestampType),
        DecimalType.SYSTEM_DEFAULT), LongType), StringType), ShortType),
      5.toShort)
    checkEvaluation(
      cast(cast(cast(cast(cast(cast("5", TimestampType, gmtId), ByteType),
        DecimalType.SYSTEM_DEFAULT), LongType), StringType), ShortType),
      null)
    checkEvaluation(cast(cast(cast(cast(cast(cast("5", DecimalType.SYSTEM_DEFAULT),
      ByteType), TimestampType), LongType), StringType), ShortType),
      5.toShort)

    checkEvaluation(cast("23", DoubleType), 23d)
    checkEvaluation(cast("23", IntegerType), 23)
    checkEvaluation(cast("23", FloatType), 23f)
    checkEvaluation(cast("23", DecimalType.USER_DEFAULT), Decimal(23))
    checkEvaluation(cast("23", ByteType), 23.toByte)
    checkEvaluation(cast("23", ShortType), 23.toShort)
    checkEvaluation(cast("2012-12-11", DoubleType), null)
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

  test("casting to fixed-precision decimals") {
    assert(cast(123, DecimalType.USER_DEFAULT).nullable === false)
    assert(cast(10.03f, DecimalType.SYSTEM_DEFAULT).nullable === true)
    assert(cast(10.03, DecimalType.SYSTEM_DEFAULT).nullable === true)
    assert(cast(Decimal(10.03), DecimalType.SYSTEM_DEFAULT).nullable === false)

    assert(cast(123, DecimalType(2, 1)).nullable === true)
    assert(cast(10.03f, DecimalType(2, 1)).nullable === true)
    assert(cast(10.03, DecimalType(2, 1)).nullable === true)
    assert(cast(Decimal(10.03), DecimalType(2, 1)).nullable === true)

    assert(cast(123, DecimalType.IntDecimal).nullable === false)
    assert(cast(10.03f, DecimalType.FloatDecimal).nullable === true)
    assert(cast(10.03, DecimalType.DoubleDecimal).nullable === true)
    assert(cast(Decimal(10.03), DecimalType(4, 2)).nullable === false)
    assert(cast(Decimal(10.03), DecimalType(5, 3)).nullable === false)

    assert(cast(Decimal(10.03), DecimalType(3, 1)).nullable === true)
    assert(cast(Decimal(10.03), DecimalType(4, 1)).nullable === false)
    assert(cast(Decimal(9.95), DecimalType(2, 1)).nullable === true)
    assert(cast(Decimal(9.95), DecimalType(3, 1)).nullable === false)

    assert(cast(Decimal("1003"), DecimalType(3, -1)).nullable === true)
    assert(cast(Decimal("1003"), DecimalType(4, -1)).nullable === false)
    assert(cast(Decimal("995"), DecimalType(2, -1)).nullable === true)
    assert(cast(Decimal("995"), DecimalType(3, -1)).nullable === false)

    assert(cast(true, DecimalType.SYSTEM_DEFAULT).nullable === false)
    assert(cast(true, DecimalType(1, 1)).nullable === true)


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
    checkEvaluation(cast(Decimal("1003"), DecimalType(3, -1)), Decimal(1000))
    checkEvaluation(cast(Decimal("1003"), DecimalType(2, -2)), Decimal(1000))
    checkEvaluation(cast(Decimal("1003"), DecimalType(1, -2)), null)
    checkEvaluation(cast(Decimal("1003"), DecimalType(2, -1)), null)
    checkEvaluation(cast(Decimal("1003"), DecimalType(3, 0)), null)

    checkEvaluation(cast(Decimal("995"), DecimalType(3, 0)), Decimal(995))
    checkEvaluation(cast(Decimal("995"), DecimalType(3, -1)), Decimal(1000))
    checkEvaluation(cast(Decimal("995"), DecimalType(2, -2)), Decimal(1000))
    checkEvaluation(cast(Decimal("995"), DecimalType(2, -1)), null)
    checkEvaluation(cast(Decimal("995"), DecimalType(1, -2)), null)

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

    val gmtId = Option("GMT")
    checkEvaluation(cast(cast(d, TimestampType, gmtId), StringType, gmtId), "1970-01-01 00:00:00")
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
      DateTimeUtils.fromJavaTimestamp(ts) * 1000)
    checkEvaluation(cast(cast(tss, IntegerType), TimestampType),
      DateTimeUtils.fromJavaTimestamp(ts) * 1000)
    checkEvaluation(cast(cast(tss, LongType), TimestampType),
      DateTimeUtils.fromJavaTimestamp(ts) * 1000)
    checkEvaluation(
      cast(cast(millis.toFloat / 1000, TimestampType), FloatType),
      millis.toFloat / 1000)
    checkEvaluation(
      cast(cast(millis.toDouble / 1000, TimestampType), DoubleType),
      millis.toDouble / 1000)
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

  test("cast from array") {
    val array = Literal.create(Seq("123", "true", "f", null),
      ArrayType(StringType, containsNull = true))
    val array_notNull = Literal.create(Seq("123", "true", "f"),
      ArrayType(StringType, containsNull = false))

    checkNullCast(ArrayType(StringType), ArrayType(IntegerType))

    {
      val ret = cast(array, ArrayType(IntegerType, containsNull = true))
      assert(ret.resolved === true)
      checkEvaluation(ret, Seq(123, null, null, null))
    }
    {
      val ret = cast(array, ArrayType(IntegerType, containsNull = false))
      assert(ret.resolved === false)
    }
    {
      val ret = cast(array, ArrayType(BooleanType, containsNull = true))
      assert(ret.resolved === true)
      checkEvaluation(ret, Seq(null, true, false, null))
    }
    {
      val ret = cast(array, ArrayType(BooleanType, containsNull = false))
      assert(ret.resolved === false)
    }

    {
      val ret = cast(array_notNull, ArrayType(IntegerType, containsNull = true))
      assert(ret.resolved === true)
      checkEvaluation(ret, Seq(123, null, null))
    }
    {
      val ret = cast(array_notNull, ArrayType(IntegerType, containsNull = false))
      assert(ret.resolved === false)
    }
    {
      val ret = cast(array_notNull, ArrayType(BooleanType, containsNull = true))
      assert(ret.resolved === true)
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
      val ret = cast(map, MapType(StringType, IntegerType, valueContainsNull = true))
      assert(ret.resolved === true)
      checkEvaluation(ret, Map("a" -> 123, "b" -> null, "c" -> null, "d" -> null))
    }
    {
      val ret = cast(map, MapType(StringType, IntegerType, valueContainsNull = false))
      assert(ret.resolved === false)
    }
    {
      val ret = cast(map, MapType(StringType, BooleanType, valueContainsNull = true))
      assert(ret.resolved === true)
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
      val ret = cast(map_notNull, MapType(StringType, IntegerType, valueContainsNull = true))
      assert(ret.resolved === true)
      checkEvaluation(ret, Map("a" -> 123, "b" -> null, "c" -> null))
    }
    {
      val ret = cast(map_notNull, MapType(StringType, IntegerType, valueContainsNull = false))
      assert(ret.resolved === false)
    }
    {
      val ret = cast(map_notNull, MapType(StringType, BooleanType, valueContainsNull = true))
      assert(ret.resolved === true)
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
        StructField("a", IntegerType, nullable = true),
        StructField("b", IntegerType, nullable = true),
        StructField("c", IntegerType, nullable = true),
        StructField("d", IntegerType, nullable = true))))
      assert(ret.resolved === true)
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
      val ret = cast(struct, StructType(Seq(
        StructField("a", BooleanType, nullable = true),
        StructField("b", BooleanType, nullable = true),
        StructField("c", BooleanType, nullable = true),
        StructField("d", BooleanType, nullable = true))))
      assert(ret.resolved === true)
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
        StructField("a", IntegerType, nullable = true),
        StructField("b", IntegerType, nullable = true),
        StructField("c", IntegerType, nullable = true))))
      assert(ret.resolved === true)
      checkEvaluation(ret, InternalRow(123, null, null))
    }
    {
      val ret = cast(struct_notNull, StructType(Seq(
        StructField("a", IntegerType, nullable = true),
        StructField("b", IntegerType, nullable = true),
        StructField("c", IntegerType, nullable = false))))
      assert(ret.resolved === false)
    }
    {
      val ret = cast(struct_notNull, StructType(Seq(
        StructField("a", BooleanType, nullable = true),
        StructField("b", BooleanType, nullable = true),
        StructField("c", BooleanType, nullable = true))))
      assert(ret.resolved === true)
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
    checkEvaluation(Cast(Literal("interval -3 month 7 hours"), CalendarIntervalType),
      new CalendarInterval(-3, 7 * CalendarInterval.MICROS_PER_HOUR))
    checkEvaluation(Cast(Literal.create(
      new CalendarInterval(15, -3 * CalendarInterval.MICROS_PER_DAY), CalendarIntervalType),
      StringType),
      "interval 1 years 3 months -3 days")
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

  test("SPARK-16729 type checking for casting to date type") {
    assert(cast("1234", DateType).checkInputDataTypes().isSuccess)
    assert(cast(new Timestamp(1), DateType).checkInputDataTypes().isSuccess)
    assert(cast(false, DateType).checkInputDataTypes().isFailure)
    assert(cast(1.toByte, DateType).checkInputDataTypes().isFailure)
    assert(cast(1.toShort, DateType).checkInputDataTypes().isFailure)
    assert(cast(1, DateType).checkInputDataTypes().isFailure)
    assert(cast(1L, DateType).checkInputDataTypes().isFailure)
    assert(cast(1.0.toFloat, DateType).checkInputDataTypes().isFailure)
    assert(cast(1.0, DateType).checkInputDataTypes().isFailure)
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

  test("SPARK-22825 Cast array to string") {
    val ret1 = cast(Literal.create(Array(1, 2, 3, 4, 5)), StringType)
    checkEvaluation(ret1, "[1, 2, 3, 4, 5]")
    val ret2 = cast(Literal.create(Array("ab", "cde", "f")), StringType)
    checkEvaluation(ret2, "[ab, cde, f]")
    val ret3 = cast(Literal.create(Array("ab", null, "c")), StringType)
    checkEvaluation(ret3, "[ab,, c]")
    val ret4 = cast(Literal.create(Array("ab".getBytes, "cde".getBytes, "f".getBytes)), StringType)
    checkEvaluation(ret4, "[ab, cde, f]")
    val ret5 = cast(
      Literal.create(Array("2014-12-03", "2014-12-04", "2014-12-06").map(Date.valueOf)),
      StringType)
    checkEvaluation(ret5, "[2014-12-03, 2014-12-04, 2014-12-06]")
    val ret6 = cast(
      Literal.create(Array("2014-12-03 13:01:00", "2014-12-04 15:05:00").map(Timestamp.valueOf)),
      StringType)
    checkEvaluation(ret6, "[2014-12-03 13:01:00, 2014-12-04 15:05:00]")
    val ret7 = cast(Literal.create(Array(Array(1, 2, 3), Array(4, 5))), StringType)
    checkEvaluation(ret7, "[[1, 2, 3], [4, 5]]")
    val ret8 = cast(
      Literal.create(Array(Array(Array("a"), Array("b", "c")), Array(Array("d")))),
      StringType)
    checkEvaluation(ret8, "[[[a], [b, c]], [[d]]]")
  }

  test("SPARK-22973 Cast map to string") {
    val ret1 = cast(Literal.create(Map(1 -> "a", 2 -> "b", 3 -> "c")), StringType)
    checkEvaluation(ret1, "[1 -> a, 2 -> b, 3 -> c]")
    val ret2 = cast(
      Literal.create(Map("1" -> "a".getBytes, "2" -> null, "3" -> "c".getBytes)),
      StringType)
    checkEvaluation(ret2, "[1 -> a, 2 ->, 3 -> c]")
    val ret3 = cast(
      Literal.create(Map(
        1 -> Date.valueOf("2014-12-03"),
        2 -> Date.valueOf("2014-12-04"),
        3 -> Date.valueOf("2014-12-05"))),
      StringType)
    checkEvaluation(ret3, "[1 -> 2014-12-03, 2 -> 2014-12-04, 3 -> 2014-12-05]")
    val ret4 = cast(
      Literal.create(Map(
        1 -> Timestamp.valueOf("2014-12-03 13:01:00"),
        2 -> Timestamp.valueOf("2014-12-04 15:05:00"))),
      StringType)
    checkEvaluation(ret4, "[1 -> 2014-12-03 13:01:00, 2 -> 2014-12-04 15:05:00]")
    val ret5 = cast(
      Literal.create(Map(
        1 -> Array(1, 2, 3),
        2 -> Array(4, 5, 6))),
      StringType)
    checkEvaluation(ret5, "[1 -> [1, 2, 3], 2 -> [4, 5, 6]]")
  }

  test("SPARK-22981 Cast struct to string") {
    val ret1 = cast(Literal.create((1, "a", 0.1)), StringType)
    checkEvaluation(ret1, "[1, a, 0.1]")
    val ret2 = cast(Literal.create(Tuple3[Int, String, String](1, null, "a")), StringType)
    checkEvaluation(ret2, "[1,, a]")
    val ret3 = cast(Literal.create(
      (Date.valueOf("2014-12-03"), Timestamp.valueOf("2014-12-03 15:05:00"))), StringType)
    checkEvaluation(ret3, "[2014-12-03, 2014-12-03 15:05:00]")
    val ret4 = cast(Literal.create(((1, "a"), 5, 0.1)), StringType)
    checkEvaluation(ret4, "[[1, a], 5, 0.1]")
    val ret5 = cast(Literal.create((Seq(1, 2, 3), "a", 0.1)), StringType)
    checkEvaluation(ret5, "[[1, 2, 3], a, 0.1]")
    val ret6 = cast(Literal.create((1, Map(1 -> "a", 2 -> "b", 3 -> "c"))), StringType)
    checkEvaluation(ret6, "[1, [1 -> a, 2 -> b, 3 -> c]]")
  }

  test("SPARK-26706: Fix Cast.mayTruncate for bytes") {
    assert(!Cast.mayTruncate(ByteType, ByteType))
    assert(!Cast.mayTruncate(DecimalType.ByteDecimal, ByteType))
    assert(Cast.mayTruncate(ShortType, ByteType))
    assert(Cast.mayTruncate(IntegerType, ByteType))
    assert(Cast.mayTruncate(LongType, ByteType))
    assert(Cast.mayTruncate(FloatType, ByteType))
    assert(Cast.mayTruncate(DoubleType, ByteType))
    assert(Cast.mayTruncate(DecimalType.IntDecimal, ByteType))
  }

  test("canSafeCast and mayTruncate must be consistent for numeric types") {
    import DataTypeTestUtils._

    def isCastSafe(from: NumericType, to: NumericType): Boolean = (from, to) match {
      case (_, dt: DecimalType) => dt.isWiderThan(from)
      case (dt: DecimalType, _) => dt.isTighterThan(to)
      case _ => numericPrecedence.indexOf(from) <= numericPrecedence.indexOf(to)
    }

    numericTypes.foreach { from =>
      val (safeTargetTypes, unsafeTargetTypes) = numericTypes.partition(to => isCastSafe(from, to))

      safeTargetTypes.foreach { to =>
        assert(Cast.canSafeCast(from, to), s"It should be possible to safely cast $from to $to")
        assert(!Cast.mayTruncate(from, to), s"No truncation is expected when casting $from to $to")
      }

      unsafeTargetTypes.foreach { to =>
        assert(!Cast.canSafeCast(from, to), s"It shouldn't be possible to safely cast $from to $to")
        assert(Cast.mayTruncate(from, to), s"Truncation is expected when casting $from to $to")
      }
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

  test("cast a timestamp before the epoch 1970-01-01 00:00:00Z") {
    withDefaultTimeZone(DateTimeUtils.TimeZoneUTC) {
      val negativeTs = Timestamp.valueOf("1900-05-05 18:34:56.1")
      assert(negativeTs.getTime < 0)
      val expectedSecs = Math.floorDiv(negativeTs.getTime, DateTimeUtils.MILLIS_PER_SECOND)
      checkEvaluation(cast(negativeTs, ByteType), expectedSecs.toByte)
      checkEvaluation(cast(negativeTs, ShortType), expectedSecs.toShort)
      checkEvaluation(cast(negativeTs, IntegerType), expectedSecs.toInt)
      checkEvaluation(cast(negativeTs, LongType), expectedSecs)
    }
  }
}
