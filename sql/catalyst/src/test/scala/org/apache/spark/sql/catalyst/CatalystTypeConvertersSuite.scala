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

package org.apache.spark.sql.catalyst

import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period}

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.{DateTimeConstants, DateTimeUtils, GenericArrayData, IntervalUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType._
import org.apache.spark.sql.types.YearMonthIntervalType._
import org.apache.spark.unsafe.types.UTF8String

class CatalystTypeConvertersSuite extends SparkFunSuite with SQLHelper {

  private val simpleTypes: Seq[DataType] = Seq(
    StringType,
    DateType,
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType.SYSTEM_DEFAULT,
    DecimalType.USER_DEFAULT)

  test("null handling in rows") {
    val schema = StructType(simpleTypes.map(t => StructField(t.getClass.getName, t)))
    val convertToCatalyst = CatalystTypeConverters.createToCatalystConverter(schema)
    val convertToScala = CatalystTypeConverters.createToScalaConverter(schema)

    val scalaRow = Row.fromSeq(Seq.fill(simpleTypes.length)(null))
    assert(convertToScala(convertToCatalyst(scalaRow)) === scalaRow)
  }

  test("null handling for individual values") {
    for (dataType <- simpleTypes) {
      assert(CatalystTypeConverters.createToScalaConverter(dataType)(null) === null)
    }
  }

  test("option handling in convertToCatalyst") {
    // convertToCatalyst doesn't handle unboxing from Options. This is inconsistent with
    // createToCatalystConverter but it may not actually matter as this is only called internally
    // in a handful of places where we don't expect to receive Options.
    assert(CatalystTypeConverters.convertToCatalyst(Some(123)) === Some(123))
  }

  test("option handling in createToCatalystConverter") {
    assert(CatalystTypeConverters.createToCatalystConverter(IntegerType)(Some(123)) === 123)
  }

  test("primitive array handling") {
    val intArray = Array(1, 100, 10000)
    val intUnsafeArray = UnsafeArrayData.fromPrimitiveArray(intArray)
    val intArrayType = ArrayType(IntegerType, false)
    assert(CatalystTypeConverters.createToScalaConverter(intArrayType)(intUnsafeArray) === intArray)

    val doubleArray = Array(1.1, 111.1, 11111.1)
    val doubleUnsafeArray = UnsafeArrayData.fromPrimitiveArray(doubleArray)
    val doubleArrayType = ArrayType(DoubleType, false)
    assert(CatalystTypeConverters.createToScalaConverter(doubleArrayType)(doubleUnsafeArray)
      === doubleArray)
  }

  test("An array with null handling") {
    val intArray = Array(1, null, 100, null, 10000)
    val intGenericArray = new GenericArrayData(intArray)
    val intArrayType = ArrayType(IntegerType, true)
    assert(CatalystTypeConverters.createToScalaConverter(intArrayType)(intGenericArray)
      === intArray)
    assert(CatalystTypeConverters.createToCatalystConverter(intArrayType)(intArray)
      == intGenericArray)

    val doubleArray = Array(1.1, null, 111.1, null, 11111.1)
    val doubleGenericArray = new GenericArrayData(doubleArray)
    val doubleArrayType = ArrayType(DoubleType, true)
    assert(CatalystTypeConverters.createToScalaConverter(doubleArrayType)(doubleGenericArray)
      === doubleArray)
    assert(CatalystTypeConverters.createToCatalystConverter(doubleArrayType)(doubleArray)
      == doubleGenericArray)
  }

  test("converting a wrong value to the struct type") {
    val structType = new StructType().add("f1", IntegerType)
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        CatalystTypeConverters.createToCatalystConverter(structType)("test")
      },
      condition = "_LEGACY_ERROR_TEMP_3219",
      parameters = Map(
        "other" -> "test",
        "otherClass" -> "java.lang.String",
        "dataType" -> "struct<f1:int>"))
  }

  test("converting a wrong value to the map type") {
    val mapType = MapType(StringType, IntegerType, false)
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        CatalystTypeConverters.createToCatalystConverter(mapType)("test")
      },
      condition = "_LEGACY_ERROR_TEMP_3221",
      parameters = Map(
        "other" -> "test",
        "otherClass" -> "java.lang.String",
        "keyType" -> "string",
        "valueType" -> "int"))
  }

  test("converting a wrong value to the array type") {
    val arrayType = ArrayType(IntegerType, true)
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        CatalystTypeConverters.createToCatalystConverter(arrayType)("test")
      },
      condition = "_LEGACY_ERROR_TEMP_3220",
      parameters = Map(
        "other" -> "test",
        "otherClass" -> "java.lang.String",
        "elementType" -> "int"))
  }

  test("converting a wrong value to the decimal type") {
    val decimalType = DecimalType(10, 0)
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        CatalystTypeConverters.createToCatalystConverter(decimalType)("test")
      },
      condition = "_LEGACY_ERROR_TEMP_3219",
      parameters = Map(
        "other" -> "test",
        "otherClass" -> "java.lang.String",
        "dataType" -> "decimal(10,0)"))
  }

  test("converting a wrong value to the string type") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        CatalystTypeConverters.createToCatalystConverter(StringType)(0.1)
      },
      condition = "_LEGACY_ERROR_TEMP_3219",
      parameters = Map(
        "other" -> "0.1",
        "otherClass" -> "java.lang.Double",
        "dataType" -> "STRING"))
  }

  test("SPARK-24571: convert Char to String") {
    val chr: Char = 'X'
    val converter = CatalystTypeConverters.createToCatalystConverter(StringType)
    val expected = UTF8String.fromString("X")
    assert(converter(chr) === expected)
    assert(CatalystTypeConverters.convertToCatalyst('a') === UTF8String.fromString("a"))
  }

  test("SPARK-33390: Make Literal support char array") {
    val ac = Array('a', 'c')
    val converter = CatalystTypeConverters.createToCatalystConverter(StringType)
    val expected = UTF8String.fromString(String.valueOf(ac))
    assert(converter(ac) === expected)
  }

  test("converting java.time.Instant to TimestampType") {
    Seq(
      "0101-02-16T10:11:32Z",
      "1582-10-02T01:02:03.04Z",
      "1582-12-31T23:59:59.999999Z",
      "1970-01-01T00:00:01.123Z",
      "1972-12-31T23:59:59.123456Z",
      "2019-02-16T18:12:30Z",
      "2119-03-16T19:13:31Z").foreach { timestamp =>
      val input = Instant.parse(timestamp)
      val result = CatalystTypeConverters.convertToCatalyst(input)
      val expected = DateTimeUtils.instantToMicros(input)
      assert(result === expected)
    }
  }

  test("converting TimestampType to java.time.Instant") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      Seq(
        -9463427405253013L,
        -244000001L,
        0L,
        99628200102030L,
        1543749753123456L).foreach { us =>
        val instant = DateTimeUtils.microsToInstant(us)
        assert(CatalystTypeConverters.createToScalaConverter(TimestampType)(us) === instant)
      }
    }
  }

  test("SPARK-35664: converting java.time.LocalDateTime to TimestampNTZType") {
    Seq(
      "0101-02-16T10:11:32",
      "1582-10-02T01:02:03.04",
      "1582-12-31T23:59:59.999999",
      "1970-01-01T00:00:01.123",
      "1972-12-31T23:59:59.123456",
      "2019-02-16T18:12:30",
      "2119-03-16T19:13:31").foreach { text =>
      val input = LocalDateTime.parse(text)
      val result = CatalystTypeConverters.convertToCatalyst(input)
      val expected = DateTimeUtils.localDateTimeToMicros(input)
      assert(result === expected)
    }
  }

  test("SPARK-35664: converting TimestampNTZType to java.time.LocalDateTime") {
    Seq(
      -9463427405253013L,
      -244000001L,
      0L,
      99628200102030L,
      1543749753123456L).foreach { us =>
      val localDateTime = DateTimeUtils.microsToLocalDateTime(us)
      assert(CatalystTypeConverters.createToScalaConverter(TimestampNTZType)(us) ===
        localDateTime)
    }
  }

  test("converting java.time.LocalDate to DateType") {
    Seq(
      "0101-02-16",
      "1582-10-02",
      "1582-12-31",
      "1970-01-01",
      "1972-12-31",
      "2019-02-16",
      "2119-03-16").foreach { date =>
      val input = LocalDate.parse(date)
      val result = CatalystTypeConverters.convertToCatalyst(input)
      val expected = DateTimeUtils.localDateToDays(input)
      assert(result === expected)
    }
  }

  test("converting DateType to java.time.LocalDate") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      Seq(
        -701265,
        -371419,
        -199722,
        -1,
        0,
        967,
        2094,
        17877,
        24837,
        1110657).foreach { days =>
        val localDate = DateTimeUtils.daysToLocalDate(days)
        assert(CatalystTypeConverters.createToScalaConverter(DateType)(days) === localDate)
      }
    }
  }

  test("SPARK-34605: converting java.time.Duration to DayTimeIntervalType") {
    Seq(
      Duration.ZERO,
      Duration.ofNanos(1),
      Duration.ofNanos(-1),
      Duration.ofSeconds(0, Long.MaxValue),
      Duration.ofSeconds(0, Long.MinValue),
      Duration.ofDays(106751991),
      Duration.ofDays(-106751991)).foreach { input =>
      val result = CatalystTypeConverters.convertToCatalyst(input)
      val expected = IntervalUtils.durationToMicros(input)
      assert(result === expected)
    }

    val errMsg = intercept[ArithmeticException] {
      IntervalUtils.durationToMicros(Duration.ofSeconds(Long.MaxValue, Long.MaxValue))
    }.getMessage
    assert(errMsg.contains("long overflow"))
  }

  test("SPARK-35726: Truncate java.time.Duration by fields of day-time interval type") {
    val duration = Duration.ofSeconds(90061, 1000000023)
    Seq((DayTimeIntervalType(DAY), 86400000000L, -86400000000L),
      (DayTimeIntervalType(DAY, HOUR), 90000000000L, -90000000000L),
      (DayTimeIntervalType(DAY, MINUTE), 90060000000L, -90060000000L),
      (DayTimeIntervalType(DAY, SECOND), 90062000000L, -90062000001L),
      (DayTimeIntervalType(HOUR), 90000000000L, -90000000000L),
      (DayTimeIntervalType(HOUR, MINUTE), 90060000000L, -90060000000L),
      (DayTimeIntervalType(HOUR, SECOND), 90062000000L, -90062000001L),
      (DayTimeIntervalType(MINUTE), 90060000000L, -90060000000L),
      (DayTimeIntervalType(MINUTE, SECOND), 90062000000L, -90062000001L),
      (DayTimeIntervalType(SECOND), 90062000000L, -90062000001L))
      .foreach { case (dt, positive, negative) =>
        assert(CatalystTypeConverters.createToCatalystConverter(dt)(duration) == positive)
        assert(
          CatalystTypeConverters.createToCatalystConverter(dt)(duration.negated()) == negative)
      }
  }

  test("SPARK-34605: converting DayTimeIntervalType to java.time.Duration") {
    Seq(
      0L,
      1L,
      999999,
      -1000000,
      Long.MaxValue).foreach { input =>
      Seq(1L, -1L).foreach { sign =>
        val us = sign * input
        val duration = IntervalUtils.microsToDuration(us)
        assert(CatalystTypeConverters.createToScalaConverter(DayTimeIntervalType())(us)
          === duration)
      }
    }
  }

  test("SPARK-34615: converting java.time.Period to YearMonthIntervalType") {
    Seq(
      Period.ZERO,
      Period.ofMonths(1),
      Period.ofMonths(-1),
      Period.ofMonths(Int.MaxValue).normalized(),
      Period.ofMonths(Int.MinValue).normalized(),
      Period.ofYears(106751991),
      Period.ofYears(-106751991)).foreach { input =>
      val result = CatalystTypeConverters.convertToCatalyst(input)
      val expected = IntervalUtils.periodToMonths(input)
      assert(result === expected)
    }

    val errMsg = intercept[ArithmeticException] {
      IntervalUtils.periodToMonths(Period.of(Int.MaxValue, Int.MaxValue, Int.MaxValue))
    }.getMessage
    assert(errMsg.contains("integer overflow"))
  }

  test("SPARK-35769: Truncate java.time.Period by fields of year-month interval type") {
    Seq(YearMonthIntervalType(YEAR) -> 12,
      YearMonthIntervalType(YEAR, MONTH) -> 13,
      YearMonthIntervalType(MONTH) -> 13)
      .foreach { case (ym, value) =>
        assert(CatalystTypeConverters.createToCatalystConverter(ym)(Period.of(1, 1, 0)) == value)
      }
  }

  test("SPARK-34615: converting YearMonthIntervalType to java.time.Period") {
    Seq(
      0,
      1,
      999999,
      1000000,
      Int.MaxValue).foreach { input =>
      Seq(1, -1).foreach { sign =>
        val months = sign * input
        val period = IntervalUtils.monthsToPeriod(months)
        assert(
          CatalystTypeConverters.createToScalaConverter(YearMonthIntervalType())(months) === period)
      }
    }
  }

  test("SPARK-35204: createToCatalystConverter for date") {
    Seq(true, false).foreach { enable =>
      withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> enable.toString) {
        Seq(-1234, 0, 1234).foreach { days =>
          val converter = CatalystTypeConverters.createToCatalystConverter(DateType)

          val ld = LocalDate.ofEpochDay(days)
          val result1 = converter(ld)

          val d = java.sql.Date.valueOf(ld)
          val result2 = converter(d)

          val expected = DateTimeUtils.localDateToDays(ld)
          assert(result1 === expected)
          assert(result2 === expected)
        }
      }
    }
  }

  test("SPARK-35204: createToCatalystConverter for timestamp") {
    Seq(true, false).foreach { enable =>
      withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> enable.toString) {
        Seq(-1234, 0, 1234).foreach { seconds =>
          val converter = CatalystTypeConverters.createToCatalystConverter(TimestampType)

          val i = Instant.ofEpochSecond(seconds)
          val result1 = converter(i)

          val t = new java.sql.Timestamp(seconds * DateTimeConstants.MILLIS_PER_SECOND)
          val result2 = converter(t)

          val expected = seconds * DateTimeConstants.MICROS_PER_SECOND
          assert(result1 === expected)
          assert(result2 === expected)
        }
      }
    }
  }
}
