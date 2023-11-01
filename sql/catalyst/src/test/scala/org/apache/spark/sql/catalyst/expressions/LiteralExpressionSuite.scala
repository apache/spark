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

import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.TimeZone

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.ExamplePointUDT
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLType
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType._
import org.apache.spark.sql.types.YearMonthIntervalType._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class LiteralExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("null") {
    checkEvaluation(Literal.create(null, BooleanType), null)
    checkEvaluation(Literal.create(null, ByteType), null)
    checkEvaluation(Literal.create(null, ShortType), null)
    checkEvaluation(Literal.create(null, IntegerType), null)
    checkEvaluation(Literal.create(null, LongType), null)
    checkEvaluation(Literal.create(null, FloatType), null)
    checkEvaluation(Literal.create(null, DoubleType), null)
    checkEvaluation(Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, BinaryType), null)
    checkEvaluation(Literal.create(null, DecimalType.USER_DEFAULT), null)
    checkEvaluation(Literal.create(null, DateType), null)
    checkEvaluation(Literal.create(null, TimestampType), null)
    checkEvaluation(Literal.create(null, CalendarIntervalType), null)
    checkEvaluation(Literal.create(null, YearMonthIntervalType()), null)
    checkEvaluation(Literal.create(null, DayTimeIntervalType()), null)
    checkEvaluation(Literal.create(null, ArrayType(ByteType, true)), null)
    checkEvaluation(Literal.create(null, ArrayType(StringType, true)), null)
    checkEvaluation(Literal.create(null, MapType(StringType, IntegerType)), null)
    checkEvaluation(Literal.create(null, StructType(Seq.empty)), null)
  }

  test("default") {
    checkEvaluation(Literal.default(BooleanType), false)
    checkEvaluation(Literal.default(ByteType), 0.toByte)
    checkEvaluation(Literal.default(ShortType), 0.toShort)
    checkEvaluation(Literal.default(IntegerType), 0)
    checkEvaluation(Literal.default(LongType), 0L)
    checkEvaluation(Literal.default(FloatType), 0.0f)
    checkEvaluation(Literal.default(DoubleType), 0.0)
    checkEvaluation(Literal.default(StringType), "")
    checkEvaluation(Literal.default(BinaryType), "".getBytes(StandardCharsets.UTF_8))
    checkEvaluation(Literal.default(DecimalType.USER_DEFAULT), Decimal(0))
    checkEvaluation(Literal.default(DecimalType.SYSTEM_DEFAULT), Decimal(0))
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "false") {
      checkEvaluation(Literal.default(DateType), DateTimeUtils.toJavaDate(0))
      checkEvaluation(Literal.default(TimestampType), DateTimeUtils.toJavaTimestamp(0L))
    }
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      checkEvaluation(Literal.default(DateType), LocalDate.ofEpochDay(0))
      checkEvaluation(Literal.default(TimestampType), Instant.ofEpochSecond(0))
    }
    checkEvaluation(Literal.default(CalendarIntervalType), new CalendarInterval(0, 0, 0L))
    checkEvaluation(Literal.default(YearMonthIntervalType()), 0)
    checkEvaluation(Literal.default(DayTimeIntervalType()), 0L)
    checkEvaluation(Literal.default(ArrayType(StringType)), Array())
    checkEvaluation(Literal.default(MapType(IntegerType, StringType)), Map())
    checkEvaluation(Literal.default(StructType(StructField("a", StringType) :: Nil)), Row(""))
    // ExamplePointUDT.sqlType is ArrayType(DoubleType, false).
    checkEvaluation(Literal.default(new ExamplePointUDT), Array())

    // DateType without default value`
    List(CharType(1), VarcharType(1)).foreach(errType => {
      checkError(
        exception = intercept[SparkException] {
          Literal.default(errType)
        },
        errorClass = "INTERNAL_ERROR",
        parameters = Map("message" -> s"No default value for type: ${toSQLType(errType)}.")
      )
    })
  }

  test("boolean literals") {
    checkEvaluation(Literal(true), true)
    checkEvaluation(Literal(false), false)

    checkEvaluation(Literal.create(true), true)
    checkEvaluation(Literal.create(false), false)
  }

  test("int literals") {
    List(0, 1, Int.MinValue, Int.MaxValue).foreach { d =>
      checkEvaluation(Literal(d), d)
      checkEvaluation(Literal(d.toLong), d.toLong)
      checkEvaluation(Literal(d.toShort), d.toShort)
      checkEvaluation(Literal(d.toByte), d.toByte)

      checkEvaluation(Literal.create(d), d)
      checkEvaluation(Literal.create(d.toLong), d.toLong)
      checkEvaluation(Literal.create(d.toShort), d.toShort)
      checkEvaluation(Literal.create(d.toByte), d.toByte)
    }
    checkEvaluation(Literal(Long.MinValue), Long.MinValue)
    checkEvaluation(Literal(Long.MaxValue), Long.MaxValue)

    checkEvaluation(Literal.create(Long.MinValue), Long.MinValue)
    checkEvaluation(Literal.create(Long.MaxValue), Long.MaxValue)
  }

  test("double literals") {
    List(0.0, -0.0, Double.NegativeInfinity, Double.PositiveInfinity).foreach { d =>
      checkEvaluation(Literal(d), d)
      checkEvaluation(Literal(d.toFloat), d.toFloat)

      checkEvaluation(Literal.create(d), d)
      checkEvaluation(Literal.create(d.toFloat), d.toFloat)
    }
    checkEvaluation(Literal(Double.MinValue), Double.MinValue)
    checkEvaluation(Literal(Double.MaxValue), Double.MaxValue)
    checkEvaluation(Literal(Float.MinValue), Float.MinValue)
    checkEvaluation(Literal(Float.MaxValue), Float.MaxValue)

    checkEvaluation(Literal.create(Double.MinValue), Double.MinValue)
    checkEvaluation(Literal.create(Double.MaxValue), Double.MaxValue)
    checkEvaluation(Literal.create(Float.MinValue), Float.MinValue)
    checkEvaluation(Literal.create(Float.MaxValue), Float.MaxValue)

  }

  test("string literals") {
    checkEvaluation(Literal(""), "")
    checkEvaluation(Literal("test"), "test")
    checkEvaluation(Literal("\u0000"), "\u0000")

    checkEvaluation(Literal.create(""), "")
    checkEvaluation(Literal.create("test"), "test")
    checkEvaluation(Literal.create("\u0000"), "\u0000")
  }

  test("sum two literals") {
    checkEvaluation(Add(Literal(1), Literal(1)), 2)
    checkEvaluation(Add(Literal.create(1), Literal.create(1)), 2)
  }

  test("binary literals") {
    checkEvaluation(Literal.create(new Array[Byte](0), BinaryType), new Array[Byte](0))
    checkEvaluation(Literal.create(new Array[Byte](2), BinaryType), new Array[Byte](2))

    checkEvaluation(Literal.create(new Array[Byte](0)), new Array[Byte](0))
    checkEvaluation(Literal.create(new Array[Byte](2)), new Array[Byte](2))
  }

  test("decimal") {
    List(-0.0001, 0.0, 0.001, 1.2, 1.1111, 5).foreach { d =>
      checkEvaluation(Literal(Decimal(d)), Decimal(d))
      checkEvaluation(Literal(Decimal(d.toInt)), Decimal(d.toInt))
      checkEvaluation(Literal(Decimal(d.toLong)), Decimal(d.toLong))
      checkEvaluation(Literal(Decimal((d * 1000L).toLong, 10, 3)),
        Decimal((d * 1000L).toLong, 10, 3))
      checkEvaluation(Literal(BigDecimal(d.toString)), Decimal(d))
      checkEvaluation(Literal(new java.math.BigDecimal(d.toString)), Decimal(d))

      checkEvaluation(Literal.create(Decimal(d)), Decimal(d))
      checkEvaluation(Literal.create(Decimal(d.toInt)), Decimal(d.toInt))
      checkEvaluation(Literal.create(Decimal(d.toLong)), Decimal(d.toLong))
      checkEvaluation(Literal.create(Decimal((d * 1000L).toLong, 10, 3)),
        Decimal((d * 1000L).toLong, 10, 3))
      checkEvaluation(Literal.create(BigDecimal(d.toString)), Decimal(d))
      checkEvaluation(Literal.create(new java.math.BigDecimal(d.toString)), Decimal(d))

    }
  }

  private def toCatalyst[T: TypeTag](value: T): Any = {
    val ScalaReflection.Schema(dataType, _) = ScalaReflection.schemaFor[T]
    CatalystTypeConverters.createToCatalystConverter(dataType)(value)
  }

  test("array") {
    def checkArrayLiteral[T: TypeTag](a: Array[T]): Unit = {
      checkEvaluation(Literal(a), toCatalyst(a))
      checkEvaluation(Literal.create(a), toCatalyst(a))
    }
    checkArrayLiteral(Array(1, 2, 3))
    checkArrayLiteral(Array("a", "b", "c"))
    checkArrayLiteral(Array(1.0, 4.0))
    checkArrayLiteral(Array(new CalendarInterval(1, 0, 0), new CalendarInterval(0, 1, 0)))
    val arr = mutable.ArraySeq.make(Array(1.0, 4.0))
    checkEvaluation(Literal(arr), toCatalyst(arr))
  }

  test("seq") {
    def checkSeqLiteral[T: TypeTag](a: Seq[T]): Unit = {
      checkEvaluation(Literal.create(a), toCatalyst(a))
    }
    checkSeqLiteral(Seq(1, 2, 3))
    checkSeqLiteral(Seq("a", "b", "c"))
    checkSeqLiteral(Seq(1.0, 4.0))
    checkSeqLiteral(Seq(new CalendarInterval(1, 0, 0), new CalendarInterval(0, 1, 0)))
    checkSeqLiteral(Seq(Period.ZERO, Period.ofMonths(1)))
    checkSeqLiteral(Seq(Duration.ZERO, Duration.ofDays(1)))
  }

  test("map") {
    def checkMapLiteral[T: TypeTag](m: T): Unit = {
      checkEvaluation(Literal.create(m), toCatalyst(m))
    }
    checkMapLiteral(Map("a" -> 1, "b" -> 2, "c" -> 3))
    checkMapLiteral(Map("1" -> 1.0, "2" -> 2.0, "3" -> 3.0))
    checkMapLiteral(Map(Period.ofMonths(1) -> Duration.ZERO))
    assert(Literal.create(Map("a" -> 1)).toString === "map(keys: [a], values: [1])")
  }

  test("struct") {
    def checkStructLiteral[T: TypeTag](s: T): Unit = {
      checkEvaluation(Literal.create(s), toCatalyst(s))
    }
    checkStructLiteral((1, 3.0, "abcde"))
    checkStructLiteral(("de", 1, 2.0f))
    checkStructLiteral((1, ("fgh", 3.0)))
    checkStructLiteral((Period.ZERO, ("abc", Duration.ofDays(1))))
  }

  test("SPARK-24571: char literals") {
    checkEvaluation(Literal('X'), "X")
    checkEvaluation(Literal.create('0'), "0")
    checkEvaluation(Literal('\u0000'), "\u0000")
    checkEvaluation(Literal.create('\n'), "\n")
  }

  test("SPARK-33390: Make Literal support char array") {
    checkEvaluation(Literal(Array('h', 'e', 'l', 'l', 'o')), "hello")
    checkEvaluation(Literal(Array("hello".toCharArray)), Array("hello"))
    // scalastyle:off
    checkEvaluation(Literal(Array('测','试')), "测试")
    checkEvaluation(Literal(Array('a', '测', 'b', '试', 'c')), "a测b试c")
    // scalastyle:on
  }

  test("SPARK-39052: Support Char in Literal.create") {
    checkEvaluation(Literal.create('a', StringType), "a")
  }

  test("construct literals from java.time.LocalDate") {
    Seq(
      LocalDate.of(1, 1, 1),
      LocalDate.of(1582, 10, 1),
      LocalDate.of(1600, 7, 30),
      LocalDate.of(1969, 12, 31),
      LocalDate.of(1970, 1, 1),
      LocalDate.of(2019, 3, 20),
      LocalDate.of(2100, 5, 17)).foreach { localDate =>
      checkEvaluation(Literal(localDate), localDate)
    }
  }

  test("construct literals from arrays of java.time.LocalDate") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val localDate0 = LocalDate.of(2019, 3, 20)
      checkEvaluation(Literal(Array(localDate0)), Array(localDate0))
      val localDate1 = LocalDate.of(2100, 4, 22)
      checkEvaluation(Literal(Array(localDate0, localDate1)), Array(localDate0, localDate1))
    }
  }

  test("construct literals from java.time.Instant") {
    Seq(
      Instant.parse("0001-01-01T00:00:00Z"),
      Instant.parse("1582-10-01T01:02:03Z"),
      Instant.parse("1970-02-28T11:12:13Z"),
      Instant.ofEpochMilli(0),
      Instant.parse("2019-03-20T10:15:30Z"),
      Instant.parse("2100-12-31T22:17:31Z")).foreach { instant =>
      checkEvaluation(Literal(instant), instant)
    }
  }

  test("construct literals from arrays of java.time.Instant") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val instant0 = Instant.ofEpochMilli(0)
      checkEvaluation(Literal(Array(instant0)), Array(instant0))
      val instant1 = Instant.parse("2019-03-20T10:15:30Z")
      checkEvaluation(Literal(Array(instant0, instant1)), Array(instant0, instant1))
    }
  }

  private def withTimeZones(
      sessionTimeZone: String,
      systemTimeZone: String)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> sessionTimeZone,
      SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val originTimeZone = TimeZone.getDefault
      try {
        TimeZone.setDefault(TimeZone.getTimeZone(systemTimeZone))
        f
      } finally {
        TimeZone.setDefault(originTimeZone)
      }
    }
  }

  test("format timestamp literal using spark.sql.session.timeZone") {
    withTimeZones(sessionTimeZone = "GMT+01:00", systemTimeZone = "GMT-08:00") {
      val timestamp = LocalDateTime.of(2019, 3, 21, 0, 2, 3, 456000000)
        .atZone(ZoneOffset.UTC)
        .toInstant
      val expected = "TIMESTAMP '2019-03-21 01:02:03.456GMT+01:00'"
      val literalStr = Literal.create(timestamp).sql
      assert(literalStr === expected)
    }
  }

  test("format date literal independently from time zone") {
    withTimeZones(sessionTimeZone = "GMT-11:00", systemTimeZone = "GMT-10:00") {
      val date = LocalDate.of(2019, 3, 21)
      val expected = "DATE '2019-03-21'"
      val literalStr = Literal.create(date).sql
      assert(literalStr === expected)
    }
  }

  test("SPARK-33860: Make CatalystTypeConverters.convertToCatalyst match special Array value") {
    assert(Literal(Array(1, 2, 3)) == Literal.create(Array(1, 2, 3), ArrayType(IntegerType)))
    assert(Literal(Array(1L, 2L, 3L)) == Literal.create(Array(1L, 2L, 3L), ArrayType(LongType)))
    assert(Literal(Array(1D, 2D, 3D)) == Literal.create(Array(1D, 2D, 3D), ArrayType(DoubleType)))
    assert(Literal("123") == Literal.create(Array('1', '2', '3'), StringType))
    assert(Literal(Array(1.toByte, 2.toByte, 3.toByte)) ==
      Literal.create(Array(1.toByte, 2.toByte, 3.toByte), BinaryType))
    assert(Literal(Array("1", "2", "3")) ==
      Literal.create(Array("1", "2", "3"), ArrayType(StringType)))
    assert(Literal(Array(Period.ofMonths(1))) ==
      Literal.create(Array(Period.ofMonths(1)), ArrayType(YearMonthIntervalType())))
  }

  test("SPARK-34342: Date/Timestamp toString") {
    assert(Literal.default(DateType).toString === "1970-01-01")
    assert(Literal.default(TimestampType).toString === "1969-12-31 16:00:00")
    withTimeZones(sessionTimeZone = "GMT+01:00", systemTimeZone = "GMT-08:00") {
      val timestamp = LocalDateTime.of(2021, 2, 3, 16, 50, 3, 456000000)
        .atZone(ZoneOffset.UTC)
        .toInstant
      val literalStr = Literal.create(timestamp).toString
      assert(literalStr === "2021-02-03 17:50:03.456")
    }
  }

  test("SPARK-36055: TimestampNTZ toString") {
    assert(Literal.default(TimestampNTZType).toString === "1970-01-01 00:00:00")
    withTimeZones(sessionTimeZone = "GMT+01:00", systemTimeZone = "GMT-08:00") {
      val timestamp = LocalDateTime.of(2021, 2, 3, 16, 50, 3, 456000000)
      val literalStr = Literal.create(timestamp).toString
      assert(literalStr === "2021-02-03 16:50:03.456")
    }
  }

  test("SPARK-35664: construct literals from java.time.LocalDateTime") {
    Seq(
      LocalDateTime.of(1, 1, 1, 0, 0, 0, 0),
      LocalDateTime.of(2021, 5, 31, 23, 59, 59, 100),
      LocalDateTime.of(2020, 2, 29, 23, 50, 57, 9999),
      LocalDateTime.parse("9999-12-31T23:59:59.999999")
    ).foreach { dateTime =>
      checkEvaluation(Literal(dateTime), dateTime)
    }
  }

  test("SPARK-34605: construct literals from java.time.Duration") {
    Seq(
      Duration.ofNanos(0),
      Duration.ofSeconds(-1),
      Duration.ofNanos(123456000),
      Duration.ofDays(106751991),
      Duration.ofDays(-106751991)).foreach { duration =>
      checkEvaluation(Literal(duration), duration)
    }
  }

  test("SPARK-34605: construct literals from arrays of java.time.Duration") {
    val duration0 = Duration.ofDays(2).plusHours(3).plusMinutes(4)
    checkEvaluation(Literal(Array(duration0)), Array(duration0))
    val duration1 = Duration.ofHours(-1024)
    checkEvaluation(Literal(Array(duration0, duration1)), Array(duration0, duration1))
  }

  test("SPARK-34615: construct literals from java.time.Period") {
    Seq(
      Period.ofYears(0),
      Period.of(-1, 11, 0),
      Period.of(1, -11, 0),
      Period.ofMonths(Int.MaxValue),
      Period.ofMonths(Int.MinValue)).foreach { period =>
      checkEvaluation(Literal(period), period)
    }
  }

  test("SPARK-34615: construct literals from arrays of java.time.Period") {
    val period0 = Period.ofYears(123).withMonths(456)
    checkEvaluation(Literal(Array(period0)), Array(period0))
    val period1 = Period.ofMonths(-1024)
    checkEvaluation(Literal(Array(period0, period1)), Array(period0, period1))
  }

  test("SPARK-35099: convert a literal of day-time interval to SQL string") {
    Seq(
      Duration.ofDays(-1) -> "-1 00:00:00",
      Duration.of(10, ChronoUnit.MICROS) -> "0 00:00:00.00001",
      Duration.of(MICROS_PER_DAY - 1, ChronoUnit.MICROS) -> "0 23:59:59.999999"
    ).foreach { case (duration, intervalPayload) =>
      val literal = Literal.apply(duration)
      val expected = s"INTERVAL '$intervalPayload' DAY TO SECOND"
      assert(literal.sql === expected)
      assert(literal.toString === expected)
    }
  }

  test("SPARK-35099: convert a literal of year-month interval to SQL string") {
    Seq(
      Period.ofYears(-1) -> "-1-0",
      Period.of(9999, 11, 0) -> "9999-11",
      Period.ofMonths(-11) -> "-0-11"
    ).foreach { case (period, intervalPayload) =>
      val literal = Literal.apply(period)
      val expected = s"INTERVAL '$intervalPayload' YEAR TO MONTH"
      assert(literal.sql === expected)
      assert(literal.toString === expected)
    }
  }

  test("SPARK-35871: Literal.create(value, dataType) should support fields") {
    val period = Period.ofMonths(13)
    DataTypeTestUtils.yearMonthIntervalTypes.foreach { dt =>
      val result = dt.endField match {
        case YEAR => 12
        case MONTH => 13
      }
      checkEvaluation(Literal.create(period, dt), result)
    }

    val duration = Duration.ofSeconds(86400 + 3600 + 60 + 1)
    DataTypeTestUtils.dayTimeIntervalTypes.foreach { dt =>
      val result = dt.endField match {
        case DAY => 86400000000L
        case HOUR => 90000000000L
        case MINUTE => 90060000000L
        case SECOND => 90061000000L
      }
      checkEvaluation(Literal.create(duration, dt), result)
    }
  }

  test("SPARK-37967: Literal.create support ObjectType") {
    checkEvaluation(
      Literal.create(UTF8String.fromString("Spark SQL"), ObjectType(classOf[UTF8String])),
      UTF8String.fromString("Spark SQL"))
  }

  private def serializationRoundTripInfo(
      before: Literal,
      serializedValue: String,
      serializedDataType: String,
      after: Literal): String = {
    s"""- Initial Literal: $before with type ${before.dataType}
       |- String after serialization: $serializedValue with type $serializedDataType
       |- Deserialized Literal: $after with type ${after.dataType}
       |""".stripMargin
  }

  test(".sql and fromSQL round trip: basic Literals") {
    Seq(
      // null
      Literal.create(null, BooleanType),
      Literal.create(null, ByteType),
      Literal.create(null, ShortType),
      Literal.create(null, IntegerType),
      Literal.create(null, LongType),
      Literal.create(null, FloatType),
      Literal.create(null, StringType),
      Literal.create(null, BinaryType),
      Literal.create(null, DecimalType.USER_DEFAULT),
      Literal.create(null, DecimalType(20, 2)),
      Literal.create(null, DateType),
      Literal.create(null, TimestampType),
      Literal.create(null, CalendarIntervalType),
      Literal.create(null, YearMonthIntervalType()),
      Literal.create(null, DayTimeIntervalType(1, 2)),
      Literal.create(null, ArrayType(ByteType, true)),
      Literal.create(null, MapType(StringType, IntegerType)),
      Literal.create(null, StructType(Seq.empty)),

      // boolean
      Literal(true),
      Literal(false),

      // int, long, short, byte
      Literal(0),
      Literal(1.toLong),
      Literal(0.toShort),
      Literal(1.toByte),
      Literal(Int.MinValue),
      Literal(Int.MinValue.toShort),
      Literal(Int.MaxValue),
      Literal(Int.MaxValue.toByte),
      Literal(Short.MinValue),
      Literal(Byte.MaxValue),
      Literal(Long.MinValue),
      Literal(Long.MaxValue),

      // float
      Literal(0.0.toFloat),
      Literal(-0.0.toFloat),
      Literal(Float.PositiveInfinity),
      Literal(Float.NegativeInfinity),
      Literal(Float.MinPositiveValue),
      Literal(Float.MaxValue),
      Literal(Float.MinValue),
      Literal(Float.NaN),

      // double
      Literal(0.0),
      Literal(-0.0),
      Literal(Double.NegativeInfinity),
      Literal(Double.PositiveInfinity),
      Literal(Double.MinValue),
      Literal(Double.MaxValue),
      Literal(Double.NaN),
      Literal(Double.MinPositiveValue),

      // Decimal -> without type it's problematic?
      Literal(Decimal(-0.0001)),
      Literal(Decimal(0.0)),
      Literal(Decimal(0.001)),
      Literal(Decimal(1.1111)),
      Literal(Decimal(5.toLong, 10, 3)),
      Literal(BigDecimal((-0.0001).toString)),
      Literal(new java.math.BigDecimal(0.0.toString)),

      // binary
      Literal.create(new Array[Byte](0), BinaryType),
      Literal.create(new Array[Byte](2), BinaryType),

      // string
      Literal(""),
      Literal("a"),
      Literal("\u0000"),
      Literal("a b"),

      // DayTimeInterval (Duration)
      Literal(Duration.ofNanos(0)),
      Literal(Duration.ofSeconds(-1)),
      Literal(Duration.ofMinutes(62)),
      Literal.create(Duration.ofMinutes(62), DayTimeIntervalType(2, 3)),
      Literal(Duration.ofHours(10)),
      Literal.create(Duration.ofDays(12345600), DayTimeIntervalType(0, 1)),

      // YearMonthInterval (Period)
      Literal(Period.ofYears(0)),
      Literal.create(Period.ofYears(1), YearMonthIntervalType(0, 0)),
      Literal.create(Period.ofYears(1), YearMonthIntervalType(1, 1)),
      Literal(Period.of(-1, 11, 0)),
      Literal(Period.ofMonths(Int.MinValue)),



      // array
      Literal(Array(1, 2, 3)),
      Literal.create(Array(1.0, 2.0), ArrayType(DoubleType, false)),
      Literal.create(Array(1.0, 2.0), ArrayType(DoubleType, true)),
      Literal.create(Array(1.0, 2.0, null), ArrayType(DoubleType, true)),
      Literal(Array("a")),

      // array of struct
      Literal(
        new GenericArrayData(
          Array(
            InternalRow(UTF8String.fromString("a"), 1),
            InternalRow(UTF8String.fromString("b"), 2)
          )
        ),
        ArrayType(
          StructType(Seq(StructField("col1", StringType), StructField("col2", IntegerType))),
          containsNull = false
        )
      ),

      // struct
      Literal.create((1, 3.0, "abc")),
      Literal(
        InternalRow(true, 1.toLong),
        StructType(
          Seq(
            StructField("col1", BooleanType, nullable = false).withComment("some-comment"),
            StructField("col2", LongType)
          )
        )
      ),

      // struct contains array
      Literal(
        InternalRow(1.0, new GenericArrayData(Array(true, false, null))),
        StructType(
          Seq(
            StructField("col1", DoubleType),
            StructField("col2", ArrayType(BooleanType, containsNull = true))
          )
        )
      ),

      // map
      Literal(
        new ArrayBasedMapData(
          new GenericArrayData(Array(UTF8String.fromString("a"), UTF8String.fromString("b"))),
          new GenericArrayData(Array(1.toShort, 222.toShort))),
        MapType(StringType, ShortType, valueContainsNull = false)
      ),

      // map contains array
      Literal(
        new ArrayBasedMapData(
          new GenericArrayData(Array(UTF8String.fromString("a"), UTF8String.fromString("b"))),
          new GenericArrayData(Array(
            new GenericArrayData(Array(1, 2)), new GenericArrayData(Array(3))
          ))
        ),
        MapType(StringType, ArrayType(IntegerType, containsNull = true))
      )

      // TODO(anchovyu): enable this test when map value comparison problem is fixed
      // mixture of complex data type
      // [map(
      //   alex -> [(12345, home), (67890, work)],
      //   bob -> [(null, home)]
      // )]
      //      , Literal(
      //        new GenericArrayData(Array(
      //          new ArrayBasedMapData(
      //            new GenericArrayData(Array(
      //              UTF8String.fromString("alex"),
      //              UTF8String.fromString("bob"))
      //            ),
      //            new GenericArrayData(Array(
      //              new GenericArrayData(Array(
      //                InternalRow(12345, UTF8String.fromString("home")),
      //                InternalRow(67890, UTF8String.fromString("work")))
      //              ),
      //              new GenericArrayData(Array(InternalRow(null, UTF8String.fromString("home"))))
      //            ))
      //          )
      //        )),
      //        ArrayType(
      //          MapType(
      //            StringType,
      //            ArrayType(
      //              StructType(Seq(
      //                StructField("phone", IntegerType).withComment("phone number"),
      //                StructField("tags", StringType)
      //              ))
      //            )
      //          )
      //        )
      //      )
    ).foreach { lit =>
      val serialized = lit.sql
      val dataTypeJson = lit.dataType.json
      val deserialized = Literal.fromSQL(serialized, dataTypeJson)
      assert(
        deserialized.equals(lit),
        serializationRoundTripInfo(lit, serialized, dataTypeJson, deserialized)
      )
    }
  }

  test(".sql and fromSQL round trip: time related Literals") {
    Seq(
      // date
      Literal(LocalDate.of(1, 1, 1)),
      Literal(LocalDate.of(2100, 5, 17)),

      // timestamp
      Literal(Instant.parse("2023-01-01T00:00:00Z")),
      Literal(Instant.ofEpochMilli(0)),
      Literal.create(
        LocalDateTime
          .of(2019, 3, 21, 0, 2, 3, 456000000)
          .atZone(ZoneOffset.UTC)
          .toInstant
      ),

      // timestampNTZ
      Literal(LocalDateTime.of(2023, 10, 23, 0, 0, 0, 456000000))
    ).foreach { lit =>
      // serialize and deserialize in the same timezone: default
      val timeZone1 = SQLConf.get.sessionLocalTimeZone
      val serialized1 = lit.sql
      val dataTypeJson1 = lit.dataType.json
      val deserialized1 = Literal.fromSQL(serialized1, dataTypeJson1)
      assert(
        deserialized1.equals(lit),
        s"""
           |Serialized and deserialized both in timezone $timeZone1,
           |current timezone to print this prompt is ${SQLConf.get.sessionLocalTimeZone}.
           |${serializationRoundTripInfo(lit, serialized1, dataTypeJson1, deserialized1)}
           |""".stripMargin
      )

      // serialize and deserialize in the same timezone: set by conf
      val timeZone2 = "GMT+1:00"
      withTimeZones(sessionTimeZone = timeZone2, systemTimeZone = "GMT-8:00") {
        val serialized2 = lit.sql
        val dataTypeJson2 = lit.dataType.json
        val deserialized2 = Literal.fromSQL(serialized2, dataTypeJson2)
        assert(
          deserialized2.equals(lit),
          s"""
             |Serialized and deserialized both in timezone $timeZone2,
             |current timezone to print this prompt is ${SQLConf.get.sessionLocalTimeZone}.
             |${serializationRoundTripInfo(lit, serialized2, dataTypeJson2, deserialized2)}
             |""".stripMargin
        )
      }

      // serialize and deserialize in different timezones
      val serializedTimeZone = SQLConf.get.sessionLocalTimeZone
      val deserializedTimeZone = "GMT+1:00"
      val serialized3 = lit.sql
      val dataTypeJson3 = lit.dataType.json
      var deserialized3 = Literal(1)
      withTimeZones(sessionTimeZone = deserializedTimeZone, systemTimeZone = "GMT-8:00") {
        deserialized3 = Literal.fromSQL(serialized3, dataTypeJson3)
      }
      assert(deserialized3.equals(lit),
        s"""
           |Serialized in timezone $serializedTimeZone,
           |deserialized in timezone $deserializedTimeZone,
           |current timezone to print this prompt is ${SQLConf.get.sessionLocalTimeZone}.
           |${serializationRoundTripInfo(lit, serialized3, dataTypeJson3, deserialized3)}
           |""".stripMargin
      )
    }
  }

  test(".sql and fromSQL round trip: CalendarInterval Literals") {
    val lit = Literal(new CalendarInterval(1, 2, 3))
    val serialized = lit.sql
    val dataTypeJson = lit.dataType.json
    val deserialized = Literal.fromSQL(serialized, dataTypeJson)
    assert(
      deserialized.equals(lit),
      serializationRoundTripInfo(lit, serialized, dataTypeJson, deserialized)
    )
  }
}
