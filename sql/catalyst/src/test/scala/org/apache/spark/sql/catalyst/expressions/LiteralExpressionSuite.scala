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
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.ExamplePointUDT
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
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
        condition = "INTERNAL_ERROR",
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
      val expected = "TIMESTAMP '2019-03-21 01:02:03.456'"
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

  // A generic internal row that throws exception when accessing null values
  class NullAccessForbiddenGenericInternalRow(override val values: Array[Any])
    extends GenericInternalRow(values) {
    override def get(ordinal: Int, dataType: DataType): AnyRef = {
      if (values(ordinal) == null) {
        throw new RuntimeException(s"Should not access null field at $ordinal!")
      }
      super.get(ordinal, dataType)
    }
  }

  test("SPARK-46634: literal validation should not drill down to null fields") {
    val exceptionInternalRow = new NullAccessForbiddenGenericInternalRow(Array(null, 1))
    val schema = StructType.fromDDL("id INT, age INT")
    // This should not fail because it should check whether the field is null before drilling down
    Literal.validateLiteralValue(exceptionInternalRow, schema)
  }

  test("SPARK-46604: Literal support immutable ArraySeq") {
    import org.apache.spark.util.ArrayImplicits._
    val immArraySeq = Array(1.0, 4.0).toImmutableArraySeq
    val expected = toCatalyst(immArraySeq)
    checkEvaluation(Literal(immArraySeq), expected)
    checkEvaluation(Literal.create(immArraySeq), expected)
    checkEvaluation(Literal.create(immArraySeq, ArrayType(DoubleType)), expected)
  }
}
