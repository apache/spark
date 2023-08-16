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
package org.apache.spark.sql.execution.datasources.xml.util

import java.math.BigDecimal
import java.util.Locale

import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.catalyst.xml.{TypeCast, XmlOptions}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

final class TypeCastSuite extends SharedSparkSession {

  test("Can parse decimal type values") {
    val options = new XmlOptions()
    val stringValues = Seq("10.05", "1,000.01", "158,058,049.001")
    val decimalValues = Seq(10.05, 1000.01, 158058049.001)
    val decimalType = DecimalType.SYSTEM_DEFAULT

    stringValues.zip(decimalValues).foreach { case (strVal, decimalVal) =>
      val dt = new BigDecimal(decimalVal.toString)
      assert(TypeCast.castTo(strVal, decimalType, options) ===
        Decimal(dt, dt.precision(), dt.scale()))
    }
  }

  test("Nullable types are handled") {
    val options = new XmlOptions(Map("nullValue" -> "-"))
    for (t <- Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
                  BooleanType, TimestampType, DateType, StringType)) {
      assert(TypeCast.castTo("-", t, options) === null)
    }
  }

  test("String type should always return the same as the input") {
    val options = new XmlOptions()
    assert(TypeCast.castTo("", StringType, options) === UTF8String.fromString(""))
  }

  test("Types are cast correctly") {
    val options = new XmlOptions()
    assert(TypeCast.castTo("10", ByteType, options) === 10)
    assert(TypeCast.castTo("10", ShortType, options) === 10)
    assert(TypeCast.castTo("10", IntegerType, options) === 10)
    assert(TypeCast.castTo("10", LongType, options) === 10)
    assert(TypeCast.castTo("1.00", FloatType, options) === 1.0)
    assert(TypeCast.castTo("1.00", DoubleType, options) === 1.0)
    assert(TypeCast.castTo("true", BooleanType, options) === true)
    assert(TypeCast.castTo("1", BooleanType, options) === true)
    assert(TypeCast.castTo("false", BooleanType, options) === false)
    assert(TypeCast.castTo("0", BooleanType, options) === false)

    {
      val ts = TypeCast.castTo("2002-05-30 21:46:54", TimestampType, options)
      assert(ts === 1022820414000000L)
      assert(ts ===
        TimestampFormatter(None, DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30 21:46:54"))
    }
    {
      val ts = TypeCast.castTo("2002-05-30T21:46:54", TimestampType, options)
      assert(ts === 1022820414000000L)
      assert(ts ===
        TimestampFormatter(None, DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30T21:46:54"))
    }
    {
      val ts = TypeCast.castTo("2002-05-30T21:46:54.1234", TimestampType, options)
      assert(ts === 1022820414123400L)
      assert(ts ===
        TimestampFormatter(None, DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30T21:46:54.1234"))
    }
    {
      val ts = TypeCast.castTo("2002-05-30T21:46:54Z", TimestampType, options)
      assert(ts === 1022795214000000L)
      assert(ts ===
        TimestampFormatter(None, DateTimeUtils.getZoneId("UTC"),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30T21:46:54Z"))
    }
    {
      val ts = TypeCast.castTo("2002-05-30T21:46:54-06:00", TimestampType, options)
      assert(ts === 1022816814000000L)
      assert(ts ===
        TimestampFormatter(None, DateTimeUtils.getZoneId("-06:00"),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30T21:46:54-06:00"))
    }
    {
      val ts = TypeCast.castTo("2002-05-30T21:46:54+06:00", TimestampType, options)
      assert(ts === 1022773614000000L)
      assert(ts ===
        TimestampFormatter(None, DateTimeUtils.getZoneId("+06:00"),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30T21:46:54+06:00"))
    }
    {
      val ts = TypeCast.castTo("2002-05-30T21:46:54.1234Z", TimestampType, options)
      assert(ts === 1022795214123400L)
      assert(ts ===
        TimestampFormatter(None, DateTimeUtils.getZoneId("UTC"),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30T21:46:54.1234Z"))
    }
    {
      val ts = TypeCast.castTo("2002-05-30T21:46:54.1234-06:00", TimestampType, options)
      assert(ts === 1022816814123400L)
      assert(ts ===
        TimestampFormatter(None, DateTimeUtils.getZoneId("-06:00"),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30T21:46:54.1234-06:00"))
    }
    {
      val ts = TypeCast.castTo("2002-05-30T21:46:54.1234+06:00", TimestampType, options)
      assert(ts === 1022773614123400L)
      assert(ts ===
        TimestampFormatter(None, DateTimeUtils.getZoneId("+06:00"),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30T21:46:54.1234+06:00"))
    }
    {
      val date = TypeCast.castTo("2002-09-24", DateType, options)
      assert(date === 11954)
      assert(date === DateFormatter(DateFormatter.defaultPattern,
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-09-24"))
    }
  }

  test("Types with sign are cast correctly") {
    val options = new XmlOptions()
    assert(TypeCast.signSafeToInt("+10", options) === 10)
    assert(TypeCast.signSafeToLong("-10", options) === -10)
    assert(TypeCast.signSafeToFloat("1.00", options) === 1.0)
    assert(TypeCast.signSafeToDouble("-1.00", options) === -1.0)
  }

  test("Types with sign are checked correctly") {
    assert(TypeCast.isBoolean("true"))
    assert(TypeCast.isInteger("10"))
    assert(TypeCast.isLong("10"))
    assert(TypeCast.isDouble("+10.1"))
    assert(!TypeCast.isDouble("8E9D"))
    assert(!TypeCast.isDouble("8E9F"))
    val timestamp = "2015-01-01 00:00:00"
    assert(TypeCast.isTimestamp(timestamp, new XmlOptions()))
  }

  test("Float and Double Types are cast correctly with Locale") {
    val options = new XmlOptions()
    val defaultLocale = Locale.getDefault
    try {
      Locale.setDefault(Locale.FRANCE)
      assert(TypeCast.castTo("1,00", FloatType, options) === 1.0)
      assert(TypeCast.castTo("1,00", DoubleType, options) === 1.0)
    } finally {
      Locale.setDefault(defaultLocale)
    }
  }

  test("Parsing built-in timestamp formatters") {
    val options = XmlOptions(Map())
    val expectedResult =
      TimestampFormatter(None, DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone),
      Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30 21:46:54")
    assert(
      TypeCast.castTo("2002-05-30 21:46:54", TimestampType, options) === expectedResult
    )
    assert(
      TypeCast.castTo("2002-05-30T21:46:54", TimestampType, options) === expectedResult
    )
    assert(
      TypeCast.castTo("2002-05-30T21:46:54+00:00", TimestampType, options) ===
        TimestampFormatter(None, DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30T21:46:54+00:00")
    )
    assert(
      TypeCast.castTo("2002-05-30T21:46:54.0000Z", TimestampType, options) ===
        TimestampFormatter(None, DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone),
          Locale.US, FAST_DATE_FORMAT, true).parse("2002-05-30T21:46:54.0000Z")
    )
  }

  test("Custom timestamp format is used to parse correctly") {
    var options = XmlOptions(Map("timestampFormat" -> "MM-dd-yyyy HH:mm:ss", "timezone" -> "UTC"))
    assert(
      TypeCast.castTo("12-03-2011 10:15:30", TimestampType, options) ===
        TimestampFormatter("MM-dd-yyyy HH:mm:ss", DateTimeUtils.getZoneId("UTC"),
          Locale.US, FAST_DATE_FORMAT, true).parse("12-03-2011 10:15:30")
    )

    options = XmlOptions(Map("timestampFormat" -> "yyyy/MM/dd HH:mm:ss", "timezone" -> "UTC"))
    assert(
      TypeCast.castTo("2011/12/03 10:15:30", TimestampType, options) ===
        TimestampFormatter("yyyy/MM/dd HH:mm:ss", DateTimeUtils.getZoneId("UTC"),
          Locale.US, FAST_DATE_FORMAT, true).parse("2011/12/03 10:15:30")
    )

    options = XmlOptions(Map("timestampFormat" -> "yyyy/MM/dd HH:mm:ss",
      "timezone" -> "Asia/Shanghai"))
    assert(
      TypeCast.castTo("2011/12/03 10:15:30", TimestampType, options) ===
        TimestampFormatter("yyyy/MM/dd HH:mm:ss", DateTimeUtils.getZoneId("Asia/Shanghai"),
          Locale.US, FAST_DATE_FORMAT, true).parse("2011/12/03 10:15:30")
    )

    options = XmlOptions(Map("timestampFormat" -> "yyyy/MM/dd HH:mm:ss",
      "timezone" -> "Asia/Shanghai"))
    assert(
      TypeCast.castTo("2011/12/03 10:15:30", TimestampType, options) ===
        TimestampFormatter("yyyy/MM/dd HH:mm:ss", DateTimeUtils.getZoneId("Asia/Shanghai"),
          Locale.US, FAST_DATE_FORMAT, true).parse("2011/12/03 10:15:30")
    )

    options = XmlOptions(Map("timestampFormat" -> "yyyy/MM/dd HH:mm:ss"))
    assert(TypeCast.castTo("2011/12/03 10:15:30", TimestampType, options) ===
        TimestampFormatter("yyyy/MM/dd HH:mm:ss",
          DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone),
          Locale.US, FAST_DATE_FORMAT, true).parse("2011/12/03 10:15:30")
    )
  }
}
