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

package org.apache.spark.sql.catalyst.util

import java.time._
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.{ChronoField, TemporalAccessor, TemporalQueries}
import java.util.{Locale, TimeZone}

import scala.util.Try

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.internal.SQLConf

sealed trait DateTimeFormatter {
  def parse(s: String): Long // returns microseconds since epoch
  def format(us: Long): String
}

trait FormatterUtils {
  protected def zoneId: ZoneId
  protected def buildFormatter(
      pattern: String,
      locale: Locale): java.time.format.DateTimeFormatter = {
    new DateTimeFormatterBuilder()
      .appendPattern(pattern)
      .parseDefaulting(ChronoField.YEAR_OF_ERA, 1970)
      .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter(locale)
  }
  protected def toInstantWithZoneId(temporalAccessor: TemporalAccessor): java.time.Instant = {
    val localDateTime = LocalDateTime.from(temporalAccessor)
    val zonedDateTime = ZonedDateTime.of(localDateTime, zoneId)
    Instant.from(zonedDateTime)
  }
}

class Iso8601DateTimeFormatter(
    pattern: String,
    timeZone: TimeZone,
    locale: Locale) extends DateTimeFormatter with FormatterUtils {
  val zoneId = timeZone.toZoneId
  val formatter = buildFormatter(pattern, locale)

  def toInstant(s: String): Instant = {
    val temporalAccessor = formatter.parse(s)
    if (temporalAccessor.query(TemporalQueries.offset()) == null) {
      toInstantWithZoneId(temporalAccessor)
    } else {
      Instant.from(temporalAccessor)
    }
  }

  private def instantToMicros(instant: Instant): Long = {
    val sec = Math.multiplyExact(instant.getEpochSecond, DateTimeUtils.MICROS_PER_SECOND)
    val result = Math.addExact(sec, instant.getNano / DateTimeUtils.NANOS_PER_MICROS)
    result
  }

  def parse(s: String): Long = instantToMicros(toInstant(s))

  def format(us: Long): String = {
    val secs = Math.floorDiv(us, DateTimeUtils.MICROS_PER_SECOND)
    val mos = Math.floorMod(us, DateTimeUtils.MICROS_PER_SECOND)
    val instant = Instant.ofEpochSecond(secs, mos * DateTimeUtils.NANOS_PER_MICROS)

    formatter.withZone(timeZone.toZoneId).format(instant)
  }
}

class LegacyDateTimeFormatter(
    pattern: String,
    timeZone: TimeZone,
    locale: Locale) extends DateTimeFormatter {
  val format = FastDateFormat.getInstance(pattern, timeZone, locale)

  protected def toMillis(s: String): Long = format.parse(s).getTime

  def parse(s: String): Long = toMillis(s) * DateTimeUtils.MICROS_PER_MILLIS

  def format(us: Long): String = {
    format.format(DateTimeUtils.toJavaTimestamp(us))
  }
}

class LegacyFallbackDateTimeFormatter(
    pattern: String,
    timeZone: TimeZone,
    locale: Locale) extends LegacyDateTimeFormatter(pattern, timeZone, locale) {
  override def toMillis(s: String): Long = {
    Try {super.toMillis(s)}.getOrElse(DateTimeUtils.stringToTime(s).getTime)
  }
}

object DateTimeFormatter {
  def apply(format: String, timeZone: TimeZone, locale: Locale): DateTimeFormatter = {
    if (SQLConf.get.legacyTimeParserEnabled) {
      new LegacyFallbackDateTimeFormatter(format, timeZone, locale)
    } else {
      new Iso8601DateTimeFormatter(format, timeZone, locale)
    }
  }
}

sealed trait DateFormatter {
  def parse(s: String): Int // returns days since epoch
  def format(days: Int): String
}

class Iso8601DateFormatter(
    pattern: String,
    locale: Locale) extends DateFormatter with FormatterUtils {

  val zoneId = ZoneId.of("GMT")

  val formatter = buildFormatter(pattern, locale)

  def toInstant(s: String): Instant = {
    val temporalAccessor = formatter.parse(s)
    toInstantWithZoneId(temporalAccessor)
  }

  override def parse(s: String): Int = {
    val seconds = toInstant(s).getEpochSecond
    val days = Math.floorDiv(seconds, DateTimeUtils.SECONDS_PER_DAY)

    days.toInt
  }

  override def format(days: Int): String = {
    val instant = Instant.ofEpochSecond(days * DateTimeUtils.SECONDS_PER_DAY)
    formatter.withZone(zoneId).format(instant)
  }
}

class LegacyDateFormatter(pattern: String, locale: Locale) extends DateFormatter {
  val format = FastDateFormat.getInstance(pattern, locale)

  def parse(s: String): Int = {
    val milliseconds = format.parse(s).getTime
    DateTimeUtils.millisToDays(milliseconds)
  }

  def format(days: Int): String = {
    val date = DateTimeUtils.toJavaDate(days)
    format.format(date)
  }
}

class LegacyFallbackDateFormatter(
    pattern: String,
    locale: Locale) extends LegacyDateFormatter(pattern, locale) {
  override def parse(s: String): Int = {
    Try(super.parse(s)).orElse {
      // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
      // compatibility.
      Try(DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(s).getTime))
    }.getOrElse {
      // In Spark 1.5.0, we store the data as number of days since epoch in string.
      // So, we just convert it to Int.
      s.toInt
    }
  }
}

object DateFormatter {
  def apply(format: String, locale: Locale): DateFormatter = {
    if (SQLConf.get.legacyTimeParserEnabled) {
      new LegacyFallbackDateFormatter(format, locale)
    } else {
      new Iso8601DateFormatter(format, locale)
    }
  }
}
