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

import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.time._
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoField.MICRO_OF_SECOND
import java.time.temporal.TemporalQueries
import java.util.{Date, Locale, TimeZone}
import java.util.concurrent.TimeUnit.SECONDS

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_MILLIS
import org.apache.spark.sql.catalyst.util.DateTimeUtils.convertSpecialTimestamp
import org.apache.spark.sql.internal.SQLConf

sealed trait TimestampFormatter extends Serializable {
  /**
   * Parses a timestamp in a string and converts it to microseconds.
   *
   * @param s - string with timestamp to parse
   * @return microseconds since epoch.
   * @throws ParseException can be thrown by legacy parser
   * @throws DateTimeParseException can be thrown by new parser
   * @throws DateTimeException unable to obtain local date or time
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  def parse(s: String): Long
  def format(us: Long): String
}

class Iso8601TimestampFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale) extends TimestampFormatter with DateTimeFormatterHelper {
  @transient
  protected lazy val formatter = getOrCreateFormatter(pattern, locale)

  override def parse(s: String): Long = {
    val specialDate = convertSpecialTimestamp(s.trim, zoneId)
    specialDate.getOrElse {
      val parsed = formatter.parse(s)
      val parsedZoneId = parsed.query(TemporalQueries.zone())
      val timeZoneId = if (parsedZoneId == null) zoneId else parsedZoneId
      val zonedDateTime = toZonedDateTime(parsed, timeZoneId)
      val epochSeconds = zonedDateTime.toEpochSecond
      val microsOfSecond = zonedDateTime.get(MICRO_OF_SECOND)

      Math.addExact(SECONDS.toMicros(epochSeconds), microsOfSecond)
    }
  }

  override def format(us: Long): String = {
    val instant = DateTimeUtils.microsToInstant(us)
    formatter.withZone(zoneId).format(instant)
  }
}

/**
 * The formatter parses/formats timestamps according to the pattern `uuuu-MM-dd HH:mm:ss.[..fff..]`
 * where `[..fff..]` is a fraction of second up to microsecond resolution. The formatter does not
 * output trailing zeros in the fraction. For example, the timestamp `2019-03-05 15:00:01.123400` is
 * formatted as the string `2019-03-05 15:00:01.1234`.
 *
 * @param zoneId the time zone identifier in which the formatter parses or format timestamps
 */
class FractionTimestampFormatter(zoneId: ZoneId)
  extends Iso8601TimestampFormatter("", zoneId, TimestampFormatter.defaultLocale) {

  @transient
  override protected lazy val formatter = DateTimeFormatterHelper.fractionFormatter
}

trait LegacyTimestampFormatter extends TimestampFormatter {
  def parseToDate(s: String): Date
  def formatTimestamp(t: Timestamp): String

  override def parse(s: String): Long = {
    parseToDate(s).getTime * MICROS_PER_MILLIS
  }

  override def format(us: Long): String = {
    val timestamp = DateTimeUtils.toJavaTimestamp(us)
    formatTimestamp(timestamp)
  }
}

class LegacyFastDateFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale) extends LegacyTimestampFormatter {
  @transient private lazy val fdf =
    FastDateFormat.getInstance(pattern, TimeZone.getTimeZone(zoneId), locale)
  override def parseToDate(s: String): Date = fdf.parse(s)
  override def formatTimestamp(t: Timestamp): String = fdf.format(t)
}

class LegacySimpleDateFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale,
    lenient: Boolean = true) extends LegacyTimestampFormatter {
  @transient private lazy val sdf = {
    val formatter = new SimpleDateFormat(pattern, locale)
    formatter.setTimeZone(TimeZone.getTimeZone(zoneId))
    formatter.setLenient(lenient)
    formatter
  }
  override def parseToDate(s: String): Date = sdf.parse(s)
  override def formatTimestamp(t: Timestamp): String = sdf.format(t)
}

object LegacyDateFormats extends Enumeration {
  type LegacyDateFormat = Value
  val FAST_DATE_FORMAT, SIMPLE_DATE_FORMAT, LENIENT_SIMPLE_DATE_FORMAT = Value
}

object TimestampFormatter {
  import LegacyDateFormats._

  val defaultLocale: Locale = Locale.US

  def defaultPattern(): String = {
    if (SQLConf.get.legacyTimeParserEnabled) {
      "yyyy-MM-dd HH:mm:ss"
    } else {
      "uuuu-MM-dd HH:mm:ss"
    }
  }

  private def getFormatter(
    format: Option[String],
    zoneId: ZoneId,
    locale: Locale,
    legacyFormat: LegacyDateFormat): TimestampFormatter = {

    val pattern = format.getOrElse(defaultPattern)
    if (SQLConf.get.legacyTimeParserEnabled) {
      legacyFormat match {
        case FAST_DATE_FORMAT =>
          new LegacyFastDateFormatter(pattern, zoneId, locale)
        case SIMPLE_DATE_FORMAT =>
          new LegacySimpleDateFormatter(pattern, zoneId, locale, lenient = false)
        case LENIENT_SIMPLE_DATE_FORMAT =>
          new LegacySimpleDateFormatter(pattern, zoneId, locale, lenient = true)
      }
    } else {
      new Iso8601TimestampFormatter(pattern, zoneId, locale)
    }
  }

  def apply(
    format: String,
    zoneId: ZoneId,
    locale: Locale,
    legacyFormat: LegacyDateFormat): TimestampFormatter = {
    getFormatter(Some(format), zoneId, locale, legacyFormat)
  }

  def apply(format: String, zoneId: ZoneId): TimestampFormatter = {
    apply(format, zoneId, defaultLocale, LENIENT_SIMPLE_DATE_FORMAT)
  }

  def apply(zoneId: ZoneId): TimestampFormatter = {
    getFormatter(None, zoneId, defaultLocale, LENIENT_SIMPLE_DATE_FORMAT)
  }

  def getFractionFormatter(zoneId: ZoneId): TimestampFormatter = {
    new FractionTimestampFormatter(zoneId)
  }

  def withStrongLegacy(format: String, zoneId: ZoneId): TimestampFormatter = {
    apply(format, zoneId, defaultLocale, SIMPLE_DATE_FORMAT)
  }
}
