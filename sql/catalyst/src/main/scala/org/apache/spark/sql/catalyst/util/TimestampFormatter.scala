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
import java.text.{ParseException, ParsePosition, SimpleDateFormat}
import java.time._
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.ChronoField.MICRO_OF_SECOND
import java.time.temporal.TemporalQueries
import java.util.{Calendar, GregorianCalendar, Locale, TimeZone}
import java.util.concurrent.TimeUnit.SECONDS

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.{LegacyDateFormat, LENIENT_SIMPLE_DATE_FORMAT}
import org.apache.spark.sql.catalyst.util.RebaseDateTime._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy._
import org.apache.spark.sql.types.Decimal

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
    locale: Locale,
    legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT,
    needVarLengthSecondFraction: Boolean)
  extends TimestampFormatter with DateTimeFormatterHelper {
  @transient
  protected lazy val formatter: DateTimeFormatter =
    getOrCreateFormatter(pattern, locale, needVarLengthSecondFraction)

  @transient
  protected lazy val legacyFormatter = TimestampFormatter.getLegacyFormatter(
    pattern, zoneId, locale, legacyFormat)

  override def parse(s: String): Long = {
    val specialDate = convertSpecialTimestamp(s.trim, zoneId)
    specialDate.getOrElse {
      try {
        val parsed = formatter.parse(s)
        val parsedZoneId = parsed.query(TemporalQueries.zone())
        val timeZoneId = if (parsedZoneId == null) zoneId else parsedZoneId
        val zonedDateTime = toZonedDateTime(parsed, timeZoneId)
        val epochSeconds = zonedDateTime.toEpochSecond
        val microsOfSecond = zonedDateTime.get(MICRO_OF_SECOND)

        Math.addExact(SECONDS.toMicros(epochSeconds), microsOfSecond)
      } catch checkDiffResult(s, legacyFormatter.parse)
    }
  }

  override def format(us: Long): String = {
    val instant = DateTimeUtils.microsToInstant(us)
    formatter.withZone(zoneId).format(instant)
  }
}

/**
 * The formatter parses/formats timestamps according to the pattern `yyyy-MM-dd HH:mm:ss.[..fff..]`
 * where `[..fff..]` is a fraction of second up to microsecond resolution. The formatter does not
 * output trailing zeros in the fraction. For example, the timestamp `2019-03-05 15:00:01.123400` is
 * formatted as the string `2019-03-05 15:00:01.1234`.
 *
 * @param zoneId the time zone identifier in which the formatter parses or format timestamps
 */
class FractionTimestampFormatter(zoneId: ZoneId)
  extends Iso8601TimestampFormatter(
    "", zoneId, TimestampFormatter.defaultLocale, needVarLengthSecondFraction = false) {

  @transient
  override protected lazy val formatter = DateTimeFormatterHelper.fractionFormatter
}

/**
 * The custom sub-class of `GregorianCalendar` is needed to get access to
 * protected `fields` immediately after parsing. We cannot use
 * the `get()` method because it performs normalization of the fraction
 * part. Accordingly, the `MILLISECOND` field doesn't contain original value.
 *
 * Also this class allows to set raw value to the `MILLISECOND` field
 * directly before formatting.
 */
class MicrosCalendar(tz: TimeZone, digitsInFraction: Int)
  extends GregorianCalendar(tz, Locale.US) {
  // Converts parsed `MILLISECOND` field to seconds fraction in microsecond precision.
  // For example if the fraction pattern is `SSSS` then `digitsInFraction` = 4, and
  // if the `MILLISECOND` field was parsed to `1234`.
  def getMicros(): SQLTimestamp = {
    // Append 6 zeros to the field: 1234 -> 1234000000
    val d = fields(Calendar.MILLISECOND) * MICROS_PER_SECOND
    // Take the first 6 digits from `d`: 1234000000 -> 123400
    // The rest contains exactly `digitsInFraction`: `0000` = 10 ^ digitsInFraction
    // So, the result is `(1234 * 1000000) / (10 ^ digitsInFraction)
    d / Decimal.POW_10(digitsInFraction)
  }

  // Converts the seconds fraction in microsecond precision to a value
  // that can be correctly formatted according to the specified fraction pattern.
  // The method performs operations opposite to `getMicros()`.
  def setMicros(micros: Long): Unit = {
    val d = micros * Decimal.POW_10(digitsInFraction)
    fields(Calendar.MILLISECOND) = (d / MICROS_PER_SECOND).toInt
  }
}

class LegacyFastTimestampFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale) extends TimestampFormatter {

  @transient private lazy val fastDateFormat =
    FastDateFormat.getInstance(pattern, TimeZone.getTimeZone(zoneId), locale)
  @transient private lazy val cal = new MicrosCalendar(
    fastDateFormat.getTimeZone,
    fastDateFormat.getPattern.count(_ == 'S'))

  def parse(s: String): SQLTimestamp = {
    cal.clear() // Clear the calendar because it can be re-used many times
    if (!fastDateFormat.parse(s, new ParsePosition(0), cal)) {
      throw new IllegalArgumentException(s"'$s' is an invalid timestamp")
    }
    val micros = cal.getMicros()
    cal.set(Calendar.MILLISECOND, 0)
    val julianMicros = Math.addExact(millisToMicros(cal.getTimeInMillis), micros)
    rebaseJulianToGregorianMicros(julianMicros)
  }

  def format(timestamp: SQLTimestamp): String = {
    val julianMicros = rebaseGregorianToJulianMicros(timestamp)
    cal.setTimeInMillis(Math.floorDiv(julianMicros, MICROS_PER_SECOND) * MILLIS_PER_SECOND)
    cal.setMicros(Math.floorMod(julianMicros, MICROS_PER_SECOND))
    fastDateFormat.format(cal)
  }
}

class LegacySimpleTimestampFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale,
    lenient: Boolean = true) extends TimestampFormatter {
  @transient private lazy val sdf = {
    val formatter = new SimpleDateFormat(pattern, locale)
    formatter.setTimeZone(TimeZone.getTimeZone(zoneId))
    formatter.setLenient(lenient)
    formatter
  }

  override def parse(s: String): Long = {
    fromJavaTimestamp(new Timestamp(sdf.parse(s).getTime))
  }

  override def format(us: Long): String = {
    sdf.format(toJavaTimestamp(us))
  }
}

object LegacyDateFormats extends Enumeration {
  type LegacyDateFormat = Value
  val FAST_DATE_FORMAT, SIMPLE_DATE_FORMAT, LENIENT_SIMPLE_DATE_FORMAT = Value
}

object TimestampFormatter {
  import LegacyDateFormats._

  val defaultLocale: Locale = Locale.US

  def defaultPattern(): String = s"${DateFormatter.defaultPattern} HH:mm:ss"

  private def getFormatter(
      format: Option[String],
      zoneId: ZoneId,
      locale: Locale = defaultLocale,
      legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT,
      needVarLengthSecondFraction: Boolean = false): TimestampFormatter = {
    val pattern = format.getOrElse(defaultPattern)
    if (SQLConf.get.legacyTimeParserPolicy == LEGACY) {
      getLegacyFormatter(pattern, zoneId, locale, legacyFormat)
    } else {
      new Iso8601TimestampFormatter(
        pattern, zoneId, locale, legacyFormat, needVarLengthSecondFraction)
    }
  }

  def getLegacyFormatter(
      pattern: String,
      zoneId: ZoneId,
      locale: Locale,
      legacyFormat: LegacyDateFormat): TimestampFormatter = {
    legacyFormat match {
      case FAST_DATE_FORMAT =>
        new LegacyFastTimestampFormatter(pattern, zoneId, locale)
      case SIMPLE_DATE_FORMAT =>
        new LegacySimpleTimestampFormatter(pattern, zoneId, locale, lenient = false)
      case LENIENT_SIMPLE_DATE_FORMAT =>
        new LegacySimpleTimestampFormatter(pattern, zoneId, locale, lenient = true)
    }
  }

  def apply(
      format: String,
      zoneId: ZoneId,
      locale: Locale,
      legacyFormat: LegacyDateFormat,
      needVarLengthSecondFraction: Boolean): TimestampFormatter = {
    getFormatter(Some(format), zoneId, locale, legacyFormat, needVarLengthSecondFraction)
  }

  def apply(
      format: String,
      zoneId: ZoneId,
      legacyFormat: LegacyDateFormat,
      needVarLengthSecondFraction: Boolean): TimestampFormatter = {
    getFormatter(Some(format), zoneId, defaultLocale, legacyFormat, needVarLengthSecondFraction)
  }

  def apply(
      format: String,
      zoneId: ZoneId,
      needVarLengthSecondFraction: Boolean = false): TimestampFormatter = {
    getFormatter(Some(format), zoneId, needVarLengthSecondFraction = needVarLengthSecondFraction)
  }

  def apply(zoneId: ZoneId): TimestampFormatter = {
    getFormatter(None, zoneId)
  }

  def getFractionFormatter(zoneId: ZoneId): TimestampFormatter = {
    new FractionTimestampFormatter(zoneId)
  }
}
