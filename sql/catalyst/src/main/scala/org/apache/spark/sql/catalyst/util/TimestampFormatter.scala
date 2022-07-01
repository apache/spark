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

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.{LegacyDateFormat, LENIENT_SIMPLE_DATE_FORMAT}
import org.apache.spark.sql.catalyst.util.RebaseDateTime._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy._
import org.apache.spark.sql.types.{Decimal, TimestampNTZType}
import org.apache.spark.unsafe.types.UTF8String

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

  /**
   * Parses a timestamp in a string and converts it to an optional number of microseconds.
   *
   * @param s - string with timestamp to parse
   * @return An optional number of microseconds since epoch. The result is None on invalid input.
   * @throws ParseException can be thrown by legacy parser
   * @throws DateTimeParseException can be thrown by new parser
   * @throws DateTimeException unable to obtain local date or time
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  def parseOptional(s: String): Option[Long] =
    try {
      Some(parse(s))
    } catch {
      case _: Exception => None
    }

  /**
   * Parses a timestamp in a string and converts it to microseconds since Unix Epoch in local time.
   *
   * @param s - string with timestamp to parse
   * @param allowTimeZone - indicates strict parsing of timezone
   * @return microseconds since epoch.
   * @throws ParseException can be thrown by legacy parser
   * @throws DateTimeParseException can be thrown by new parser
   * @throws DateTimeException unable to obtain local date or time
   * @throws IllegalStateException The formatter for timestamp without time zone should always
   *                               implement this method. The exception should never be hit.
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  @throws(classOf[IllegalStateException])
  def parseWithoutTimeZone(s: String, allowTimeZone: Boolean): Long =
    throw new IllegalStateException(
      s"The method `parseWithoutTimeZone(s: String, allowTimeZone: Boolean)` should be " +
        "implemented in the formatter of timestamp without time zone")

  /**
   * Parses a timestamp in a string and converts it to an optional number of microseconds since
   * Unix Epoch in local time.
   *
   * @param s - string with timestamp to parse
   * @param allowTimeZone - indicates strict parsing of timezone
   * @return An optional number of microseconds since epoch. The result is None on invalid input.
   * @throws ParseException can be thrown by legacy parser
   * @throws DateTimeParseException can be thrown by new parser
   * @throws DateTimeException unable to obtain local date or time
   * @throws IllegalStateException The formatter for timestamp without time zone should always
   *                               implement this method. The exception should never be hit.
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  @throws(classOf[IllegalStateException])
  def parseWithoutTimeZoneOptional(s: String, allowTimeZone: Boolean): Option[Long] =
    try {
      Some(parseWithoutTimeZone(s, allowTimeZone))
    } catch {
      case _: Exception => None
    }

  /**
   * Parses a timestamp in a string and converts it to microseconds since Unix Epoch in local time.
   * Zone-id and zone-offset components are ignored.
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  @throws(classOf[IllegalStateException])
  final def parseWithoutTimeZone(s: String): Long =
    // This is implemented to adhere to the original behaviour of `parseWithoutTimeZone` where we
    // did not fail if timestamp contained zone-id or zone-offset component and instead ignored it.
    parseWithoutTimeZone(s, true)

  def format(us: Long): String
  def format(ts: Timestamp): String
  def format(instant: Instant): String

  @throws(classOf[IllegalStateException])
  def format(localDateTime: LocalDateTime): String =
    throw new IllegalStateException(
      s"The method `format(localDateTime: LocalDateTime)` should be implemented in the formatter " +
        "of timestamp without time zone")

  /**
   * Validates the pattern string.
   * @param checkLegacy  if true and the pattern is invalid, check whether the pattern is valid for
   *                     legacy formatters and show hints for using legacy formatter.
   *                     Otherwise, simply check the pattern string.
   */
  def validatePatternString(checkLegacy: Boolean): Unit
}

class Iso8601TimestampFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale,
    legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT,
    isParsing: Boolean)
  extends TimestampFormatter with DateTimeFormatterHelper {
  @transient
  protected lazy val formatter: DateTimeFormatter =
    getOrCreateFormatter(pattern, locale, isParsing)

  @transient
  protected lazy val legacyFormatter = TimestampFormatter.getLegacyFormatter(
    pattern, zoneId, locale, legacyFormat)

  override def parse(s: String): Long = {
    try {
      val parsed = formatter.parse(s)
      val parsedZoneId = parsed.query(TemporalQueries.zone())
      val timeZoneId = if (parsedZoneId == null) zoneId else parsedZoneId
      val zonedDateTime = toZonedDateTime(parsed, timeZoneId)
      val epochSeconds = zonedDateTime.toEpochSecond
      val microsOfSecond = zonedDateTime.get(MICRO_OF_SECOND)

      Math.addExact(Math.multiplyExact(epochSeconds, MICROS_PER_SECOND), microsOfSecond)
    } catch checkParsedDiff(s, legacyFormatter.parse)
  }

  override def parseWithoutTimeZone(s: String, allowTimeZone: Boolean): Long = {
    try {
      val parsed = formatter.parse(s)
      if (!allowTimeZone && parsed.query(TemporalQueries.zone()) != null) {
        throw QueryExecutionErrors.cannotParseStringAsDataTypeError(pattern, s, TimestampNTZType)
      }
      val localDate = toLocalDate(parsed)
      val localTime = toLocalTime(parsed)
      DateTimeUtils.localDateTimeToMicros(LocalDateTime.of(localDate, localTime))
    } catch checkParsedDiff(s, legacyFormatter.parse)
  }

  override def format(instant: Instant): String = {
    try {
      formatter.withZone(zoneId).format(instant)
    } catch checkFormattedDiff(toJavaTimestamp(instantToMicros(instant)),
      (t: Timestamp) => format(t))
  }

  override def format(us: Long): String = {
    val instant = DateTimeUtils.microsToInstant(us)
    format(instant)
  }

  override def format(ts: Timestamp): String = {
    legacyFormatter.format(ts)
  }

  override def format(localDateTime: LocalDateTime): String = {
    localDateTime.format(formatter)
  }

  override def validatePatternString(checkLegacy: Boolean): Unit = {
    if (checkLegacy) {
      try {
        formatter
      } catch checkLegacyFormatter(pattern,
        legacyFormatter.validatePatternString(checkLegacy = true))
      ()
    } else {
      try {
        formatter
      } catch checkInvalidPattern(pattern)
    }
  }
}

/**
 * The formatter for timestamps which doesn't require users to specify a pattern. While formatting,
 * it uses the default pattern [[TimestampFormatter.defaultPattern()]]. In parsing, it follows
 * the CAST logic in conversion of strings to Catalyst's TimestampType.
 *
 * @param zoneId The time zone ID in which timestamps should be formatted or parsed.
 * @param locale The locale overrides the system locale and is used in formatting.
 * @param legacyFormat Defines the formatter used for legacy timestamps.
 * @param isParsing Whether the formatter is used for parsing (`true`) or for formatting (`false`).
 */
class DefaultTimestampFormatter(
    zoneId: ZoneId,
    locale: Locale,
    legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT,
    isParsing: Boolean)
  extends Iso8601TimestampFormatter(
    TimestampFormatter.defaultPattern(), zoneId, locale, legacyFormat, isParsing) {

  override def parse(s: String): Long = {
    try {
      DateTimeUtils.stringToTimestampAnsi(UTF8String.fromString(s), zoneId)
    } catch checkParsedDiff(s, legacyFormatter.parse)
  }

  override def parseOptional(s: String): Option[Long] =
    DateTimeUtils.stringToTimestamp(UTF8String.fromString(s), zoneId)

  override def parseWithoutTimeZone(s: String, allowTimeZone: Boolean): Long = {
    try {
      val utf8Value = UTF8String.fromString(s)
      DateTimeUtils.stringToTimestampWithoutTimeZone(utf8Value, allowTimeZone).getOrElse {
        throw QueryExecutionErrors.cannotParseStringAsDataTypeError(
          TimestampFormatter.defaultPattern(), s, TimestampNTZType)
      }
    } catch checkParsedDiff(s, legacyFormatter.parse)
  }

  override def parseWithoutTimeZoneOptional(s: String, allowTimeZone: Boolean): Option[Long] = {
    val utf8Value = UTF8String.fromString(s)
    DateTimeUtils.stringToTimestampWithoutTimeZone(utf8Value, allowTimeZone)
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
    TimestampFormatter.defaultPattern,
    zoneId,
    TimestampFormatter.defaultLocale,
    LegacyDateFormats.FAST_DATE_FORMAT,
    isParsing = false) {

  @transient
  override protected lazy val formatter = DateTimeFormatterHelper.fractionFormatter

  // The new formatter will omit the trailing 0 in the timestamp string, but the legacy formatter
  // can't. Here we use the legacy formatter to format the given timestamp up to seconds fractions,
  // and custom implementation to format the fractional part without trailing zeros.
  override def format(ts: Timestamp): String = {
    val formatted = legacyFormatter.format(ts)
    var nanos = ts.getNanos
    if (nanos == 0) {
      formatted
    } else {
      // Formats non-zero seconds fraction w/o trailing zeros. For example:
      //   formatted = '2020-05:27 15:55:30'
      //   nanos = 001234000
      // Counts the length of the fractional part: 001234000 -> 6
      var fracLen = 9
      while (nanos % 10 == 0) {
        nanos /= 10
        fracLen -= 1
      }
      // Places `nanos` = 1234 after '2020-05:27 15:55:30.'
      val fracOffset = formatted.length + 1
      val totalLen = fracOffset + fracLen
      // The buffer for the final result: '2020-05:27 15:55:30.001234'
      val buf = new Array[Char](totalLen)
      formatted.getChars(0, formatted.length, buf, 0)
      buf(formatted.length) = '.'
      var i = totalLen
      do {
        i -= 1
        buf(i) = ('0' + (nanos % 10)).toChar
        nanos /= 10
      } while (i > fracOffset)
      new String(buf)
    }
  }
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
  def getMicros(): Long = {
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

  override def parse(s: String): Long = {
    cal.clear() // Clear the calendar because it can be re-used many times
    if (!fastDateFormat.parse(s, new ParsePosition(0), cal)) {
      throw new IllegalArgumentException(s"'$s' is an invalid timestamp")
    }
    val micros = cal.getMicros()
    cal.set(Calendar.MILLISECOND, 0)
    val julianMicros = Math.addExact(millisToMicros(cal.getTimeInMillis), micros)
    rebaseJulianToGregorianMicros(julianMicros)
  }

  override def format(timestamp: Long): String = {
    val julianMicros = rebaseGregorianToJulianMicros(timestamp)
    cal.setTimeInMillis(Math.floorDiv(julianMicros, MICROS_PER_SECOND) * MILLIS_PER_SECOND)
    cal.setMicros(Math.floorMod(julianMicros, MICROS_PER_SECOND))
    fastDateFormat.format(cal)
  }

  override def format(ts: Timestamp): String = {
    if (ts.getNanos == 0) {
      fastDateFormat.format(ts)
    } else {
      format(fromJavaTimestamp(ts))
    }
  }

  override def format(instant: Instant): String = {
    format(instantToMicros(instant))
  }

  override def validatePatternString(checkLegacy: Boolean): Unit = fastDateFormat
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

  override def format(ts: Timestamp): String = {
    sdf.format(ts)
  }

  override def format(instant: Instant): String = {
    format(instantToMicros(instant))
  }

  override def validatePatternString(checkLegacy: Boolean): Unit = sdf
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
      isParsing: Boolean,
      forTimestampNTZ: Boolean = false): TimestampFormatter = {
    val formatter = if (SQLConf.get.legacyTimeParserPolicy == LEGACY && !forTimestampNTZ) {
      getLegacyFormatter(format.getOrElse(defaultPattern), zoneId, locale, legacyFormat)
    } else {
      format
        .map(new Iso8601TimestampFormatter(_, zoneId, locale, legacyFormat, isParsing))
        .getOrElse(new DefaultTimestampFormatter(zoneId, locale, legacyFormat, isParsing))
    }
    formatter.validatePatternString(checkLegacy = !forTimestampNTZ)
    formatter
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
      format: Option[String],
      zoneId: ZoneId,
      locale: Locale,
      legacyFormat: LegacyDateFormat,
      isParsing: Boolean): TimestampFormatter = {
    getFormatter(format, zoneId, locale, legacyFormat, isParsing)
  }

  def apply(
      format: String,
      zoneId: ZoneId,
      locale: Locale,
      legacyFormat: LegacyDateFormat,
      isParsing: Boolean): TimestampFormatter = {
    getFormatter(Some(format), zoneId, locale, legacyFormat, isParsing)
  }

  def apply(
      format: String,
      zoneId: ZoneId,
      legacyFormat: LegacyDateFormat,
      isParsing: Boolean): TimestampFormatter = {
    getFormatter(Some(format), zoneId, defaultLocale, legacyFormat, isParsing)
  }

  def apply(
      format: Option[String],
      zoneId: ZoneId,
      legacyFormat: LegacyDateFormat,
      isParsing: Boolean,
      forTimestampNTZ: Boolean): TimestampFormatter = {
    getFormatter(format, zoneId, defaultLocale, legacyFormat, isParsing, forTimestampNTZ)
  }

  def apply(
      format: String,
      zoneId: ZoneId,
      legacyFormat: LegacyDateFormat,
      isParsing: Boolean,
      forTimestampNTZ: Boolean): TimestampFormatter = {
    getFormatter(Some(format), zoneId, defaultLocale, legacyFormat, isParsing,
      forTimestampNTZ)
  }

  def apply(
      format: String,
      zoneId: ZoneId,
      isParsing: Boolean): TimestampFormatter = {
    getFormatter(Some(format), zoneId, isParsing = isParsing)
  }

  def apply(zoneId: ZoneId): TimestampFormatter = {
    getFormatter(None, zoneId, isParsing = false)
  }

  def getFractionFormatter(zoneId: ZoneId): TimestampFormatter = {
    new FractionTimestampFormatter(zoneId)
  }
}
