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
import java.time.temporal.{TemporalAccessor, TemporalQueries}
import java.time.temporal.ChronoField.MICRO_OF_SECOND
import java.util.{Calendar, GregorianCalendar, Locale, TimeZone}

import scala.util.control.NonFatal

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.{SparkException, SparkIllegalArgumentException, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.{LegacyDateFormat, LENIENT_SIMPLE_DATE_FORMAT}
import org.apache.spark.sql.catalyst.util.RebaseDateTime._
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils._
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.internal.LegacyBehaviorPolicy._
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{Decimal, TimestampNTZType}
import org.apache.spark.unsafe.types.{TimestampNanosVal, UTF8String}

sealed trait TimestampFormatter extends Serializable {

  /**
   * Parses a timestamp in a string and converts it to microseconds.
   *
   * @param s
   *   \- string with timestamp to parse
   * @return
   *   microseconds since epoch.
   * @throws ParseException
   *   can be thrown by legacy parser
   * @throws DateTimeParseException
   *   can be thrown by new parser
   * @throws DateTimeException
   *   unable to obtain local date or time
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  def parse(s: String): Long

  /**
   * Parses a timestamp in a string and converts it to an optional number of microseconds.
   *
   * @param s
   *   \- string with timestamp to parse
   * @return
   *   An optional number of microseconds since epoch. The result is None on invalid input.
   * @throws ParseException
   *   can be thrown by legacy parser
   * @throws DateTimeParseException
   *   can be thrown by new parser
   * @throws DateTimeException
   *   unable to obtain local date or time
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
   * Parses a timestamp in a string and converts it to microseconds since Unix Epoch in local
   * time.
   *
   * @param s
   *   \- string with timestamp to parse
   * @param allowTimeZone
   *   \- indicates strict parsing of timezone
   * @return
   *   microseconds since epoch.
   * @throws ParseException
   *   can be thrown by legacy parser
   * @throws DateTimeParseException
   *   can be thrown by new parser
   * @throws DateTimeException
   *   unable to obtain local date or time
   * @throws IllegalStateException
   *   The formatter for timestamp without time zone should always implement this method. The
   *   exception should never be hit.
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  @throws(classOf[IllegalStateException])
  def parseWithoutTimeZone(s: String, allowTimeZone: Boolean): Long =
    throw SparkException.internalError(
      s"The method `parseWithoutTimeZone(s: String, allowTimeZone: Boolean)` should be " +
        "implemented in the formatter of timestamp without time zone")

  /**
   * Parses a timestamp in a string and converts it to an optional number of microseconds since
   * Unix Epoch in local time.
   *
   * @param s
   *   \- string with timestamp to parse
   * @param allowTimeZone
   *   \- indicates strict parsing of timezone
   * @return
   *   An optional number of microseconds since epoch. The result is None on invalid input.
   * @throws ParseException
   *   can be thrown by legacy parser
   * @throws DateTimeParseException
   *   can be thrown by new parser
   * @throws DateTimeException
   *   unable to obtain local date or time
   * @throws IllegalStateException
   *   The formatter for timestamp without time zone should always implement this method. The
   *   exception should never be hit.
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
   * Parses a timestamp in a string and converts it to microseconds since Unix Epoch in local
   * time. Zone-id and zone-offset components are ignored.
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  @throws(classOf[IllegalStateException])
  final def parseWithoutTimeZone(s: String): Long =
    // This is implemented to adhere to the original behaviour of `parseWithoutTimeZone` where we
    // did not fail if timestamp contained zone-id or zone-offset component and instead ignored it.
    parseWithoutTimeZone(s, true)

  /**
   * Parses a timestamp in a string and converts it to a [[TimestampNanosVal]] (epoch microseconds
   * plus a sub-microsecond remainder in `[0, 999]`) for `TIMESTAMP_LTZ(precision)`. Fractional
   * digits beyond `precision` are truncated (floored), matching the cast/parse rule used by the
   * microsecond path and `SparkDateTimeUtils`.
   *
   * @param s
   *   \- string with timestamp to parse
   * @param precision
   *   \- the target fractional-second precision in `[7, 9]`
   * @return
   *   the parsed value as a [[TimestampNanosVal]].
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  def parseNanos(s: String, precision: Int): TimestampNanosVal

  /**
   * Optional counterpart of [[parseNanos]]. The result is `None` on invalid input.
   */
  def parseNanosOptional(s: String, precision: Int): Option[TimestampNanosVal] =
    try {
      Some(parseNanos(s, precision))
    } catch {
      case _: Exception => None
    }

  /**
   * Parses a timestamp in a string and converts it to a [[TimestampNanosVal]] for
   * `TIMESTAMP_NTZ(precision)`. The result is independent of time zones; a time zone component is
   * discarded when `allowTimeZone` is `true` and rejected otherwise. Fractional digits beyond
   * `precision` are truncated (floored).
   *
   * @param s
   *   \- string with timestamp to parse
   * @param precision
   *   \- the target fractional-second precision in `[7, 9]`
   * @param allowTimeZone
   *   \- indicates strict parsing of timezone
   * @throws IllegalStateException
   *   The formatter for timestamp without time zone should always implement this method. The
   *   exception should never be hit.
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  @throws(classOf[IllegalStateException])
  def parseWithoutTimeZoneNanos(
      s: String,
      precision: Int,
      allowTimeZone: Boolean): TimestampNanosVal =
    throw SparkException.internalError(
      s"The method `parseWithoutTimeZoneNanos(s: String, precision: Int, allowTimeZone: " +
        "Boolean)` should be implemented in the formatter of timestamp without time zone")

  /**
   * Optional counterpart of [[parseWithoutTimeZoneNanos]]. The result is `None` on invalid input.
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  @throws(classOf[IllegalStateException])
  def parseWithoutTimeZoneNanosOptional(
      s: String,
      precision: Int,
      allowTimeZone: Boolean): Option[TimestampNanosVal] =
    try {
      Some(parseWithoutTimeZoneNanos(s, precision, allowTimeZone))
    } catch {
      case _: Exception => None
    }

  /**
   * Parses a timestamp in a string to a [[TimestampNanosVal]] for `TIMESTAMP_NTZ(precision)`.
   * Zone-id and zone-offset components are ignored.
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  @throws(classOf[IllegalStateException])
  final def parseWithoutTimeZoneNanos(s: String, precision: Int): TimestampNanosVal =
    parseWithoutTimeZoneNanos(s, precision, true)

  /**
   * Formats a [[TimestampNanosVal]] to a string at the target fractional-second `precision` in
   * `[7, 9]`. Sub-`precision` digits are truncated (floored) before rendering; the number of
   * fractional digits actually emitted follows the formatter pattern (e.g. the count of `S`
   * letters), consistent with the microsecond `format` overloads.
   */
  def formatNanos(v: TimestampNanosVal, precision: Int): String

  def format(us: Long): String
  def format(ts: Timestamp): String
  def format(instant: Instant): String

  @throws(classOf[IllegalStateException])
  def format(localDateTime: LocalDateTime): String =
    throw SparkException.internalError(
      s"The method `format(localDateTime: LocalDateTime)` should be implemented in the formatter " +
        "of timestamp without time zone")

  /**
   * Validates the pattern string.
   * @param checkLegacy
   *   if true and the pattern is invalid, check whether the pattern is valid for legacy
   *   formatters and show hints for using legacy formatter. Otherwise, simply check the pattern
   *   string.
   */
  def validatePatternString(checkLegacy: Boolean): Unit
}

class Iso8601TimestampFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale,
    legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT,
    isParsing: Boolean)
    extends TimestampFormatter
    with DateTimeFormatterHelper {
  @transient
  protected lazy val formatter: DateTimeFormatter =
    getOrCreateFormatter(pattern, locale, isParsing)

  @transient
  private lazy val zonedFormatter: DateTimeFormatter = formatter.withZone(zoneId)

  @transient
  protected lazy val legacyFormatter =
    TimestampFormatter.getLegacyFormatter(pattern, zoneId, locale, legacyFormat)

  override def parseOptional(s: String): Option[Long] = {
    try {
      val parsePosition = new ParsePosition(0)
      val parsed = formatter.parseUnresolved(s, parsePosition)
      if (parsed != null && s.length == parsePosition.getIndex) {
        Some(extractMicros(parsed))
      } else {
        None
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  private def extractMicros(parsed: TemporalAccessor): Long = {
    val parsedZoneId = parsed.query(TemporalQueries.zone())
    val timeZoneId = if (parsedZoneId == null) zoneId else parsedZoneId
    val zonedDateTime = toZonedDateTime(parsed, timeZoneId)
    val epochSeconds = zonedDateTime.toEpochSecond
    val microsOfSecond = zonedDateTime.get(MICRO_OF_SECOND)
    Math.addExact(Math.multiplyExact(epochSeconds, MICROS_PER_SECOND), microsOfSecond)
  }

  override def parse(s: String): Long = {
    try {
      val parsed = formatter.parse(s)
      extractMicros(parsed)
    } catch checkParsedDiff(s, legacyFormatter.parse)
  }

  // `checkParsedDiff` only uses the legacy parse to decide whether to raise an upgrade exception
  // and never returns its result, so the legacy formatter (microsecond-only) is fine here even on
  // the nanos path. The returned `TimestampNanosVal.ZERO` is discarded.
  protected def legacyNanosParse(str: String): TimestampNanosVal = {
    legacyFormatter.parse(str)
    TimestampNanosVal.ZERO
  }

  override def parseNanosOptional(s: String, precision: Int): Option[TimestampNanosVal] = {
    try {
      val parsePosition = new ParsePosition(0)
      val parsed = formatter.parseUnresolved(s, parsePosition)
      if (parsed != null && s.length == parsePosition.getIndex) {
        Some(extractNanos(parsed, precision))
      } else {
        None
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  private def extractNanos(parsed: TemporalAccessor, precision: Int): TimestampNanosVal = {
    val parsedZoneId = parsed.query(TemporalQueries.zone())
    val timeZoneId = if (parsedZoneId == null) zoneId else parsedZoneId
    val zonedDateTime = toZonedDateTime(parsed, timeZoneId)
    SparkDateTimeUtils.instantToTimestampNanos(zonedDateTime.toInstant, precision)
  }

  override def parseNanos(s: String, precision: Int): TimestampNanosVal = {
    try {
      val parsed = formatter.parse(s)
      extractNanos(parsed, precision)
    } catch checkParsedDiff(s, legacyNanosParse)
  }

  override def parseWithoutTimeZoneOptional(s: String, allowTimeZone: Boolean): Option[Long] = {
    try {
      val parsePosition = new ParsePosition(0)
      val parsed = formatter.parseUnresolved(s, parsePosition)
      if (parsed != null && s.length == parsePosition.getIndex) {
        Some(extractMicrosNTZ(s, parsed, allowTimeZone))
      } else {
        None
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  private def extractMicrosNTZ(
      s: String,
      parsed: TemporalAccessor,
      allowTimeZone: Boolean): Long = {
    if (!allowTimeZone && parsed.query(TemporalQueries.zone()) != null) {
      throw ExecutionErrors.cannotParseStringAsDataTypeError(pattern, s, TimestampNTZType)
    }
    val localDate = toLocalDate(parsed)
    val localTime = toLocalTime(parsed)
    SparkDateTimeUtils.localDateTimeToMicros(LocalDateTime.of(localDate, localTime))
  }

  override def parseWithoutTimeZone(s: String, allowTimeZone: Boolean): Long = {
    try {
      val parsed = formatter.parse(s)
      extractMicrosNTZ(s, parsed, allowTimeZone)
    } catch checkParsedDiff(s, legacyFormatter.parse)
  }

  override def parseWithoutTimeZoneNanosOptional(
      s: String,
      precision: Int,
      allowTimeZone: Boolean): Option[TimestampNanosVal] = {
    try {
      val parsePosition = new ParsePosition(0)
      val parsed = formatter.parseUnresolved(s, parsePosition)
      if (parsed != null && s.length == parsePosition.getIndex) {
        Some(extractNanosNTZ(s, parsed, precision, allowTimeZone))
      } else {
        None
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  private def extractNanosNTZ(
      s: String,
      parsed: TemporalAccessor,
      precision: Int,
      allowTimeZone: Boolean): TimestampNanosVal = {
    if (!allowTimeZone && parsed.query(TemporalQueries.zone()) != null) {
      throw ExecutionErrors.cannotParseStringAsDataTypeError(pattern, s, TimestampNTZType)
    }
    val localDate = toLocalDate(parsed)
    val localTime = toLocalTime(parsed)
    SparkDateTimeUtils.localDateTimeToTimestampNanos(
      LocalDateTime.of(localDate, localTime),
      precision)
  }

  override def parseWithoutTimeZoneNanos(
      s: String,
      precision: Int,
      allowTimeZone: Boolean): TimestampNanosVal = {
    try {
      val parsed = formatter.parse(s)
      extractNanosNTZ(s, parsed, precision, allowTimeZone)
    } catch checkParsedDiff(s, legacyNanosParse)
  }

  override def format(instant: Instant): String = {
    try {
      zonedFormatter.format(instant)
    } catch
      checkFormattedDiff(toJavaTimestamp(instantToMicros(instant)), (t: Timestamp) => format(t))
  }

  override def format(us: Long): String = {
    val instant = SparkDateTimeUtils.microsToInstant(us)
    format(instant)
  }

  override def format(ts: Timestamp): String = {
    legacyFormatter.format(ts)
  }

  override def format(localDateTime: LocalDateTime): String = {
    localDateTime.format(formatter)
  }

  override def formatNanos(v: TimestampNanosVal, precision: Int): String = {
    // Floor sub-`precision` digits using the shared `SparkDateTimeUtils` truncation rule, then
    // render the reconstructed instant. The number of fractional digits emitted follows the
    // formatter pattern (count of `S` letters), consistent with the microsecond `format` paths.
    val truncated = SparkDateTimeUtils.instantToTimestampNanos(
      SparkDateTimeUtils.timestampNanosToInstant(v),
      precision)
    format(SparkDateTimeUtils.timestampNanosToInstant(truncated))
  }

  override def validatePatternString(checkLegacy: Boolean): Unit = {
    if (checkLegacy) {
      try {
        formatter
      } catch
        checkLegacyFormatter(pattern, legacyFormatter.validatePatternString(checkLegacy = true))
      ()
    } else {
      try {
        formatter
      } catch checkInvalidPattern(pattern)
    }
  }
}

/**
 * The formatter for timestamps which doesn't require users to specify a pattern. While
 * formatting, it uses the default pattern [[TimestampFormatter.defaultPattern()]]. In parsing, it
 * follows the CAST logic in conversion of strings to Catalyst's TimestampType.
 *
 * @param zoneId
 *   The time zone ID in which timestamps should be formatted or parsed.
 * @param locale
 *   The locale overrides the system locale and is used in formatting.
 * @param legacyFormat
 *   Defines the formatter used for legacy timestamps.
 * @param isParsing
 *   Whether the formatter is used for parsing (`true`) or for formatting (`false`).
 */
class DefaultTimestampFormatter(
    zoneId: ZoneId,
    locale: Locale,
    legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT,
    isParsing: Boolean)
    extends Iso8601TimestampFormatter(
      TimestampFormatter.defaultPattern(),
      zoneId,
      locale,
      legacyFormat,
      isParsing) {

  override def parse(s: String): Long = {
    try {
      SparkDateTimeUtils.stringToTimestampAnsi(UTF8String.fromString(s), zoneId)
    } catch checkParsedDiff(s, legacyFormatter.parse)
  }

  override def parseOptional(s: String): Option[Long] =
    SparkDateTimeUtils.stringToTimestamp(UTF8String.fromString(s), zoneId)

  override def parseWithoutTimeZone(s: String, allowTimeZone: Boolean): Long = {
    try {
      val utf8Value = UTF8String.fromString(s)
      SparkDateTimeUtils.stringToTimestampWithoutTimeZone(utf8Value, allowTimeZone).getOrElse {
        throw ExecutionErrors.cannotParseStringAsDataTypeError(
          TimestampFormatter.defaultPattern(),
          s,
          TimestampNTZType)
      }
    } catch checkParsedDiff(s, legacyFormatter.parse)
  }

  override def parseWithoutTimeZoneOptional(s: String, allowTimeZone: Boolean): Option[Long] = {
    val utf8Value = UTF8String.fromString(s)
    SparkDateTimeUtils.stringToTimestampWithoutTimeZone(utf8Value, allowTimeZone)
  }

  override def parseNanos(s: String, precision: Int): TimestampNanosVal = {
    try {
      SparkDateTimeUtils.stringToTimestampLTZNanosAnsi(UTF8String.fromString(s), precision, zoneId)
    } catch checkParsedDiff(s, legacyNanosParse)
  }

  override def parseNanosOptional(s: String, precision: Int): Option[TimestampNanosVal] =
    SparkDateTimeUtils.stringToTimestampLTZNanos(UTF8String.fromString(s), precision, zoneId)

  override def parseWithoutTimeZoneNanos(
      s: String,
      precision: Int,
      allowTimeZone: Boolean): TimestampNanosVal = {
    try {
      val utf8Value = UTF8String.fromString(s)
      SparkDateTimeUtils.stringToTimestampNTZNanos(utf8Value, precision, allowTimeZone).getOrElse {
        throw ExecutionErrors.cannotParseStringAsDataTypeError(
          TimestampFormatter.defaultPattern(),
          s,
          TimestampNTZType)
      }
    } catch checkParsedDiff(s, legacyNanosParse)
  }

  override def parseWithoutTimeZoneNanosOptional(
      s: String,
      precision: Int,
      allowTimeZone: Boolean): Option[TimestampNanosVal] =
    SparkDateTimeUtils.stringToTimestampNTZNanos(UTF8String.fromString(s), precision, allowTimeZone)
}

/**
 * The formatter parses/formats timestamps according to the pattern `yyyy-MM-dd
 * HH:mm:ss.[..fff..]` where `[..fff..]` is a fraction of second up to microsecond resolution. The
 * formatter does not output trailing zeros in the fraction. For example, the timestamp
 * `2019-03-05 15:00:01.123400` is formatted as the string `2019-03-05 15:00:01.1234`.
 *
 * @param zoneId
 *   the time zone identifier in which the formatter parses or format timestamps
 */
class FractionTimestampFormatter(zoneId: ZoneId)
    extends Iso8601TimestampFormatter(
      TimestampFormatter.defaultPattern(),
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
 * The custom sub-class of `GregorianCalendar` is needed to get access to protected `fields`
 * immediately after parsing. We cannot use the `get()` method because it performs normalization
 * of the fraction part. Accordingly, the `MILLISECOND` field doesn't contain original value.
 *
 * Also this class allows to set raw value to the `MILLISECOND` field directly before formatting.
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

class LegacyFastTimestampFormatter(pattern: String, zoneId: ZoneId, locale: Locale)
    extends TimestampFormatter {

  @transient private lazy val fastDateFormat =
    FastDateFormat.getInstance(pattern, TimeZone.getTimeZone(zoneId), locale)
  @transient private lazy val cal =
    new MicrosCalendar(fastDateFormat.getTimeZone, fastDateFormat.getPattern.count(_ == 'S'))

  override def parse(s: String): Long = {
    cal.clear() // Clear the calendar because it can be re-used many times
    if (!fastDateFormat.parse(s, new ParsePosition(0), cal)) {
      throw new SparkIllegalArgumentException(
        errorClass = "_LEGACY_ERROR_TEMP_3260",
        messageParameters = Map("s" -> s))
    }
    extractMicros(cal)
  }

  override def parseOptional(s: String): Option[Long] = {
    cal.clear() // Clear the calendar because it can be re-used many times
    try {
      if (fastDateFormat.parse(s, new ParsePosition(0), cal)) {
        Some(extractMicros(cal))
      } else {
        None
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  private def extractMicros(cal: MicrosCalendar): Long = {
    val micros = cal.getMicros()
    cal.set(Calendar.MILLISECOND, 0)
    val julianMicros = Math.addExact(millisToMicros(cal.getTimeInMillis), micros)
    rebaseJulianToGregorianMicros(TimeZone.getTimeZone(zoneId), julianMicros)
  }

  override def format(timestamp: Long): String = {
    val julianMicros = rebaseGregorianToJulianMicros(TimeZone.getTimeZone(zoneId), timestamp)
    cal.setTimeInMillis(Math.floorDiv(julianMicros, MICROS_PER_SECOND) * MILLIS_PER_SECOND)
    cal.setMicros(Math.floorMod(julianMicros, MICROS_PER_SECOND))
    fastDateFormat.format(cal)
  }

  override def format(ts: Timestamp): String = {
    if (ts.getNanos == 0) {
      fastDateFormat.format(ts)
    } else {
      format(fromJavaTimestamp(zoneId.getId, ts))
    }
  }

  override def format(instant: Instant): String = {
    format(instantToMicros(instant))
  }

  override def parseNanos(s: String, precision: Int): TimestampNanosVal =
    throw TimestampFormatter.legacyNanosUnsupported()

  // Without this override the trait default throws SparkException.internalError instead of the
  // user-facing legacyNanosUnsupported error.
  override def parseWithoutTimeZoneNanos(
      s: String,
      precision: Int,
      allowTimeZone: Boolean): TimestampNanosVal =
    throw TimestampFormatter.legacyNanosUnsupported()

  override def formatNanos(v: TimestampNanosVal, precision: Int): String =
    throw TimestampFormatter.legacyNanosUnsupported()

  override def validatePatternString(checkLegacy: Boolean): Unit = fastDateFormat
}

class LegacySimpleTimestampFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale,
    lenient: Boolean = true)
    extends TimestampFormatter {
  @transient private lazy val sdf = {
    val formatter = new SimpleDateFormat(pattern, locale)
    formatter.setTimeZone(TimeZone.getTimeZone(zoneId))
    formatter.setLenient(lenient)
    formatter
  }

  override def parse(s: String): Long = {
    fromJavaTimestamp(zoneId.getId, new Timestamp(sdf.parse(s).getTime))
  }

  override def parseOptional(s: String): Option[Long] = {
    val date = sdf.parse(s, new ParsePosition(0))
    if (date == null) {
      None
    } else {
      Some(fromJavaTimestamp(zoneId.getId, new Timestamp(date.getTime)))
    }
  }

  override def format(us: Long): String = {
    sdf.format(toJavaTimestamp(zoneId.getId, us))
  }

  override def format(ts: Timestamp): String = {
    sdf.format(ts)
  }

  override def format(instant: Instant): String = {
    format(instantToMicros(instant))
  }

  override def parseNanos(s: String, precision: Int): TimestampNanosVal =
    throw TimestampFormatter.legacyNanosUnsupported()

  // Without this override the trait default throws SparkException.internalError instead of the
  // user-facing legacyNanosUnsupported error.
  override def parseWithoutTimeZoneNanos(
      s: String,
      precision: Int,
      allowTimeZone: Boolean): TimestampNanosVal =
    throw TimestampFormatter.legacyNanosUnsupported()

  override def formatNanos(v: TimestampNanosVal, precision: Int): String =
    throw TimestampFormatter.legacyNanosUnsupported()

  override def validatePatternString(checkLegacy: Boolean): Unit = sdf
}

object LegacyDateFormats extends Enumeration {
  type LegacyDateFormat = Value
  val FAST_DATE_FORMAT, SIMPLE_DATE_FORMAT, LENIENT_SIMPLE_DATE_FORMAT = Value
}

object TimestampFormatter {
  import LegacyDateFormats._

  val defaultLocale: Locale = Locale.US

  def defaultPattern(): String =
    s"${DateFormatter.defaultPattern} ${TimeFormatter.defaultPattern}"

  /**
   * The legacy formatters (`FastDateFormat` / `SimpleDateFormat`) cap at millisecond/microsecond
   * resolution and cannot represent the sub-microsecond remainder of a [[TimestampNanosVal]].
   * Nanosecond-capable timestamp types are therefore unsupported under the `LEGACY` time parser
   * policy. This is a user-facing error (not an internal error) because the `LEGACY` policy is
   * user-configurable and a caller may legitimately combine it with nanosecond timestamps.
   */
  def legacyNanosUnsupported(): SparkUnsupportedOperationException =
    ExecutionErrors.nanosTimestampUnsupportedWithLegacyParserError()

  private def getFormatter(
      format: Option[String],
      zoneId: ZoneId,
      locale: Locale = defaultLocale,
      legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT,
      isParsing: Boolean,
      forTimestampNTZ: Boolean = false): TimestampFormatter = {
    try {
      val formatter = if (SqlApiConf.get.legacyTimeParserPolicy == LEGACY && !forTimestampNTZ) {
        getLegacyFormatter(format.getOrElse(defaultPattern()), zoneId, locale, legacyFormat)
      } else {
        format
          .map(new Iso8601TimestampFormatter(_, zoneId, locale, legacyFormat, isParsing))
          .getOrElse(new DefaultTimestampFormatter(zoneId, locale, legacyFormat, isParsing))
      }
      formatter.validatePatternString(checkLegacy = !forTimestampNTZ)
      formatter
    } catch {
      case e: IllegalArgumentException =>
        // Wrap Java's IllegalArgumentException with proper Spark Exception.
        // Spark's SparkIllegalArgumentException should pass through unchanged.
        e match {
          case _: SparkIllegalArgumentException => throw e
          case _ =>
            throw ExecutionErrors.failToRecognizePatternError(
              format.getOrElse(defaultPattern()),
              e)
        }
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
    getFormatter(Some(format), zoneId, defaultLocale, legacyFormat, isParsing, forTimestampNTZ)
  }

  def apply(format: String, zoneId: ZoneId, isParsing: Boolean): TimestampFormatter = {
    getFormatter(Some(format), zoneId, isParsing = isParsing)
  }

  def apply(
      format: String,
      zoneId: ZoneId,
      isParsing: Boolean,
      forTimestampNTZ: Boolean): TimestampFormatter = {
    getFormatter(Some(format), zoneId, isParsing = isParsing, forTimestampNTZ = forTimestampNTZ)
  }

  def apply(zoneId: ZoneId): TimestampFormatter = {
    getFormatter(None, zoneId, isParsing = false)
  }

  def getFractionFormatter(zoneId: ZoneId): TimestampFormatter = {
    new FractionTimestampFormatter(zoneId)
  }
}
