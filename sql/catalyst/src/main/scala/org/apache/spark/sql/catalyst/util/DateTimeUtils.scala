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
import java.time.format.TextStyle
import java.time.temporal.{ChronoField, ChronoUnit, IsoFields, Temporal}
import java.util.Locale
import java.util.concurrent.TimeUnit._

import scala.util.control.NonFatal

import org.apache.spark.{QueryContext, SparkException, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{Decimal, DoubleExactNumeric, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Helper functions for converting between internal and external date and time representations.
 * Dates are exposed externally as java.sql.Date and are represented internally as the number of
 * dates since the Unix epoch (1970-01-01). Timestamps are exposed externally as java.sql.Timestamp
 * and are stored internally as longs, which are capable of storing timestamps with microsecond
 * precision.
 */
object DateTimeUtils extends SparkDateTimeUtils {

  // See http://stackoverflow.com/questions/466321/convert-unix-timestamp-to-julian
  // It's 2440587.5, rounding up to be compatible with Hive.
  final val JULIAN_DAY_OF_EPOCH = 2440588

  val TIMEZONE_OPTION = "timeZone"

  /**
   * Returns the number of microseconds since epoch from Julian day and nanoseconds in a day.
   */
  def fromJulianDay(days: Int, nanos: Long): Long = {
    // use Long to avoid rounding errors
    (days - JULIAN_DAY_OF_EPOCH).toLong * MICROS_PER_DAY + nanos / NANOS_PER_MICROS
  }

  /**
   * Returns Julian day and nanoseconds in a day from the number of microseconds
   *
   * Note: support timestamp since 4717 BC (without negative nanoseconds, compatible with Hive).
   */
  def toJulianDay(micros: Long): (Int, Long) = {
    val julianUs = micros + JULIAN_DAY_OF_EPOCH * MICROS_PER_DAY
    val days = julianUs / MICROS_PER_DAY
    val us = julianUs % MICROS_PER_DAY
    (days.toInt, MICROSECONDS.toNanos(us))
  }

  private final val gmtUtf8 = UTF8String.fromString("GMT")
  // The method is called by JSON/CSV parser to clean up the legacy timestamp string by removing
  // the "GMT" string. For example, it returns 2000-01-01T00:00+01:00 for 2000-01-01T00:00GMT+01:00.
  def cleanLegacyTimestampStr(s: UTF8String): UTF8String = s.replace(gmtUtf8, UTF8String.EMPTY_UTF8)

  def doubleToTimestampAnsi(d: Double, context: QueryContext): Long = {
    if (d.isNaN || d.isInfinite) {
      throw QueryExecutionErrors.invalidInputInCastToDatetimeError(d, TimestampType, context)
    } else {
      DoubleExactNumeric.toLong(d * MICROS_PER_SECOND)
    }
  }

  /**
   * Trims and parses a given UTF8 string to a corresponding [[Long]] value which representing the
   * number of microseconds since the epoch. The result is independent of time zones. Zone id
   * component will be ignored.
   * The return type is [[Option]] in order to distinguish between 0L and null. Please
   * refer to `parseTimestampString` for the allowed formats.
   */
  def stringToTimestampWithoutTimeZone(s: UTF8String): Option[Long] = {
    stringToTimestampWithoutTimeZone(s, true)
  }

  def stringToTimestampWithoutTimeZoneAnsi(
      s: UTF8String,
      context: QueryContext): Long = {
    stringToTimestampWithoutTimeZone(s, true).getOrElse {
      throw QueryExecutionErrors.invalidInputInCastToDatetimeError(s, TimestampNTZType, context)
    }
  }

  /**
   * Returns the hour value of a given timestamp value. The timestamp is expressed in microseconds.
   */
  def getHours(micros: Long, zoneId: ZoneId): Int = {
    getLocalDateTime(micros, zoneId).getHour
  }

  /**
   * Returns the minute value of a given timestamp value. The timestamp is expressed in
   * microseconds since the epoch.
   */
  def getMinutes(micros: Long, zoneId: ZoneId): Int = {
    getLocalDateTime(micros, zoneId).getMinute
  }

  /**
   * Returns the second value of a given timestamp value. The timestamp is expressed in
   * microseconds since the epoch.
   */
  def getSeconds(micros: Long, zoneId: ZoneId): Int = {
    getLocalDateTime(micros, zoneId).getSecond
  }

  /**
   * Returns the seconds part and its fractional part with microseconds.
   */
  def getSecondsWithFraction(micros: Long, zoneId: ZoneId): Decimal = {
    Decimal(getMicroseconds(micros, zoneId), 8, 6)
  }

  /**
   * Returns local seconds, including fractional parts, multiplied by 1000000.
   *
   * @param micros The number of microseconds since the epoch.
   * @param zoneId The time zone id which milliseconds should be obtained in.
   */
  def getMicroseconds(micros: Long, zoneId: ZoneId): Int = {
    val lt = getLocalDateTime(micros, zoneId)
    (lt.getLong(ChronoField.MICRO_OF_SECOND) + lt.getSecond * MICROS_PER_SECOND).toInt
  }

  /**
   * Returns the 'day in year' value for the given number of days since 1970-01-01.
   */
  def getDayInYear(days: Int): Int = daysToLocalDate(days).getDayOfYear

  /**
   * Returns the year value for the given number of days since 1970-01-01.
   */
  def getYear(days: Int): Int = daysToLocalDate(days).getYear

  /**
   * Returns the year which conforms to ISO 8601. Each ISO 8601 week-numbering
   * year begins with the Monday of the week containing the 4th of January.
   */
  def getWeekBasedYear(days: Int): Int = daysToLocalDate(days).get(IsoFields.WEEK_BASED_YEAR)

  /** Returns the quarter for the given number of days since 1970-01-01. */
  def getQuarter(days: Int): Int = daysToLocalDate(days).get(IsoFields.QUARTER_OF_YEAR)

  /**
   * Returns the month value for the given number of days since 1970-01-01.
   * January is month 1.
   */
  def getMonth(days: Int): Int = daysToLocalDate(days).getMonthValue

  /**
   * Returns the 'day of month' value for the given number of days since 1970-01-01.
   */
  def getDayOfMonth(days: Int): Int = daysToLocalDate(days).getDayOfMonth

  /**
   * Returns the day of the week for the given number of days since 1970-01-01
   * (1 = Sunday, 2 = Monday, ..., 7 = Saturday).
   */
  def getDayOfWeek(days: Int): Int = LocalDate.ofEpochDay(days).getDayOfWeek.plus(1).getValue

  /**
   * Returns the day of the week for the given number of days since 1970-01-01
   * (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
   */
  def getWeekDay(days: Int): Int = LocalDate.ofEpochDay(days).getDayOfWeek.ordinal()

  /**
   * Returns the week of the year of the given date expressed as the number of days from 1970-01-01.
   * A week is considered to start on a Monday and week 1 is the first week with > 3 days.
   */
  def getWeekOfYear(days: Int): Int = {
    LocalDate.ofEpochDay(days).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
  }

  /**
   * Adds an year-month interval to a date represented as days since 1970-01-01.
   * @return a date value, expressed in days since 1970-01-01.
   */
  def dateAddMonths(days: Int, months: Int): Int = {
    localDateToDays(daysToLocalDate(days).plusMonths(months))
  }

  /**
   * Returns the three-letter abbreviated month name for the given number of days since 1970-01-01.
   */
  def getMonthName(days: Int): UTF8String = {
    val monthName = Month
      .of(getMonth(days))
      .getDisplayName(TextStyle.SHORT, DateFormatter.defaultLocale)

    UTF8String.fromString(monthName)
  }

  /**
   * Returns the three-letter abbreviated day name for the given number of days since 1970-01-01.
   */
  def getDayName(days: Int): UTF8String = {
    val dayName = DayOfWeek
      .of(getWeekDay(days) + 1)
      .getDisplayName(TextStyle.SHORT, DateFormatter.defaultLocale)

    UTF8String.fromString(dayName)
  }

  /**
   * Adds months to a timestamp at the given time zone. It converts the input timestamp to a local
   * timestamp at the given time zone, adds months, and converts the resulted local timestamp
   * back to a timestamp, expressed in microseconds since 1970-01-01 00:00:00Z.
   *
   * @param micros The input timestamp value, expressed in microseconds since 1970-01-01 00:00:00Z
   * @param months The amount of months to add. It can be positive or negative.
   * @param zoneId The time zone ID at which the operation is performed.
   * @return A timestamp value, expressed in microseconds since 1970-01-01 00:00:00Z.
   */
  def timestampAddMonths(micros: Long, months: Int, zoneId: ZoneId): Long = {
    instantToMicros(microsToInstant(micros).atZone(zoneId).plusMonths(months).toInstant)
  }

  /**
   * Adds a day-time interval expressed in microseconds to a timestamp at the given time zone.
   * It converts the input timestamp to a local timestamp, and adds the interval by:
   *   - Splitting the interval to days and microsecond adjustment in a day, and
   *   - First of all, it adds days and then the time part.
   * The resulted local timestamp is converted back to an instant at the given time zone.
   *
   * @param micros The input timestamp value, expressed in microseconds since 1970-01-01 00:00:00Z.
   * @param dayTime The amount of microseconds to add. It can be positive or negative.
   * @param zoneId The time zone ID at which the operation is performed.
   * @return A timestamp value, expressed in microseconds since 1970-01-01 00:00:00Z.
   */
  def timestampAddDayTime(micros: Long, dayTime: Long, zoneId: ZoneId): Long = {
    val days = dayTime / MICROS_PER_DAY
    val microseconds = dayTime - days * MICROS_PER_DAY
    val resultTimestamp = microsToInstant(micros)
      .atZone(zoneId)
      .plusDays(days)
      .plus(microseconds, ChronoUnit.MICROS)
    instantToMicros(resultTimestamp.toInstant)
  }

  /**
   * Adds a full interval (months, days, microseconds) to a timestamp represented as the number of
   * microseconds since 1970-01-01 00:00:00Z.
   * @return A timestamp value, expressed in microseconds since 1970-01-01 00:00:00Z.
   */
  def timestampAddInterval(
      start: Long,
      months: Int,
      days: Int,
      microseconds: Long,
      zoneId: ZoneId): Long = {
    val resultTimestamp = microsToInstant(start)
      .atZone(zoneId)
      .plusMonths(months)
      .plusDays(days)
      .plus(microseconds, ChronoUnit.MICROS)
    instantToMicros(resultTimestamp.toInstant)
  }

  /**
   * Adds a full interval (months, days, microseconds) to a timestamp without time zone
   * represented as a local time in microsecond precision, which is independent of time zone.
   * @return A timestamp without time zone value, expressed in range
   *         [0001-01-01T00:00:00.000000, 9999-12-31T23:59:59.999999].
   */
  def timestampNTZAddInterval(
      start: Long,
      months: Int,
      days: Int,
      microseconds: Long,
      zoneId: ZoneId): Long = {
    val localDateTime = microsToLocalDateTime(start)
      .plusMonths(months)
      .plusDays(days)
      .plus(microseconds, ChronoUnit.MICROS)
    localDateTimeToMicros(localDateTime)
  }

  /**
   * Adds the interval's months and days to a date expressed as days since the epoch.
   * @return A date value, expressed in days since 1970-01-01.
   *
   * @throws DateTimeException if the result exceeds the supported date range
   * @throws IllegalArgumentException if the interval has `microseconds` part
   */
  def dateAddInterval(
     start: Int,
     interval: CalendarInterval): Int = {
    if (interval.microseconds != 0) {
      throw QueryExecutionErrors.ansiIllegalIntervalArgumentValue()
    }
    val ld = daysToLocalDate(start).plusMonths(interval.months).plusDays(interval.days)
    localDateToDays(ld)
  }

  /**
   * Splits date (expressed in days since 1970-01-01) into four fields:
   * year, month (Jan is Month 1), dayInMonth, daysToMonthEnd (0 if it's last day of month).
   */
  private def splitDate(days: Int): (Int, Int, Int, Int) = {
    val ld = daysToLocalDate(days)
    (ld.getYear, ld.getMonthValue, ld.getDayOfMonth, ld.lengthOfMonth() - ld.getDayOfMonth)
  }

  /**
   * Returns number of months between micros1 and micros2. micros1 and micros2 are expressed in
   * microseconds since 1970-01-01. If micros1 is later than micros2, the result is positive.
   *
   * If micros1 and micros2 are on the same day of month, or both are the last day of month,
   * returns, time of day will be ignored.
   *
   * Otherwise, the difference is calculated based on 31 days per month.
   * The result is rounded to 8 decimal places if `roundOff` is set to true.
   */
  def monthsBetween(
      micros1: Long,
      micros2: Long,
      roundOff: Boolean,
      zoneId: ZoneId): Double = {
    val date1 = microsToDays(micros1, zoneId)
    val date2 = microsToDays(micros2, zoneId)
    val (year1, monthInYear1, dayInMonth1, daysToMonthEnd1) = splitDate(date1)
    val (year2, monthInYear2, dayInMonth2, daysToMonthEnd2) = splitDate(date2)

    val months1 = year1 * 12 + monthInYear1
    val months2 = year2 * 12 + monthInYear2

    val monthDiff = (months1 - months2).toDouble

    if (dayInMonth1 == dayInMonth2 || ((daysToMonthEnd1 == 0) && (daysToMonthEnd2 == 0))) {
      return monthDiff
    }
    // using milliseconds can cause precision loss with more than 8 digits
    // we follow Hive's implementation which uses seconds
    val secondsInDay1 = MICROSECONDS.toSeconds(micros1 - daysToMicros(date1, zoneId))
    val secondsInDay2 = MICROSECONDS.toSeconds(micros2 - daysToMicros(date2, zoneId))
    val secondsDiff = (dayInMonth1 - dayInMonth2) * SECONDS_PER_DAY + secondsInDay1 - secondsInDay2
    val secondsInMonth = DAYS.toSeconds(31)
    val diff = monthDiff + secondsDiff / secondsInMonth.toDouble
    if (roundOff) {
      // rounding to 8 digits
      math.round(diff * 1e8) / 1e8
    } else {
      diff
    }
  }

  // Thursday = 0 since 1970/Jan/01 => Thursday
  private val SUNDAY = 3
  private val MONDAY = 4
  private val TUESDAY = 5
  private val WEDNESDAY = 6
  private val THURSDAY = 0
  private val FRIDAY = 1
  private val SATURDAY = 2

  /**
   * Returns day of week from String. Starting from Thursday, marked as 0.
   * (Because 1970-01-01 is Thursday).
   * @throws SparkIllegalArgumentException if the input is not a valid day of week.
   */
  def getDayOfWeekFromString(string: UTF8String): Int = {
    val dowString = string.toString.toUpperCase(Locale.ROOT)
    dowString match {
      case "SU" | "SUN" | "SUNDAY" => SUNDAY
      case "MO" | "MON" | "MONDAY" => MONDAY
      case "TU" | "TUE" | "TUESDAY" => TUESDAY
      case "WE" | "WED" | "WEDNESDAY" => WEDNESDAY
      case "TH" | "THU" | "THURSDAY" => THURSDAY
      case "FR" | "FRI" | "FRIDAY" => FRIDAY
      case "SA" | "SAT" | "SATURDAY" => SATURDAY
      case _ =>
        throw new SparkIllegalArgumentException(
          errorClass = "ILLEGAL_DAY_OF_WEEK",
          messageParameters = Map("string" -> string.toString))
    }
  }

  /**
   * Returns the first date which is later than startDate and is of the given dayOfWeek.
   * dayOfWeek is an integer ranges in [0, 6], and 0 is Thu, 1 is Fri, etc,.
   */
  def getNextDateForDayOfWeek(startDay: Int, dayOfWeek: Int): Int = {
    startDay + 1 + ((dayOfWeek - 1 - startDay) % 7 + 7) % 7
  }

  /** Returns last day of the month for the given number of days since 1970-01-01. */
  def getLastDayOfMonth(days: Int): Int = {
    val localDate = daysToLocalDate(days)
    (days - localDate.getDayOfMonth) + localDate.lengthOfMonth()
  }

  // The constants are visible for testing purpose only.
  private[sql] val TRUNC_INVALID = -1
  // The levels from TRUNC_TO_MICROSECOND to TRUNC_TO_DAY are used in truncations
  // of TIMESTAMP values only.
  private[sql] val TRUNC_TO_MICROSECOND = 0
  private[sql] val MIN_LEVEL_OF_TIMESTAMP_TRUNC = TRUNC_TO_MICROSECOND
  private[sql] val TRUNC_TO_MILLISECOND = 1
  private[sql] val TRUNC_TO_SECOND = 2
  private[sql] val TRUNC_TO_MINUTE = 3
  private[sql] val TRUNC_TO_HOUR = 4
  private[sql] val TRUNC_TO_DAY = 5
  // The levels from TRUNC_TO_WEEK to TRUNC_TO_YEAR are used in truncations
  // of DATE and TIMESTAMP values.
  private[sql] val TRUNC_TO_WEEK = 6
  private[sql] val MIN_LEVEL_OF_DATE_TRUNC = TRUNC_TO_WEEK
  private[sql] val TRUNC_TO_MONTH = 7
  private[sql] val TRUNC_TO_QUARTER = 8
  private[sql] val TRUNC_TO_YEAR = 9

  /**
   * Returns the trunc date from original date and trunc level.
   * Trunc level should be generated using `parseTruncLevel()`, should be between 6 and 9.
   */
  def truncDate(days: Int, level: Int): Int = {
    level match {
      case TRUNC_TO_WEEK => getNextDateForDayOfWeek(days - 7, MONDAY)
      case TRUNC_TO_MONTH => days - getDayOfMonth(days) + 1
      case TRUNC_TO_QUARTER =>
        localDateToDays(daysToLocalDate(days).`with`(IsoFields.DAY_OF_QUARTER, 1L))
      case TRUNC_TO_YEAR => days - getDayInYear(days) + 1
      case _ =>
        // caller make sure that this should never be reached
        throw QueryExecutionErrors.unreachableError(s": Invalid trunc level: $level")
    }
  }

  private def truncToUnit(micros: Long, zoneId: ZoneId, unit: ChronoUnit): Long = {
    val truncated = microsToInstant(micros).atZone(zoneId).truncatedTo(unit)
    instantToMicros(truncated.toInstant)
  }

  /**
   * Returns the trunc date time from original date time and trunc level.
   * Trunc level should be generated using `parseTruncLevel()`, should be between 0 and 9.
   */
  def truncTimestamp(micros: Long, level: Int, zoneId: ZoneId): Long = {
    // Time zone offsets have a maximum precision of seconds (see `java.time.ZoneOffset`). Hence
    // truncation to microsecond, millisecond, and second can be done
    // without using time zone information. This results in a performance improvement.
    level match {
      case TRUNC_TO_MICROSECOND => micros
      case TRUNC_TO_MILLISECOND =>
        micros - Math.floorMod(micros, MICROS_PER_MILLIS)
      case TRUNC_TO_SECOND =>
        micros - Math.floorMod(micros, MICROS_PER_SECOND)
      case TRUNC_TO_MINUTE => truncToUnit(micros, zoneId, ChronoUnit.MINUTES)
      case TRUNC_TO_HOUR => truncToUnit(micros, zoneId, ChronoUnit.HOURS)
      case TRUNC_TO_DAY => truncToUnit(micros, zoneId, ChronoUnit.DAYS)
      case _ => // Try to truncate date levels
        val dDays = microsToDays(micros, zoneId)
        daysToMicros(truncDate(dDays, level), zoneId)
    }
  }

  /**
   * Returns the truncate level, could be from TRUNC_TO_MICROSECOND to TRUNC_TO_YEAR,
   * or TRUNC_INVALID, TRUNC_INVALID means unsupported truncate level.
   */
  def parseTruncLevel(format: UTF8String): Int = {
    if (format == null) {
      TRUNC_INVALID
    } else {
      format.toString.toUpperCase(Locale.ROOT) match {
        case "MICROSECOND" => TRUNC_TO_MICROSECOND
        case "MILLISECOND" => TRUNC_TO_MILLISECOND
        case "SECOND" => TRUNC_TO_SECOND
        case "MINUTE" => TRUNC_TO_MINUTE
        case "HOUR" => TRUNC_TO_HOUR
        case "DAY" | "DD" => TRUNC_TO_DAY
        case "WEEK" => TRUNC_TO_WEEK
        case "MON" | "MONTH" | "MM" => TRUNC_TO_MONTH
        case "QUARTER" => TRUNC_TO_QUARTER
        case "YEAR" | "YYYY" | "YY" => TRUNC_TO_YEAR
        case _ => TRUNC_INVALID
      }
    }
  }

  /**
   * Converts a timestamp without time zone from a source to target time zone.
   *
   * @param sourceTz The time zone for the input timestamp without time zone.
   * @param targetTz The time zone to which the input timestamp should be converted.
   * @param micros The offset in microseconds represents a local timestamp.
   * @return The timestamp without time zone represents the same moment (physical time) as
   *         the input timestamp in the input time zone, but in the destination time zone.
   */
  def convertTimestampNtzToAnotherTz(sourceTz: String, targetTz: String, micros: Long): Long = {
    val ldt = microsToLocalDateTime(micros)
      .atZone(getZoneId(sourceTz))
      .withZoneSameInstant(getZoneId(targetTz))
      .toLocalDateTime
    localDateTimeToMicros(ldt)
  }

  /**
   * Returns a timestamp of given timezone from UTC timestamp, with the same string
   * representation in their timezone.
   */
  def fromUTCTime(micros: Long, timeZone: String): Long = {
    convertTz(micros, ZoneOffset.UTC, getZoneId(timeZone))
  }

  /**
   * Returns a utc timestamp from a given timestamp from a given timezone, with the same
   * string representation in their timezone.
   */
  def toUTCTime(micros: Long, timeZone: String): Long = {
    convertTz(micros, getZoneId(timeZone), ZoneOffset.UTC)
  }

  /**
   * Obtains the current date as days since the epoch in the specified time-zone.
   */
  def currentDate(zoneId: ZoneId): Int = localDateToDays(LocalDate.now(zoneId))

  private def today(zoneId: ZoneId): ZonedDateTime = {
    Instant.now().atZone(zoneId).`with`(LocalTime.MIDNIGHT)
  }

  private val specialValueRe = """(\p{Alpha}+)\p{Blank}*(.*)""".r

  /**
   * Extracts special values from an input string ignoring case.
   *
   * @param input A trimmed string
   * @return Some special value in lower case or None.
   */
  private def extractSpecialValue(input: String): Option[String] = {
    def isValid(value: String, timeZoneId: String): Boolean = {
      // Special value can be without any time zone
      if (timeZoneId.isEmpty) return true
      // "now" must not have the time zone field
      if (value.compareToIgnoreCase("now") == 0) return false
      // If the time zone field presents in the input, it must be resolvable
      try {
        getZoneId(timeZoneId)
        true
      } catch {
        case NonFatal(_) => false
      }
    }

    assert(input.trim.length == input.length)
    if (input.length < 3 || !input(0).isLetter) return None
    input match {
      case specialValueRe(v, z) if isValid(v, z) => Some(v.toLowerCase(Locale.US))
      case _ => None
    }
  }

  /**
   * Converts notational shorthands that are converted to ordinary timestamps.
   *
   * @param input A string to parse. It can contain trailing or leading whitespaces.
   * @param zoneId Zone identifier used to get the current timestamp.
   * @return Some of microseconds since the epoch if the conversion completed
   *         successfully otherwise None.
   */
  def convertSpecialTimestamp(input: String, zoneId: ZoneId): Option[Long] = {
    extractSpecialValue(input.trim).flatMap {
      case "epoch" => Some(0)
      case "now" => Some(instantToMicros(Instant.now()))
      case "today" => Some(instantToMicros(today(zoneId).toInstant))
      case "tomorrow" => Some(instantToMicros(today(zoneId).plusDays(1).toInstant))
      case "yesterday" => Some(instantToMicros(today(zoneId).minusDays(1).toInstant))
      case _ => None
    }
  }


  /**
   * Converts notational shorthands that are converted to ordinary timestamps without time zone.
   *
   * @param input A string to parse. It can contain trailing or leading whitespaces.
   * @param zoneId Zone identifier used to get the current local timestamp.
   * @return Some of microseconds since the epoch if the conversion completed
   *         successfully otherwise None.
   */
  def convertSpecialTimestampNTZ(input: String, zoneId: ZoneId): Option[Long] = {
    val localDateTime = extractSpecialValue(input.trim).flatMap {
      case "epoch" => Some(LocalDateTime.of(1970, 1, 1, 0, 0))
      case "now" => Some(LocalDateTime.now(zoneId))
      case "today" => Some(LocalDateTime.now(zoneId).`with`(LocalTime.MIDNIGHT))
      case "tomorrow" =>
        Some(LocalDateTime.now(zoneId).`with`(LocalTime.MIDNIGHT).plusDays(1))
      case "yesterday" =>
        Some(LocalDateTime.now(zoneId).`with`(LocalTime.MIDNIGHT).minusDays(1))
      case _ => None
    }
    localDateTime.map(localDateTimeToMicros)
  }

  /**
   * Converts notational shorthands that are converted to ordinary dates.
   *
   * @param input A string to parse. It can contain trailing or leading whitespaces.
   * @param zoneId Zone identifier used to get the current date.
   * @return Some of days since the epoch if the conversion completed successfully otherwise None.
   */
  def convertSpecialDate(input: String, zoneId: ZoneId): Option[Int] = {
    extractSpecialValue(input.trim).flatMap {
      case "epoch" => Some(0)
      case "now" | "today" => Some(currentDate(zoneId))
      case "tomorrow" => Some(Math.addExact(currentDate(zoneId), 1))
      case "yesterday" => Some(Math.subtractExact(currentDate(zoneId), 1))
      case _ => None
    }
  }

  /**
   * Subtracts two dates expressed as days since 1970-01-01.
   *
   * @param endDay The end date, exclusive
   * @param startDay The start date, inclusive
   * @return An interval between two dates. The interval can be negative
   *         if the end date is before the start date.
   */
  def subtractDates(endDay: Int, startDay: Int): CalendarInterval = {
    val period = Period.between(daysToLocalDate(startDay), daysToLocalDate(endDay))
    val months = Math.toIntExact(period.toTotalMonths)
    val days = period.getDays
    new CalendarInterval(months, days, 0)
  }

  /**
   * Subtracts two timestamps expressed as microseconds since 1970-01-01 00:00:00Z, and returns
   * the difference in microseconds between local timestamps at the given time zone.
   *
   * @param endMicros The end timestamp as microseconds since the epoch, exclusive
   * @param startMicros The end timestamp as microseconds since the epoch, inclusive
   * @param zoneId The time zone ID in which the subtraction is performed
   * @return The difference in microseconds between local timestamps corresponded to the input
   *         instants `end` and `start`.
   */
  def subtractTimestamps(endMicros: Long, startMicros: Long, zoneId: ZoneId): Long = {
    val localEndTs = getLocalDateTime(endMicros, zoneId)
    val localStartTs = getLocalDateTime(startMicros, zoneId)
    ChronoUnit.MICROS.between(localStartTs, localEndTs)
  }

  /**
   * Adds the specified number of units to a timestamp.
   *
   * @param unit A keyword that specifies the interval units to add to the input timestamp.
   * @param quantity The amount of `unit`s to add. It can be positive or negative.
   * @param micros The input timestamp value, expressed in microseconds since 1970-01-01 00:00:00Z.
   * @param zoneId The time zone ID at which the operation is performed.
   * @return A timestamp value, expressed in microseconds since 1970-01-01 00:00:00Z.
   */
  def timestampAdd(unit: String, quantity: Int, micros: Long, zoneId: ZoneId): Long = {
    try {
      unit.toUpperCase(Locale.ROOT) match {
        case "MICROSECOND" =>
          timestampAddInterval(micros, 0, 0, quantity, zoneId)
        case "MILLISECOND" =>
          timestampAddInterval(micros, 0, 0,
            Math.multiplyExact(quantity.toLong, MICROS_PER_MILLIS), zoneId)
        case "SECOND" =>
          timestampAddInterval(micros, 0, 0,
            Math.multiplyExact(quantity.toLong, MICROS_PER_SECOND), zoneId)
        case "MINUTE" =>
          timestampAddInterval(micros, 0, 0,
            Math.multiplyExact(quantity.toLong, MICROS_PER_MINUTE), zoneId)
        case "HOUR" =>
          timestampAddInterval(micros, 0, 0,
            Math.multiplyExact(quantity.toLong, MICROS_PER_HOUR), zoneId)
        case "DAY" | "DAYOFYEAR" =>
          timestampAddInterval(micros, 0, quantity, 0, zoneId)
        case "WEEK" =>
          timestampAddInterval(micros, 0, Math.multiplyExact(quantity, DAYS_PER_WEEK), 0, zoneId)
        case "MONTH" =>
          timestampAddMonths(micros, quantity, zoneId)
        case "QUARTER" =>
          timestampAddMonths(micros, Math.multiplyExact(quantity, 3), zoneId)
        case "YEAR" =>
          timestampAddMonths(micros, Math.multiplyExact(quantity, MONTHS_PER_YEAR), zoneId)
      }
    } catch {
      case _: scala.MatchError =>
        throw QueryExecutionErrors.invalidDatetimeUnitError("TIMESTAMPADD", unit)
      case _: ArithmeticException | _: DateTimeException =>
        throw QueryExecutionErrors.timestampAddOverflowError(micros, quantity, unit)
      case e: Throwable =>
        throw SparkException.internalError(s"Failure of 'timestampAdd': ${e.getMessage}")
    }
  }

  private val timestampDiffMap = Map[String, (Temporal, Temporal) => Long](
    "MICROSECOND" -> ChronoUnit.MICROS.between,
    "MILLISECOND" -> ChronoUnit.MILLIS.between,
    "SECOND" -> ChronoUnit.SECONDS.between,
    "MINUTE" -> ChronoUnit.MINUTES.between,
    "HOUR" -> ChronoUnit.HOURS.between,
    "DAY" -> ChronoUnit.DAYS.between,
    "WEEK" -> ChronoUnit.WEEKS.between,
    "MONTH" -> ChronoUnit.MONTHS.between,
    "QUARTER" -> ((startTs: Temporal, endTs: Temporal) =>
      ChronoUnit.MONTHS.between(startTs, endTs) / 3),
    "YEAR" -> ChronoUnit.YEARS.between)

  /**
   * Gets the difference between two timestamps.
   *
   * @param unit Specifies the interval units in which to express the difference between
   *             the two timestamp parameters.
   * @param startTs A timestamp which the function subtracts from `endTs`.
   * @param endTs A timestamp from which the function subtracts `startTs`.
   * @param zoneId The time zone ID at which the operation is performed.
   * @return The time span between two timestamp values, in the units specified.
   */
  def timestampDiff(unit: String, startTs: Long, endTs: Long, zoneId: ZoneId): Long = {
    val unitInUpperCase = unit.toUpperCase(Locale.ROOT)
    if (timestampDiffMap.contains(unitInUpperCase)) {
      val startLocalTs = getLocalDateTime(startTs, zoneId)
      val endLocalTs = getLocalDateTime(endTs, zoneId)
      timestampDiffMap(unitInUpperCase)(startLocalTs, endLocalTs)
    } else {
      throw QueryExecutionErrors.invalidDatetimeUnitError("TIMESTAMPDIFF", unit)
    }
  }
}
