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

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time._
import java.time.temporal.{ChronoField, ChronoUnit, IsoFields}
import java.util.{Locale, TimeZone}
import java.util.concurrent.TimeUnit._

import scala.util.control.NonFatal

import sun.util.calendar.ZoneInfo

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.RebaseDateTime._
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Helper functions for converting between internal and external date and time representations.
 * Dates are exposed externally as java.sql.Date and are represented internally as the number of
 * dates since the Unix epoch (1970-01-01). Timestamps are exposed externally as java.sql.Timestamp
 * and are stored internally as longs, which are capable of storing timestamps with microsecond
 * precision.
 */
object DateTimeUtils {

  // we use Int and Long internally to represent [[DateType]] and [[TimestampType]]
  type SQLDate = Int
  type SQLTimestamp = Long

  // see http://stackoverflow.com/questions/466321/convert-unix-timestamp-to-julian
  // it's 2440587.5, rounding up to compatible with Hive
  final val JULIAN_DAY_OF_EPOCH = 2440588

  final val julianCommonEraStart = Timestamp.valueOf("0001-01-01 00:00:00")

  final val TimeZoneGMT = TimeZone.getTimeZone("GMT")
  final val TimeZoneUTC = TimeZone.getTimeZone("UTC")

  val TIMEZONE_OPTION = "timeZone"

  def defaultTimeZone(): TimeZone = TimeZone.getDefault()

  def getZoneId(timeZoneId: String): ZoneId = {
    // To support the (+|-)h:mm format because it was supported before Spark 3.0.
    ZoneId.of(timeZoneId.replaceFirst("(\\+|\\-)(\\d):", "$10$2:"), ZoneId.SHORT_IDS)
  }
  def getTimeZone(timeZoneId: String): TimeZone = {
    TimeZone.getTimeZone(getZoneId(timeZoneId))
  }

  // we should use the exact day as Int, for example, (year, month, day) -> day
  def millisToDays(millisUtc: Long): SQLDate = {
    millisToDays(millisUtc, defaultTimeZone().toZoneId)
  }

  def millisToDays(millisUtc: Long, zoneId: ZoneId): SQLDate = {
    val instant = microsToInstant(fromMillis(millisUtc))
    localDateToDays(LocalDateTime.ofInstant(instant, zoneId).toLocalDate)
  }

  // reverse of millisToDays
  def daysToMillis(days: SQLDate): Long = {
    daysToMillis(days, defaultTimeZone().toZoneId)
  }

  def daysToMillis(days: SQLDate, zoneId: ZoneId): Long = {
    val instant = daysToLocalDate(days).atStartOfDay(zoneId).toInstant
    toMillis(instantToMicros(instant))
  }

  // Converts Timestamp to string according to Hive TimestampWritable convention.
  def timestampToString(tf: TimestampFormatter, us: SQLTimestamp): String = {
    tf.format(us)
  }

  /**
   * Converts a local date at the default JVM time zone to the number of days since 1970-01-01
   * in the hybrid calendar (Julian + Gregorian) by discarding the time part. The resulted days are
   * rebased from the hybrid to Proleptic Gregorian calendar. The days rebasing is performed via
   * UTC time zone for simplicity because the difference between two calendars is the same in
   * any given time zone and UTC time zone.
   *
   * Note: The date is shifted by the offset of the default JVM time zone for backward compatibility
   *       with Spark 2.4 and earlier versions. The goal of the shift is to get a local date derived
   *       from the number of days that has the same date fields (year, month, day) as the original
   *       `date` at the default JVM time zone.
   *
   * @param date It represents a specific instant in time based on the hybrid calendar which
   *             combines Julian and Gregorian calendars.
   * @return The number of days since the epoch in Proleptic Gregorian calendar.
   */
  def fromJavaDate(date: Date): SQLDate = {
    val millisUtc = date.getTime
    val millisLocal = millisUtc + TimeZone.getDefault.getOffset(millisUtc)
    val julianDays = Math.toIntExact(Math.floorDiv(millisLocal, MILLIS_PER_DAY))
    rebaseJulianToGregorianDays(julianDays)
  }

  /**
   * Converts days since the epoch 1970-01-01 in Proleptic Gregorian calendar to a local date
   * at the default JVM time zone in the hybrid calendar (Julian + Gregorian). It rebases the given
   * days from Proleptic Gregorian to the hybrid calendar at UTC time zone for simplicity because
   * the difference between two calendars doesn't depend on any time zone. The result is shifted
   * by the time zone offset in wall clock to have the same date fields (year, month, day)
   * at the default JVM time zone as the input `daysSinceEpoch` in Proleptic Gregorian calendar.
   *
   * Note: The date is shifted by the offset of the default JVM time zone for backward compatibility
   *       with Spark 2.4 and earlier versions.
   *
   * @param daysSinceEpoch The number of days since 1970-01-01 in Proleptic Gregorian calendar.
   * @return A local date in the hybrid calendar as `java.sql.Date` from number of days since epoch.
   */
  def toJavaDate(daysSinceEpoch: SQLDate): Date = {
    val rebasedDays = rebaseGregorianToJulianDays(daysSinceEpoch)
    val localMillis = Math.multiplyExact(rebasedDays, MILLIS_PER_DAY)
    val timeZoneOffset = TimeZone.getDefault match {
      case zoneInfo: ZoneInfo => zoneInfo.getOffsetsByWall(localMillis, null)
      case timeZone: TimeZone => timeZone.getOffset(localMillis - timeZone.getRawOffset)
    }
    new Date(localMillis - timeZoneOffset)
  }

  /**
   * Converts microseconds since the epoch to an instance of `java.sql.Timestamp`
   * via creating a local timestamp at the system time zone in Proleptic Gregorian
   * calendar, extracting date and time fields like `year` and `hours`, and forming
   * new timestamp in the hybrid calendar from the extracted fields.
   *
   * The conversion is based on the JVM system time zone because the `java.sql.Timestamp`
   * uses the time zone internally.
   *
   * The method performs the conversion via local timestamp fields to have the same date-time
   * representation as `year`, `month`, `day`, ..., `seconds` in the original calendar
   * and in the target calendar.
   *
   * @param us The number of microseconds since 1970-01-01T00:00:00.000000Z.
   * @return A `java.sql.Timestamp` from number of micros since epoch.
   */
  def toJavaTimestamp(us: SQLTimestamp): Timestamp = {
    val rebasedMicros = rebaseGregorianToJulianMicros(us)
    val seconds = Math.floorDiv(rebasedMicros, MICROS_PER_SECOND)
    val ts = new Timestamp(seconds * MILLIS_PER_SECOND)
    val nanos = (rebasedMicros - seconds * MICROS_PER_SECOND) * NANOS_PER_MICROS
    ts.setNanos(nanos.toInt)
    ts
  }

  /**
   * Converts an instance of `java.sql.Timestamp` to the number of microseconds since
   * 1970-01-01T00:00:00.000000Z. It extracts date-time fields from the input, builds
   * a local timestamp in Proleptic Gregorian calendar from the fields, and binds
   * the timestamp to the system time zone. The resulted instant is converted to
   * microseconds since the epoch.
   *
   * The conversion is performed via the system time zone because it is used internally
   * in `java.sql.Timestamp` while extracting date-time fields.
   *
   * The goal of the function is to have the same local date-time in the original calendar
   * - the hybrid calendar (Julian + Gregorian) and in the target calendar which is
   * Proleptic Gregorian calendar, see SPARK-26651.
   *
   * @param t It represents a specific instant in time based on
   *          the hybrid calendar which combines Julian and
   *          Gregorian calendars.
   * @return The number of micros since epoch from `java.sql.Timestamp`.
   */
  def fromJavaTimestamp(t: Timestamp): SQLTimestamp = {
    val micros = fromMillis(t.getTime) + (t.getNanos / NANOS_PER_MICROS) % MICROS_PER_MILLIS
    rebaseJulianToGregorianMicros(micros)
  }

  /**
   * Returns the number of microseconds since epoch from Julian day
   * and nanoseconds in a day
   */
  def fromJulianDay(day: Int, nanoseconds: Long): SQLTimestamp = {
    // use Long to avoid rounding errors
    val seconds = (day - JULIAN_DAY_OF_EPOCH).toLong * SECONDS_PER_DAY
    val micros = SECONDS.toMicros(seconds) + NANOSECONDS.toMicros(nanoseconds)
    val rebased = rebaseJulianToGregorianMicros(micros)
    rebased
  }

  /**
   * Returns Julian day and nanoseconds in a day from the number of microseconds
   *
   * Note: support timestamp since 4717 BC (without negative nanoseconds, compatible with Hive).
   */
  def toJulianDay(us: SQLTimestamp): (Int, Long) = {
    val julian_us = rebaseGregorianToJulianMicros(us) + JULIAN_DAY_OF_EPOCH * MICROS_PER_DAY
    val day = julian_us / MICROS_PER_DAY
    val micros = julian_us % MICROS_PER_DAY
    (day.toInt, MICROSECONDS.toNanos(micros))
  }

  /*
   * Converts the timestamp to milliseconds since epoch. In spark timestamp values have microseconds
   * precision, so this conversion is lossy.
   */
  def toMillis(us: SQLTimestamp): Long = {
    // When the timestamp is negative i.e before 1970, we need to adjust the millseconds portion.
    // Example - 1965-01-01 10:11:12.123456 is represented as (-157700927876544) in micro precision.
    // In millis precision the above needs to be represented as (-157700927877).
    Math.floorDiv(us, MICROS_PER_MILLIS)
  }

  /*
   * Converts milliseconds since epoch to SQLTimestamp.
   */
  def fromMillis(millis: Long): SQLTimestamp = {
    Math.multiplyExact(millis, MICROS_PER_MILLIS)
  }

  def microsToEpochDays(epochMicros: SQLTimestamp, zoneId: ZoneId): SQLDate = {
    localDateToDays(microsToInstant(epochMicros).atZone(zoneId).toLocalDate)
  }

  def epochDaysToMicros(epochDays: SQLDate, zoneId: ZoneId): SQLTimestamp = {
    val localDate = LocalDate.ofEpochDay(epochDays)
    val zeroLocalTime = LocalTime.MIDNIGHT
    val localDateTime = LocalDateTime.of(localDate, zeroLocalTime)
    instantToMicros(localDateTime.atZone(zoneId).toInstant)
  }

  // A method called by JSON/CSV parser to clean up the legacy timestamp string by removing the
  // "GMT" string.
  def cleanLegacyTimestampStr(s: String): String = {
    val indexOfGMT = s.indexOf("GMT")
    if (indexOfGMT != -1) {
      // ISO8601 with a weird time zone specifier (2000-01-01T00:00GMT+01:00)
      val s0 = s.substring(0, indexOfGMT)
      val s1 = s.substring(indexOfGMT + 3)
      // Mapped to 2000-01-01T00:00+01:00
      s0 + s1
    } else {
      s
    }
  }

  /**
   * Trim and parse a given UTF8 date string to the corresponding a corresponding [[Long]] value.
   * The return type is [[Option]] in order to distinguish between 0L and null. The following
   * formats are allowed:
   *
   * `yyyy`
   * `yyyy-[m]m`
   * `yyyy-[m]m-[d]d`
   * `yyyy-[m]m-[d]d `
   * `yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   * `yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]`
   *
   * where `zone_id` should have one of the forms:
   *   - Z - Zulu time zone UTC+0
   *   - +|-[h]h:[m]m
   *   - A short id, see https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#SHORT_IDS
   *   - An id with one of the prefixes UTC+, UTC-, GMT+, GMT-, UT+ or UT-,
   *     and a suffix in the formats:
   *     - +|-h[h]
   *     - +|-hh[:]mm
   *     - +|-hh:mm:ss
   *     - +|-hhmmss
   *  - Region-based zone IDs in the form `area/city`, such as `Europe/Paris`
   */
  def stringToTimestamp(s: UTF8String, timeZoneId: ZoneId): Option[SQLTimestamp] = {
    if (s == null) {
      return None
    }
    var tz: Option[String] = None
    val segments: Array[Int] = Array[Int](1, 1, 1, 0, 0, 0, 0, 0, 0)
    var i = 0
    var currentSegmentValue = 0
    val bytes = s.trimAll().getBytes
    val specialTimestamp = convertSpecialTimestamp(bytes, timeZoneId)
    if (specialTimestamp.isDefined) return specialTimestamp
    var j = 0
    var digitsMilli = 0
    var justTime = false
    while (j < bytes.length) {
      val b = bytes(j)
      val parsedValue = b - '0'.toByte
      if (parsedValue < 0 || parsedValue > 9) {
        if (j == 0 && b == 'T') {
          justTime = true
          i += 3
        } else if (i < 2) {
          if (b == '-') {
            if (i == 0 && j != 4) {
              // year should have exact four digits
              return None
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
          } else if (i == 0 && b == ':') {
            justTime = true
            segments(3) = currentSegmentValue
            currentSegmentValue = 0
            i = 4
          } else {
            return None
          }
        } else if (i == 2) {
          if (b == ' ' || b == 'T') {
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
          } else {
            return None
          }
        } else if (i == 3 || i == 4) {
          if (b == ':') {
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
          } else {
            return None
          }
        } else if (i == 5 || i == 6) {
          if (b == '-' || b == '+') {
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
            tz = Some(new String(bytes, j, 1))
          } else if (b == '.' && i == 5) {
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
          } else {
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
            tz = Some(new String(bytes, j, bytes.length - j))
            j = bytes.length - 1
          }
          if (i == 6  && b != '.') {
            i += 1
          }
        } else {
          if (i < segments.length && (b == ':' || b == ' ')) {
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
          } else {
            return None
          }
        }
      } else {
        if (i == 6) {
          digitsMilli += 1
        }
        currentSegmentValue = currentSegmentValue * 10 + parsedValue
      }
      j += 1
    }

    segments(i) = currentSegmentValue
    if (!justTime && i == 0 && j != 4) {
      // year should have exact four digits
      return None
    }

    while (digitsMilli < 6) {
      segments(6) *= 10
      digitsMilli += 1
    }

    // We are truncating the nanosecond part, which results in loss of precision
    while (digitsMilli > 6) {
      segments(6) /= 10
      digitsMilli -= 1
    }
    try {
      val zoneId = tz match {
        case None => timeZoneId
        case Some("+") => ZoneOffset.ofHoursMinutes(segments(7), segments(8))
        case Some("-") => ZoneOffset.ofHoursMinutes(-segments(7), -segments(8))
        case Some(zoneName: String) => getZoneId(zoneName.trim)
      }
      val nanoseconds = MICROSECONDS.toNanos(segments(6))
      val localTime = LocalTime.of(segments(3), segments(4), segments(5), nanoseconds.toInt)
      val localDate = if (justTime) {
        LocalDate.now(zoneId)
      } else {
        LocalDate.of(segments(0), segments(1), segments(2))
      }
      val localDateTime = LocalDateTime.of(localDate, localTime)
      val zonedDateTime = ZonedDateTime.of(localDateTime, zoneId)
      val instant = Instant.from(zonedDateTime)
      Some(instantToMicros(instant))
    } catch {
      case NonFatal(_) => None
    }
  }

  // See issue SPARK-35679
  // min second cause overflow in instant to micro
  private val MIN_SECONDS = Math.floorDiv(Long.MinValue, MICROS_PER_SECOND)

  /**
   * Gets the number of microseconds since the epoch of 1970-01-01 00:00:00Z from the given
   * instance of `java.time.Instant`. The epoch microsecond count is a simple incrementing count of
   * microseconds where microsecond 0 is 1970-01-01 00:00:00Z.
   */
  def instantToMicros(instant: Instant): Long = {
    val secs = instant.getEpochSecond
    if (secs == MIN_SECONDS) {
      val us = Math.multiplyExact(secs + 1, MICROS_PER_SECOND)
      Math.addExact(us, NANOSECONDS.toMicros(instant.getNano) - MICROS_PER_SECOND)
    } else {
      val us = Math.multiplyExact(secs, MICROS_PER_SECOND)
      Math.addExact(us, NANOSECONDS.toMicros(instant.getNano))
    }
  }

  def microsToInstant(us: Long): Instant = {
    val secs = Math.floorDiv(us, MICROS_PER_SECOND)
    val mos = Math.floorMod(us, MICROS_PER_SECOND)
    Instant.ofEpochSecond(secs, mos * NANOS_PER_MICROS)
  }

  def instantToDays(instant: Instant): Int = {
    val seconds = instant.getEpochSecond
    val days = Math.floorDiv(seconds, SECONDS_PER_DAY)
    days.toInt
  }

  def localDateToDays(localDate: LocalDate): Int = {
    Math.toIntExact(localDate.toEpochDay)
  }

  def daysToLocalDate(days: Int): LocalDate = LocalDate.ofEpochDay(days)

  /**
   * Trim and parse a given UTF8 date string to a corresponding [[Int]] value.
   * The return type is [[Option]] in order to distinguish between 0 and null. The following
   * formats are allowed:
   *
   * `yyyy`
   * `yyyy-[m]m`
   * `yyyy-[m]m-[d]d`
   * `yyyy-[m]m-[d]d `
   * `yyyy-[m]m-[d]d *`
   * `yyyy-[m]m-[d]dT*`
   */
  def stringToDate(s: UTF8String, zoneId: ZoneId): Option[SQLDate] = {
    if (s == null) {
      return None
    }
    val segments: Array[Int] = Array[Int](1, 1, 1)
    var i = 0
    var currentSegmentValue = 0
    val bytes = s.trimAll().getBytes
    val specialDate = convertSpecialDate(bytes, zoneId)
    if (specialDate.isDefined) return specialDate
    var j = 0
    while (j < bytes.length && (i < 3 && !(bytes(j) == ' ' || bytes(j) == 'T'))) {
      val b = bytes(j)
      if (i < 2 && b == '-') {
        if (i == 0 && j != 4) {
          // year should have exact four digits
          return None
        }
        segments(i) = currentSegmentValue
        currentSegmentValue = 0
        i += 1
      } else {
        val parsedValue = b - '0'.toByte
        if (parsedValue < 0 || parsedValue > 9) {
          return None
        } else {
          currentSegmentValue = currentSegmentValue * 10 + parsedValue
        }
      }
      j += 1
    }
    if (i == 0 && j != 4) {
      // year should have exact four digits
      return None
    }
    if (i < 2 && j < bytes.length) {
      // For the `yyyy` and `yyyy-[m]m` formats, entire input must be consumed.
      return None
    }
    segments(i) = currentSegmentValue
    try {
      val localDate = LocalDate.of(segments(0), segments(1), segments(2))
      Some(localDateToDays(localDate))
    } catch {
      case NonFatal(_) => None
    }
  }

  private def localTimestamp(microsec: SQLTimestamp, zoneId: ZoneId): LocalDateTime = {
    microsToInstant(microsec).atZone(zoneId).toLocalDateTime
  }

  /**
   * Returns the hour value of a given timestamp value. The timestamp is expressed in microseconds.
   */
  def getHours(microsec: SQLTimestamp, zoneId: ZoneId): Int = {
    localTimestamp(microsec, zoneId).getHour
  }

  /**
   * Returns the minute value of a given timestamp value. The timestamp is expressed in
   * microseconds.
   */
  def getMinutes(microsec: SQLTimestamp, zoneId: ZoneId): Int = {
    localTimestamp(microsec, zoneId).getMinute
  }

  /**
   * Returns the second value of a given timestamp value. The timestamp is expressed in
   * microseconds.
   */
  def getSeconds(microsec: SQLTimestamp, zoneId: ZoneId): Int = {
    localTimestamp(microsec, zoneId).getSecond
  }

  /**
   * Returns the seconds part and its fractional part with microseconds.
   */
  def getSecondsWithFraction(microsec: SQLTimestamp, zoneId: ZoneId): Decimal = {
    Decimal(getMicroseconds(microsec, zoneId), 8, 6)
  }

  /**
   * Returns seconds, including fractional parts, multiplied by 1000000. The timestamp
   * is expressed in microseconds since the epoch.
   */
  def getMicroseconds(timestamp: SQLTimestamp, zoneId: ZoneId): Int = {
    val lt = localTimestamp(timestamp, zoneId)
    (lt.getLong(ChronoField.MICRO_OF_SECOND) + lt.getSecond * MICROS_PER_SECOND).toInt
  }

  /**
   * Returns the 'day in year' value for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getDayInYear(date: SQLDate): Int = {
    LocalDate.ofEpochDay(date).getDayOfYear
  }

  /**
   * Returns the year value for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getYear(date: SQLDate): Int = {
    LocalDate.ofEpochDay(date).getYear
  }

  /**
   * Returns the year which conforms to ISO 8601. Each ISO 8601 week-numbering
   * year begins with the Monday of the week containing the 4th of January.
   */
  def getWeekBasedYear(date: SQLDate): Int = {
    daysToLocalDate(date).get(IsoFields.WEEK_BASED_YEAR)
  }

  /**
   * Returns the quarter for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getQuarter(date: SQLDate): Int = {
    LocalDate.ofEpochDay(date).get(IsoFields.QUARTER_OF_YEAR)
  }

  /**
   * Split date (expressed in days since 1.1.1970) into four fields:
   * year, month (Jan is Month 1), dayInMonth, daysToMonthEnd (0 if it's last day of month).
   */
  def splitDate(date: SQLDate): (Int, Int, Int, Int) = {
    val ld = LocalDate.ofEpochDay(date)
    (ld.getYear, ld.getMonthValue, ld.getDayOfMonth, ld.lengthOfMonth() - ld.getDayOfMonth)
  }

  /**
   * Returns the month value for the given date. The date is expressed in days
   * since 1.1.1970. January is month 1.
   */
  def getMonth(date: SQLDate): Int = {
    LocalDate.ofEpochDay(date).getMonthValue
  }

  /**
   * Returns the 'day of month' value for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getDayOfMonth(date: SQLDate): Int = {
    LocalDate.ofEpochDay(date).getDayOfMonth
  }

  /**
   * Add date and year-month interval.
   * Returns a date value, expressed in days since 1.1.1970.
   */
  def dateAddMonths(days: SQLDate, months: Int): SQLDate = {
    LocalDate.ofEpochDay(days).plusMonths(months).toEpochDay.toInt
  }

  /**
   * Add timestamp and full interval.
   * Returns a timestamp value, expressed in microseconds since 1.1.1970 00:00:00.
   */
  def timestampAddInterval(
      start: SQLTimestamp,
      months: Int,
      days: Int,
      microseconds: Long,
      zoneId: ZoneId): SQLTimestamp = {
    val resultTimestamp = microsToInstant(start)
      .atZone(zoneId)
      .plusMonths(months)
      .plusDays(days)
      .plus(microseconds, ChronoUnit.MICROS)
    instantToMicros(resultTimestamp.toInstant)
  }

  /**
   * Add the date and the interval's months and days.
   * Returns a date value, expressed in days since 1.1.1970.
   *
   * @throws DateTimeException if the result exceeds the supported date range
   * @throws IllegalArgumentException if the interval has `microseconds` part
   */
  def dateAddInterval(
     start: SQLDate,
     interval: CalendarInterval): SQLDate = {
    require(interval.microseconds == 0,
      "Cannot add hours, minutes or seconds, milliseconds, microseconds to a date")
    val ld = LocalDate.ofEpochDay(start).plusMonths(interval.months).plusDays(interval.days)
    localDateToDays(ld)
  }

  /**
   * Returns number of months between time1 and time2. time1 and time2 are expressed in
   * microseconds since 1.1.1970. If time1 is later than time2, the result is positive.
   *
   * If time1 and time2 are on the same day of month, or both are the last day of month,
   * returns, time of day will be ignored.
   *
   * Otherwise, the difference is calculated based on 31 days per month.
   * The result is rounded to 8 decimal places if `roundOff` is set to true.
   */
  def monthsBetween(
      time1: SQLTimestamp,
      time2: SQLTimestamp,
      roundOff: Boolean,
      zoneId: ZoneId): Double = {
    val millis1 = toMillis(time1)
    val millis2 = toMillis(time2)
    val date1 = millisToDays(millis1, zoneId)
    val date2 = millisToDays(millis2, zoneId)
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
    val secondsInDay1 = MILLISECONDS.toSeconds(millis1 - daysToMillis(date1, zoneId))
    val secondsInDay2 = MILLISECONDS.toSeconds(millis2 - daysToMillis(date2, zoneId))
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

  /*
   * Returns day of week from String. Starting from Thursday, marked as 0.
   * (Because 1970-01-01 is Thursday).
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
      case _ => -1
    }
  }

  /**
   * Returns the first date which is later than startDate and is of the given dayOfWeek.
   * dayOfWeek is an integer ranges in [0, 6], and 0 is Thu, 1 is Fri, etc,.
   */
  def getNextDateForDayOfWeek(startDate: SQLDate, dayOfWeek: Int): SQLDate = {
    startDate + 1 + ((dayOfWeek - 1 - startDate) % 7 + 7) % 7
  }

  /**
   * Returns last day of the month for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getLastDayOfMonth(date: SQLDate): SQLDate = {
    val localDate = LocalDate.ofEpochDay(date)
    (date - localDate.getDayOfMonth) + localDate.lengthOfMonth()
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
  def truncDate(d: SQLDate, level: Int): SQLDate = {
    level match {
      case TRUNC_TO_WEEK => getNextDateForDayOfWeek(d - 7, MONDAY)
      case TRUNC_TO_MONTH => d - DateTimeUtils.getDayOfMonth(d) + 1
      case TRUNC_TO_QUARTER =>
        localDateToDays(daysToLocalDate(d).`with`(IsoFields.DAY_OF_QUARTER, 1L))
      case TRUNC_TO_YEAR => d - DateTimeUtils.getDayInYear(d) + 1
      case _ =>
        // caller make sure that this should never be reached
        sys.error(s"Invalid trunc level: $level")
    }
  }

  private def truncToUnit(t: SQLTimestamp, zoneId: ZoneId, unit: ChronoUnit): SQLTimestamp = {
    val truncated = microsToInstant(t).atZone(zoneId).truncatedTo(unit)
    instantToMicros(truncated.toInstant)
  }

  /**
   * Returns the trunc date time from original date time and trunc level.
   * Trunc level should be generated using `parseTruncLevel()`, should be between 0 and 9.
   */
  def truncTimestamp(t: SQLTimestamp, level: Int, zoneId: ZoneId): SQLTimestamp = {
    // Time zone offsets have a maximum precision of seconds (see `java.time.ZoneOffset`). Hence
    // truncation to microsecond, millisecond, and second can be done
    // without using time zone information. This results in a performance improvement.
    level match {
      case TRUNC_TO_MICROSECOND => t
      case TRUNC_TO_MINUTE => truncToUnit(t, zoneId, ChronoUnit.MINUTES)
      case TRUNC_TO_HOUR => truncToUnit(t, zoneId, ChronoUnit.HOURS)
      case TRUNC_TO_DAY => truncToUnit(t, zoneId, ChronoUnit.DAYS)
      case _ =>
        val millis = toMillis(t)
        val truncated = level match {
          case TRUNC_TO_MILLISECOND => millis
          case TRUNC_TO_SECOND =>
            millis - Math.floorMod(millis, MILLIS_PER_SECOND)
          case _ => // Try to truncate date levels
            val dDays = millisToDays(millis, zoneId)
            daysToMillis(truncDate(dDays, level), zoneId)
        }
        fromMillis(truncated)
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
   * Convert the timestamp `ts` from one timezone to another.
   *
   * TODO: Because of DST, the conversion between UTC and human time is not exactly one-to-one
   * mapping, the conversion here may return wrong result, we should make the timestamp
   * timezone-aware.
   */
  def convertTz(ts: SQLTimestamp, fromZone: ZoneId, toZone: ZoneId): SQLTimestamp = {
    val rebasedDateTime = microsToInstant(ts).atZone(toZone).toLocalDateTime.atZone(fromZone)
    instantToMicros(rebasedDateTime.toInstant)
  }

  /**
   * Returns a timestamp of given timezone from utc timestamp, with the same string
   * representation in their timezone.
   */
  def fromUTCTime(time: SQLTimestamp, timeZone: String): SQLTimestamp = {
    convertTz(time, ZoneOffset.UTC, getZoneId(timeZone))
  }

  /**
   * Returns a utc timestamp from a given timestamp from a given timezone, with the same
   * string representation in their timezone.
   */
  def toUTCTime(time: SQLTimestamp, timeZone: String): SQLTimestamp = {
    convertTz(time, getZoneId(timeZone), ZoneOffset.UTC)
  }

  def currentTimestamp(): SQLTimestamp = instantToMicros(Instant.now())

  def currentDate(zoneId: ZoneId): SQLDate = localDateToDays(LocalDate.now(zoneId))

  private def today(zoneId: ZoneId): ZonedDateTime = {
    Instant.now().atZone(zoneId).`with`(LocalTime.MIDNIGHT)
  }

  private val specialValueRe = """(\p{Alpha}+)\p{Blank}*(.*)""".r

  /**
   * Extracts special values from an input string ignoring case.
   * @param input A trimmed string
   * @param zoneId Zone identifier used to get the current date.
   * @return Some special value in lower case or None.
   */
  private def extractSpecialValue(input: String, zoneId: ZoneId): Option[String] = {
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
   * @param input A trimmed string
   * @param zoneId Zone identifier used to get the current date.
   * @return Some of microseconds since the epoch if the conversion completed
   *         successfully otherwise None.
   */
  def convertSpecialTimestamp(input: String, zoneId: ZoneId): Option[SQLTimestamp] = {
    extractSpecialValue(input, zoneId).flatMap {
      case "epoch" => Some(0)
      case "now" => Some(currentTimestamp())
      case "today" => Some(instantToMicros(today(zoneId).toInstant))
      case "tomorrow" => Some(instantToMicros(today(zoneId).plusDays(1).toInstant))
      case "yesterday" => Some(instantToMicros(today(zoneId).minusDays(1).toInstant))
      case _ => None
    }
  }

  private def convertSpecialTimestamp(bytes: Array[Byte], zoneId: ZoneId): Option[SQLTimestamp] = {
    if (bytes.length > 0 && Character.isAlphabetic(bytes(0))) {
      convertSpecialTimestamp(new String(bytes, StandardCharsets.UTF_8), zoneId)
    } else {
      None
    }
  }

  /**
   * Converts notational shorthands that are converted to ordinary dates.
   * @param input A trimmed string
   * @param zoneId Zone identifier used to get the current date.
   * @return Some of days since the epoch if the conversion completed successfully otherwise None.
   */
  def convertSpecialDate(input: String, zoneId: ZoneId): Option[SQLDate] = {
    extractSpecialValue(input, zoneId).flatMap {
      case "epoch" => Some(0)
      case "now" | "today" => Some(currentDate(zoneId))
      case "tomorrow" => Some(Math.addExact(currentDate(zoneId), 1))
      case "yesterday" => Some(Math.subtractExact(currentDate(zoneId), 1))
      case _ => None
    }
  }

  private def convertSpecialDate(bytes: Array[Byte], zoneId: ZoneId): Option[SQLDate] = {
    if (bytes.length > 0 && Character.isAlphabetic(bytes(0))) {
      convertSpecialDate(new String(bytes, StandardCharsets.UTF_8), zoneId)
    } else {
      None
    }
  }

  /**
   * Subtracts two dates.
   * @param endDate The end date, exclusive
   * @param startDate The start date, inclusive
   * @return An interval between two dates. The interval can be negative
   *         if the end date is before the start date.
   */
  def subtractDates(endDate: SQLDate, startDate: SQLDate): CalendarInterval = {
    val period = Period.between(
      LocalDate.ofEpochDay(startDate),
      LocalDate.ofEpochDay(endDate))
    val months = period.getMonths + 12 * period.getYears
    val days = period.getDays
    new CalendarInterval(months, days, 0)
  }
}
