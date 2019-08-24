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

import java.sql.{Date, Timestamp}
import java.time._
import java.time.temporal.{ChronoField, ChronoUnit, IsoFields}
import java.util.{Locale, TimeZone}
import java.util.concurrent.TimeUnit._

import scala.util.control.NonFatal

import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

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

  // Pre-calculated values can provide an opportunity of additional optimizations
  // to the compiler like constants propagation and folding.
  final val NANOS_PER_MICROS: Long = 1000
  final val MICROS_PER_MILLIS: Long = 1000
  final val MILLIS_PER_SECOND: Long = 1000
  final val SECONDS_PER_DAY: Long = 24 * 60 * 60
  final val MICROS_PER_SECOND: Long = MILLIS_PER_SECOND * MICROS_PER_MILLIS
  final val NANOS_PER_MILLIS: Long = NANOS_PER_MICROS * MICROS_PER_MILLIS
  final val NANOS_PER_SECOND: Long = NANOS_PER_MICROS * MICROS_PER_SECOND
  final val MICROS_PER_DAY: Long = SECONDS_PER_DAY * MICROS_PER_SECOND
  final val MILLIS_PER_MINUTE: Long = 60 * MILLIS_PER_SECOND
  final val MILLIS_PER_HOUR: Long = 60 * MILLIS_PER_MINUTE
  final val MILLIS_PER_DAY: Long = SECONDS_PER_DAY * MILLIS_PER_SECOND

  // number of days between 1.1.1970 and 1.1.2001
  final val to2001 = -11323

  // this is year -17999, calculation: 50 * daysIn400Year
  final val YearZero = -17999
  final val toYearZero = to2001 + 7304850
  final val TimeZoneGMT = TimeZone.getTimeZone("GMT")
  final val TimeZoneUTC = TimeZone.getTimeZone("UTC")

  val TIMEZONE_OPTION = "timeZone"

  def defaultTimeZone(): TimeZone = TimeZone.getDefault()

  def getZoneId(timeZoneId: String): ZoneId = ZoneId.of(timeZoneId, ZoneId.SHORT_IDS)
  def getTimeZone(timeZoneId: String): TimeZone = {
    TimeZone.getTimeZone(getZoneId(timeZoneId))
  }

  // we should use the exact day as Int, for example, (year, month, day) -> day
  def millisToDays(millisUtc: Long): SQLDate = {
    millisToDays(millisUtc, defaultTimeZone())
  }

  def millisToDays(millisUtc: Long, timeZone: TimeZone): SQLDate = {
    // SPARK-6785: use Math.floorDiv so negative number of days (dates before 1970)
    // will correctly work as input for function toJavaDate(Int)
    val millisLocal = millisUtc + timeZone.getOffset(millisUtc)
    Math.floorDiv(millisLocal, MILLIS_PER_DAY).toInt
  }

  // reverse of millisToDays
  def daysToMillis(days: SQLDate): Long = {
    daysToMillis(days, defaultTimeZone())
  }

  def daysToMillis(days: SQLDate, timeZone: TimeZone): Long = {
    val millisLocal = days.toLong * MILLIS_PER_DAY
    millisLocal - getOffsetFromLocalMillis(millisLocal, timeZone)
  }

  // Converts Timestamp to string according to Hive TimestampWritable convention.
  def timestampToString(tf: TimestampFormatter, us: SQLTimestamp): String = {
    tf.format(us)
  }

  /**
   * Returns the number of days since epoch from java.sql.Date.
   */
  def fromJavaDate(date: Date): SQLDate = {
    millisToDays(date.getTime)
  }

  /**
   * Returns a java.sql.Date from number of days since epoch.
   */
  def toJavaDate(daysSinceEpoch: SQLDate): Date = {
    new Date(daysToMillis(daysSinceEpoch))
  }

  /**
   * Returns a java.sql.Timestamp from number of micros since epoch.
   */
  def toJavaTimestamp(us: SQLTimestamp): Timestamp = {
    Timestamp.from(microsToInstant(us))
  }

  /**
   * Returns the number of micros since epoch from java.sql.Timestamp.
   */
  def fromJavaTimestamp(t: Timestamp): SQLTimestamp = {
    instantToMicros(t.toInstant)
  }

  /**
   * Returns the number of microseconds since epoch from Julian day
   * and nanoseconds in a day
   */
  def fromJulianDay(day: Int, nanoseconds: Long): SQLTimestamp = {
    // use Long to avoid rounding errors
    val seconds = (day - JULIAN_DAY_OF_EPOCH).toLong * SECONDS_PER_DAY
    SECONDS.toMicros(seconds) + NANOSECONDS.toMicros(nanoseconds)
  }

  /**
   * Returns Julian day and nanoseconds in a day from the number of microseconds
   *
   * Note: support timestamp since 4717 BC (without negative nanoseconds, compatible with Hive).
   */
  def toJulianDay(us: SQLTimestamp): (Int, Long) = {
    val julian_us = us + JULIAN_DAY_OF_EPOCH * MICROS_PER_DAY
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
    MILLISECONDS.toMicros(millis)
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

  /**
   * Trim and parse a given UTF8 date string to the corresponding a corresponding [[Long]] value.
   * The return type is [[Option]] in order to distinguish between 0L and null. The following
   * formats are allowed:
   *
   * `yyyy`
   * `yyyy-[m]m`
   * `yyyy-[m]m-[d]d`
   * `yyyy-[m]m-[d]d `
   * `yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]`
   * `yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z`
   * `yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m`
   * `yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m`
   * `yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]`
   * `yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z`
   * `yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m`
   * `yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m`
   * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]`
   * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z`
   * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m`
   * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m`
   * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]`
   * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z`
   * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m`
   * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m`
   */
  def stringToTimestamp(s: UTF8String, timeZoneId: ZoneId): Option[SQLTimestamp] = {
    if (s == null) {
      return None
    }
    var tz: Option[Byte] = None
    val segments: Array[Int] = Array[Int](1, 1, 1, 0, 0, 0, 0, 0, 0)
    var i = 0
    var currentSegmentValue = 0
    val bytes = s.trim.getBytes
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
          if (b == 'Z') {
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
            tz = Some(43)
          } else if (b == '-' || b == '+') {
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
            tz = Some(b)
          } else if (b == '.' && i == 5) {
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
          } else {
            return None
          }
          if (i == 6  && b != '.') {
            i += 1
          }
        } else {
          if (b == ':' || b == ' ') {
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
      val zoneId = if (tz.isEmpty) {
        timeZoneId
      } else {
        val sign = if (tz.get.toChar == '-') -1 else 1
        ZoneOffset.ofHoursMinutes(sign * segments(7), sign * segments(8))
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

  def instantToMicros(instant: Instant): Long = {
    val us = Math.multiplyExact(instant.getEpochSecond, MICROS_PER_SECOND)
    val result = Math.addExact(us, NANOSECONDS.toMicros(instant.getNano))
    result
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
  def stringToDate(s: UTF8String): Option[SQLDate] = {
    if (s == null) {
      return None
    }
    val segments: Array[Int] = Array[Int](1, 1, 1)
    var i = 0
    var currentSegmentValue = 0
    val bytes = s.trim.getBytes
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

  /**
   * Returns the microseconds since year zero (-17999) from microseconds since epoch.
   */
  private def absoluteMicroSecond(microsec: SQLTimestamp): SQLTimestamp = {
    microsec + toYearZero * MICROS_PER_DAY
  }

  private def localTimestamp(microsec: SQLTimestamp, timeZone: TimeZone): SQLTimestamp = {
    val zoneOffsetUs = MILLISECONDS.toMicros(timeZone.getOffset(MICROSECONDS.toMillis(microsec)))
    absoluteMicroSecond(microsec) + zoneOffsetUs
  }

  /**
   * Returns the hour value of a given timestamp value. The timestamp is expressed in microseconds.
   */
  def getHours(microsec: SQLTimestamp, timeZone: TimeZone): Int = {
    (MICROSECONDS.toHours(localTimestamp(microsec, timeZone)) % 24).toInt
  }

  /**
   * Returns the minute value of a given timestamp value. The timestamp is expressed in
   * microseconds.
   */
  def getMinutes(microsec: SQLTimestamp, timeZone: TimeZone): Int = {
    (MICROSECONDS.toMinutes(localTimestamp(microsec, timeZone)) % 60).toInt
  }

  /**
   * Returns the second value of a given timestamp value. The timestamp is expressed in
   * microseconds.
   */
  def getSeconds(microsec: SQLTimestamp, timeZone: TimeZone): Int = {
    (MICROSECONDS.toSeconds(localTimestamp(microsec, timeZone)) % 60).toInt
  }

  /**
   * Returns seconds, including fractional parts, multiplied by 1000. The timestamp
   * is expressed in microseconds since the epoch.
   */
  def getMilliseconds(timestamp: SQLTimestamp, timeZone: TimeZone): Decimal = {
    val micros = Decimal(getMicroseconds(timestamp, timeZone))
    (micros / Decimal(MICROS_PER_MILLIS)).toPrecision(8, 3)
  }

  /**
   * Returns seconds, including fractional parts, multiplied by 1000000. The timestamp
   * is expressed in microseconds since the epoch.
   */
  def getMicroseconds(timestamp: SQLTimestamp, timeZone: TimeZone): Int = {
    Math.floorMod(localTimestamp(timestamp, timeZone), MICROS_PER_SECOND * 60).toInt
  }

  /**
   * Returns the 'day in year' value for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getDayInYear(date: SQLDate): Int = {
    LocalDate.ofEpochDay(date).getDayOfYear
  }

  private def extractFromYear(date: SQLDate, divider: Int): Int = {
    val localDate = daysToLocalDate(date)
    val yearOfEra = localDate.get(ChronoField.YEAR_OF_ERA)
    var result = yearOfEra / divider
    if ((yearOfEra % divider) != 0 || yearOfEra <= 1) result += 1
    if (localDate.get(ChronoField.ERA) == 0) result = -result
    result
  }

  /** Returns the millennium for the given date. The date is expressed in days since 1.1.1970. */
  def getMillennium(date: SQLDate): Int = extractFromYear(date, 1000)

  /** Returns the century for the given date. The date is expressed in days since 1.1.1970. */
  def getCentury(date: SQLDate): Int = extractFromYear(date, 100)

  /** Returns the decade for the given date. The date is expressed in days since 1.1.1970. */
  def getDecade(date: SQLDate): Int = Math.floorDiv(getYear(date), 10)

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
  def getIsoYear(date: SQLDate): Int = {
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
      microseconds: Long,
      zoneId: ZoneId): SQLTimestamp = {
    val resultTimestamp = microsToInstant(start)
      .atZone(zoneId)
      .plusMonths(months)
      .plus(microseconds, ChronoUnit.MICROS)
    instantToMicros(resultTimestamp.toInstant)
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
      timeZone: TimeZone): Double = {
    val millis1 = MICROSECONDS.toMillis(time1)
    val millis2 = MICROSECONDS.toMillis(time2)
    val date1 = millisToDays(millis1, timeZone)
    val date2 = millisToDays(millis2, timeZone)
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
    val secondsInDay1 = MILLISECONDS.toSeconds(millis1 - daysToMillis(date1, timeZone))
    val secondsInDay2 = MILLISECONDS.toSeconds(millis2 - daysToMillis(date2, timeZone))
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
  // The levels from TRUNC_TO_WEEK to TRUNC_TO_MILLENNIUM are used in truncations
  // of DATE and TIMESTAMP values.
  private[sql] val TRUNC_TO_WEEK = 6
  private[sql] val MIN_LEVEL_OF_DATE_TRUNC = TRUNC_TO_WEEK
  private[sql] val TRUNC_TO_MONTH = 7
  private[sql] val TRUNC_TO_QUARTER = 8
  private[sql] val TRUNC_TO_YEAR = 9
  private[sql] val TRUNC_TO_DECADE = 10
  private[sql] val TRUNC_TO_CENTURY = 11
  private[sql] val TRUNC_TO_MILLENNIUM = 12

  /**
   * Returns the trunc date from original date and trunc level.
   * Trunc level should be generated using `parseTruncLevel()`, should be between 0 and 6.
   */
  def truncDate(d: SQLDate, level: Int): SQLDate = {
    def truncToYearLevel(divider: Int, adjust: Int): SQLDate = {
      val oldYear = getYear(d)
      var newYear = Math.floorDiv(oldYear, divider)
      if (adjust > 0 && Math.floorMod(oldYear, divider) == 0) {
        newYear -= 1
      }
      newYear = newYear * divider + adjust
      localDateToDays(LocalDate.of(newYear, 1, 1))
    }
    level match {
      case TRUNC_TO_WEEK => getNextDateForDayOfWeek(d - 7, MONDAY)
      case TRUNC_TO_MONTH => d - DateTimeUtils.getDayOfMonth(d) + 1
      case TRUNC_TO_QUARTER =>
        localDateToDays(daysToLocalDate(d).`with`(IsoFields.DAY_OF_QUARTER, 1L))
      case TRUNC_TO_YEAR => d - DateTimeUtils.getDayInYear(d) + 1
      case TRUNC_TO_DECADE => truncToYearLevel(10, 0)
      case TRUNC_TO_CENTURY => truncToYearLevel(100, 1)
      case TRUNC_TO_MILLENNIUM => truncToYearLevel(1000, 1)
      case _ =>
        // caller make sure that this should never be reached
        sys.error(s"Invalid trunc level: $level")
    }
  }

  /**
   * Returns the trunc date time from original date time and trunc level.
   * Trunc level should be generated using `parseTruncLevel()`, should be between 0 and 12.
   */
  def truncTimestamp(t: SQLTimestamp, level: Int, timeZone: TimeZone): SQLTimestamp = {
    if (level == TRUNC_TO_MICROSECOND) return t
    var millis = MICROSECONDS.toMillis(t)
    val truncated = level match {
      case TRUNC_TO_MILLISECOND => millis
      case TRUNC_TO_SECOND =>
        millis - millis % MILLIS_PER_SECOND
      case TRUNC_TO_MINUTE =>
        millis - millis % MILLIS_PER_MINUTE
      case TRUNC_TO_HOUR =>
        val offset = timeZone.getOffset(millis)
        millis += offset
        millis - millis % MILLIS_PER_HOUR - offset
      case TRUNC_TO_DAY =>
        val offset = timeZone.getOffset(millis)
        millis += offset
        millis - millis % MILLIS_PER_DAY - offset
      case _ => // Try to truncate date levels
        val dDays = millisToDays(millis, timeZone)
        daysToMillis(truncDate(dDays, level), timeZone)
    }
    truncated * MICROS_PER_MILLIS
  }

  /**
   * Returns the truncate level, could be from TRUNC_TO_MICROSECOND to TRUNC_TO_MILLENNIUM,
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
        case "DECADE" => TRUNC_TO_DECADE
        case "CENTURY" => TRUNC_TO_CENTURY
        case "MILLENNIUM" => TRUNC_TO_MILLENNIUM
        case _ => TRUNC_INVALID
      }
    }
  }

  /**
   * Lookup the offset for given millis seconds since 1970-01-01 00:00:00 in given timezone.
   * TODO: Improve handling of normalization differences.
   * TODO: Replace with JSR-310 or similar system - see SPARK-16788
   */
  private[sql] def getOffsetFromLocalMillis(millisLocal: Long, tz: TimeZone): Long = {
    var guess = tz.getRawOffset
    // the actual offset should be calculated based on milliseconds in UTC
    val offset = tz.getOffset(millisLocal - guess)
    if (offset != guess) {
      guess = tz.getOffset(millisLocal - offset)
      if (guess != offset) {
        // fallback to do the reverse lookup using java.time.LocalDateTime
        // this should only happen near the start or end of DST
        val localDate = LocalDate.ofEpochDay(MILLISECONDS.toDays(millisLocal))
        val localTime = LocalTime.ofNanoOfDay(MILLISECONDS.toNanos(
          Math.floorMod(millisLocal, MILLIS_PER_DAY)))
        val localDateTime = LocalDateTime.of(localDate, localTime)
        val millisEpoch = localDateTime.atZone(tz.toZoneId).toInstant.toEpochMilli

        guess = (millisLocal - millisEpoch).toInt
      }
    }
    guess
  }

  /**
   * Convert the timestamp `ts` from one timezone to another.
   *
   * TODO: Because of DST, the conversion between UTC and human time is not exactly one-to-one
   * mapping, the conversion here may return wrong result, we should make the timestamp
   * timezone-aware.
   */
  def convertTz(ts: SQLTimestamp, fromZone: TimeZone, toZone: TimeZone): SQLTimestamp = {
    // We always use local timezone to parse or format a timestamp
    val localZone = defaultTimeZone()
    val utcTs = if (fromZone.getID == localZone.getID) {
      ts
    } else {
      // get the human time using local time zone, that actually is in fromZone.
      val localZoneOffsetMs = localZone.getOffset(MICROSECONDS.toMillis(ts))
      val localTsUs = ts + MILLISECONDS.toMicros(localZoneOffsetMs)  // in fromZone
      val offsetFromLocalMs = getOffsetFromLocalMillis(MICROSECONDS.toMillis(localTsUs), fromZone)
      localTsUs - MILLISECONDS.toMicros(offsetFromLocalMs)
    }
    if (toZone.getID == localZone.getID) {
      utcTs
    } else {
      val toZoneOffsetMs = toZone.getOffset(MICROSECONDS.toMillis(utcTs))
      val localTsUs = utcTs + MILLISECONDS.toMicros(toZoneOffsetMs)  // in toZone
      // treat it as local timezone, convert to UTC (we could get the expected human time back)
      val offsetFromLocalMs = getOffsetFromLocalMillis(MICROSECONDS.toMillis(localTsUs), localZone)
      localTsUs - MILLISECONDS.toMicros(offsetFromLocalMs)
    }
  }

  /**
   * Returns a timestamp of given timezone from utc timestamp, with the same string
   * representation in their timezone.
   */
  def fromUTCTime(time: SQLTimestamp, timeZone: String): SQLTimestamp = {
    convertTz(time, TimeZoneGMT, getTimeZone(timeZone))
  }

  /**
   * Returns a utc timestamp from a given timestamp from a given timezone, with the same
   * string representation in their timezone.
   */
  def toUTCTime(time: SQLTimestamp, timeZone: String): SQLTimestamp = {
    convertTz(time, getTimeZone(timeZone), TimeZoneGMT)
  }

  /**
   * Returns the number of seconds with fractional part in microsecond precision
   * since 1970-01-01 00:00:00 local time.
   */
  def getEpoch(timestamp: SQLTimestamp, zoneId: ZoneId): Decimal = {
    val offset = zoneId.getRules.getOffset(microsToInstant(timestamp)).getTotalSeconds
    val sinceEpoch = BigDecimal(timestamp) / MICROS_PER_SECOND + offset
    new Decimal().set(sinceEpoch, 20, 6)
  }
}
