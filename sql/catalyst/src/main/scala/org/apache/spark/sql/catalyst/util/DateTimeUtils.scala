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
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, Month, ZonedDateTime}
import java.time.Year.isLeap
import java.time.temporal.IsoFields
import java.util.{Locale, TimeZone}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.function.{Function => JFunction}

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
  final val SECONDS_PER_DAY = 60 * 60 * 24L
  final val MICROS_PER_MILLIS = 1000L
  final val MICROS_PER_SECOND = MICROS_PER_MILLIS * MILLIS_PER_SECOND
  final val MILLIS_PER_SECOND = 1000L
  final val NANOS_PER_SECOND = MICROS_PER_SECOND * 1000L
  final val MICROS_PER_DAY = MICROS_PER_SECOND * SECONDS_PER_DAY
  final val NANOS_PER_MICROS = 1000L
  final val MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L

  // number of days between 1.1.1970 and 1.1.2001
  final val to2001 = -11323

  // this is year -17999, calculation: 50 * daysIn400Year
  final val YearZero = -17999
  final val toYearZero = to2001 + 7304850
  final val TimeZoneGMT = TimeZone.getTimeZone("GMT")
  final val TimeZoneUTC = TimeZone.getTimeZone("UTC")
  final val MonthOf31Days = Set(1, 3, 5, 7, 8, 10, 12)

  val TIMEZONE_OPTION = "timeZone"

  def defaultTimeZone(): TimeZone = TimeZone.getDefault()

  private val computedTimeZones = new ConcurrentHashMap[String, TimeZone]
  private val computeTimeZone = new JFunction[String, TimeZone] {
    override def apply(timeZoneId: String): TimeZone = TimeZone.getTimeZone(timeZoneId)
  }

  def getTimeZone(timeZoneId: String): TimeZone = {
    computedTimeZones.computeIfAbsent(timeZoneId, computeTimeZone)
  }

  // we should use the exact day as Int, for example, (year, month, day) -> day
  def millisToDays(millisUtc: Long): SQLDate = {
    millisToDays(millisUtc, defaultTimeZone())
  }

  def millisToDays(millisUtc: Long, timeZone: TimeZone): SQLDate = {
    // SPARK-6785: use Math.floor so negative number of days (dates before 1970)
    // will correctly work as input for function toJavaDate(Int)
    val millisLocal = millisUtc + timeZone.getOffset(millisUtc)
    Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
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
    val ts = toJavaTimestamp(us)
    val timestampString = ts.toString
    val formatted = tf.format(us)

    if (timestampString.length > 19 && timestampString.substring(19) != ".0") {
      formatted + timestampString.substring(19)
    } else {
      formatted
    }
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
    // setNanos() will overwrite the millisecond part, so the milliseconds should be
    // cut off at seconds
    var seconds = us / MICROS_PER_SECOND
    var micros = us % MICROS_PER_SECOND
    // setNanos() can not accept negative value
    if (micros < 0) {
      micros += MICROS_PER_SECOND
      seconds -= 1
    }
    val t = new Timestamp(seconds * 1000)
    t.setNanos(micros.toInt * 1000)
    t
  }

  /**
   * Returns the number of micros since epoch from java.sql.Timestamp.
   */
  def fromJavaTimestamp(t: Timestamp): SQLTimestamp = {
    if (t != null) {
      t.getTime() * 1000L + (t.getNanos().toLong / 1000) % 1000L
    } else {
      0L
    }
  }

  /**
   * Returns the number of microseconds since epoch from Julian day
   * and nanoseconds in a day
   */
  def fromJulianDay(day: Int, nanoseconds: Long): SQLTimestamp = {
    // use Long to avoid rounding errors
    val seconds = (day - JULIAN_DAY_OF_EPOCH).toLong * SECONDS_PER_DAY
    seconds * MICROS_PER_SECOND + nanoseconds / 1000L
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
    (day.toInt, micros * 1000L)
  }

  /*
   * Converts the timestamp to milliseconds since epoch. In spark timestamp values have microseconds
   * precision, so this conversion is lossy.
   */
  def toMillis(us: SQLTimestamp): Long = {
    // When the timestamp is negative i.e before 1970, we need to adjust the millseconds portion.
    // Example - 1965-01-01 10:11:12.123456 is represented as (-157700927876544) in micro precision.
    // In millis precision the above needs to be represented as (-157700927877).
    Math.floor(us.toDouble / MILLIS_PER_SECOND).toLong
  }

  /*
   * Converts millseconds since epoch to SQLTimestamp.
   */
  def fromMillis(millis: Long): SQLTimestamp = {
    millis * 1000L
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
  def stringToTimestamp(s: UTF8String, timeZone: TimeZone): Option[SQLTimestamp] = {
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

    if (!justTime && isInvalidDate(segments(0), segments(1), segments(2))) {
      return None
    }

    if (segments(3) < 0 || segments(3) > 23 || segments(4) < 0 || segments(4) > 59 ||
        segments(5) < 0 || segments(5) > 59 || segments(6) < 0 || segments(6) > 999999 ||
        segments(7) < 0 || segments(7) > 23 || segments(8) < 0 || segments(8) > 59) {
      return None
    }

    val zoneId = if (tz.isEmpty) {
      timeZone.toZoneId
    } else {
      getTimeZone(f"GMT${tz.get.toChar}${segments(7)}%02d:${segments(8)}%02d").toZoneId
    }
    val nanoseconds = TimeUnit.MICROSECONDS.toNanos(segments(6))
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
  }

  def instantToMicros(instant: Instant): Long = {
    val sec = Math.multiplyExact(instant.getEpochSecond, MICROS_PER_SECOND)
    val result = Math.addExact(sec, instant.getNano / NANOS_PER_MICROS)
    result
  }

  def instantToDays(instant: Instant): Int = {
    val seconds = instant.getEpochSecond
    val days = Math.floorDiv(seconds, SECONDS_PER_DAY)
    days.toInt
  }

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
    segments(i) = currentSegmentValue
    if (isInvalidDate(segments(0), segments(1), segments(2))) {
      return None
    }

    val localDate = LocalDate.of(segments(0), segments(1), segments(2))
    val instant = localDate.atStartOfDay(TimeZoneUTC.toZoneId).toInstant
    Some(instantToDays(instant))
  }

  /**
   * Return true if the date is invalid.
   */
  private def isInvalidDate(year: Int, month: Int, day: Int): Boolean = {
    if (year < 0 || year > 9999 || month < 1 || month > 12 || day < 1 || day > 31) {
      return true
    }
    if (month == 2) {
      if (isLeap(year) && day > 29) {
        return true
      } else if (!isLeap(year) && day > 28) {
        return true
      }
    } else if (!MonthOf31Days.contains(month) && day > 30) {
      return true
    }
    false
  }

  /**
   * Returns the microseconds since year zero (-17999) from microseconds since epoch.
   */
  private def absoluteMicroSecond(microsec: SQLTimestamp): SQLTimestamp = {
    microsec + toYearZero * MICROS_PER_DAY
  }

  private def localTimestamp(microsec: SQLTimestamp, timeZone: TimeZone): SQLTimestamp = {
    absoluteMicroSecond(microsec) + timeZone.getOffset(microsec / 1000) * 1000L
  }

  /**
   * Returns the hour value of a given timestamp value. The timestamp is expressed in microseconds.
   */
  def getHours(microsec: SQLTimestamp, timeZone: TimeZone): Int = {
    ((localTimestamp(microsec, timeZone) / MICROS_PER_SECOND / 3600) % 24).toInt
  }

  /**
   * Returns the minute value of a given timestamp value. The timestamp is expressed in
   * microseconds.
   */
  def getMinutes(microsec: SQLTimestamp, timeZone: TimeZone): Int = {
    ((localTimestamp(microsec, timeZone) / MICROS_PER_SECOND / 60) % 60).toInt
  }

  /**
   * Returns the second value of a given timestamp value. The timestamp is expressed in
   * microseconds.
   */
  def getSeconds(microsec: SQLTimestamp, timeZone: TimeZone): Int = {
    ((localTimestamp(microsec, timeZone) / MICROS_PER_SECOND) % 60).toInt
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
   * The number of days for each month (not leap year)
   */
  private val monthDays = Array(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

  /**
   * Returns the date value for the first day of the given month.
   * The month is expressed in months since year zero (17999 BC), starting from 0.
   */
  private def firstDayOfMonth(absoluteMonth: Int): SQLDate = {
    val absoluteYear = absoluteMonth / 12
    var monthInYear = absoluteMonth - absoluteYear * 12
    var date = getDateFromYear(absoluteYear)
    if (monthInYear >= 2 && isLeap(absoluteYear + YearZero)) {
      date += 1
    }
    while (monthInYear > 0) {
      date += monthDays(monthInYear - 1)
      monthInYear -= 1
    }
    date
  }

  /**
   * Returns the date value for January 1 of the given year.
   * The year is expressed in years since year zero (17999 BC), starting from 0.
   */
  private def getDateFromYear(absoluteYear: Int): SQLDate = {
    val absoluteDays = (absoluteYear * 365 + absoluteYear / 400 - absoluteYear / 100
      + absoluteYear / 4)
    absoluteDays - toYearZero
  }

  /**
   * Add date and year-month interval.
   * Returns a date value, expressed in days since 1.1.1970.
   */
  def dateAddMonths(days: SQLDate, months: Int): SQLDate = {
    val (year, monthInYear, dayOfMonth, daysToMonthEnd) = splitDate(days)
    val absoluteMonth = (year - YearZero) * 12 + monthInYear - 1 + months
    val nonNegativeMonth = if (absoluteMonth >= 0) absoluteMonth else 0
    val currentMonthInYear = nonNegativeMonth % 12
    val currentYear = nonNegativeMonth / 12

    val leapDay = if (currentMonthInYear == 1 && isLeap(currentYear + YearZero)) 1 else 0
    val lastDayOfMonth = monthDays(currentMonthInYear) + leapDay

    val currentDayInMonth = if (daysToMonthEnd == 0 || dayOfMonth >= lastDayOfMonth) {
      // last day of the month
      lastDayOfMonth
    } else {
      dayOfMonth
    }
    firstDayOfMonth(nonNegativeMonth) + currentDayInMonth - 1
  }

  /**
   * Add timestamp and full interval.
   * Returns a timestamp value, expressed in microseconds since 1.1.1970 00:00:00.
   */
  def timestampAddInterval(
      start: SQLTimestamp,
      months: Int,
      microseconds: Long,
      timeZone: TimeZone): SQLTimestamp = {
    val days = millisToDays(start / 1000L, timeZone)
    val newDays = dateAddMonths(days, months)
    start +
      daysToMillis(newDays, timeZone) * 1000L - daysToMillis(days, timeZone) * 1000L +
      microseconds
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
    val millis1 = time1 / 1000L
    val millis2 = time2 / 1000L
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
    val secondsInDay1 = (millis1 - daysToMillis(date1, timeZone)) / 1000L
    val secondsInDay2 = (millis2 - daysToMillis(date2, timeZone)) / 1000L
    val secondsDiff = (dayInMonth1 - dayInMonth2) * SECONDS_PER_DAY + secondsInDay1 - secondsInDay2
    // 2678400D is the number of seconds in 31 days
    // every month is considered to be 31 days long in this function
    val diff = monthDiff + secondsDiff / 2678400D
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

  // Visible for testing.
  private[sql] val TRUNC_TO_YEAR = 1
  private[sql] val TRUNC_TO_MONTH = 2
  private[sql] val TRUNC_TO_QUARTER = 3
  private[sql] val TRUNC_TO_WEEK = 4
  private[sql] val TRUNC_TO_DAY = 5
  private[sql] val TRUNC_TO_HOUR = 6
  private[sql] val TRUNC_TO_MINUTE = 7
  private[sql] val TRUNC_TO_SECOND = 8
  private[sql] val TRUNC_INVALID = -1

  /**
   * Returns the trunc date from original date and trunc level.
   * Trunc level should be generated using `parseTruncLevel()`, should only be 1 or 2.
   */
  def truncDate(d: SQLDate, level: Int): SQLDate = {
    if (level == TRUNC_TO_YEAR) {
      d - DateTimeUtils.getDayInYear(d) + 1
    } else if (level == TRUNC_TO_MONTH) {
      d - DateTimeUtils.getDayOfMonth(d) + 1
    } else {
      // caller make sure that this should never be reached
      sys.error(s"Invalid trunc level: $level")
    }
  }

  /**
   * Returns the trunc date time from original date time and trunc level.
   * Trunc level should be generated using `parseTruncLevel()`, should be between 1 and 8
   */
  def truncTimestamp(t: SQLTimestamp, level: Int, timeZone: TimeZone): SQLTimestamp = {
    var millis = t / MICROS_PER_MILLIS
    val truncated = level match {
      case TRUNC_TO_YEAR =>
        val dDays = millisToDays(millis, timeZone)
        daysToMillis(truncDate(dDays, level), timeZone)
      case TRUNC_TO_MONTH =>
        val dDays = millisToDays(millis, timeZone)
        daysToMillis(truncDate(dDays, level), timeZone)
      case TRUNC_TO_DAY =>
        val offset = timeZone.getOffset(millis)
        millis += offset
        millis - millis % (MILLIS_PER_SECOND * SECONDS_PER_DAY) - offset
      case TRUNC_TO_HOUR =>
        val offset = timeZone.getOffset(millis)
        millis += offset
        millis - millis % (60 * 60 * MILLIS_PER_SECOND) - offset
      case TRUNC_TO_MINUTE =>
        millis - millis % (60 * MILLIS_PER_SECOND)
      case TRUNC_TO_SECOND =>
        millis - millis % MILLIS_PER_SECOND
      case TRUNC_TO_WEEK =>
        val dDays = millisToDays(millis, timeZone)
        val prevMonday = getNextDateForDayOfWeek(dDays - 7, MONDAY)
        daysToMillis(prevMonday, timeZone)
      case TRUNC_TO_QUARTER =>
        val dDays = millisToDays(millis, timeZone)
        val month = getQuarter(dDays) match {
          case 1 => Month.JANUARY
          case 2 => Month.APRIL
          case 3 => Month.JULY
          case 4 => Month.OCTOBER
        }
        millis = daysToMillis(truncDate(dDays, TRUNC_TO_MONTH), timeZone)
        val instant = Instant.ofEpochMilli(millis)
        val localDateTime = LocalDateTime.ofInstant(instant, timeZone.toZoneId)
        val truncated = localDateTime.withMonth(month.getValue)
        truncated.atZone(timeZone.toZoneId).toInstant.toEpochMilli
      case _ =>
        // caller make sure that this should never be reached
        sys.error(s"Invalid trunc level: $level")
    }
    truncated * MICROS_PER_MILLIS
  }

  /**
   * Returns the truncate level, could be TRUNC_YEAR, TRUNC_MONTH, TRUNC_TO_DAY, TRUNC_TO_HOUR,
   * TRUNC_TO_MINUTE, TRUNC_TO_SECOND, TRUNC_TO_WEEK, TRUNC_TO_QUARTER or TRUNC_INVALID,
   * TRUNC_INVALID means unsupported truncate level.
   */
  def parseTruncLevel(format: UTF8String): Int = {
    if (format == null) {
      TRUNC_INVALID
    } else {
      format.toString.toUpperCase(Locale.ROOT) match {
        case "YEAR" | "YYYY" | "YY" => TRUNC_TO_YEAR
        case "MON" | "MONTH" | "MM" => TRUNC_TO_MONTH
        case "DAY" | "DD" => TRUNC_TO_DAY
        case "HOUR" => TRUNC_TO_HOUR
        case "MINUTE" => TRUNC_TO_MINUTE
        case "SECOND" => TRUNC_TO_SECOND
        case "WEEK" => TRUNC_TO_WEEK
        case "QUARTER" => TRUNC_TO_QUARTER
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
        val localDate = LocalDate.ofEpochDay(TimeUnit.MILLISECONDS.toDays(millisLocal))
        val localTime = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(
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
      val localTs = ts + localZone.getOffset(ts / 1000L) * 1000L  // in fromZone
      localTs - getOffsetFromLocalMillis(localTs / 1000L, fromZone) * 1000L
    }
    if (toZone.getID == localZone.getID) {
      utcTs
    } else {
      val localTs = utcTs + toZone.getOffset(utcTs / 1000L) * 1000L  // in toZone
      // treat it as local timezone, convert to UTC (we could get the expected human time back)
      localTs - getOffsetFromLocalMillis(localTs / 1000L, localZone) * 1000L
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
}
