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
import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, TimeZone}
import javax.xml.bind.DatatypeConverter

import scala.annotation.tailrec

import org.apache.spark.unsafe.types.UTF8String

/**
 * Helper functions for converting between internal and external date and time representations.
 * Dates are exposed externally as java.sql.Date and are represented internally as the number of
 * dates since the Unix epoch (1970-01-01). Timestamps are exposed externally as java.sql.Timestamp
 * and are stored internally as longs, which are capable of storing timestamps with 100 nanosecond
 * precision.
 */
object DateTimeUtils {

  // we use Int and Long internally to represent [[DateType]] and [[TimestampType]]
  type SQLDate = Int
  type SQLTimestamp = Long

  // see http://stackoverflow.com/questions/466321/convert-unix-timestamp-to-julian
  // it's 2440587.5, rounding up to compatible with Hive
  final val JULIAN_DAY_OF_EPOCH = 2440588

  final val SECONDS_PER_MINUTE = 60L
  final val SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60L
  final val SECONDS_PER_DAY = SECONDS_PER_HOUR * 24L

  final val MILLIS_PER_SECOND = 1000L
  final val MILLIS_PER_MINUTE = SECONDS_PER_MINUTE * MILLIS_PER_SECOND
  final val MILLIS_PER_HOUR = SECONDS_PER_HOUR * MILLIS_PER_SECOND
  final val MILLIS_PER_DAY = SECONDS_PER_DAY * MILLIS_PER_SECOND

  final val MICROS_PER_SECOND = MILLIS_PER_SECOND * 1000L
  final val MICROS_PER_DAY = SECONDS_PER_DAY * MICROS_PER_SECOND

  final val NANOS_PER_SECOND = MICROS_PER_SECOND * 1000L

  // number of days in 400 years
  final val daysIn400Years: Int = 146097
  // number of days between 1.1.1970 and 1.1.2001
  final val to2001 = -11323

  // this is year -17999, calculation: 50 * daysIn400Year
  final val YearZero = -17999
  final val toYearZero = to2001 + 7304850
  final val TimeZoneGMT = TimeZone.getTimeZone("GMT")
  final val MonthOf31Days = Set(1, 3, 5, 7, 8, 10, 12)

  @transient lazy val defaultTimeZone = TimeZone.getDefault

  // Reuse the Calendar object in each thread as it is expensive to create in each method call.
  private val threadLocalGmtCalendar = new ThreadLocal[Calendar] {
    override protected def initialValue: Calendar = {
      Calendar.getInstance(TimeZoneGMT)
    }
  }

  // Java TimeZone has no mention of thread safety. Use thread local instance to be safe.
  private val threadLocalLocalTimeZone = new ThreadLocal[TimeZone] {
    override protected def initialValue: TimeZone = {
      Calendar.getInstance.getTimeZone
    }
  }

  // `SimpleDateFormat` is not thread-safe.
  val threadLocalTimestampFormat = new ThreadLocal[DateFormat] {
    override def initialValue(): SimpleDateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }
  }

  // `SimpleDateFormat` is not thread-safe.
  private val threadLocalDateFormat = new ThreadLocal[DateFormat] {
    override def initialValue(): SimpleDateFormat = {
      new SimpleDateFormat("yyyy-MM-dd")
    }
  }

  // A wrapper function to apply date related functions with timestamps. This truncates
  // time parts.
  private def dateFuncToTimestampFunc(
      timestamp: SQLTimestamp,
      dateFunc: (SQLDate) => SQLDate): SQLTimestamp = {
    daysToMillis(dateFunc(millisToDays(timestamp / 1000L))) * 1000L
  }

  // we should use the exact day as Int, for example, (year, month, day) -> day
  def millisToDays(millisUtc: Long): SQLDate = {
    // SPARK-6785: use Math.floor so negative number of days (dates before 1970)
    // will correctly work as input for function toJavaDate(Int)
    val millisLocal = millisUtc + threadLocalLocalTimeZone.get().getOffset(millisUtc)
    Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
  }

  // reverse of millisToDays
  def daysToMillis(days: SQLDate): Long = {
    val millisLocal = days.toLong * MILLIS_PER_DAY
    millisLocal - getOffsetFromLocalMillis(millisLocal, threadLocalLocalTimeZone.get())
  }

  def dateToString(days: SQLDate): String =
    threadLocalDateFormat.get.format(toJavaDate(days))

  // Converts Timestamp to string according to Hive TimestampWritable convention.
  def timestampToString(us: SQLTimestamp): String = {
    val ts = toJavaTimestamp(us)
    val timestampString = ts.toString
    val formatted = threadLocalTimestampFormat.get.format(ts)

    if (timestampString.length > 19 && timestampString.substring(19) != ".0") {
      formatted + timestampString.substring(19)
    } else {
      formatted
    }
  }

  @tailrec
  def stringToTime(s: String): java.util.Date = {
    val indexOfGMT = s.indexOf("GMT")
    if (indexOfGMT != -1) {
      // ISO8601 with a weird time zone specifier (2000-01-01T00:00GMT+01:00)
      val s0 = s.substring(0, indexOfGMT)
      val s1 = s.substring(indexOfGMT + 3)
      // Mapped to 2000-01-01T00:00+01:00
      stringToTime(s0 + s1)
    } else if (!s.contains('T')) {
      // JDBC escape string
      if (s.contains(' ')) {
        Timestamp.valueOf(s)
      } else {
        Date.valueOf(s)
      }
    } else {
      DatatypeConverter.parseDateTime(s).getTime()
    }
  }

  /**
   * Returns the number of days since epoch from from java.sql.Date.
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

  /**
   * Parses a given UTF8 date string to the corresponding a corresponding [[Long]] value.
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
  def stringToTimestamp(s: UTF8String): Option[SQLTimestamp] = {
    if (s == null) {
      return None
    }
    var timeZone: Option[Byte] = None
    val segments: Array[Int] = Array[Int](1, 1, 1, 0, 0, 0, 0, 0, 0)
    var i = 0
    var currentSegmentValue = 0
    val bytes = s.getBytes
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
            timeZone = Some(43)
          } else if (b == '-' || b == '+') {
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            i += 1
            timeZone = Some(b)
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

    if (!justTime && isInvalidDate(segments(0), segments(1), segments(2))) {
      return None
    }

    // Instead of return None, we truncate the fractional seconds to prevent inserting NULL
    if (segments(6) > 999999) {
      segments(6) = segments(6).toString.take(6).toInt
    }

    if (segments(3) < 0 || segments(3) > 23 || segments(4) < 0 || segments(4) > 59 ||
        segments(5) < 0 || segments(5) > 59 || segments(6) < 0 || segments(6) > 999999 ||
        segments(7) < 0 || segments(7) > 23 || segments(8) < 0 || segments(8) > 59) {
      return None
    }

    val c = if (timeZone.isEmpty) {
      Calendar.getInstance()
    } else {
      Calendar.getInstance(
        TimeZone.getTimeZone(f"GMT${timeZone.get.toChar}${segments(7)}%02d:${segments(8)}%02d"))
    }
    c.set(Calendar.MILLISECOND, 0)

    if (justTime) {
      c.set(Calendar.HOUR_OF_DAY, segments(3))
      c.set(Calendar.MINUTE, segments(4))
      c.set(Calendar.SECOND, segments(5))
    } else {
      c.set(segments(0), segments(1) - 1, segments(2), segments(3), segments(4), segments(5))
    }

    Some(c.getTimeInMillis * 1000 + segments(6))
  }

  /**
   * Parses a given UTF8 date string to the corresponding a corresponding [[Int]] value.
   * The return type is [[Option]] in order to distinguish between 0 and null. The following
   * formats are allowed:
   *
   * `yyyy`,
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
    val bytes = s.getBytes
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

    val c = threadLocalGmtCalendar.get()
    c.clear()
    c.set(segments(0), segments(1) - 1, segments(2), 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    Some((c.getTimeInMillis / MILLIS_PER_DAY).toInt)
  }

  /**
   * Return true if the date is invalid.
   */
  private def isInvalidDate(year: Int, month: Int, day: Int): Boolean = {
    if (year < 0 || year > 9999 || month < 1 || month > 12 || day < 1 || day > 31) {
      return true
    }
    if (month == 2) {
      if (isLeapYear(year) && day > 29) {
        return true
      } else if (!isLeapYear(year) && day > 28) {
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

  private def localTimestamp(microsec: SQLTimestamp): SQLTimestamp = {
    absoluteMicroSecond(microsec) + defaultTimeZone.getOffset(microsec / 1000) * 1000L
  }

  /**
   * Returns the hour value of a given timestamp value. The timestamp is expressed in microseconds.
   */
  def getHours(microsec: SQLTimestamp): Int = {
    ((localTimestamp(microsec) / MICROS_PER_SECOND / 3600) % 24).toInt
  }

  /**
   * Returns the minute value of a given timestamp value. The timestamp is expressed in
   * microseconds.
   */
  def getMinutes(microsec: SQLTimestamp): Int = {
    ((localTimestamp(microsec) / MICROS_PER_SECOND / 60) % 60).toInt
  }

  /**
   * Returns the second value of a given timestamp value. The timestamp is expressed in
   * microseconds.
   */
  def getSeconds(microsec: SQLTimestamp): Int = {
    ((localTimestamp(microsec) / MICROS_PER_SECOND) % 60).toInt
  }

  private[this] def isLeapYear(year: Int): Boolean = {
    (year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0)
  }

  /**
   * Return the number of days since the start of 400 year period.
   * The second year of a 400 year period (year 1) starts on day 365.
   */
  private[this] def yearBoundary(year: Int): Int = {
    year * 365 + ((year / 4 ) - (year / 100) + (year / 400))
  }

  /**
   * Calculates the number of years for the given number of days. This depends
   * on a 400 year period.
   * @param days days since the beginning of the 400 year period
   * @return (number of year, days in year)
   */
  private[this] def numYears(days: Int): (Int, Int) = {
    val year = days / 365
    val boundary = yearBoundary(year)
    if (days > boundary) (year, days - boundary) else (year - 1, days - yearBoundary(year - 1))
  }

  /**
   * Calculates the year and and the number of the day in the year for the given
   * number of days. The given days is the number of days since 1.1.1970.
   *
   * The calculation uses the fact that the period 1.1.2001 until 31.12.2400 is
   * equals to the period 1.1.1601 until 31.12.2000.
   */
  private[this] def getYearAndDayInYear(daysSince1970: SQLDate): (Int, Int) = {
    // add the difference (in days) between 1.1.1970 and the artificial year 0 (-17999)
    val daysNormalized = daysSince1970 + toYearZero
    val numOfQuarterCenturies = daysNormalized / daysIn400Years
    val daysInThis400 = daysNormalized % daysIn400Years + 1
    val (years, dayInYear) = numYears(daysInThis400)
    val year: Int = (2001 - 20000) + 400 * numOfQuarterCenturies + years
    (year, dayInYear)
  }

  /**
   * Returns the 'day in year' value for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getDayInYear(date: SQLDate): Int = {
    getYearAndDayInYear(date)._2
  }

  /**
   * Returns the year value for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getYear(date: SQLDate): Int = {
    getYearAndDayInYear(date)._1
  }

  /**
   * Returns the quarter for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getQuarter(date: SQLDate): Int = {
    var (year, dayInYear) = getYearAndDayInYear(date)
    if (isLeapYear(year)) {
      dayInYear = dayInYear - 1
    }
    if (dayInYear <= 90) {
      1
    } else if (dayInYear <= 181) {
      2
    } else if (dayInYear <= 273) {
      3
    } else {
      4
    }
  }

  /**
   * Split date (expressed in days since 1.1.1970) into four fields:
   * year, month (Jan is Month 1), dayInMonth, daysToMonthEnd (0 if it's last day of month).
   */
  def splitDate(date: SQLDate): (Int, Int, Int, Int) = {
    var (year, dayInYear) = getYearAndDayInYear(date)
    val isLeap = isLeapYear(year)
    if (isLeap && dayInYear == 60) {
      (year, 2, 29, 0)
    } else {
      if (isLeap && dayInYear > 60) dayInYear -= 1

      if (dayInYear <= 181) {
        if (dayInYear <= 31) {
          (year, 1, dayInYear, 31 - dayInYear)
        } else if (dayInYear <= 59) {
          (year, 2, dayInYear - 31, if (isLeap) 60 - dayInYear else 59 - dayInYear)
        } else if (dayInYear <= 90) {
          (year, 3, dayInYear - 59, 90 - dayInYear)
        } else if (dayInYear <= 120) {
          (year, 4, dayInYear - 90, 120 - dayInYear)
        } else if (dayInYear <= 151) {
          (year, 5, dayInYear - 120, 151 - dayInYear)
        } else {
          (year, 6, dayInYear - 151, 181 - dayInYear)
        }
      } else {
        if (dayInYear <= 212) {
          (year, 7, dayInYear - 181, 212 - dayInYear)
        } else if (dayInYear <= 243) {
          (year, 8, dayInYear - 212, 243 - dayInYear)
        } else if (dayInYear <= 273) {
          (year, 9, dayInYear - 243, 273 - dayInYear)
        } else if (dayInYear <= 304) {
          (year, 10, dayInYear - 273, 304 - dayInYear)
        } else if (dayInYear <= 334) {
          (year, 11, dayInYear - 304, 334 - dayInYear)
        } else {
          (year, 12, dayInYear - 334, 365 - dayInYear)
        }
      }
    }
  }

  /**
   * Returns the month value for the given date. The date is expressed in days
   * since 1.1.1970. January is month 1.
   */
  def getMonth(date: SQLDate): Int = {
    var (year, dayInYear) = getYearAndDayInYear(date)
    if (isLeapYear(year)) {
      if (dayInYear == 60) {
        return 2
      } else if (dayInYear > 60) {
        dayInYear = dayInYear - 1
      }
    }

    if (dayInYear <= 31) {
      1
    } else if (dayInYear <= 59) {
      2
    } else if (dayInYear <= 90) {
      3
    } else if (dayInYear <= 120) {
      4
    } else if (dayInYear <= 151) {
      5
    } else if (dayInYear <= 181) {
      6
    } else if (dayInYear <= 212) {
      7
    } else if (dayInYear <= 243) {
      8
    } else if (dayInYear <= 273) {
      9
    } else if (dayInYear <= 304) {
      10
    } else if (dayInYear <= 334) {
      11
    } else {
      12
    }
  }

  /**
   * Returns the 'day of month' value for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getDayOfMonth(date: SQLDate): Int = {
    var (year, dayInYear) = getYearAndDayInYear(date)
    if (isLeapYear(year)) {
      if (dayInYear == 60) {
        return 29
      } else if (dayInYear > 60) {
        dayInYear = dayInYear - 1
      }
    }

    if (dayInYear <= 31) {
      dayInYear
    } else if (dayInYear <= 59) {
      dayInYear - 31
    } else if (dayInYear <= 90) {
      dayInYear - 59
    } else if (dayInYear <= 120) {
      dayInYear - 90
    } else if (dayInYear <= 151) {
      dayInYear - 120
    } else if (dayInYear <= 181) {
      dayInYear - 151
    } else if (dayInYear <= 212) {
      dayInYear - 181
    } else if (dayInYear <= 243) {
      dayInYear - 212
    } else if (dayInYear <= 273) {
      dayInYear - 243
    } else if (dayInYear <= 304) {
      dayInYear - 273
    } else if (dayInYear <= 334) {
      dayInYear - 304
    } else {
      dayInYear - 334
    }
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
    if (monthInYear >= 2 && isLeapYear(absoluteYear + YearZero)) {
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

    val leapDay = if (currentMonthInYear == 1 && isLeapYear(currentYear + YearZero)) 1 else 0
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
  def timestampAddInterval(start: SQLTimestamp, months: Int, microseconds: Long): SQLTimestamp = {
    val days = millisToDays(start / 1000L)
    val newDays = dateAddMonths(days, months)
    daysToMillis(newDays) * 1000L + start - daysToMillis(days) * 1000L + microseconds
  }

  /**
   * Add timestamp and days interval.
   * Returns a timestamp value, expressed in microseconds since 1.1.1970 00:00:00.
   */
  def timestampAddDays(start: SQLTimestamp, days: Int): SQLTimestamp = {
    start + days * MICROS_PER_DAY
  }

  /**
   * Returns number of months between time1 and time2. time1 and time2 are expressed in
   * microseconds since 1.1.1970.
   *
   * If time1 and time2 having the same day of month, or both are the last day of month,
   * it returns an integer (time under a day will be ignored).
   *
   * Otherwise, the difference is calculated based on 31 days per month, and rounding to
   * 8 digits.
   */
  def monthsBetween(time1: SQLTimestamp, time2: SQLTimestamp): Double = {
    val millis1 = time1 / 1000L
    val millis2 = time2 / 1000L
    val date1 = millisToDays(millis1)
    val date2 = millisToDays(millis2)
    val (year1, monthInYear1, dayInMonth1, daysToMonthEnd1) = splitDate(date1)
    val (year2, monthInYear2, dayInMonth2, daysToMonthEnd2) = splitDate(date2)

    val months1 = year1 * 12 + monthInYear1
    val months2 = year2 * 12 + monthInYear2

    if (dayInMonth1 == dayInMonth2 || ((daysToMonthEnd1 == 0) && (daysToMonthEnd2 == 0))) {
      return (months1 - months2).toDouble
    }
    // milliseconds is enough for 8 digits precision on the right side
    val timeInDay1 = millis1 - daysToMillis(date1)
    val timeInDay2 = millis2 - daysToMillis(date2)
    val timesBetween = (timeInDay1 - timeInDay2).toDouble / MILLIS_PER_DAY
    val diff = (months1 - months2).toDouble + (dayInMonth1 - dayInMonth2 + timesBetween) / 31.0
    // rounding to 8 digits
    math.round(diff * 1e8) / 1e8
  }

  /*
   * Returns day of week from String. Starting from Thursday, marked as 0.
   * (Because 1970-01-01 is Thursday).
   */
  def getDayOfWeekFromString(string: UTF8String): Int = {
    val dowString = string.toString.toUpperCase
    dowString match {
      case "SU" | "SUN" | "SUNDAY" => 3
      case "MO" | "MON" | "MONDAY" => 4
      case "TU" | "TUE" | "TUESDAY" => 5
      case "WE" | "WED" | "WEDNESDAY" => 6
      case "TH" | "THU" | "THURSDAY" => 0
      case "FR" | "FRI" | "FRIDAY" => 1
      case "SA" | "SAT" | "SATURDAY" => 2
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
    val (_, _, _, daysToMonthEnd) = splitDate(date)
    date + daysToMonthEnd
  }

  private val TRUNC_TO_YEAR = 1
  private val TRUNC_TO_MONTH = 2
  private val TRUNC_TO_DAY = 3
  private val TRUNC_TO_HOUR = 4
  private val TRUNC_TO_MINUTE = 5
  private val TRUNC_TO_SECOND = 6
  private val TRUNC_INVALID = -1

  /**
   * Returns the trunc timestamp from original timestamp and trunc level.
   * Trunc level should be generated using `parseTruncLevel()`, should only be 1 - 6.
   */
  def truncDate(ts: SQLTimestamp, level: Int): SQLTimestamp = {
    if (level == TRUNC_TO_YEAR || level == TRUNC_TO_MONTH) {
      dateFuncToTimestampFunc(ts, truncDate(_: SQLDate, level))
    } else {
      truncTime(ts, level)
    }
  }

  /**
   * Returns the trunc date from original date and trunc level.
   * Trunc level should be generated using `parseTruncLevel()`, should only be 1 or 2.
   */
  private def truncDate(d: SQLDate, level: Int): SQLDate = {
    if (level == TRUNC_TO_YEAR) {
      d - DateTimeUtils.getDayInYear(d) + 1
    } else if (level == TRUNC_TO_MONTH) {
      d - DateTimeUtils.getDayOfMonth(d) + 1
    } else {
      // caller make sure that this should never be reached
      sys.error(s"Invalid trunc level: $level")
    }
  }

  private def truncTime(ts: SQLTimestamp, level: Int): SQLTimestamp = {
    val unitInMillis = level match {
      case TRUNC_TO_DAY => MILLIS_PER_DAY
      case TRUNC_TO_HOUR => MILLIS_PER_HOUR
      case TRUNC_TO_MINUTE => MILLIS_PER_MINUTE
      case TRUNC_TO_SECOND => MILLIS_PER_SECOND
      case _ =>
        // caller make sure that this should never be reached
        sys.error(s"Invalid trunc level: $level")
    }

    val millisUtc = ts / 1000L
    val millisLocal = millisUtc + threadLocalLocalTimeZone.get().getOffset(millisUtc)
    val days = Math.floor(millisLocal.toDouble / unitInMillis).toInt
    val truncatedMillisLocal = days.toLong * unitInMillis
    val offset = getOffsetFromLocalMillis(truncatedMillisLocal, threadLocalLocalTimeZone.get())
    val truncatedMillis = truncatedMillisLocal - offset
    truncatedMillis * 1000L
  }

  /**
   * Returns the truncate level, could be TRUNC_YEAR, TRUNC_MONTH, TRUNC_TO_DAY, TRUNC_TO_HOUR,
   * TRUNC_TO_MINUTE, TRUNC_TO_SECOND or TRUNC_INVALID.
   * TRUNC_INVALID means unsupported truncate level.
   */
  def parseTruncLevel(format: UTF8String): Int = {
    if (format == null) {
      TRUNC_INVALID
    } else {
      format.toString.toUpperCase match {
        case "YEAR" | "YYYY" | "YY" => TRUNC_TO_YEAR
        case "MON" | "MONTH" | "MM" => TRUNC_TO_MONTH
        case "DAY" | "DD" => TRUNC_TO_DAY
        case "HOUR" | "HH" => TRUNC_TO_HOUR
        case "MI" => TRUNC_TO_MINUTE
        case "SEC" | "SS" => TRUNC_TO_SECOND
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
        // fallback to do the reverse lookup using java.sql.Timestamp
        // this should only happen near the start or end of DST
        val days = Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
        val year = getYear(days)
        val month = getMonth(days)
        val day = getDayOfMonth(days)

        var millisOfDay = (millisLocal % MILLIS_PER_DAY).toInt
        if (millisOfDay < 0) {
          millisOfDay += MILLIS_PER_DAY.toInt
        }
        val seconds = (millisOfDay / 1000L).toInt
        val hh = seconds / 3600
        val mm = seconds / 60 % 60
        val ss = seconds % 60
        val ms = millisOfDay % 1000
        val calendar = Calendar.getInstance(tz)
        calendar.set(year, month - 1, day, hh, mm, ss)
        calendar.set(Calendar.MILLISECOND, ms)
        guess = (millisLocal - calendar.getTimeInMillis()).toInt
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
    val localZone = threadLocalLocalTimeZone.get()
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
      val localTs2 = utcTs + toZone.getOffset(utcTs / 1000L) * 1000L  // in toZone
      // treat it as local timezone, convert to UTC (we could get the expected human time back)
      localTs2 - getOffsetFromLocalMillis(localTs2 / 1000L, localZone) * 1000L
    }
  }

  /**
   * Returns a timestamp of given timezone from utc timestamp, with the same string
   * representation in their timezone.
   */
  def fromUTCTime(time: SQLTimestamp, timeZone: String): SQLTimestamp = {
    convertTz(time, TimeZoneGMT, TimeZone.getTimeZone(timeZone))
  }

  /**
   * Returns a utc timestamp from a given timestamp from a given timezone, with the same
   * string representation in their timezone.
   */
  def toUTCTime(time: SQLTimestamp, timeZone: String): SQLTimestamp = {
    convertTz(time, TimeZone.getTimeZone(timeZone), TimeZoneGMT)
  }

  /**
   * Re-initialize the current thread's thread locals. Exposed for testing.
   */
  private[util] def resetThreadLocals(): Unit = {
    threadLocalGmtCalendar.remove()
    threadLocalLocalTimeZone.remove()
    threadLocalTimestampFormat.remove()
    threadLocalDateFormat.remove()
  }
}
