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
import java.text.{DateFormat, ParsePosition, SimpleDateFormat}
import java.time.Instant
import java.util.{Calendar, GregorianCalendar, Locale, TimeZone}
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Function => JFunction}
import javax.xml.bind.DatatypeConverter

import scala.annotation.tailrec

import org.apache.commons.lang3.time.FastDateFormat
import sun.util.calendar.ZoneInfo

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
  final val SECONDS_PER_DAY = 60 * 60 * 24L
  final val MICROS_PER_MILLIS = 1000L
  final val MICROS_PER_SECOND = MICROS_PER_MILLIS * MILLIS_PER_SECOND
  final val MILLIS_PER_SECOND = 1000L
  final val NANOS_PER_SECOND = MICROS_PER_SECOND * 1000L
  final val MICROS_PER_DAY = MICROS_PER_SECOND * SECONDS_PER_DAY
  final val NANOS_PER_MICROS = 1000L
  final val MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L

  // number of days in 400 years
  final val daysIn400Years: Int = 146097
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

  // Reuse the Calendar object in each thread as it is expensive to create in each method call.
  private val threadLocalGmtCalendar = new ThreadLocal[Calendar] {
    override protected def initialValue: Calendar = {
      Calendar.getInstance(TimeZoneGMT)
    }
  }

  // `SimpleDateFormat` is not thread-safe.
  private val threadLocalTimestampFormat = new ThreadLocal[DateFormat] {
    override def initialValue(): SimpleDateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    }
  }

  def getThreadLocalTimestampFormat(timeZone: TimeZone): DateFormat = {
    val sdf = threadLocalTimestampFormat.get()
    sdf.setTimeZone(timeZone)
    sdf
  }

  // `SimpleDateFormat` is not thread-safe.
  private val threadLocalDateFormat = new ThreadLocal[DateFormat] {
    override def initialValue(): SimpleDateFormat = {
      new SimpleDateFormat("yyyy-MM-dd", Locale.US)
    }
  }

  def getThreadLocalDateFormat(timeZone: TimeZone): DateFormat = {
    val sdf = threadLocalDateFormat.get()
    sdf.setTimeZone(timeZone)
    sdf
  }

  private val computedTimeZones = new ConcurrentHashMap[String, TimeZone]
  private val computeTimeZone = new JFunction[String, TimeZone] {
    override def apply(timeZoneId: String): TimeZone = TimeZone.getTimeZone(timeZoneId)
  }

  def getTimeZone(timeZoneId: String): TimeZone = {
    computedTimeZones.computeIfAbsent(timeZoneId, computeTimeZone)
  }

  def newDateFormat(formatString: String, timeZone: TimeZone): DateFormat = {
    val sdf = new SimpleDateFormat(formatString, Locale.US)
    sdf.setTimeZone(timeZone)
    // Enable strict parsing, if the input date/format is invalid, it will throw an exception.
    // e.g. to parse invalid date '2016-13-12', or '2016-01-12' with  invalid format 'yyyy-aa-dd',
    // an exception will be throwed.
    sdf.setLenient(false)
    sdf
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

  def dateToString(days: SQLDate): String =
    getThreadLocalDateFormat(defaultTimeZone()).format(toJavaDate(days))

  def dateToString(days: SQLDate, timeZone: TimeZone): String = {
    getThreadLocalDateFormat(timeZone).format(toJavaDate(days))
  }

  // Converts Timestamp to string according to Hive TimestampWritable convention.
  def timestampToString(us: SQLTimestamp): String = {
    timestampToString(us, defaultTimeZone())
  }

  // Converts Timestamp to string according to Hive TimestampWritable convention.
  def timestampToString(us: SQLTimestamp, timeZone: TimeZone): String = {
    val ts = toJavaTimestamp(us)
    val timestampString = ts.toString
    val timestampFormat = getThreadLocalTimestampFormat(timeZone)
    val formatted = timestampFormat.format(ts)

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
    Math.floorDiv(us, MICROS_PER_MILLIS)
  }

  /*
   * Converts milliseconds since epoch to SQLTimestamp.
   */
  def fromMillis(millis: Long): SQLTimestamp = {
    millis * MICROS_PER_MILLIS
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
    stringToTimestamp(s, defaultTimeZone())
  }

  def stringToTimestamp(s: UTF8String, timeZone: TimeZone): Option[SQLTimestamp] = {
    if (s == null) {
      return None
    }
    var tz: Option[Byte] = None
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

    if (!justTime && isInvalidDate(segments(0), segments(1), segments(2))) {
      return None
    }

    if (segments(3) < 0 || segments(3) > 23 || segments(4) < 0 || segments(4) > 59 ||
        segments(5) < 0 || segments(5) > 59 || segments(6) < 0 || segments(6) > 999999 ||
        segments(7) < 0 || segments(7) > 23 || segments(8) < 0 || segments(8) > 59) {
      return None
    }

    val c = if (tz.isEmpty) {
      Calendar.getInstance(timeZone)
    } else {
      Calendar.getInstance(
        getTimeZone(f"GMT${tz.get.toChar}${segments(7)}%02d:${segments(8)}%02d"))
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
   * Parses a given UTF8 date string to a corresponding [[Int]] value.
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
    if (i < 2 && j < bytes.length) {
      // For the `yyyy` and `yyyy-[m]m` formats, entire input must be consumed.
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
    localTimestamp(microsec, defaultTimeZone())
  }

  private def localTimestamp(microsec: SQLTimestamp, timeZone: TimeZone): SQLTimestamp = {
    absoluteMicroSecond(microsec) + timeZone.getOffset(microsec / 1000) * 1000L
  }

  /**
   * Returns the hour value of a given timestamp value. The timestamp is expressed in microseconds.
   */
  def getHours(microsec: SQLTimestamp): Int = {
    ((localTimestamp(microsec) / MICROS_PER_SECOND / 3600) % 24).toInt
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
  def getMinutes(microsec: SQLTimestamp): Int = {
    ((localTimestamp(microsec) / MICROS_PER_SECOND / 60) % 60).toInt
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
  def getSeconds(microsec: SQLTimestamp): Int = {
    ((localTimestamp(microsec) / MICROS_PER_SECOND) % 60).toInt
  }

  /**
   * Returns the second value of a given timestamp value. The timestamp is expressed in
   * microseconds.
   */
  def getSeconds(microsec: SQLTimestamp, timeZone: TimeZone): Int = {
    ((localTimestamp(microsec, timeZone) / MICROS_PER_SECOND) % 60).toInt
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
   * Calculates the year and the number of the day in the year for the given
   * number of days. The given days is the number of days since 1.1.1970.
   *
   * The calculation uses the fact that the period 1.1.2001 until 31.12.2400 is
   * equals to the period 1.1.1601 until 31.12.2000.
   */
  private[this] def getYearAndDayInYear(daysSince1970: SQLDate): (Int, Int) = {
    // add the difference (in days) between 1.1.1970 and the artificial year 0 (-17999)
    var  daysSince1970Tmp = daysSince1970
    // Since Julian calendar was replaced with the Gregorian calendar,
    // the 10 days after Oct. 4 were skipped.
    // (1582-10-04) -141428 days since 1970-01-01
    if (daysSince1970 <= -141428) {
      daysSince1970Tmp -= 10
    }
    val daysNormalized = daysSince1970Tmp + toYearZero
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
    timestampAddInterval(start, months, microseconds, defaultTimeZone())
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
    val (_, _, _, daysToMonthEnd) = splitDate(date)
    date + daysToMonthEnd
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
        millis - Math.floorMod(millis, MILLIS_PER_SECOND * SECONDS_PER_DAY) - offset
      case TRUNC_TO_HOUR =>
        val offset = timeZone.getOffset(millis)
        millis += offset
        millis - Math.floorMod(millis, 60 * 60 * MILLIS_PER_SECOND) - offset
      case TRUNC_TO_MINUTE =>
        millis - Math.floorMod(millis, 60 * MILLIS_PER_SECOND)
      case TRUNC_TO_SECOND =>
        millis - Math.floorMod(millis, MILLIS_PER_SECOND)
      case TRUNC_TO_WEEK =>
        val dDays = millisToDays(millis, timeZone)
        val prevMonday = getNextDateForDayOfWeek(dDays - 7, MONDAY)
        daysToMillis(prevMonday, timeZone)
      case TRUNC_TO_QUARTER =>
        val dDays = millisToDays(millis, timeZone)
        millis = daysToMillis(truncDate(dDays, TRUNC_TO_MONTH), timeZone)
        val cal = Calendar.getInstance()
        cal.setTimeInMillis(millis)
        val quarter = getQuarter(dDays)
        val month = quarter match {
          case 1 => Calendar.JANUARY
          case 2 => Calendar.APRIL
          case 3 => Calendar.JULY
          case 4 => Calendar.OCTOBER
        }
        cal.set(Calendar.MONTH, month)
        cal.getTimeInMillis()
      case _ =>
        // caller make sure that this should never be reached
        sys.error(s"Invalid trunc level: $level")
    }
    truncated * MICROS_PER_MILLIS
  }

  def truncTimestamp(d: SQLTimestamp, level: Int): SQLTimestamp = {
    truncTimestamp(d, level, defaultTimeZone())
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
   */
  private[sql] def getOffsetFromLocalMillis(millisLocal: Long, tz: TimeZone): Long = tz match {
    case zoneInfo: ZoneInfo => zoneInfo.getOffsetsByWall(millisLocal, null)
    case timeZone: TimeZone => timeZone.getOffset(millisLocal - timeZone.getRawOffset)
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

  /**
   * Re-initialize the current thread's thread locals. Exposed for testing.
   */
  private[util] def resetThreadLocals(): Unit = {
    threadLocalGmtCalendar.remove()
    threadLocalTimestampFormat.remove()
    threadLocalDateFormat.remove()
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
  private class MicrosCalendar(tz: TimeZone, digitsInFraction: Int)
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

  /**
   * An instance of the class is aimed to re-use many times. It contains helper objects
   * `cal` which is reused between `parse()` and `format` invokes.
   */
  class TimestampParser(fastDateFormat: FastDateFormat) {
    private val cal = new MicrosCalendar(
      fastDateFormat.getTimeZone,
      fastDateFormat.getPattern.count(_ == 'S'))

    def parse(s: String): SQLTimestamp = {
      cal.clear() // Clear the calendar because it can be re-used many times
      if (!fastDateFormat.parse(s, new ParsePosition(0), cal)) {
        throw new IllegalArgumentException(s"'$s' is an invalid timestamp")
      }
      val micros = cal.getMicros()
      cal.set(Calendar.MILLISECOND, 0)
      cal.getTimeInMillis * MICROS_PER_MILLIS + micros
    }

    def format(timestamp: SQLTimestamp): String = {
      cal.setTimeInMillis(Math.floorDiv(timestamp, MICROS_PER_SECOND) * MILLIS_PER_SECOND)
      cal.setMicros(Math.floorMod(timestamp, MICROS_PER_SECOND))
      fastDateFormat.format(cal)
    }
  }
}
