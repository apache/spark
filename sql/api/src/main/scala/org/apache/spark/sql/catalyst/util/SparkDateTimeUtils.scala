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

import java.lang.invoke.{MethodHandles, MethodType}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZonedDateTime, ZoneId, ZoneOffset}
import java.time.temporal.ChronoField.NANO_OF_DAY
import java.util.TimeZone
import java.util.concurrent.TimeUnit.{MICROSECONDS, NANOSECONDS}
import java.util.regex.Pattern

import scala.util.control.NonFatal

import org.apache.spark.{QueryContext, SparkException}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.RebaseDateTime.{rebaseGregorianToJulianDays, rebaseGregorianToJulianMicros, rebaseJulianToGregorianDays, rebaseJulianToGregorianMicros}
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types.{DateType, TimestampLTZNanosType, TimestampNTZNanosType, TimestampType, TimeType}
import org.apache.spark.unsafe.types.{TimestampNanosVal, UTF8String}
import org.apache.spark.util.SparkClassUtils

trait SparkDateTimeUtils {

  final val TimeZoneUTC = TimeZone.getTimeZone("UTC")

  final val singleHourTz = Pattern.compile("(\\+|\\-)(\\d):")
  final val singleMinuteTz = Pattern.compile("(\\+|\\-)(\\d\\d):(\\d)$")

  def getZoneId(timeZoneId: String): ZoneId = {
    try {
      // To support the (+|-)h:mm format because it was supported before Spark 3.0.
      var formattedZoneId = singleHourTz.matcher(timeZoneId).replaceFirst("$10$2:")
      // To support the (+|-)hh:m format because it was supported before Spark 3.0.
      formattedZoneId = singleMinuteTz.matcher(formattedZoneId).replaceFirst("$1$2:0$3")
      ZoneId.of(formattedZoneId, ZoneId.SHORT_IDS)
    } catch {
      case e: java.time.DateTimeException =>
        throw ExecutionErrors.zoneOffsetError(timeZoneId, e)
    }
  }

  def getTimeZone(timeZoneId: String): TimeZone = TimeZone.getTimeZone(getZoneId(timeZoneId))

  /**
   * Converts an Java object to days.
   *
   * @param obj
   *   Either an object of `java.sql.Date` or `java.time.LocalDate`.
   * @return
   *   The number of days since 1970-01-01.
   */
  def anyToDays(obj: Any): Int = obj match {
    case d: Date => fromJavaDate(d)
    case ld: LocalDate => localDateToDays(ld)
  }

  /**
   * Converts an Java object to microseconds.
   *
   * @param obj
   *   Either an object of `java.sql.Timestamp` or `java.time.{Instant,LocalDateTime}`.
   * @return
   *   The number of micros since the epoch.
   */
  def anyToMicros(obj: Any): Long = obj match {
    case t: Timestamp => fromJavaTimestamp(t)
    case i: Instant => instantToMicros(i)
    case ldt: LocalDateTime => localDateTimeToMicros(ldt)
  }

  /**
   * Converts the time to microseconds since midnight. In Spark time values have nanoseconds
   * precision, so this conversion is lossy.
   */
  def nanosToMicros(nanos: Long): Long = Math.floorDiv(nanos, MICROS_PER_MILLIS)

  /**
   * Converts the timestamp to milliseconds since epoch. In Spark timestamp values have
   * microseconds precision, so this conversion is lossy.
   */
  def microsToMillis(micros: Long): Long = {
    // When the timestamp is negative i.e before 1970, we need to adjust the milliseconds portion.
    // Example - 1965-01-01 10:11:12.123456 is represented as (-157700927876544) in micro precision.
    // In millis precision the above needs to be represented as (-157700927877).
    Math.floorDiv(micros, MICROS_PER_MILLIS)
  }

  /**
   * Converts milliseconds since the epoch to microseconds.
   */
  def millisToMicros(millis: Long): Long = {
    Math.multiplyExact(millis, MICROS_PER_MILLIS)
  }

  /**
   * Converts microseconds since the midnight to nanoseconds.
   */
  def microsToNanos(micros: Long): Long = Math.multiplyExact(micros, NANOS_PER_MICROS)

  // See issue SPARK-35679
  // min second cause overflow in instant to micro
  private val MIN_SECONDS = Math.floorDiv(Long.MinValue, MICROS_PER_SECOND)

  /**
   * Obtains an instance of `java.time.Instant` using microseconds from the epoch of 1970-01-01
   * 00:00:00Z.
   */
  def microsToInstant(micros: Long): Instant = {
    val secs = Math.floorDiv(micros, MICROS_PER_SECOND)
    // Unfolded Math.floorMod(us, MICROS_PER_SECOND) to reuse the result of
    // the above calculation of `secs` via `floorDiv`.
    val mos = micros - secs * MICROS_PER_SECOND
    Instant.ofEpochSecond(secs, mos * NANOS_PER_MICROS)
  }

  /**
   * Gets the number of microseconds since the epoch of 1970-01-01 00:00:00Z from the given
   * instance of `java.time.Instant`. The epoch microsecond count is a simple incrementing count
   * of microseconds where microsecond 0 is 1970-01-01 00:00:00Z.
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

  /**
   * Gets the number of nanoseconds since midnight using the given time zone.
   */
  def instantToNanosOfDay(instant: Instant, timezone: String): Long = {
    instantToNanosOfDay(instant, getZoneId(timezone))
  }

  /**
   * Gets the number of nanoseconds since midnight using the given time zone.
   */
  def instantToNanosOfDay(instant: Instant, zoneId: ZoneId): Long = {
    val localDateTime = LocalDateTime.ofInstant(instant, zoneId)
    localDateTime.toLocalTime.getLong(NANO_OF_DAY)
  }

  /**
   * Truncates a time value (in nanoseconds) to the specified fractional precision `p`.
   *
   * For example, if `p = 3`, we keep millisecond resolution and discard any digits beyond the
   * thousand-nanosecond place. So a value like `123456` microseconds (12:34:56.123456) becomes
   * `123000` microseconds (12:34:56.123).
   *
   * @param nanos
   *   The original time in nanoseconds.
   * @param p
   *   The fractional second precision (range 0 to 6).
   * @return
   *   The truncated nanosecond value, preserving only `p` fractional digits.
   */
  def truncateTimeToPrecision(nanos: Long, p: Int): Long = {
    assert(
      TimeType.MIN_PRECISION <= p && p <= TimeType.MAX_PRECISION,
      s"Fractional second precision $p out" +
        s" of range [${TimeType.MIN_PRECISION}..${TimeType.MAX_PRECISION}].")
    val scale = TimeType.NANOS_PRECISION - p
    val factor = math.pow(10, scale).toLong
    (nanos / factor) * factor
  }

  /**
   * Converts the timestamp `micros` from one timezone to another.
   *
   * Time-zone rules, such as daylight savings, mean that not every local date-time is valid for
   * the `toZone` time zone, thus the local date-time may be adjusted.
   */
  def convertTz(micros: Long, fromZone: ZoneId, toZone: ZoneId): Long = {
    val rebasedDateTime = getLocalDateTime(micros, toZone).atZone(fromZone)
    instantToMicros(rebasedDateTime.toInstant)
  }

  // Gets the local date-time parts (year, month, day and time) of the instant expressed as the
  // number of microseconds since the epoch at the given time zone ID.
  protected def getLocalDateTime(micros: Long, zoneId: ZoneId): LocalDateTime = {
    microsToInstant(micros).atZone(zoneId).toLocalDateTime
  }

  def microsToLocalDateTime(micros: Long): LocalDateTime = {
    getLocalDateTime(micros, ZoneOffset.UTC)
  }

  def localDateTimeToMicros(localDateTime: LocalDateTime): Long = {
    instantToMicros(localDateTime.toInstant(ZoneOffset.UTC))
  }

  /**
   * Truncates the sub-microsecond nanosecond part to the given timestamp precision `p` in [7, 9].
   * Precision 9 keeps all three digits, 8 zeros the last digit, 7 zeros the last two.
   *
   * The input is the already-extracted `nanosWithinMicro` component (`0..999`), so truncation is
   * independent of the epoch sign of the original timestamp value.
   *
   * Precisions outside `[7, 9]` are passed through unchanged because the surrounding timestamp
   * nanos types validate the bound.
   */
  private def truncateNanosWithinMicroToPrecision(nanosWithinMicro: Int, precision: Int): Int = {
    precision match {
      case 7 => (nanosWithinMicro / 100) * 100
      case 8 => (nanosWithinMicro / 10) * 10
      case _ => nanosWithinMicro
    }
  }

  /**
   * Converts a `java.time.LocalDateTime` into the composite `(epochMicros, nanosWithinMicro)`
   * pair used by `TimestampNTZNanosType(precision)` (interpreted at UTC). `epochMicros` comes
   * from [[localDateTimeToMicros]] (which is floor toward `-inf` for the integral micro part);
   * the last three decimal digits of `localDateTime.getNano` (`[0, 999]`) become
   * `nanosWithinMicro` after dropping `(9 - precision)` low digits.
   *
   * Combined, the result is the floor toward `-inf` of the original nanosecond value rounded down
   * to the precision step (10^(9 - precision) ns). At `precision = 9` the conversion is lossless
   * within the valid range; at 7 / 8 the lowest 2 / 1 sub-micro digits are dropped. The same
   * flooring will be the basis of the future `CAST(... AS TIMESTAMP_NTZ(precision))` rule.
   */
  def localDateTimeToTimestampNanos(
      localDateTime: LocalDateTime,
      precision: Int): TimestampNanosVal = {
    val epochMicros = localDateTimeToMicros(localDateTime)
    val rawNanosWithinMicro = localDateTime.getNano % NANOS_PER_MICROS.toInt
    val nanosWithinMicro = truncateNanosWithinMicroToPrecision(rawNanosWithinMicro, precision)
    TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro.toShort)
  }

  /**
   * Reverse of [[localDateTimeToTimestampNanos]]: rebuilds a `java.time.LocalDateTime` (at UTC)
   * from a `TimestampNanosVal`. `nanosWithinMicro` is in `[0, 999]` so `plusNanos` never crosses
   * the second boundary.
   */
  def timestampNanosToLocalDateTime(v: TimestampNanosVal): LocalDateTime = {
    microsToLocalDateTime(v.epochMicros).plusNanos(v.nanosWithinMicro.toLong)
  }

  /**
   * Converts a `java.time.Instant` into the composite `(epochMicros, nanosWithinMicro)` pair used
   * by `TimestampLTZNanosType(precision)`. `epochMicros` comes from [[instantToMicros]] (floor
   * toward `-inf` for the integral micro part); the last three decimal digits of
   * `instant.getNano` (`[0, 999]`) become `nanosWithinMicro` after dropping `(9 - precision)` low
   * digits.
   *
   * Combined, the result is the floor toward `-inf` of the original nanosecond value rounded down
   * to the precision step (10^(9 - precision) ns). At `precision = 9` the conversion is lossless
   * within the valid range; at 7 / 8 the lowest 2 / 1 sub-micro digits are dropped. The same
   * flooring will be the basis of the future `CAST(... AS TIMESTAMP_LTZ(precision))` rule.
   */
  def instantToTimestampNanos(instant: Instant, precision: Int): TimestampNanosVal = {
    val epochMicros = instantToMicros(instant)
    val rawNanosWithinMicro = instant.getNano % NANOS_PER_MICROS.toInt
    val nanosWithinMicro = truncateNanosWithinMicroToPrecision(rawNanosWithinMicro, precision)
    TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro.toShort)
  }

  /**
   * Reverse of [[instantToTimestampNanos]]: rebuilds a `java.time.Instant` from a
   * `TimestampNanosVal`. `nanosWithinMicro` is in `[0, 999]` so `plusNanos` never crosses the
   * second boundary.
   */
  def timestampNanosToInstant(v: TimestampNanosVal): Instant = {
    microsToInstant(v.epochMicros).plusNanos(v.nanosWithinMicro.toLong)
  }

  /**
   * Converts the local date to the number of days since 1970-01-01.
   */
  def localDateToDays(localDate: LocalDate): Int = MathUtils.toIntExact(localDate.toEpochDay)

  /**
   * Obtains an instance of `java.time.LocalDate` from the epoch day count.
   */
  def daysToLocalDate(days: Int): LocalDate = LocalDate.ofEpochDay(days)

  /**
   * Converts microseconds since 1970-01-01 00:00:00Z to days since 1970-01-01 at the given zone
   * ID.
   */
  def microsToDays(micros: Long, zoneId: ZoneId): Int = {
    localDateToDays(getLocalDateTime(micros, zoneId).toLocalDate)
  }

  /**
   * Converts days since 1970-01-01 at the given zone ID to microseconds since 1970-01-01
   * 00:00:00Z. When `zoneId eq ZoneOffset.UTC`, takes a direct-multiply fast path that skips the
   * `LocalDate`/`ZonedDateTime`/`Instant` chain.
   */
  def daysToMicros(days: Int, zoneId: ZoneId): Long = {
    if (zoneId eq ZoneOffset.UTC) {
      Math.multiplyExact(days.toLong, MICROS_PER_DAY)
    } else {
      val instant = daysToLocalDate(days).atStartOfDay(zoneId).toInstant
      instantToMicros(instant)
    }
  }

  /**
   * Converts the local time to the number of nanoseconds within the day, from 0 to (24 * 60 * 60
   * * 1000 * 1000 * 1000) - 1.
   */
  def localTimeToNanos(localTime: LocalTime): Long = localTime.getLong(NANO_OF_DAY)

  /**
   * Converts the number of nanoseconds within the day to the local time.
   */
  def nanosToLocalTime(nanos: Long): LocalTime = LocalTime.ofNanoOfDay(nanos)

  /**
   * Converts a local date at the default JVM time zone to the number of days since 1970-01-01 in
   * the hybrid calendar (Julian + Gregorian) by discarding the time part. The resulted days are
   * rebased from the hybrid to Proleptic Gregorian calendar. The days rebasing is performed via
   * UTC time zone for simplicity because the difference between two calendars is the same in any
   * given time zone and UTC time zone.
   *
   * Note: The date is shifted by the offset of the default JVM time zone for backward
   * compatibility with Spark 2.4 and earlier versions. The goal of the shift is to get a local
   * date derived from the number of days that has the same date fields (year, month, day) as the
   * original `date` at the default JVM time zone.
   *
   * @param date
   *   It represents a specific instant in time based on the hybrid calendar which combines Julian
   *   and Gregorian calendars.
   * @return
   *   The number of days since the epoch in Proleptic Gregorian calendar.
   */
  def fromJavaDate(date: Date): Int = {
    val millisUtc = date.getTime
    val millisLocal = millisUtc + TimeZone.getDefault.getOffset(millisUtc)
    val julianDays = Math.toIntExact(Math.floorDiv(millisLocal, MILLIS_PER_DAY))
    rebaseJulianToGregorianDays(julianDays)
  }

  private val zoneInfoClassName = "sun.util.calendar.ZoneInfo"
  private lazy val getOffsetsByWallHandle = {
    val lookup = MethodHandles.lookup()
    val classType = SparkClassUtils.classForName(zoneInfoClassName)
    val methodName = "getOffsetsByWall"
    val methodType = MethodType.methodType(classOf[Int], classOf[Long], classOf[Array[Int]])
    lookup.findVirtual(classType, methodName, methodType)
  }

  /**
   * Converts days since the epoch 1970-01-01 in Proleptic Gregorian calendar to a local date at
   * the default JVM time zone in the hybrid calendar (Julian + Gregorian). It rebases the given
   * days from Proleptic Gregorian to the hybrid calendar at UTC time zone for simplicity because
   * the difference between two calendars doesn't depend on any time zone. The result is shifted
   * by the time zone offset in wall clock to have the same date fields (year, month, day) at the
   * default JVM time zone as the input `daysSinceEpoch` in Proleptic Gregorian calendar.
   *
   * Note: The date is shifted by the offset of the default JVM time zone for backward
   * compatibility with Spark 2.4 and earlier versions.
   *
   * @param days
   *   The number of days since 1970-01-01 in Proleptic Gregorian calendar.
   * @return
   *   A local date in the hybrid calendar as `java.sql.Date` from number of days since epoch.
   */
  def toJavaDate(days: Int): Date = {
    val rebasedDays = rebaseGregorianToJulianDays(days)
    val localMillis = Math.multiplyExact(rebasedDays, MILLIS_PER_DAY)
    val timeZoneOffset = TimeZone.getDefault match {
      case zoneInfo: TimeZone if zoneInfo.getClass.getName == zoneInfoClassName =>
        getOffsetsByWallHandle.invoke(zoneInfo, localMillis, null).asInstanceOf[Int]
      case timeZone: TimeZone =>
        timeZone.getOffset(localMillis - timeZone.getRawOffset)
    }
    new Date(localMillis - timeZoneOffset)
  }

  /**
   * Converts microseconds since the epoch to an instance of `java.sql.Timestamp` via creating a
   * local timestamp at the system time zone in Proleptic Gregorian calendar, extracting date and
   * time fields like `year` and `hours`, and forming new timestamp in the hybrid calendar from
   * the extracted fields.
   *
   * The conversion is based on the JVM system time zone because the `java.sql.Timestamp` uses the
   * time zone internally.
   *
   * The method performs the conversion via local timestamp fields to have the same date-time
   * representation as `year`, `month`, `day`, ..., `seconds` in the original calendar and in the
   * target calendar.
   *
   * @param micros
   *   The number of microseconds since 1970-01-01T00:00:00.000000Z.
   * @return
   *   A `java.sql.Timestamp` from number of micros since epoch.
   */
  def toJavaTimestamp(micros: Long): Timestamp =
    toJavaTimestampNoRebase(rebaseGregorianToJulianMicros(micros))

  def toJavaTimestamp(timeZoneId: String, micros: Long): Timestamp =
    toJavaTimestampNoRebase(rebaseGregorianToJulianMicros(timeZoneId, micros))

  /**
   * Converts microseconds since the epoch to an instance of `java.sql.Timestamp`.
   *
   * @param micros
   *   The number of microseconds since 1970-01-01T00:00:00.000000Z.
   * @return
   *   A `java.sql.Timestamp` from number of micros since epoch.
   */
  def toJavaTimestampNoRebase(micros: Long): Timestamp = {
    val seconds = Math.floorDiv(micros, MICROS_PER_SECOND)
    val ts = new Timestamp(seconds * MILLIS_PER_SECOND)
    val nanos = (micros - seconds * MICROS_PER_SECOND) * NANOS_PER_MICROS
    ts.setNanos(nanos.toInt)
    ts
  }

  /**
   * Converts an instance of `java.sql.Timestamp` to the number of microseconds since
   * 1970-01-01T00:00:00.000000Z. It extracts date-time fields from the input, builds a local
   * timestamp in Proleptic Gregorian calendar from the fields, and binds the timestamp to the
   * system time zone. The resulted instant is converted to microseconds since the epoch.
   *
   * The conversion is performed via the system time zone because it is used internally in
   * `java.sql.Timestamp` while extracting date-time fields.
   *
   * The goal of the function is to have the same local date-time in the original calendar
   *   - the hybrid calendar (Julian + Gregorian) and in the target calendar which is Proleptic
   *     Gregorian calendar, see SPARK-26651.
   *
   * @param t
   *   It represents a specific instant in time based on the hybrid calendar which combines Julian
   *   and Gregorian calendars.
   * @return
   *   The number of micros since epoch from `java.sql.Timestamp`.
   */
  def fromJavaTimestamp(t: Timestamp): Long =
    rebaseJulianToGregorianMicros(fromJavaTimestampNoRebase(t))

  def fromJavaTimestamp(timeZoneId: String, t: Timestamp): Long =
    rebaseJulianToGregorianMicros(timeZoneId, fromJavaTimestampNoRebase(t))

  /**
   * Converts an instance of `java.sql.Timestamp` to the number of microseconds since
   * 1970-01-01T00:00:00.000000Z.
   *
   * @param t
   *   an instance of `java.sql.Timestamp`.
   * @return
   *   The number of micros since epoch from `java.sql.Timestamp`.
   */
  def fromJavaTimestampNoRebase(t: Timestamp): Long =
    millisToMicros(t.getTime) + (t.getNanos / NANOS_PER_MICROS) % MICROS_PER_MILLIS

  /**
   * Trims and parses a given UTF8 date string to a corresponding [[Int]] value. The return type
   * is [[Option]] in order to distinguish between 0 and null. The following formats are allowed:
   *
   * `[+-]yyyy*` `[+-]yyyy*-[m]m` `[+-]yyyy*-[m]m-[d]d` `[+-]yyyy*-[m]m-[d]d `
   * `[+-]yyyy*-[m]m-[d]d *` `[+-]yyyy*-[m]m-[d]dT*`
   */
  def stringToDate(s: UTF8String): Option[Int] = {
    def isValidDigits(segment: Int, digits: Int): Boolean = {
      // An integer is able to represent a date within [+-]5 million years.
      val maxDigitsYear = 7
      (segment == 0 && digits >= 4 && digits <= maxDigitsYear) ||
      (segment != 0 && digits > 0 && digits <= 2)
    }
    if (s == null) {
      return None
    }

    val segments: Array[Int] = Array[Int](1, 1, 1)
    var sign = 1
    var i = 0
    var currentSegmentValue = 0
    var currentSegmentDigits = 0
    val bytes = s.getBytes
    var j = getTrimmedStart(bytes)
    val strEndTrimmed = getTrimmedEnd(j, bytes)

    if (j == strEndTrimmed) {
      return None
    }

    if (bytes(j) == '-' || bytes(j) == '+') {
      sign = if (bytes(j) == '-') -1 else 1
      j += 1
    }
    while (j < strEndTrimmed && (i < 3 && !(bytes(j) == ' ' || bytes(j) == 'T'))) {
      val b = bytes(j)
      if (i < 2 && b == '-') {
        if (!isValidDigits(i, currentSegmentDigits)) {
          return None
        }
        segments(i) = currentSegmentValue
        currentSegmentValue = 0
        currentSegmentDigits = 0
        i += 1
      } else {
        val parsedValue = b - '0'.toByte
        if (parsedValue < 0 || parsedValue > 9) {
          return None
        } else {
          currentSegmentValue = currentSegmentValue * 10 + parsedValue
          currentSegmentDigits += 1
        }
      }
      j += 1
    }
    if (!isValidDigits(i, currentSegmentDigits)) {
      return None
    }
    if (i < 2 && j < strEndTrimmed) {
      // For the `yyyy` and `yyyy-[m]m` formats, entire input must be consumed.
      return None
    }
    segments(i) = currentSegmentValue
    try {
      val localDate = LocalDate.of(sign * segments(0), segments(1), segments(2))
      Some(localDateToDays(localDate))
    } catch {
      case NonFatal(_) => None
    }
  }

  def stringToDateAnsi(s: UTF8String, context: QueryContext = null): Int = {
    stringToDate(s).getOrElse {
      throw ExecutionErrors.invalidInputInCastToDatetimeError(s, DateType, context)
    }
  }

  /**
   * Trims and parses a given UTF8 timestamp string to the corresponding timestamp segments, time
   * zone id and whether it is just time without a date. value. The return type is [[Option]] in
   * order to distinguish between 0L and null. The following formats are allowed:
   *
   * `[+-]yyyy*` `[+-]yyyy*-[m]m` `[+-]yyyy*-[m]m-[d]d` `[+-]yyyy*-[m]m-[d]d `
   * `[+-]yyyy*-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][ns][ns][ns][zone_id]`
   * `[+-]yyyy*-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][ns][ns][ns][zone_id]`
   * `[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][ns][ns][ns][zone_id]`
   * `T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][ns][ns][ns][zone_id]`
   *
   * where `zone_id` should have one of the forms:
   *   - Z - Zulu time zone UTC+0
   *   - +|-[h]h:[m]m
   *   - A short id, see https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#SHORT_IDS
   *   - An id with one of the prefixes UTC+, UTC-, GMT+, GMT-, UT+ or UT-, and a suffix in the
   *     formats:
   *     - +|-h[h]
   *     - +|-hh[:]mm
   *     - +|-hh:mm:ss
   *     - +|-hhmmss
   *   - Region-based zone IDs in the form `area/city`, such as `Europe/Paris`
   *
   * Up to 9 fractional-second digits are accepted. Digits 1-6 are kept as microseconds in
   * `segments(6)` (backward-compatible micro behavior), digits 7-9 are kept as the
   * sub-microsecond remainder in `segments(9)` (a value in [0, 999]), and digits beyond the 9th
   * are dropped.
   *
   * @return
   *   timestamp segments, time zone id and whether the input is just time without a date. If the
   *   input string can't be parsed as timestamp, the result timestamp segments are empty.
   */
  def parseTimestampString(s: UTF8String): (Array[Int], Option[ZoneId], Boolean) = {
    def isValidDigits(segment: Int, digits: Int): Boolean = {
      // A Long is able to represent a timestamp within [+-]200 thousand years
      val maxDigitsYear = 6
      // Fractional digits 1-6 form microseconds; digits 7-9 are retained as the sub-microsecond
      // remainder in segments(9); only digits beyond the 9th are dropped.
      segment == 6 || (segment == 0 && digits >= 4 && digits <= maxDigitsYear) ||
      // For the zoneId segment(7), it's could be zero digits when it's a region-based zone ID
      (segment == 7 && digits <= 2) ||
      (segment != 0 && segment != 6 && segment != 7 && digits > 0 && digits <= 2)
    }
    if (s == null) {
      return (Array.empty, None, false)
    }
    var tz: Option[String] = None
    // Indices 0-6 hold year, month, day, hour, minute, second and the microsecond part of the
    // fractional second (digits 1-6). Index 9 is an output-only slot that holds the
    // sub-microsecond remainder (fractional digits 7-9) as a value in [0, 999]; it is never
    // written by the parsing loop below. Indices 7-8 are written by the loop as `i` advances
    // but their values are never read by any caller.
    val segments: Array[Int] = Array[Int](1, 1, 1, 0, 0, 0, 0, 0, 0, 0)
    var i = 0
    var currentSegmentValue = 0
    var currentSegmentDigits = 0
    val bytes = s.getBytes
    var j = getTrimmedStart(bytes)
    val strEndTrimmed = getTrimmedEnd(j, bytes)

    if (j == strEndTrimmed) {
      return (Array.empty, None, false)
    }

    var digitsMilli = 0
    var nanosWithinMicro = 0
    var justTime = false
    var yearSign: Option[Int] = None
    if (bytes(j) == '-' || bytes(j) == '+') {
      yearSign = if (bytes(j) == '-') Some(-1) else Some(1)
      j += 1
    }
    while (j < strEndTrimmed) {
      val b = bytes(j)
      val parsedValue = b - '0'.toByte
      if (parsedValue < 0 || parsedValue > 9) {
        if (j == 0 && b == 'T') {
          justTime = true
          i += 3
        } else if (i < 2) {
          if (b == '-') {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
          } else if (i == 0 && b == ':' && yearSign.isEmpty) {
            justTime = true
            if (!isValidDigits(3, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(3) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i = 4
          } else {
            return (Array.empty, None, false)
          }
        } else if (i == 2) {
          if (b == ' ' || b == 'T') {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
          } else {
            return (Array.empty, None, false)
          }
        } else if (i == 3 || i == 4) {
          if (b == ':') {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
          } else {
            return (Array.empty, None, false)
          }
        } else if (i == 5 || i == 6) {
          if (b == '.' && i == 5) {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
          } else {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
            tz = Some(new String(bytes, j, strEndTrimmed - j))
            j = strEndTrimmed - 1
          }
          if (i == 6 && b != '.') {
            i += 1
          }
        } else {
          // Bound is fixed at 9 (the original number of parsed segments) so that the trailing
          // output-only slot segments(9) is never written by the parsing loop.
          if (i < 9 && (b == ':' || b == ' ')) {
            if (!isValidDigits(i, currentSegmentDigits)) {
              return (Array.empty, None, false)
            }
            segments(i) = currentSegmentValue
            currentSegmentValue = 0
            currentSegmentDigits = 0
            i += 1
          } else {
            return (Array.empty, None, false)
          }
        }
      } else {
        if (i == 6) {
          digitsMilli += 1
        }
        if (i != 6 || currentSegmentDigits < 6) {
          // Fractional digits 1-6 form the microsecond part stored in segments(6).
          currentSegmentValue = currentSegmentValue * 10 + parsedValue
        } else if (currentSegmentDigits < 9) {
          // Fractional digits 7-9 are retained as the sub-microsecond remainder. Digits beyond
          // the 9th are dropped (loss of precision below the nanosecond grid).
          nanosWithinMicro = nanosWithinMicro * 10 + parsedValue
        }
        currentSegmentDigits += 1
      }
      j += 1
    }

    if (!isValidDigits(i, currentSegmentDigits)) {
      return (Array.empty, None, false)
    }
    segments(i) = currentSegmentValue

    while (digitsMilli < 6) {
      segments(6) *= 10
      digitsMilli += 1
    }

    // Right-pad the captured sub-microsecond digits (the 7th to 9th fractional digits) so that
    // segments(9) always holds a value in [0, 999]. The number of captured digits is
    // clamp(digitsMilli - 6, 0, 3); fewer captured digits means the remainder is left-aligned and
    // must be scaled up (e.g. ".0000001" -> 100, ".00000012" -> 120, ".000000123" -> 123).
    var subMicroDigits = math.max(0, math.min(digitsMilli, 9) - 6)
    while (subMicroDigits < 3) {
      nanosWithinMicro *= 10
      subMicroDigits += 1
    }
    segments(9) = nanosWithinMicro

    // This step also validates time zone part
    val zoneId = tz.map(zoneName => getZoneId(zoneName.trim))
    segments(0) *= yearSign.getOrElse(1)
    (segments, zoneId, justTime)
  }

  /**
   * Parses a UTF8 timestamp string into the [[Instant]] it denotes, shared by the LTZ entry points
   * `stringToTimestamp` (micros) and `stringToTimestampLTZNanos` (nanos). The full fractional part
   * (including sub-microsecond digits) is carried in the [[Instant]]; each caller then narrows to
   * its own precision (`instantToMicros` floors the sub-micro digits, `instantToTimestampNanos`
   * truncates to the requested precision), so this helper is behavior-preserving for the micro
   * path. Callers are expected to wrap the call in a `try`/`catch` that maps `NonFatal` to `None`.
   *
   * Returns `null` (rather than [[Option]]) when the string is unparseable. The `null` sentinel
   * keeps these cast hot paths allocation-free: no intermediate `Option`/closure is materialized,
   * and the small body inlines into the caller. Callers must null-check the result.
   */
  private def parseTimestampToInstant(s: UTF8String, timeZoneId: ZoneId): Instant = {
    val (segments, parsedZoneId, justTime) = parseTimestampString(s)
    if (segments.isEmpty) {
      return null
    }
    val zoneId = parsedZoneId.getOrElse(timeZoneId)
    // Combine the microsecond part (digits 1-6) and the sub-microsecond remainder (digits 7-9)
    // into a full nano-of-second. This is harmless for the micro path because `instantToMicros`
    // floors the sub-microsecond digits away.
    val nanoOfSecond = (MICROSECONDS.toNanos(segments(6)) + segments(9)).toInt
    val localTime = LocalTime.of(segments(3), segments(4), segments(5), nanoOfSecond)
    val localDate = if (justTime) {
      LocalDate.now(zoneId)
    } else {
      LocalDate.of(segments(0), segments(1), segments(2))
    }
    val localDateTime = LocalDateTime.of(localDate, localTime)
    val zonedDateTime = ZonedDateTime.of(localDateTime, zoneId)
    Instant.from(zonedDateTime)
  }

  /**
   * Trims and parses a given UTF8 timestamp string to the corresponding a corresponding [[Long]]
   * value. The return type is [[Option]] in order to distinguish between 0L and null. Please
   * refer to `parseTimestampString` for the allowed formats
   */
  def stringToTimestamp(s: UTF8String, timeZoneId: ZoneId): Option[Long] = {
    try {
      // `null` here means the string was unparseable (see `parseTimestampToInstant`).
      val instant = parseTimestampToInstant(s, timeZoneId)
      if (instant == null) None else Some(instantToMicros(instant))
    } catch {
      case NonFatal(_) => None
    }
  }

  def stringToTimestampAnsi(
      s: UTF8String,
      timeZoneId: ZoneId,
      context: QueryContext = null): Long = {
    stringToTimestamp(s, timeZoneId).getOrElse {
      throw ExecutionErrors.invalidInputInCastToDatetimeError(s, TimestampType, context)
    }
  }

  /**
   * Trims and parses a given UTF8 string to a corresponding [[Long]] value which representing the
   * number of microseconds since the epoch. The result will be independent of time zones.
   *
   * If the input string contains a component associated with time zone, the method will return
   * `None` if `allowTimeZone` is set to `false`. If `allowTimeZone` is set to `true`, the method
   * will simply discard the time zone component. Enable the check to detect situations like
   * parsing a timestamp with time zone as TimestampNTZType.
   *
   * The return type is [[Option]] in order to distinguish between 0L and null. Please refer to
   * `parseTimestampString` for the allowed formats.
   */
  /**
   * Parses a UTF8 timestamp string into the zone-independent [[LocalDateTime]] it denotes, shared
   * by the NTZ entry points `stringToTimestampWithoutTimeZone` (micros) and
   * `stringToTimestampNTZNanos` (nanos). A time zone component is discarded when `allowTimeZone`
   * is `true` and rejected otherwise. The full fractional part (including sub-microsecond digits)
   * is carried in the [[LocalDateTime]]; each caller then narrows to its own precision
   * (`localDateTimeToMicros` floors the sub-micro digits, `localDateTimeToTimestampNanos`
   * truncates to the requested precision), so this helper is behavior-preserving for the micro
   * path. Callers are expected to wrap the call in a `try`/`catch` that maps `NonFatal` to `None`.
   *
   * Returns `null` (rather than [[Option]]) when the string is unparseable, contains only a time
   * part, or carries a time zone while `allowTimeZone` is `false`. The `null` sentinel keeps these
   * cast hot paths allocation-free: no intermediate `Option`/closure is materialized, and the
   * small body inlines into the caller. Callers must null-check the result.
   */
  private def parseTimestampToLocalDateTime(
      s: UTF8String,
      allowTimeZone: Boolean): LocalDateTime = {
    val (segments, zoneIdOpt, justTime) = parseTimestampString(s)
    // If the input string can't be parsed as a timestamp without time zone, or it contains only
    // the time part of a timestamp and we can't determine its date, signal failure with `null`.
    if (segments.isEmpty || justTime || !allowTimeZone && zoneIdOpt.isDefined) {
      return null
    }
    // Combine the microsecond part (digits 1-6) and the sub-microsecond remainder (digits 7-9)
    // into a full nano-of-second. This is harmless for the micro path because
    // `localDateTimeToMicros` floors the sub-microsecond digits away.
    val nanoOfSecond = (MICROSECONDS.toNanos(segments(6)) + segments(9)).toInt
    val localTime = LocalTime.of(segments(3), segments(4), segments(5), nanoOfSecond)
    val localDate = LocalDate.of(segments(0), segments(1), segments(2))
    LocalDateTime.of(localDate, localTime)
  }

  def stringToTimestampWithoutTimeZone(s: UTF8String, allowTimeZone: Boolean): Option[Long] = {
    try {
      // `null` here means the string was unparseable (see `parseTimestampToLocalDateTime`).
      val localDateTime = parseTimestampToLocalDateTime(s, allowTimeZone)
      if (localDateTime == null) None else Some(localDateTimeToMicros(localDateTime))
    } catch {
      case NonFatal(_) => None
    }
  }

  /**
   * Trims and parses a given UTF8 string into a [[TimestampNanosVal]] (epoch microseconds plus a
   * sub-microsecond remainder in [0, 999]) for `TIMESTAMP_LTZ(precision)` with `precision` in [7,
   * 9]. Fractional digits beyond `precision` are truncated. The return type is [[Option]] in
   * order to distinguish between a valid zero value and null. Please refer to
   * `parseTimestampString` for the allowed formats.
   */
  def stringToTimestampLTZNanos(
      s: UTF8String,
      precision: Int,
      timeZoneId: ZoneId): Option[TimestampNanosVal] = {
    if (precision < 7 || precision > 9) {
      throw SparkException.internalError(
        s"stringToTimestampLTZNanos: precision $precision is out of range [7, 9]")
    }
    try {
      // `null` here means the string was unparseable (see `parseTimestampToInstant`). The shared
      // helper carries the full fraction in the `Instant`; `instantToTimestampNanos` then splits
      // it into (epochMicros, nanosWithinMicro) and applies the `precision` truncation.
      val instant = parseTimestampToInstant(s, timeZoneId)
      if (instant == null) None else Some(instantToTimestampNanos(instant, precision))
    } catch {
      case NonFatal(_) => None
    }
  }

  def stringToTimestampLTZNanosAnsi(
      s: UTF8String,
      precision: Int,
      timeZoneId: ZoneId,
      context: QueryContext = null): TimestampNanosVal = {
    stringToTimestampLTZNanos(s, precision, timeZoneId).getOrElse {
      throw ExecutionErrors.invalidInputInCastToDatetimeError(
        s,
        TimestampLTZNanosType(precision),
        context)
    }
  }

  /**
   * Trims and parses a given UTF8 string into a [[TimestampNanosVal]] (epoch microseconds plus a
   * sub-microsecond remainder in [0, 999]) for `TIMESTAMP_NTZ(precision)` with `precision` in [7,
   * 9]. Fractional digits beyond `precision` are truncated. The result is independent of time
   * zones; a time zone component is discarded when `allowTimeZone` is `true` and rejected
   * (returns `None`) otherwise. The return type is [[Option]] in order to distinguish between a
   * valid zero value and null. Please refer to `parseTimestampString` for the allowed formats.
   */
  def stringToTimestampNTZNanos(
      s: UTF8String,
      precision: Int,
      allowTimeZone: Boolean = true): Option[TimestampNanosVal] = {
    if (precision < 7 || precision > 9) {
      throw SparkException.internalError(
        s"stringToTimestampNTZNanos: precision $precision is out of range [7, 9]")
    }
    try {
      // `null` here means the string was unparseable (see `parseTimestampToLocalDateTime`). The
      // shared helper carries the full fraction in the `LocalDateTime`;
      // `localDateTimeToTimestampNanos` then splits it into (epochMicros, nanosWithinMicro) and
      // applies the `precision` truncation.
      val localDateTime = parseTimestampToLocalDateTime(s, allowTimeZone)
      if (localDateTime == null) {
        None
      } else {
        Some(localDateTimeToTimestampNanos(localDateTime, precision))
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  /**
   * ANSI variant of [[stringToTimestampNTZNanos]]. Throws
   * [[org.apache.spark.SparkDateTimeException]] on invalid input. Uses `allowTimeZone = true`: a
   * time zone component in the string is silently discarded rather than rejected. Callers that
   * need strict NTZ rejection should call [[stringToTimestampNTZNanos]] directly with
   * `allowTimeZone = false`.
   */
  def stringToTimestampNTZNanosAnsi(
      s: UTF8String,
      precision: Int,
      context: QueryContext = null): TimestampNanosVal = {
    // TODO(SPARK-57032): when this is wired to a user-facing CAST(... AS TIMESTAMP_NTZ(p)), the
    // cast must decide `allowTimeZone` explicitly (per ANSI/legacy mode) instead of relying on
    // the `true` default used here, which silently discards a zone suffix.
    stringToTimestampNTZNanos(s, precision).getOrElse {
      throw ExecutionErrors.invalidInputInCastToDatetimeError(
        s,
        TimestampNTZNanosType(precision),
        context)
    }
  }

  /**
   * Trims and parses a given UTF8 string to a corresponding [[Long]] value which representing the
   * number of microseconds since the midnight. The result will be independent of time zones.
   *
   * The return type is [[Option]] in order to distinguish between 0L and null. Please refer to
   * `parseTimestampString` for the allowed formats.
   */
  def stringToTime(s: UTF8String): Option[Long] = {
    try {
      // Check for the AM/PM suffix.
      val trimmed = s.trimRight
      val numChars = trimmed.numChars()
      var (isAM, isPM, hasSuffix) = (false, false, false)
      if (numChars > 2) {
        val lc = trimmed.getChar(numChars - 1)
        if (lc == 'M' || lc == 'm') {
          val slc = trimmed.getChar(numChars - 2)
          isAM = slc == 'A' || slc == 'a'
          isPM = slc == 'P' || slc == 'p'
          hasSuffix = isAM || isPM
        }
      }
      val timeString = if (hasSuffix) {
        trimmed.substring(0, numChars - 2)
      } else {
        trimmed
      }

      val (segments, zoneIdOpt, justTime) = parseTimestampString(timeString)

      // If the input string can't be parsed as a time, or it contains not only
      // the time part or has time zone information, return None.
      if (segments.isEmpty || !justTime || zoneIdOpt.isDefined) {
        return None
      }

      // Unpack the segments.
      var (hr, min, sec, ms) = (segments(3), segments(4), segments(5), segments(6))

      // Handle AM/PM conversion in separate cases.
      if (!hasSuffix) {
        // For 24-hour format, validate hour range: 0-23.
        if (hr < 0 || hr > 23) {
          return None
        }
      } else {
        // For 12-hour format, validate hour range: 1-12.
        if (hr < 1 || hr > 12) {
          return None
        }
        // For 12-hour format, convert to 24-hour format.
        if (isAM) {
          // AM: 12:xx:xx becomes 00:xx:xx, 1-11:xx:xx stays the same.
          if (hr == 12) {
            hr = 0
          }
        } else {
          // PM: 12:xx:xx stays 12:xx:xx, 1-11:xx:xx becomes 13-23:xx:xx.
          if (hr != 12) {
            hr += 12
          }
        }
      }

      val localTime = LocalTime.of(hr, min, sec, MICROSECONDS.toNanos(ms).toInt)
      Some(localTimeToNanos(localTime))
    } catch {
      case NonFatal(_) => None
    }
  }

  def stringToTimeAnsi(s: UTF8String, context: QueryContext = null): Long = {
    stringToTime(s).getOrElse {
      throw ExecutionErrors.invalidInputInCastToDatetimeError(s, TimeType(), context)
    }
  }

  /**
   * Returns the index of the first non-whitespace and non-ISO control character in the byte
   * array.
   *
   * @param bytes
   *   The byte array to be processed.
   * @return
   *   The start index after trimming.
   */
  @inline private def getTrimmedStart(bytes: Array[Byte]) = {
    var start = 0

    while (start < bytes.length && UTF8String.isWhitespaceOrISOControl(bytes(start))) {
      start += 1
    }

    start
  }

  /**
   * Returns the index of the last non-whitespace and non-ISO control character in the byte array.
   *
   * @param start
   *   The starting index for the search.
   * @param bytes
   *   The byte array to be processed.
   * @return
   *   The end index after trimming.
   */
  @inline private def getTrimmedEnd(start: Int, bytes: Array[Byte]) = {
    var end = bytes.length - 1

    while (end > start && UTF8String.isWhitespaceOrISOControl(bytes(end))) {
      end -= 1
    }

    end + 1
  }
}

object SparkDateTimeUtils extends SparkDateTimeUtils
