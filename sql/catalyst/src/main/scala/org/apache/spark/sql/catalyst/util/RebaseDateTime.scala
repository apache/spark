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

import java.time.{LocalDateTime, ZoneId}
import java.time.temporal.ChronoField
import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._

object RebaseDateTime {
  /**
   * Rebases days since the epoch from an original to an target calendar, for instance,
   * from a hybrid (Julian + Gregorian) to Proleptic Gregorian calendar.
   *
   * It finds the latest switch day which is less than `value`, and adds the difference
   * in days associated with the switch days to the given `value`.
   * The function is based on linear search which starts from the most recent switch days.
   * This allows to perform less comparisons for modern dates.
   *
   * @param switches The days when difference in days between original and target
   *                   calendar was changed.
   * @param diffs The differences in days between calendars.
   * @param value The number of days since the epoch 1970-01-01 to be rebased to the
   *             target calendar.
   * @return The rebased days.
   */
  private def rebaseDays(switches: Array[Int], diffs: Array[Int], value: Int): Int = {
    var i = switches.length
    do { i -= 1 } while (i > 0 && value < switches(i))
    value + diffs(i)
  }

  // The differences in days between Julian and Proleptic Gregorian dates.
  // The diff at the index `i` is applicable for all days in the date interval:
  // [julianGregDiffSwitchDay(i), julianGregDiffSwitchDay(i+1))
  private val julianGregDiffs = Array(2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, 0)
  // The sorted days in Julian calendar when difference in days between Julian and
  // Proleptic Gregorian calendars was changed.
  // The starting point is the `0001-01-01` (-719164 days since the epoch in
  // Julian calendar). All dates before the staring point have the same difference
  // of 2 days in Julian and Proleptic Gregorian calendars.
  private val julianGregDiffSwitchDay = Array(
    -719164, -682945, -646420, -609895, -536845, -500320, -463795,
    -390745, -354220, -317695, -244645, -208120, -171595, -141427)

  /**
   * Converts the given number of days since the epoch day 1970-01-01 to
   * a local date in Julian calendar, interprets the result as a local
   * date in Proleptic Gregorian calendar, and take the number of days
   * since the epoch from the Gregorian date.
   *
   * @param days The number of days since the epoch in Julian calendar.
   * @return The rebased number of days in Gregorian calendar.
   */
  def rebaseJulianToGregorianDays(days: Int): Int = {
    rebaseDays(julianGregDiffSwitchDay, julianGregDiffs, days)
  }

  // The differences in days between Proleptic Gregorian and Julian dates.
  // The diff at the index `i` is applicable for all days in the date interval:
  // [gregJulianDiffSwitchDay(i), gregJulianDiffSwitchDay(i+1))
  private val grepJulianDiffs = Array(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0)
  // The sorted days in Proleptic Gregorian calendar when difference in days between
  // Proleptic Gregorian and Julian was changed.
  // The starting point is the `0001-01-01` (-719162 days since the epoch in
  // Proleptic Gregorian calendar). All dates before the staring point have the same
  // difference of -2 days in Proleptic Gregorian and Julian calendars.
  private val gregJulianDiffSwitchDay = Array(
    -719162, -682944, -646420, -609896, -536847, -500323, -463799,
    -390750, -354226, -317702, -244653, -208129, -171605, -141427)

  /**
   * Rebasing days since the epoch to store the same number of days
   * as by Spark 2.4 and earlier versions. Spark 3.0 switched to
   * Proleptic Gregorian calendar (see SPARK-26651), and as a consequence of that,
   * this affects dates before 1582-10-15. Spark 2.4 and earlier versions use
   * Julian calendar for dates before 1582-10-15. So, the same local date may
   * be mapped to different number of days since the epoch in different calendars.
   *
   * For example:
   *   Proleptic Gregorian calendar: 1582-01-01 -> -141714
   *   Julian calendar: 1582-01-01 -> -141704
   * The code below converts -141714 to -141704.
   *
   * @param days The number of days since the epoch 1970-01-01. It can be negative.
   * @return The rebased number of days since the epoch in Julian calendar.
   */
  def rebaseGregorianToJulianDays(days: Int): Int = {
    rebaseDays(gregJulianDiffSwitchDay, grepJulianDiffs, days)
  }


  /**
   * Converts the given microseconds to a local date-time in UTC time zone in Proleptic Gregorian
   * calendar, interprets the result as a local date-time in Julian calendar in UTC time zone.
   * And takes microseconds since the epoch from the Julian timestamp.
   *
   * @param zoneId The time zone ID at which the rebasing should be performed.
   * @param micros The number of microseconds since the epoch '1970-01-01T00:00:00Z'.
   * @return The rebased microseconds since the epoch in Julian calendar.
   */
  def rebaseGregorianToJulianMicros(zoneId: ZoneId, micros: Long): Long = {
    val instant = microsToInstant(micros)
    val ldt = instant.atZone(zoneId).toLocalDateTime
    val cal = new Calendar.Builder()
      // `gregory` is a hybrid calendar that supports both
      // the Julian and Gregorian calendar systems
      .setCalendarType("gregory")
      .setDate(ldt.getYear, ldt.getMonthValue - 1, ldt.getDayOfMonth)
      .setTimeOfDay(ldt.getHour, ldt.getMinute, ldt.getSecond)
      // Local time-line can overlaps, such as at an autumn daylight savings cutover.
      // This setting selects the original local timestamp mapped to the given `micros`.
      .set(Calendar.DST_OFFSET, zoneId.getRules.getDaylightSavings(instant).toMillis.toInt)
      .build()
    millisToMicros(cal.getTimeInMillis) + ldt.get(ChronoField.MICRO_OF_SECOND)
  }

  /**
   * Rebases micros since the epoch from an original to an target calendar, for instance,
   * from a hybrid (Julian + Gregorian) to Proleptic Gregorian calendar.
   *
   * It finds the latest switch micros which is less than `value`, and adds the difference
   * in micros associated with the switch micros to the given `value`.
   * The function is based on linear search which starts from the most recent switch micros.
   * This allows to perform less comparisons for modern timestamps.
   *
   * @param switches The micros when difference in micros between original and target
   *                   calendar was changed.
   * @param diffs The differences in micros between calendars.
   * @param value The number of micros since the epoch 1970-01-01 to be rebased to the
   *             target calendar.
   * @return The rebased micros.
   */
  private def rebaseMicros(switches: Array[Long], diffs: Array[Long], value: Long): Long = {
    var i = switches.length
    do { i -= 1 } while (i > 0 && value < switches(i))
    value + diffs(i)
  }

  private val grepJulianDiffsMicros = Map(
    "America/Los_Angeles" -> Array(
      -172378000000L, -85978000000L, 422000000L, 86822000000L,
      173222000000L, 259622000000L, 346022000000L, 432422000000L,
      518822000000L, 605222000000L, 691622000000L, 778022000000L,
      864422000000L, 422000000L, 0L))
  private val gregJulianDiffSwitchMicros = Map(
    "America/Los_Angeles" -> Array(
      -62135568422000000L, -59006333222000000L, -55850659622000000L, -52694986022000000L,
      -46383552422000000L, -43227878822000000L, -40072205222000000L, -33760771622000000L,
      -30605098022000000L, -27449424422000000L, -21137990822000000L, -17982317222000000L,
      -14826643622000000L, -12219264422000000L, -2717638622000000L)
  )

  def rebaseGregorianToJulianMicros(micros: Long): Long = {
    val timeZone = TimeZone.getDefault
    val tzId = timeZone.getID
    val diffs = grepJulianDiffsMicros.get(tzId)
    if (diffs.isEmpty) {
      rebaseGregorianToJulianMicros(timeZone.toZoneId, micros)
    } else {
      rebaseMicros(gregJulianDiffSwitchMicros(tzId), diffs.get, micros)
    }
  }

  /**
   * Converts the given microseconds to a local date-time in UTC time zone in Julian calendar,
   * interprets the result as a local date-time in Proleptic Gregorian calendar in UTC time zone.
   * And takes microseconds since the epoch from the Gregorian timestamp.
   *
   * @param zoneId The time zone ID at which the rebasing should be performed.
   * @param micros The number of microseconds since the epoch '1970-01-01T00:00:00Z'.
   * @return The rebased microseconds since the epoch in Proleptic Gregorian calendar.
   */
  def rebaseJulianToGregorianMicros(zoneId: ZoneId, micros: Long): Long = {
    val cal = new Calendar.Builder()
      // `gregory` is a hybrid calendar that supports both
      // the Julian and Gregorian calendar systems
      .setCalendarType("gregory")
      .setInstant(microsToMillis(micros))
      .build()
    val localDateTime = LocalDateTime.of(
      cal.get(Calendar.YEAR),
      cal.get(Calendar.MONTH) + 1,
      // The number of days will be added later to handle non-existing
      // Julian dates in Proleptic Gregorian calendar.
      // For example, 1000-02-29 exists in Julian calendar because 1000
      // is a leap year but it is not a leap year in Gregorian calendar.
      1,
      cal.get(Calendar.HOUR_OF_DAY),
      cal.get(Calendar.MINUTE),
      cal.get(Calendar.SECOND),
      (Math.floorMod(micros, MICROS_PER_SECOND) * NANOS_PER_MICROS).toInt)
      .plusDays(cal.get(Calendar.DAY_OF_MONTH) - 1)
    val zonedDateTime = localDateTime.atZone(zoneId)
    // Zero DST offset means that local clocks have switched to the winter time already.
    // So, clocks go back one hour. We should correct zoned date-time and change
    // the zone offset to the later of the two valid offsets at a local time-line overlap.
    val adjustedZdt = if (cal.get(Calendar.DST_OFFSET) == 0) {
      zonedDateTime.withLaterOffsetAtOverlap()
    } else {
      zonedDateTime
    }
    instantToMicros(adjustedZdt.toInstant)
  }

  private val julianGrepDiffsMicros = Map(
    "America/Los_Angeles" -> Array(
      172378000000L, 85978000000L, -422000000L, -86822000000L,
      -173222000000L, -259622000000L, -346022000000L, -432422000000L,
      -518822000000L, -605222000000L, -691622000000L, -778022000000L,
      -864422000000L, -422000000L, 0L))
  private val julianGrepDiffSwitchMicros = Map(
    "America/Los_Angeles" -> Array(
      -62135740800000000L, -59006419200000000L, -55850659200000000L, -52694899200000000L,
      -46383379200000000L, -43227619200000000L, -40071859200000000L, -33760339200000000L,
      -30604579200000000L, -27448819200000000L, -21137299200000000L, -17981539200000000L,
      -14825779200000000L, -12219264000000000L, -2717640000000000L)
  )

  def rebaseJulianToGregorianMicros(micros: Long): Long = {
    val timeZone = TimeZone.getDefault
    val tzId = timeZone.getID
    val diffs = julianGrepDiffsMicros.get(tzId)
    if (diffs.isEmpty) {
      rebaseJulianToGregorianMicros(timeZone.toZoneId, micros)
    } else {
      rebaseMicros(julianGrepDiffSwitchMicros(tzId), diffs.get, micros)
    }
  }
}
