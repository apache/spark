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

import scala.collection.mutable.AnyRefMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._

/**
 * The collection of functions for rebasing days and microseconds from/to the hybrid calendar
 * (Julian + Gregorian since 1582-10-15) which is used by Spark 2.4 and earlier versions
 * to/from Proleptic Gregorian calendar used in Spark SQL since version 3.0, see SPARK-26651.
 */
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
   * @param switches The days when difference in days between original
   *                 and target calendar was changed.
   * @param diffs The differences in days between calendars.
   * @param days The number of days since the epoch 1970-01-01 to be rebased
   *             to the target calendar.
   * @return The rebased days.
   */
  private def rebaseDays(switches: Array[Int], diffs: Array[Int], days: Int): Int = {
    var i = switches.length
    do { i -= 1 } while (i > 0 && days < switches(i))
    days + diffs(i)
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
  private val gregJulianDiffs = Array(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0)
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
    rebaseDays(gregJulianDiffSwitchDay, gregJulianDiffs, days)
  }

  /**
   * Converts the given microseconds to a local date-time in UTC time zone in Proleptic Gregorian
   * calendar, interprets the result as a local date-time in Julian calendar in UTC time zone.
   * And takes microseconds since the epoch from the Julian timestamp.
   *
   * @param zoneId The time zone ID at which the rebasing should be performed.
   * @param micros The number of microseconds since the epoch '1970-01-01T00:00:00Z'
   *               in Proleptic Gregorian calendar. It can be negative.
   * @return The rebased microseconds since the epoch in Julian calendar.
   */
  private[sql] def rebaseGregorianToJulianMicros(zoneId: ZoneId, micros: Long): Long = {
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
   * The class describes JSON records with microseconds rebasing info.
   * Here is an example of JSON file:
   * {{{
   *   [
   *     {
   *       "tz": "Europe/Paris",
   *       "switches": [-123, 0],
   *       "diffs": [422000000, 0]
   *     }
   *   ]
   * }}}
   *
   * @param tz One of time zone ID which is expected to be acceptable by `ZoneId.of`.
   * @param switches An ordered array of seconds since the epoch when the diff between
   *                 two calendars are changed.
   * @param diffs Differences in seconds associated with elements of `switches`.
   */
  private case class JsonRebaseRecord(tz: String, switches: Array[Long], diffs: Array[Long])

  /**
   * Rebasing info used to convert microseconds from an original to a target calendar.
   *
   * @param switches An ordered array of microseconds since the epoch when the diff between
   *                 two calendars are changed.
   * @param diffs Differences in microseconds associated with elements of `switches`.
   */
  private[sql] case class RebaseInfo(switches: Array[Long], diffs: Array[Long])

  /**
   * Rebases micros since the epoch from an original to an target calendar, for instance,
   * from a hybrid (Julian + Gregorian) to Proleptic Gregorian calendar.
   *
   * It finds the latest switch micros which is less than `value`, and adds the difference
   * in micros associated with the switch micros to the given `value`.
   * The function is based on linear search which starts from the most recent switch micros.
   * This allows to perform less comparisons for modern timestamps.
   *
   * @param rebaseInfo The rebasing info contains an ordered micros when difference in micros
   *                   between original and target calendar was changed,
   *                   and differences in micros between calendars
   * @param micros The number of micros since the epoch 1970-01-01T00:00:00Z to be rebased
   *               to the target calendar. It can be negative.
   * @return The rebased micros.
   */
  private def rebaseMicros(rebaseInfo: RebaseInfo, micros: Long): Long = {
    val switches = rebaseInfo.switches
    var i = switches.length
    do { i -= 1 } while (i > 0 && micros < switches(i))
    micros + rebaseInfo.diffs(i)
  }

  // Loads rebasing info from an JSON file. JSON records in the files should conform to
  // `JsonRebaseRecord`. AnyRefMap is used here instead of Scala's immutable map because
  // it is 2 times faster in DateTimeRebaseBenchmark.
  private[sql] def loadRebaseRecords(fileName: String): AnyRefMap[String, RebaseInfo] = {
    val file = Thread.currentThread().getContextClassLoader.getResource(fileName)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val jsonRebaseRecords = mapper.readValue[Seq[JsonRebaseRecord]](file)
    val anyRefMap = new AnyRefMap[String, RebaseInfo]((3 * jsonRebaseRecords.size) / 2)
    jsonRebaseRecords.foreach { jsonRecord =>
      val rebaseInfo = RebaseInfo(jsonRecord.switches, jsonRecord.diffs)
      var i = 0
      while (i < rebaseInfo.switches.length) {
        rebaseInfo.switches(i) = rebaseInfo.switches(i) * MICROS_PER_SECOND
        rebaseInfo.diffs(i) = rebaseInfo.diffs(i) * MICROS_PER_SECOND
        i += 1
      }
      anyRefMap.update(jsonRecord.tz, rebaseInfo)
    }
    anyRefMap
  }

  /**
   * A map of time zone IDs to its ordered time points (instants in microseconds since the epoch)
   * when the difference between 2 instances associated with the same local timestamp in
   * Proleptic Gregorian and the hybrid calendar was changed, and to the diff at the index `i` is
   * applicable for all microseconds in the time interval:
   *   [gregJulianDiffSwitchMicros(i), gregJulianDiffSwitchMicros(i+1))
   */
  private val gregJulianRebaseMap = loadRebaseRecords("gregorian-julian-rebase-micros.json")

  /**
   * Rebases the given `micros` to the number of microseconds since the epoch via a local
   * timestamp that have the same date-time fields in Proleptic Gregorian and in the hybrid
   * calendars.
   *
   * The function may optimize the rebasing by using pre-calculated rebasing maps. If the maps
   * don't contains information about the current JVM system time zone, the functions falls back
   * to regular conversion mechanism via building local timestamps.
   *
   * Note: The function assumes that the input micros was derived from a local timestamp
   *       at the default system JVM time zone in Proleptic Gregorian calendar.
   *
   * @param micros The microseconds since the epoch 1970-01-01T00:00:00Z
   *               in Proleptic Gregorian calendar. It can be negative.
   * @return The microseconds since the epoch that have the same local timestamp representation
   *         in the hybrid calendar (Julian + Gregorian) as the original `micros` in
   *         Proleptic Gregorian calendar.
   */
  def rebaseGregorianToJulianMicros(micros: Long): Long = {
    val timeZone = TimeZone.getDefault
    val tzId = timeZone.getID
    val rebaseRecord = gregJulianRebaseMap.getOrNull(tzId)
    if (rebaseRecord == null) {
      rebaseGregorianToJulianMicros(timeZone.toZoneId, micros)
    } else {
      rebaseMicros(rebaseRecord, micros)
    }
  }

  /**
   * Converts the given microseconds to a local date-time in UTC time zone in the hybrid
   * calendar (Julian + Gregorian since 1582-10-15), interprets the result as a local date-time
   * in Proleptic Gregorian calendar in UTC time zone. And takes microseconds since the epoch
   * from the Gregorian timestamp.
   *
   * @param zoneId The time zone ID at which the rebasing should be performed.
   * @param micros The number of microseconds since the epoch '1970-01-01T00:00:00Z'
   *               in the hybrid calendar (Julian + Gregorian). It can be negative.
   * @return The rebased microseconds since the epoch in Proleptic Gregorian calendar.
   */
  private[sql] def rebaseJulianToGregorianMicros(zoneId: ZoneId, micros: Long): Long = {
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

  // The rebasing maps to convert microseconds from the hybrid calendar (Julian + Gregorian)
  // to Proleptic Gregorian calendar. It maps time zone IDs to ordered timestamps (ascending order)
  // where at every timestamps the difference between 2 calendars was changed, and to ordered
  // differences between 2 calendars. The diff at the index `i` is applicable for all timestamps
  // in the interval: [julianGregDiffSwitchMicros(i), julianGregDiffSwitchMicros(i+1))
  private val julianGregRebaseMap = loadRebaseRecords("julian-gregorian-rebase-micros.json")

  /**
   * This is an opposite to `rebaseGregorianToJulianMicros` function which rebases the given
   * microseconds since the epoch 1970-01-01T00:00:00Z via local timestamps from the
   * hybrid calendar (Julian + Gregorian) to Proleptic Gregorian calendar.
   * For example, the input `micros` -12243196799876544 is mapped to 1582-01-01 00:00:00.123456 in
   * Julian calendar. The same local timestamp in Proleptic Gregorian calendar is mapped to
   * the different number of micros -12244061221876544 since the epoch. Semantically, the function
   * performs such conversion either via direct conversion to local timestamps,
   * or via pre-calculated rebasing tables.
   *
   * Note: The function assumes that the input micros was derived from a local timestamp
   *       at the default system JVM time zone in the hybrid calendar (Julian and Gregorian
   *       since 1582-10-15)
   *
   * @param micros The number of microseconds since the epoch 1970-01-01T00:00:00Z
   *               in the hybrid calendar (Julian + Gregorian). It can be negative.
   * @return The rebased number of microseconds since the epoch which is mapped to the same
   *         local timestamp in Proleptic Gregorian calendar as `micros` in the hybrid calendar
   *         at the system time zone.
   */
  def rebaseJulianToGregorianMicros(micros: Long): Long = {
    val timeZone = TimeZone.getDefault
    val tzId = timeZone.getID
    val rebaseRecord = julianGregRebaseMap.getOrNull(tzId)
    if (rebaseRecord == null) {
      rebaseJulianToGregorianMicros(timeZone.toZoneId, micros)
    } else {
      rebaseMicros(rebaseRecord, micros)
    }
  }
}
