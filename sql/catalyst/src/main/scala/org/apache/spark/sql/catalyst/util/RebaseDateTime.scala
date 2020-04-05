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
}
