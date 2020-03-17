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

package org.apache.spark.sql.hive

import java.io.{DataInput, DataOutput, IOException}
import java.sql.Date
import java.time.LocalDate
import java.util.Calendar

import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.hadoop.io.WritableUtils

import org.apache.spark.sql.catalyst.util.{DateTimeConstants, DateTimeUtils}

/**
 * The class accepts/returns days in Gregorian calendar and rebase them
 * via conversion to local date in Julian calendar for dates before 1582-10-15
 * in read/write for backward compatibility with Spark 2.4 and earlier versions.
 *
 * @param gregorianDays The number of days since the epoch 1970-01-01 in
 *                      Gregorian calendar.
 * @param julianDays The number of days since the epoch 1970-01-01 in
 *                   Julian calendar.
 */
private[hive] class DaysWritable(
    var gregorianDays: Int,
    var julianDays: Int)
  extends DateWritable {

  def this(gregorianDays: Int) =
    this(gregorianDays, DaysWritable.rebaseGregorianToJulianDays(gregorianDays))
  def this(dateWritable: DateWritable) = {
    this(
      gregorianDays = dateWritable match {
        case daysWritable: DaysWritable => daysWritable.gregorianDays
        case dateWritable: DateWritable =>
        DaysWritable.rebaseJulianToGregorianDays(dateWritable.getDays)
      },
      julianDays = dateWritable.getDays)
  }

  override def getDays: Int = julianDays
  override def get(): Date = new Date(DateWritable.daysToMillis(julianDays))

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    WritableUtils.writeVInt(out, julianDays)
  }

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    julianDays = WritableUtils.readVInt(in)
    gregorianDays = DaysWritable.rebaseJulianToGregorianDays(julianDays)
  }
}

private[hive] object DaysWritable {
  // Rebasing days since the epoch to store the same number of days
  // as by Spark 2.4 and earlier versions. Spark 3.0 switched to
  // Proleptic Gregorian calendar (see SPARK-26651), and as a consequence of that,
  // this affects dates before 1582-10-15. Spark 2.4 and earlier versions use
  // Julian calendar for dates before 1582-10-15. So, the same local date may
  // be mapped to different number of days since the epoch in different calendars.
  // For example:
  // Proleptic Gregorian calendar: 1582-01-01 -> -141714
  // Julian calendar: 1582-01-01 -> -141704
  // The code below converts -141714 to -141704.
  def rebaseGregorianToJulianDays(daysSinceEpoch: Int): Int = {
    if (daysSinceEpoch < DateTimeUtils.GREGORIAN_CUTOVER_DAY) {
      val millis = Math.multiplyExact(daysSinceEpoch, DateTimeConstants.MILLIS_PER_DAY)
      val utcCal = new Calendar.Builder()
        .setCalendarType("gregory")
        .setTimeZone(DateTimeUtils.TimeZoneUTC)
        .setInstant(millis)
        .build()
      val localDate = LocalDate.of(
        utcCal.get(Calendar.YEAR),
        utcCal.get(Calendar.MONTH) + 1,
        utcCal.get(Calendar.DAY_OF_MONTH))
      Math.toIntExact(localDate.toEpochDay)
    } else {
      daysSinceEpoch
    }
  }

  def rebaseJulianToGregorianDays(daysSinceEpoch: Int): Int = {
    if (daysSinceEpoch < JULIAN_CUTOVER_DAY) {
      val localDate = LocalDate.ofEpochDay(daysSinceEpoch)
      val utcCal = new Calendar.Builder()
        .setCalendarType("gregory")
        .setTimeZone(DateTimeUtils.TimeZoneUTC)
        .setDate(localDate.getYear, localDate.getMonthValue - 1, localDate.getDayOfMonth)
        .build()
      Math.toIntExact(Math.floorDiv(utcCal.getTimeInMillis, DateTimeConstants.MILLIS_PER_DAY))
    } else {
      daysSinceEpoch
    }
  }

  final val JULIAN_CUTOVER_DAY =
    rebaseGregorianToJulianDays(DateTimeUtils.GREGORIAN_CUTOVER_DAY.toInt)
}
