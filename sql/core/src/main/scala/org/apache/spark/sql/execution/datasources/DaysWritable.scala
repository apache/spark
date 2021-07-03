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

package org.apache.spark.sql.execution.datasources

import java.io.{DataInput, DataOutput, IOException}
import java.sql.Date

import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.hadoop.io.WritableUtils

import org.apache.spark.sql.catalyst.util.RebaseDateTime.{rebaseGregorianToJulianDays, rebaseJulianToGregorianDays}

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
class DaysWritable(
    var gregorianDays: Int,
    var julianDays: Int)
  extends DateWritable {

  def this() = this(0, 0)
  def this(gregorianDays: Int) =
    this(gregorianDays, rebaseGregorianToJulianDays(gregorianDays))
  def this(dateWritable: DateWritable) = {
    this(
      gregorianDays = dateWritable match {
        case daysWritable: DaysWritable => daysWritable.gregorianDays
        case dateWritable: DateWritable =>
        rebaseJulianToGregorianDays(dateWritable.getDays)
      },
      julianDays = dateWritable.getDays)
  }

  override def getDays: Int = julianDays
  override def get: Date = {
    new Date(DateWritable.daysToMillis(julianDays))
  }
  override def get(doesTimeMatter: Boolean): Date = {
    new Date(DateWritable.daysToMillis(julianDays, doesTimeMatter))
  }

  override def set(d: Int): Unit = {
    gregorianDays = d
    julianDays = rebaseGregorianToJulianDays(d)
  }

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    WritableUtils.writeVInt(out, julianDays)
  }

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    julianDays = WritableUtils.readVInt(in)
    gregorianDays = rebaseJulianToGregorianDays(julianDays)
  }
}
