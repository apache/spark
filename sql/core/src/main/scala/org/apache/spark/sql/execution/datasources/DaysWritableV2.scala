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

import org.apache.hadoop.hive.common.`type`.Date
import org.apache.hadoop.hive.serde2.io.DateWritableV2

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
class DaysWritableV2 extends DateWritableV2 {
  private var gregorianDays: Date = null

  def this(d: Date) = {
    this()
    gregorianDays = d
    super.set(d)
  }

  def this(d: Int) = {
    this(Date.ofEpochDay(d))
  }

  def this(dateWritableV2: DateWritableV2) = {
    this()
    gregorianDays = dateWritableV2 match {
      case daysWritableV2: DaysWritableV2 => daysWritableV2.gregorianDays
      case dateWritableV2: DateWritableV2 =>
        Date.ofEpochDay(rebaseJulianToGregorianDays(dateWritableV2.getDays))
    }
    super.set(rebaseGregorianToJulianDays(gregorianDays.toEpochDay))
  }

  def getGregorianDays: Int = gregorianDays.toEpochDay

  override def getDays: Int = super.getDays

  override def get: Date = {
    super.get()
  }

  override def set(d: Date): Unit = {
    gregorianDays = d
    super.set(d.toEpochDay)
  }

  override def set(d: Int): Unit = {
    gregorianDays = Date.ofEpochDay(d)
    super.set(d)
  }

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    super.write(out)
  }

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    super.readFields(in)
    gregorianDays = Date.ofEpochDay(rebaseJulianToGregorianDays(getDays))
  }

}
