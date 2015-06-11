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

import java.sql.{Timestamp, Date}
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.catalyst.expressions.Cast

/**
 * Helper function to convert between Int value of days since 1970-01-01 and java.sql.Date
 */
object DateTimeUtils {
  private val MILLIS_PER_DAY = 86400000
  private val HUNDRED_NANOS_PER_SECOND = 10000000L

  // Java TimeZone has no mention of thread safety. Use thread local instance to be safe.
  private val LOCAL_TIMEZONE = new ThreadLocal[TimeZone] {
    override protected def initialValue: TimeZone = {
      Calendar.getInstance.getTimeZone
    }
  }

  private def javaDateToDays(d: Date): Int = {
    millisToDays(d.getTime)
  }

  // we should use the exact day as Int, for example, (year, month, day) -> day
  def millisToDays(millisLocal: Long): Int = {
    ((millisLocal + LOCAL_TIMEZONE.get().getOffset(millisLocal)) / MILLIS_PER_DAY).toInt
  }

  def toMillisSinceEpoch(days: Int): Long = {
    val millisUtc = days.toLong * MILLIS_PER_DAY
    millisUtc - LOCAL_TIMEZONE.get().getOffset(millisUtc)
  }

  def fromJavaDate(date: Date): Int = {
    javaDateToDays(date)
  }

  def toJavaDate(daysSinceEpoch: Int): Date = {
    new Date(toMillisSinceEpoch(daysSinceEpoch))
  }

  def toString(days: Int): String = Cast.threadLocalDateFormat.get.format(toJavaDate(days))

  def stringToTime(s: String): java.util.Date = {
    if (!s.contains('T')) {
      // JDBC escape string
      if (s.contains(' ')) {
        Timestamp.valueOf(s)
      } else {
        Date.valueOf(s)
      }
    } else if (s.endsWith("Z")) {
      // this is zero timezone of ISO8601
      stringToTime(s.substring(0, s.length - 1) + "GMT-00:00")
    } else if (s.indexOf("GMT") == -1) {
      // timezone with ISO8601
      val inset = "+00.00".length
      val s0 = s.substring(0, s.length - inset)
      val s1 = s.substring(s.length - inset, s.length)
      if (s0.substring(s0.lastIndexOf(':')).contains('.')) {
        stringToTime(s0 + "GMT" + s1)
      } else {
        stringToTime(s0 + ".0GMT" + s1)
      }
    } else {
      // ISO8601 with GMT insert
      val ISO8601GMT: SimpleDateFormat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSz" )
      ISO8601GMT.parse(s)
    }
  }

  /**
   * Return a java.sql.Timestamp from number of 100ns since epoch
   */
  def toJavaTimestamp(num100ns: Long): Timestamp = {
    // setNanos() will overwrite the millisecond part, so the milliseconds should be
    // cut off at seconds
    var seconds = num100ns / HUNDRED_NANOS_PER_SECOND
    var nanos = num100ns % HUNDRED_NANOS_PER_SECOND
    // setNanos() can not accept negative value
    if (nanos < 0) {
      nanos += HUNDRED_NANOS_PER_SECOND
      seconds -= 1
    }
    val t = new Timestamp(seconds * 1000)
    t.setNanos(nanos.toInt * 100)
    t
  }

  /**
   * Return the number of 100ns since epoch from java.sql.Timestamp.
   */
  def fromJavaTimestamp(t: Timestamp): Long = {
    if (t != null) {
      t.getTime() * 10000L + (t.getNanos().toLong / 100) % 10000L
    } else {
      0L
    }
  }
}
