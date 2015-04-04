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

package org.apache.spark.sql.types

import java.sql.Date
import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.catalyst.expressions.Cast

/**
 * helper function to convert between Int value of days since 1970-01-01 and java.sql.Date
 */
object DateUtils {
  private val MILLIS_PER_DAY = 86400000

  // Java TimeZone has no mention of thread safety. Use thread local instance to be safe.
  private val LOCAL_TIMEZONE = new ThreadLocal[TimeZone] {
    override protected def initialValue: TimeZone = {
      Calendar.getInstance.getTimeZone
    }
  }

  private def javaDateToDays(d: Date): Int = {
    millisToDays(d.getTime)
  }

  def millisToDays(millisLocal: Long): Int = {
    ((millisLocal + LOCAL_TIMEZONE.get().getOffset(millisLocal)) / MILLIS_PER_DAY).toInt
  }

  private def toMillisSinceEpoch(days: Int): Long = {
    val millisUtc = days.toLong * MILLIS_PER_DAY
    millisUtc - LOCAL_TIMEZONE.get().getOffset(millisUtc)
  }

  def fromJavaDate(date: java.sql.Date): Int = {
    javaDateToDays(date)
  }

  def toJavaDate(daysSinceEpoch: Int): java.sql.Date = {
    new java.sql.Date(toMillisSinceEpoch(daysSinceEpoch))
  }

  def toString(days: Int): String = Cast.threadLocalDateFormat.get.format(toJavaDate(days))
}
