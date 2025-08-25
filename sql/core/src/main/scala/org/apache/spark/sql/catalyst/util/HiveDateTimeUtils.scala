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

import java.sql.{Timestamp => SqlTimestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.ZoneOffset.UTC

import org.apache.hadoop.hive.common.`type`.{Date => HiveDate, Timestamp => HiveTimestamp}

import org.apache.spark.sql.catalyst.util.RebaseDateTime.rebaseJulianToGregorianDays
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils.instantToMicros

object HiveDateTimeUtils {
  private val zoneId = ZoneId.systemDefault()

  private def toSqlTimestamp(t: HiveTimestamp): SqlTimestamp = {
    val millis = t.toEpochMilli(zoneId)
    val ts = new SqlTimestamp(millis)
    ts.setNanos(t.getNanos)
    ts
  }

  def fromHiveTimestamp(t: HiveTimestamp): Long = {
    // get Hive localDateTime
    var localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(t.toEpochSecond, t.getNanos),
      UTC)
    val julianDate = rebaseJulianToGregorianDays(localDateTime.toLocalDate.toEpochDay.toInt)
    localDateTime = LocalDateTime.of(LocalDate.ofEpochDay(julianDate), localDateTime.toLocalTime)
    instantToMicros(localDateTime.toInstant(zoneId.getRules.getOffset(localDateTime)))
  }

  def fromHiveDate(d: HiveDate): Int = {
    d.toEpochDay
  }

  def toHiveTimestamp(t: Long): HiveTimestamp = {
    val javaTimestamp = DateTimeUtils.toJavaTimestamp(t)
    val hiveTimestamp = new HiveTimestamp(javaTimestamp.toLocalDateTime)
    // hiveTimestamp.setNanos(javaTimestamp.getNanos)
    hiveTimestamp
  }

  def toHiveDate(d: Int): HiveDate = {
    // val julian = RebaseDateTime.rebaseGregorianToJulianDays(d)
    HiveDate.ofEpochDay(d)
  }

}
