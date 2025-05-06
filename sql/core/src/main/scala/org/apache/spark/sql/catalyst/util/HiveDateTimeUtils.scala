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
import java.time.ZoneId

import org.apache.hadoop.hive.common.`type`.{Date => HiveDate, Timestamp => HiveTimestamp}

import org.apache.spark.sql.catalyst.util.RebaseDateTime

object HiveDateTimeUtils {
  private val zoneId = ZoneId.systemDefault()

  private def toSqlTimestamp(t: HiveTimestamp): SqlTimestamp = {
    val ts = new SqlTimestamp(t.toEpochMilli(zoneId))
    ts.setNanos (t.getNanos)
    ts
  }

  def fromHiveTimestamp(t: HiveTimestamp): Long = {
    DateTimeUtils.fromJavaTimestamp(toSqlTimestamp(t))
  }

  def fromHiveDate(d: HiveDate): Int = {
    d.toEpochDay
  }

  def toHiveTimestamp(t: Long): HiveTimestamp = {
    val javaTimestamp = DateTimeUtils.toJavaTimestamp(t)
    val hiveTimestamp = HiveTimestamp.ofEpochMilli(javaTimestamp.getTime, zoneId)
    hiveTimestamp.setNanos(javaTimestamp.getNanos)
    hiveTimestamp
  }

  def toHiveDate(d: Int): HiveDate = {
    val julian = RebaseDateTime.rebaseGregorianToJulianDays(d)
    HiveDate.ofEpochDay(julian)
  }

}
