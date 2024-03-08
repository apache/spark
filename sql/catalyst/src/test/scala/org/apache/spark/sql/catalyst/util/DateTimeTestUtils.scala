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

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId, ZoneOffset}
import java.util.TimeZone
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getZoneId

/**
 * Helper functions for testing date and time functionality.
 */
object DateTimeTestUtils {

  val CEST = getZoneId("+02:00")
  val CET = getZoneId("+01:00")
  val JST = getZoneId("+09:00")
  val LA = getZoneId("America/Los_Angeles")
  val MIT = getZoneId("-09:30")
  val PST = getZoneId("-08:00")
  val UTC = getZoneId("+00:00")

  val UTC_OPT = Option("UTC")

  val ALL_TIMEZONES: Seq[ZoneId] = ZoneId.getAvailableZoneIds.asScala.map(getZoneId).toSeq

  val outstandingTimezonesIds: Seq[String] = Seq(
    "UTC",
    PST.getId,
    CET.getId,
    "Africa/Dakar",
    LA.getId,
    "Asia/Urumqi",
    "Asia/Hong_Kong",
    "Europe/Brussels")
  val outstandingZoneIds: Seq[ZoneId] = outstandingTimezonesIds.map(getZoneId)

  def withDefaultTimeZone[T](newDefaultTimeZone: ZoneId)(block: => T): T = {
    val originalDefaultTimeZone = ZoneId.systemDefault()
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(newDefaultTimeZone))
      block
    } finally {
      TimeZone.setDefault(TimeZone.getTimeZone(originalDefaultTimeZone))
    }
  }

  def localDateTimeToMicros(localDateTime: LocalDateTime, zoneId: ZoneId): Long = {
    val instant = localDateTime.atZone(zoneId).toInstant
    DateTimeUtils.instantToMicros(instant)
  }

  // Returns microseconds since epoch for the given date
  def date(
      year: Int,
      month: Byte = 1,
      day: Byte = 1,
      hour: Byte = 0,
      minute: Byte = 0,
      sec: Byte = 0,
      micros: Int = 0,
      zid: ZoneId = ZoneOffset.UTC): Long = {
    val nanos = TimeUnit.MICROSECONDS.toNanos(micros).toInt
    val localDateTime = LocalDateTime.of(year, month, day, hour, minute, sec, nanos)
    localDateTimeToMicros(localDateTime, zid)
  }

  // Returns number of days since epoch for the given date
  def days(
      year: Int,
      month: Byte = 1,
      day: Byte = 1): Int = {
    LocalDate.of(year, month, day).toEpochDay.toInt
  }

  // Returns microseconds since epoch for current date and give time
  def time(
      hour: Byte = 0,
      minute: Byte = 0,
      sec: Byte = 0,
      micros: Int = 0,
      zid: ZoneId = ZoneOffset.UTC): Long = {
    val nanos = TimeUnit.MICROSECONDS.toNanos(micros).toInt
    val localDate = LocalDate.now(zid)
    val localTime = LocalTime.of(hour, minute, sec, nanos)
    val localDateTime = LocalDateTime.of(localDate, localTime)
    localDateTimeToMicros(localDateTime, zid)
  }

  def secFrac(seconds: Int, milliseconds: Int, microseconds: Int): Long = {
    var result: Long = microseconds
    result = Math.addExact(result, Math.multiplyExact(milliseconds, MICROS_PER_MILLIS))
    result = Math.addExact(result, Math.multiplyExact(seconds, MICROS_PER_SECOND))
    result
  }
}
