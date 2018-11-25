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

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalQueries
import java.util.{Locale, TimeZone}

import scala.util.Try

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.internal.SQLConf

sealed trait TimeParser {
  def toMillis(s: String): Long
  def toMicros(s: String): Long
  def toDays(s: String): Int

  def fromMicros(us: Long): String
}

class Iso8601TimeParser(pattern: String, timeZone: TimeZone, locale: Locale) extends TimeParser {
  val formatter = DateTimeFormatter.ofPattern(pattern, locale)

  def toInstant(s: String): Instant = {
    val temporalAccessor = formatter.parse(s)
    if (temporalAccessor.query(TemporalQueries.offset()) == null) {
      val localDateTime = LocalDateTime.from(temporalAccessor)
      val zonedDateTime = ZonedDateTime.of(localDateTime, timeZone.toZoneId)
      Instant.from(zonedDateTime)
    } else {
      Instant.from(temporalAccessor)
    }
  }

  def conv(instant: Instant, secMul: Long, nanoDiv: Long): Long = {
    val sec = Math.multiplyExact(instant.getEpochSecond, secMul)
    val result = Math.addExact(sec, instant.getNano / nanoDiv)
    result
  }

  def toMillis(s: String): Long = conv(toInstant(s), 1000, 1000000)

  def toMicros(s: String): Long = conv(toInstant(s), 1000000, 1000)

  def toDays(s: String): Int = {
    val instant = toInstant(s)
    (instant.getEpochSecond / DateTimeUtils.SECONDS_PER_DAY).toInt
  }

  def fromMicros(us: Long): String = {
    val secs = Math.floorDiv(us, 1000000)
    val mos = Math.floorMod(us, 1000000)
    val instant = Instant.ofEpochSecond(secs, mos * 1000)

    formatter.withZone(timeZone.toZoneId).format(instant)
  }
}

class LegacyTimeParser(pattern: String, timeZone: TimeZone, locale: Locale) extends TimeParser {
  val format = FastDateFormat.getInstance(pattern, timeZone, locale)

  def toMillis(s: String): Long = format.parse(s).getTime

  def toMicros(s: String): Long = toMillis(s) * DateTimeUtils.MICROS_PER_MILLIS

  def toDays(s: String): Int = DateTimeUtils.millisToDays(toMillis(s))

  def fromMicros(us: Long): String = {
    format.format(DateTimeUtils.toJavaTimestamp(us))
  }
}

class LegacyFallbackTimeParser(
    pattern: String,
    timeZone: TimeZone,
    locale: Locale) extends LegacyTimeParser(pattern, timeZone, locale) {
  override def toMillis(s: String): Long = {
    Try {super.toMillis(s)}.getOrElse(DateTimeUtils.stringToTime(s).getTime)
  }
}

object TimeParser {
  def apply(format: String, timeZone: TimeZone, locale: Locale): TimeParser = {
    if (SQLConf.get.legacyTimeParserEnabled) {
      new LegacyFallbackTimeParser(format, timeZone, locale)
    } else {
      new Iso8601TimeParser(format, timeZone, locale)
    }
  }
}
