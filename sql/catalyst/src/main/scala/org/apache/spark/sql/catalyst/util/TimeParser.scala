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

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.{ChronoField, ChronoUnit}
import java.util.{Locale, TimeZone}

import scala.util.Try
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.internal.SQLConf

sealed trait TimeParser {
  def toMillis(s: String): Long
  def toMicros(s: String): Long
  def toDays(s: String): Int
}

class Iso8601TimeParser(pattern: String, timeZone: TimeZone, locale: Locale) extends TimeParser {
  val formatter = DateTimeFormatter.ofPattern(pattern)
    .withLocale(locale)
    .withZone(timeZone.toZoneId)
    .withResolverStyle(ResolverStyle.SMART)

  // Seconds since 1970-01-01T00:00:00
  val epoch = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)

  def toMillis(s: String): Long = {
    val localDateTime = LocalDateTime.parse(s, formatter)
    ChronoUnit.MILLIS.between(epoch, localDateTime)
  }

  def toMicros(s: String): Long = {
    val localDateTime = LocalDateTime.parse(s, formatter)
    ChronoUnit.MICROS.between(epoch, localDateTime)
  }

  def toDays(s: String): Int = {
    val localDateTime = LocalDateTime.parse(s, formatter)
    ChronoUnit.DAYS.between(epoch, localDateTime).toInt
  }
}

class LegacyTimeParser(pattern: String, timeZone: TimeZone, locale: Locale) extends TimeParser {
  val format = FastDateFormat.getInstance(pattern, timeZone, locale)

  def toMillis(s: String): Long = format.parse(s).getTime

  def toMicros(s: String): Long = toMillis(s) * DateTimeUtils.MICROS_PER_MILLIS

  def toDays(s: String): Int = DateTimeUtils.millisToDays(toMillis(s))
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
