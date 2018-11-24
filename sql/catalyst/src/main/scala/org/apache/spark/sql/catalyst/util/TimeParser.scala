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

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, ResolverStyle}
import java.time.temporal.ChronoField
import java.util.{Locale, TimeZone}

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.internal.SQLConf

sealed trait TimeParser {
  def toMicros(s: String): Long
}

class Iso8601TimeParser(pattern: String, timeZone: TimeZone, locale: Locale) extends TimeParser {
  val format = DateTimeFormatter.ofPattern(pattern)
    .withLocale(Locale.US)
    .withZone(timeZone.toZoneId)
    .withResolverStyle(ResolverStyle.SMART)

  def toMicros(s: String): Long = {
    val localDateTime = LocalDateTime.parse(s, format)
    val microOfSecond = localDateTime.getLong(ChronoField.MICRO_OF_SECOND)
    val epochSecond = localDateTime.atZone(timeZone.toZoneId).toInstant.getEpochSecond

    epochSecond * DateTimeUtils.MICROS_PER_SECOND + microOfSecond
  }
}

class LegacyTimeParser(pattern: String, timeZone: TimeZone, locale: Locale) extends TimeParser {
  val format = FastDateFormat.getInstance(pattern, timeZone, locale)

  def toMicros(s: String): Long = {
    format.parse(s).getTime * DateTimeUtils.MICROS_PER_MILLIS
  }
}

object TimeParser {
  def apply(format: String, timeZone: TimeZone, locale: Locale): TimeParser = {
    if (SQLConf.get.legacyTimeParserEnabled) {
      new LegacyTimeParser(format, timeZone, locale)
    } else {
      new Iso8601TimeParser(format, timeZone, locale)
    }
  }
}
