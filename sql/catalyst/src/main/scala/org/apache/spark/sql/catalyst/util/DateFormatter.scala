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

import java.time.{Instant, ZoneId}
import java.util.Locale

import scala.util.Try

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.internal.SQLConf

sealed trait DateFormatter extends Serializable {
  def parse(s: String): Int // returns days since epoch
  def format(days: Int): String
}

class Iso8601DateFormatter(
    pattern: String,
    locale: Locale) extends DateFormatter with DateTimeFormatterHelper {

  @transient
  private lazy val formatter = buildFormatter(pattern, locale)
  private val UTC = ZoneId.of("UTC")

  private def toInstant(s: String): Instant = {
    val temporalAccessor = formatter.parse(s)
    toInstantWithZoneId(temporalAccessor, UTC)
  }

  override def parse(s: String): Int = {
    val seconds = toInstant(s).getEpochSecond
    val days = Math.floorDiv(seconds, DateTimeUtils.SECONDS_PER_DAY)
    days.toInt
  }

  override def format(days: Int): String = {
    val instant = Instant.ofEpochSecond(days * DateTimeUtils.SECONDS_PER_DAY)
    formatter.withZone(UTC).format(instant)
  }
}

class LegacyDateFormatter(pattern: String, locale: Locale) extends DateFormatter {
  @transient
  private lazy val format = FastDateFormat.getInstance(pattern, locale)

  override def parse(s: String): Int = {
    val milliseconds = format.parse(s).getTime
    DateTimeUtils.millisToDays(milliseconds)
  }

  override def format(days: Int): String = {
    val date = DateTimeUtils.toJavaDate(days)
    format.format(date)
  }
}

class LegacyFallbackDateFormatter(
    pattern: String,
    locale: Locale) extends LegacyDateFormatter(pattern, locale) {
  override def parse(s: String): Int = {
    Try(super.parse(s)).orElse {
      // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
      // compatibility.
      Try(DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(s).getTime))
    }.getOrElse {
      // In Spark 1.5.0, we store the data as number of days since epoch in string.
      // So, we just convert it to Int.
      s.toInt
    }
  }
}

object DateFormatter {
  def apply(format: String, locale: Locale): DateFormatter = {
    if (SQLConf.get.legacyTimeParserEnabled) {
      new LegacyFallbackDateFormatter(format, locale)
    } else {
      new Iso8601DateFormatter(format, locale)
    }
  }
}
