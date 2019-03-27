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

import java.util.{Locale, TimeZone}

import scala.util.Try

import org.apache.commons.lang3.time.FastDateFormat

sealed trait DateTimeFormatter {
  def parse(s: String): Long // returns microseconds since epoch
  def format(us: Long): String
}

class LegacyDateTimeFormatter(
    pattern: String,
    timeZone: TimeZone,
    locale: Locale) extends DateTimeFormatter {
  val format = FastDateFormat.getInstance(pattern, timeZone, locale)

  protected def toMillis(s: String): Long = format.parse(s).getTime

  override def parse(s: String): Long = toMillis(s) * DateTimeUtils.MICROS_PER_MILLIS

  override def format(us: Long): String = {
    format.format(DateTimeUtils.toJavaTimestamp(us))
  }
}

class LegacyFallbackDateTimeFormatter(
    pattern: String,
    timeZone: TimeZone,
    locale: Locale) extends LegacyDateTimeFormatter(pattern, timeZone, locale) {
  override def toMillis(s: String): Long = {
    Try {super.toMillis(s)}.getOrElse(DateTimeUtils.stringToTime(s).getTime)
  }
}

object DateTimeFormatter {
  def apply(format: String, timeZone: TimeZone, locale: Locale): DateTimeFormatter = {
    new LegacyFallbackDateTimeFormatter(format, timeZone, locale)
  }
}
