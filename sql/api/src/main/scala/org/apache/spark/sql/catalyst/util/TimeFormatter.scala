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

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils._
import org.apache.spark.unsafe.types.UTF8String

sealed trait TimeFormatter extends Serializable {
  def parse(s: String): Long // returns microseconds since midnight

  def format(localTime: LocalTime): String
  // Converts microseconds since the midnight to time string
  def format(micros: Long): String

  def validatePatternString(): Unit
}

/**
 * The ISO time formatter is capable of formatting and parsing the ISO-8601 extended time format.
 */
class Iso8601TimeFormatter(pattern: String, locale: Locale, isParsing: Boolean)
    extends TimeFormatter
    with DateTimeFormatterHelper {

  @transient
  protected lazy val formatter: DateTimeFormatter =
    getOrCreateFormatter(pattern, locale, isParsing)

  override def parse(s: String): Long = {
    val localTime = toLocalTime(formatter.parse(s))
    localTimeToMicros(localTime)
  }

  override def format(localTime: LocalTime): String = {
    localTime.format(formatter)
  }

  override def format(micros: Long): String = {
    format(microsToLocalTime(micros))
  }

  override def validatePatternString(): Unit = {
    try {
      formatter
    } catch checkInvalidPattern(pattern)
    ()
  }
}

/**
 * The formatter parses/formats times according to the pattern `HH:mm:ss.[..fff..]` where
 * `[..fff..]` is a fraction of second up to microsecond resolution. The formatter does not output
 * trailing zeros in the fraction. For example, the time `15:00:01.123400` is formatted as the
 * string `15:00:01.1234`.
 */
class FractionTimeFormatter
    extends Iso8601TimeFormatter(
      TimeFormatter.defaultPattern,
      TimeFormatter.defaultLocale,
      isParsing = false) {

  @transient
  override protected lazy val formatter: DateTimeFormatter =
    DateTimeFormatterHelper.fractionTimeFormatter
}

/**
 * The formatter for time values which doesn't require users to specify a pattern. While
 * formatting, it uses the default pattern [[TimeFormatter.defaultPattern()]]. In parsing, it
 * follows the CAST logic in conversion of strings to Catalyst's TimeType.
 *
 * @param locale
 *   The locale overrides the system locale and is used in formatting.
 * @param isParsing
 *   Whether the formatter is used for parsing (`true`) or for formatting (`false`).
 */
class DefaultTimeFormatter(locale: Locale, isParsing: Boolean)
    extends Iso8601TimeFormatter(TimeFormatter.defaultPattern, locale, isParsing) {

  override def parse(s: String): Long = {
    SparkDateTimeUtils.stringToTimeAnsi(UTF8String.fromString(s))
  }
}

object TimeFormatter {

  val defaultLocale: Locale = Locale.US

  val defaultPattern: String = "HH:mm:ss"

  private def getFormatter(
      format: Option[String],
      locale: Locale = defaultLocale,
      isParsing: Boolean): TimeFormatter = {
    val formatter = format
      .map(new Iso8601TimeFormatter(_, locale, isParsing))
      .getOrElse(new DefaultTimeFormatter(locale, isParsing))
    formatter.validatePatternString()
    formatter
  }

  def apply(format: String, locale: Locale, isParsing: Boolean): TimeFormatter = {
    getFormatter(Some(format), locale, isParsing)
  }

  def apply(format: Option[String], isParsing: Boolean): TimeFormatter = {
    getFormatter(format, defaultLocale, isParsing)
  }

  def apply(format: String, isParsing: Boolean): TimeFormatter = apply(Some(format), isParsing)

  def apply(format: String): TimeFormatter = {
    getFormatter(Some(format), defaultLocale, isParsing = false)
  }

  def apply(isParsing: Boolean): TimeFormatter = {
    getFormatter(None, defaultLocale, isParsing)
  }
}
