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

object TimeFormatter {

  val defaultLocale: Locale = Locale.US

  val defaultPattern: String = "HH:mm:ss"

  def apply(
      format: String = defaultPattern,
      locale: Locale = defaultLocale,
      isParsing: Boolean = false): TimeFormatter = {
    val formatter = new Iso8601TimeFormatter(format, locale, isParsing)
    formatter.validatePatternString()
    formatter
  }
}
