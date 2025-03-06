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
import java.util.Locale

import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils._

sealed trait TimeFormatter extends Serializable {
  def parse(s: String): Long // returns microseconds since midnight

  def format(localTime: LocalTime): String
  def format(micros: Long): String
}

class Iso8601TimeFormatter(pattern: String, locale: Locale, isParsing: Boolean)
    extends TimeFormatter
    with DateTimeFormatterHelper {

  @transient
  private lazy val formatter = getOrCreateFormatter(pattern, locale, isParsing)

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
}

object TimeFormatter {

  val defaultLocale: Locale = Locale.US

  def defaultPattern(): String = "HH:mm:ss.SSSSSS"

  def apply(
      format: Option[String],
      locale: Locale = defaultLocale,
      isParsing: Boolean): TimeFormatter = {
    new Iso8601TimeFormatter(format.getOrElse(defaultPattern()), locale, isParsing)
  }

  def apply(isParsing: Boolean): TimeFormatter = apply(format = None, isParsing = isParsing)

  def apply(): TimeFormatter = apply(isParsing = false)
}
