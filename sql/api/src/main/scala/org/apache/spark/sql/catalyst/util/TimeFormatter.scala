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

import java.util.Locale

import org.apache.spark.unsafe.types.UTF8String

sealed trait TimeFormatter extends Serializable {
  def parse(s: String): Long // returns microseconds since midnight

  def format(micros: Long): String

  def validatePatternString(): Unit
}

class Iso8601TimeFormatter(
    pattern: String,
    locale: Locale,
    isParsing: Boolean)
  extends TimeFormatter with DateTimeFormatterHelper {

  @transient
  private lazy val formatter = getOrCreateFormatter(pattern, locale, isParsing)

  override def parse(s: String): Long = {
    val accessor = formatter.parse(s)
    val localTime = toLocalTime(accessor)
    TimeUtils.localTimeToMicros(localTime)
  }

  override def format(micros: Long): String = {
    val localTime = TimeUtils.microsToLocalTime(micros)
    localTime.format(formatter)
  }

  override def validatePatternString(): Unit = {
    formatter
    ()
  }
}

/**
 * The formatter for TIME which doesn't require users to specify a pattern.
 * In parsing, it follows the CAST logic in conversion of strings to Catalyst's TimeType.
 */
class DefaultTimeFormatter(
    locale: Locale,
    isParsing: Boolean)
  extends Iso8601TimeFormatter(TimeFormatter.defaultPattern, locale, isParsing) {

  override def parse(s: String): Long = {
    TimeUtils.stringToTimeInternal(UTF8String.fromString(s))
  }

  override def format(micros: Long): String = {
    TimeUtils.timeToStringInternal(micros)
  }
}

object TimeFormatter {
  val defaultPattern: String = "HH:mm:ss.SSSSSS"

  def apply(
      format: Option[String],
      locale: Locale = Locale.US,
      isParsing: Boolean = false): TimeFormatter = {
    val tf = format
      .map(new Iso8601TimeFormatter(_, locale, isParsing))
      .getOrElse(new DefaultTimeFormatter(locale, isParsing))
    tf.validatePatternString()
    tf
  }

  def apply(format: String): TimeFormatter = {
    apply(Some(format))
  }
}
