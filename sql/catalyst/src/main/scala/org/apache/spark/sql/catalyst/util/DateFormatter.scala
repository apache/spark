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

object DateFormatter {
  def apply(format: String, locale: Locale): DateFormatter = {
    new Iso8601DateFormatter(format, locale)
  }
}
