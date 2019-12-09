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

import java.time.{LocalDate, ZoneId}
import java.util.Locale

import DateTimeUtils.{convertSpecialDate, localDateToDays}

sealed trait DateFormatter extends Serializable {
  def parse(s: String): Int // returns days since epoch
  def format(days: Int): String
}

class Iso8601DateFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale) extends DateFormatter with DateTimeFormatterHelper {

  @transient
  private lazy val formatter = getOrCreateFormatter(pattern, locale)

  override def parse(s: String): Int = {
    val specialDate = convertSpecialDate(s.trim, zoneId)
    specialDate.getOrElse {
      val localDate = LocalDate.parse(s, formatter)
      localDateToDays(localDate)
    }
  }

  override def format(days: Int): String = {
    LocalDate.ofEpochDay(days).format(formatter)
  }
}

object DateFormatter {
  val defaultPattern: String = "uuuu-MM-dd"
  val defaultLocale: Locale = Locale.US

  def apply(format: String, zoneId: ZoneId, locale: Locale): DateFormatter = {
    new Iso8601DateFormatter(format, zoneId, locale)
  }

  def apply(format: String, zoneId: ZoneId): DateFormatter = {
    apply(format, zoneId, defaultLocale)
  }

  def apply(zoneId: ZoneId): DateFormatter = apply(defaultPattern, zoneId)
}
