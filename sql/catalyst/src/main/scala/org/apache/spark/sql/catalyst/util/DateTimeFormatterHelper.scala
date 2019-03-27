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

import java.time.{Instant, LocalDateTime, ZonedDateTime, ZoneId}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.{ChronoField, TemporalAccessor}
import java.util.Locale

trait DateTimeFormatterHelper {

  protected def buildFormatter(pattern: String, locale: Locale): DateTimeFormatter = {
    new DateTimeFormatterBuilder()
      .appendPattern(pattern)
      .parseDefaulting(ChronoField.YEAR_OF_ERA, 1970)
      .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter(locale)
  }

  protected def toInstantWithZoneId(temporalAccessor: TemporalAccessor, zoneId: ZoneId): Instant = {
    val localDateTime = LocalDateTime.from(temporalAccessor)
    val zonedDateTime = ZonedDateTime.of(localDateTime, zoneId)
    Instant.from(zonedDateTime)
  }
}
