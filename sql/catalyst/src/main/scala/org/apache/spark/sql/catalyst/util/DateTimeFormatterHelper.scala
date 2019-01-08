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

import java.time._
import java.time.chrono.IsoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.{ChronoField, TemporalAccessor, TemporalQueries}
import java.util.Locale
import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.cache.CacheBuilder
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}

trait DateTimeFormatterHelper {
  protected def toInstantWithZoneId(temporalAccessor: TemporalAccessor, zoneId: ZoneId): Instant = {
    val localTime = if (temporalAccessor.query(TemporalQueries.localTime) == null) {
      LocalTime.ofNanoOfDay(0)
    } else {
      LocalTime.from(temporalAccessor)
    }
    val localDate = LocalDate.from(temporalAccessor)
    val localDateTime = LocalDateTime.of(localDate, localTime)
    val zonedDateTime = ZonedDateTime.of(localDateTime, zoneId)
    Instant.from(zonedDateTime)
  }
}

object DateTimeFormatterHelper {
  private val cache = CacheBuilder.newBuilder()
    .maximumSize(128)
    .build[(String, Locale), DateTimeFormatter]()


  private def buildFormatter(pattern: String, locale: Locale): DateTimeFormatter = {
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendPattern(pattern)
      .parseDefaulting(ChronoField.ERA, 1)
      .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter(locale)
      .withChronology(IsoChronology.INSTANCE)
      .withResolverStyle(ResolverStyle.STRICT)
  }

  def getFormatter(pattern: String, locale: Locale): DateTimeFormatter = {
    try {
      val key = (pattern, locale)
      var formatter = cache.getIfPresent(key)
      if (formatter == null) {
        formatter = buildFormatter(pattern, locale)
        cache.put(key, formatter)
      }
      formatter
    } catch {
      // Cache.get() may wrap the original exception.
      case e @ (_: UncheckedExecutionException | _: ExecutionError) =>
        throw e.getCause
    }
  }
}
