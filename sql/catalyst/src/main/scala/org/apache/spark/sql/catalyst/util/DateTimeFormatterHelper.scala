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
import java.time.temporal.{ChronoField, TemporalAccessor}
import java.time.temporal.ChronoField.{DAY_OF_MONTH, ERA, MONTH_OF_YEAR, YEAR, YEAR_OF_ERA}
import java.util.Locale

import com.google.common.cache.CacheBuilder

trait DateTimeFormatterHelper {
  import DateTimeFormatterHelper._

  protected def getLocalDate(str: String, parsed: TemporalAccessor): LocalDate = {
    val year = if (parsed.isSupported(YEAR)) {
      parsed.get(YEAR)
    } else if (parsed.isSupported(YEAR_OF_ERA) && !parsed.isSupported(ERA)) {
      // This can happen when the year pattern is "yyyy" without "G" to specify era. We assume the
      // era is AC, and just treat YEAR_OF_ERA as YEAR.
      parsed.get(YEAR_OF_ERA)
    } else {
      throw new DateTimeException("Cannot obtain year from timestamp string " + str)
    }
    LocalDate.of(year, parsed.get(MONTH_OF_YEAR), parsed.get(DAY_OF_MONTH))
  }

  // Gets a formatter from the cache or creates new one. The buildFormatter method can be called
  // a few times with the same parameters in parallel if the cache does not contain values
  // associated to those parameters. Since the formatter is immutable, it does not matter.
  // In this way, synchronised is intentionally omitted in this method to make parallel calls
  // less synchronised.
  // The Cache.get method is not used here to avoid creation of additional instances of Callable.
  protected def getOrCreateFormatter(pattern: String, locale: Locale): DateTimeFormatter = {
    val key = (pattern, locale)
    var formatter = cache.getIfPresent(key)
    if (formatter == null) {
      formatter = buildFormatter(pattern, locale)
      cache.put(key, formatter)
    }
    formatter
  }
}

private object DateTimeFormatterHelper {
  val cache = CacheBuilder.newBuilder()
    .maximumSize(128)
    .build[(String, Locale), DateTimeFormatter]()

  def createBuilder(): DateTimeFormatterBuilder = {
    new DateTimeFormatterBuilder().parseCaseInsensitive()
  }

  def toFormatter(builder: DateTimeFormatterBuilder, locale: Locale): DateTimeFormatter = {
    builder
      .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter(locale)
      .withChronology(IsoChronology.INSTANCE)
      .withResolverStyle(ResolverStyle.STRICT)
  }

  def buildFormatter(pattern: String, locale: Locale): DateTimeFormatter = {
    val builder = createBuilder().appendPattern(pattern)
    toFormatter(builder, locale)
  }

  lazy val fractionFormatter: DateTimeFormatter = {
    val builder = createBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral(' ')
      .appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    toFormatter(builder, TimestampFormatter.defaultLocale)
  }
}
