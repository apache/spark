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
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, DateTimeParseException, ResolverStyle}
import java.time.temporal.{ChronoField, TemporalAccessor, TemporalQueries}
import java.util.Locale

import com.google.common.cache.CacheBuilder

import org.apache.spark.SparkUpgradeException
import org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy._

trait DateTimeFormatterHelper {
  // Converts the parsed temporal object to ZonedDateTime. It sets time components to zeros
  // if they does not exist in the parsed object.
  protected def toZonedDateTime(
      temporalAccessor: TemporalAccessor,
      zoneId: ZoneId): ZonedDateTime = {
    // Parsed input might not have time related part. In that case, time component is set to zeros.
    val parsedLocalTime = temporalAccessor.query(TemporalQueries.localTime)
    val localTime = if (parsedLocalTime == null) LocalTime.MIDNIGHT else parsedLocalTime
    // Parsed input must have date component. At least, year must present in temporalAccessor.
    val localDate = temporalAccessor.query(TemporalQueries.localDate)

    ZonedDateTime.of(localDate, localTime, zoneId)
  }

  // Gets a formatter from the cache or creates new one. The buildFormatter method can be called
  // a few times with the same parameters in parallel if the cache does not contain values
  // associated to those parameters. Since the formatter is immutable, it does not matter.
  // In this way, synchronised is intentionally omitted in this method to make parallel calls
  // less synchronised.
  // The Cache.get method is not used here to avoid creation of additional instances of Callable.
  protected def getOrCreateFormatter(pattern: String, locale: Locale): DateTimeFormatter = {
    val newPattern = DateTimeUtils.convertIncompatiblePattern(pattern)
    val key = (newPattern, locale)
    var formatter = cache.getIfPresent(key)
    if (formatter == null) {
      formatter = buildFormatter(newPattern, locale)
      cache.put(key, formatter)
    }
    formatter
  }

  // When legacy time parser policy set to EXCEPTION, check whether we will get different results
  // between legacy parser and new parser. If new parser fails but legacy parser works, throw a
  // SparkUpgradeException. On the contrary, if the legacy policy set to CORRECTED,
  // DateTimeParseException will address by the caller side.
  protected def checkDiffResult[T](
      s: String, legacyParseFunc: String => T): PartialFunction[Throwable, T] = {
    case e: DateTimeParseException if SQLConf.get.legacyTimeParserPolicy == EXCEPTION =>
      val res = try {
        Some(legacyParseFunc(s))
      } catch {
        case _: Throwable => None
      }
      if (res.nonEmpty) {
        throw new SparkUpgradeException("3.0", s"Fail to parse '$s' in the new parser. You can " +
          s"set ${SQLConf.LEGACY_TIME_PARSER_POLICY.key} to LEGACY to restore the behavior " +
          s"before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string.", e)
      } else {
        throw e
      }
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
      .parseDefaulting(ChronoField.ERA, 1)
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
