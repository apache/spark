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

import java.text.ParseException
import java.time._
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoField.MICRO_OF_SECOND
import java.time.temporal.TemporalQueries
import java.util.{Locale, TimeZone}
import java.util.concurrent.TimeUnit.SECONDS

sealed trait TimestampFormatter extends Serializable {
  /**
   * Parses a timestamp in a string and converts it to microseconds.
   *
   * @param s - string with timestamp to parse
   * @return microseconds since epoch.
   * @throws ParseException can be thrown by legacy parser
   * @throws DateTimeParseException can be thrown by new parser
   * @throws DateTimeException unable to obtain local date or time
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  def parse(s: String): Long
  def format(us: Long): String
}

class Iso8601TimestampFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale) extends TimestampFormatter with DateTimeFormatterHelper {
  @transient
  protected lazy val formatter = getOrCreateFormatter(pattern, locale)

  override def parse(s: String): Long = {
    val parsed = formatter.parse(s)
    val parsedZoneId = parsed.query(TemporalQueries.zone())
    val timeZoneId = if (parsedZoneId == null) zoneId else parsedZoneId
    val zonedDateTime = toZonedDateTime(parsed, timeZoneId)
    val epochSeconds = zonedDateTime.toEpochSecond
    val microsOfSecond = zonedDateTime.get(MICRO_OF_SECOND)

    Math.addExact(SECONDS.toMicros(epochSeconds), microsOfSecond)
  }

  override def format(us: Long): String = {
    val instant = DateTimeUtils.microsToInstant(us)
    formatter.withZone(zoneId).format(instant)
  }
}

/**
 * The formatter parses/formats timestamps according to the pattern `yyyy-MM-dd HH:mm:ss.[..fff..]`
 * where `[..fff..]` is a fraction of second up to microsecond resolution. The formatter does not
 * output trailing zeros in the fraction. For example, the timestamp `2019-03-05 15:00:01.123400` is
 * formatted as the string `2019-03-05 15:00:01.1234`.
 *
 * @param zoneId the time zone identifier in which the formatter parses or format timestamps
 */
class FractionTimestampFormatter(zoneId: ZoneId)
  extends Iso8601TimestampFormatter("", zoneId, TimestampFormatter.defaultLocale) {

  @transient
  override protected lazy val formatter = DateTimeFormatterHelper.fractionFormatter
}

object TimestampFormatter {
  val defaultPattern: String = "yyyy-MM-dd HH:mm:ss"
  val defaultLocale: Locale = Locale.US

  def apply(format: String, zoneId: ZoneId, locale: Locale): TimestampFormatter = {
    new Iso8601TimestampFormatter(format, zoneId, locale)
  }

  def apply(format: String, zoneId: ZoneId): TimestampFormatter = {
    apply(format, zoneId, defaultLocale)
  }

  def apply(zoneId: ZoneId): TimestampFormatter = {
    apply(defaultPattern, zoneId, defaultLocale)
  }

  def getFractionFormatter(zoneId: ZoneId): TimestampFormatter = {
    new FractionTimestampFormatter(zoneId)
  }
}
