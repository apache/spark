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

package org.apache.spark.sql.catalyst.xml

import java.sql.Timestamp
import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import scala.util.Try

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.unsafe.types.TimestampNanosVal

private[xml] class XmlTimestampParser(options: XmlOptions) extends Serializable {
  private val legacy = SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY

  @transient private lazy val formatter = TimestampFormatter(
    options.timestampFormatInRead,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  @transient private lazy val customLegacyFormatter =
    options.parameters.get(XmlOptions.TIMESTAMP_FORMAT).map { pattern =>
      val format = DateTimeFormatter.ofPattern(pattern)
      if (pattern.exists("VzOXxZ".contains(_))) {
        format
      } else {
        format.withZone(options.timezone.map(ZoneId.of).orNull)
      }
    }

  def parse(value: String): Long = {
    if (legacy) {
      parseLegacy(value).getOrElse {
        throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3260",
          messageParameters = Map("s" -> value))
      }
    } else {
      formatter.parse(value)
    }
  }

  def parseOptional(value: String): Option[Long] = {
    if (legacy) parseLegacy(value) else formatter.parseOptional(value)
  }

  def parseNanos(value: String, precision: Int): TimestampNanosVal =
    formatter.parseNanos(value, precision)

  def parseNanosOptional(value: String, precision: Int): Option[TimestampNanosVal] =
    formatter.parseNanosOptional(value, precision)

  private def parseLegacy(value: String): Option[Long] = {
    val isoTimestamp = if (value.nonEmpty && Character.isDigit(value.head)) {
      XmlTimestampParser.legacyFormatters.iterator
        .map(parseWithFormatter(value, _))
        .collectFirst { case Some(timestamp) => timestamp }
    } else {
      None
    }
    isoTimestamp.orElse(customLegacyFormatter.flatMap(parseWithFormatter(value, _)))
  }

  private def parseWithFormatter(value: String, format: DateTimeFormatter): Option[Long] = {
    // Preserve spark-xml's java.sql.Timestamp conversion, including historical calendar rebasing.
    Try(
      DateTimeUtils.fromJavaTimestamp(Timestamp.from(Instant.from(format.parse(value))))).toOption
  }
}

private[xml] object XmlTimestampParser {
  private val legacyFormatters = Seq(
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral(' ')
      .append(DateTimeFormatter.ISO_LOCAL_TIME)
      .toFormatter()
      .withZone(ZoneOffset.UTC),
    DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneOffset.UTC),
    DateTimeFormatter.ISO_OFFSET_DATE_TIME,
    DateTimeFormatter.ISO_INSTANT)
}
