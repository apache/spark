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

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeParseException
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_MILLIS
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy._

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
      val localDate = try {
        LocalDate.parse(s, formatter)
      } catch {
        case e: DateTimeParseException if DateFormatter.hasDiffResult(s, pattern, zoneId) =>
          throw new RuntimeException(e.getMessage + ", set " +
            s"${SQLConf.LEGACY_TIME_PARSER_POLICY.key} to LEGACY to restore the behavior before " +
            "Spark 3.0. Set to CORRECTED to use the new approach, which would return null for " +
            "this record. See more details in SPARK-30668.")
      }
      localDateToDays(localDate)
    }
  }

  override def format(days: Int): String = {
    LocalDate.ofEpochDay(days).format(formatter)
  }
}

trait LegacyDateFormatter extends DateFormatter {
  def parseToDate(s: String): Date
  def formatDate(d: Date): String

  override def parse(s: String): Int = {
    val micros = DateTimeUtils.millisToMicros(parseToDate(s).getTime)
    DateTimeUtils.microsToDays(micros)
  }

  override def format(days: Int): String = {
    val date = DateTimeUtils.toJavaDate(days)
    formatDate(date)
  }
}

class LegacyFastDateFormatter(pattern: String, locale: Locale) extends LegacyDateFormatter {
  @transient
  private lazy val fdf = FastDateFormat.getInstance(pattern, locale)
  override def parseToDate(s: String): Date = fdf.parse(s)
  override def formatDate(d: Date): String = fdf.format(d)
}

class LegacySimpleDateFormatter(pattern: String, locale: Locale) extends LegacyDateFormatter {
  @transient
  private lazy val sdf = new SimpleDateFormat(pattern, locale)
  override def parseToDate(s: String): Date = sdf.parse(s)
  override def formatDate(d: Date): String = sdf.format(d)
}

object DateFormatter {
  import LegacyDateFormats._

  val defaultLocale: Locale = Locale.US

  def defaultPattern(): String = {
    if (SQLConf.get.legacyTimeParserPolicy == LEGACY) "yyyy-MM-dd" else "uuuu-MM-dd"
  }

  private def getFormatter(
    format: Option[String],
    zoneId: ZoneId,
    locale: Locale = defaultLocale,
    legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT): DateFormatter = {

    val pattern = if (format.nonEmpty) {
      checkIncompatiblePattern(format.get)
      format.get
    } else {
      defaultPattern()
    }
    if (SQLConf.get.legacyTimeParserPolicy == LEGACY) {
      legacyFormat match {
        case FAST_DATE_FORMAT =>
          new LegacyFastDateFormatter(pattern, locale)
        case SIMPLE_DATE_FORMAT | LENIENT_SIMPLE_DATE_FORMAT =>
          new LegacySimpleDateFormatter(pattern, locale)
      }
    } else {
      new Iso8601DateFormatter(pattern, zoneId, locale)
    }
  }

  def apply(
    format: String,
    zoneId: ZoneId,
    locale: Locale,
    legacyFormat: LegacyDateFormat): DateFormatter = {
    getFormatter(Some(format), zoneId, locale, legacyFormat)
  }

  def apply(format: String, zoneId: ZoneId): DateFormatter = {
    getFormatter(Some(format), zoneId)
  }

  def apply(zoneId: ZoneId): DateFormatter = {
    getFormatter(None, zoneId)
  }

  def hasDiffResult(s: String, format: String, zoneId: ZoneId): Boolean = {
    // Only check whether we will get different results between legacy format and new format, while
    // legacy time parser policy set to EXCEPTION. For legacy parser, DateTimeParseException will
    // not be thrown. On the contrary, if the legacy policy set to CORRECTED,
    // DateTimeParseException will address by the caller side.
    if (LegacyBehaviorPolicy.withName(
        SQLConf.get.getConf(SQLConf.LEGACY_TIME_PARSER_POLICY)) == EXCEPTION) {
      val formatter = new LegacySimpleTimestampFormatter(
        format, zoneId, defaultLocale, lenient = false)
      val res = try {
        Some(formatter.parse(s))
      } catch {
        case _: Throwable => None
      }
      res.nonEmpty
    } else {
      false
    }
  }

  def checkIncompatiblePattern(pattern: String): Unit = {
    // Only check whether we have incompatible pattern for user provided pattern string.
    // Currently, the only incompatible pattern string is 'u', which represents
    // 'Day number of week' in legacy parser but 'Year' in new parser.
    if (LegacyBehaviorPolicy.withName(
      SQLConf.get.getConf(SQLConf.LEGACY_TIME_PARSER_POLICY)) == EXCEPTION) {
      // Text can be quoted using single quotes, we only check the non-quote parts.
      val isIncompatible = pattern.split("'").zipWithIndex.exists {
        case (patternPart, index) =>
          index % 2 == 0 && patternPart.contains("u")
      }
      if (isIncompatible) {
        throw new RuntimeException(s"The pattern $pattern provided is incompatible between " +
          "legacy parser and new parser after Spark 3.0. Please change the pattern or set " +
          s"${SQLConf.LEGACY_TIME_PARSER_POLICY.key} to LEGACY or CORRECTED to explicitly choose " +
          "the parser.")
      }
    }
  }
}
