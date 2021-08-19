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
import java.time.LocalDate
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy._
import org.apache.spark.unsafe.types.UTF8String

sealed trait DateFormatter extends Serializable {
  def parse(s: String): Int // returns days since epoch

  def format(days: Int): String
  def format(date: Date): String
  def format(localDate: LocalDate): String

  def validatePatternString(): Unit
}

class Iso8601DateFormatter(
    pattern: String,
    locale: Locale,
    legacyFormat: LegacyDateFormats.LegacyDateFormat,
    isParsing: Boolean)
  extends DateFormatter with DateTimeFormatterHelper {

  @transient
  private lazy val formatter = getOrCreateFormatter(pattern, locale, isParsing)

  @transient
  protected lazy val legacyFormatter =
    DateFormatter.getLegacyFormatter(pattern, locale, legacyFormat)

  override def parse(s: String): Int = {
    try {
      val localDate = toLocalDate(formatter.parse(s))
      localDateToDays(localDate)
    } catch checkParsedDiff(s, legacyFormatter.parse)
  }

  override def format(localDate: LocalDate): String = {
    try {
      localDate.format(formatter)
    } catch checkFormattedDiff(toJavaDate(localDateToDays(localDate)),
      (d: Date) => format(d))
  }

  override def format(days: Int): String = {
    format(LocalDate.ofEpochDay(days))
  }

  override def format(date: Date): String = {
    legacyFormatter.format(date)
  }

  override def validatePatternString(): Unit = {
    try {
      formatter
    } catch checkLegacyFormatter(pattern, legacyFormatter.validatePatternString)
  }
}

/**
 * The formatter for dates which doesn't require users to specify a pattern. While formatting,
 * it uses the default pattern [[DateFormatter.defaultPattern]]. In parsing, it follows the CAST
 * logic in conversion of strings to Catalyst's DateType.
 *
 * @param locale The locale overrides the system locale and is used in formatting.
 * @param legacyFormat Defines the formatter used for legacy dates.
 * @param isParsing Whether the formatter is used for parsing (`true`) or for formatting (`false`).
 */
class DefaultDateFormatter(
    locale: Locale,
    legacyFormat: LegacyDateFormats.LegacyDateFormat,
    isParsing: Boolean)
  extends Iso8601DateFormatter(DateFormatter.defaultPattern, locale, legacyFormat, isParsing) {

  override def parse(s: String): Int = {
    try {
      DateTimeUtils.stringToDateAnsi(UTF8String.fromString(s))
    } catch checkParsedDiff(s, legacyFormatter.parse)
  }
}

trait LegacyDateFormatter extends DateFormatter {
  def parseToDate(s: String): Date

  override def parse(s: String): Int = {
    fromJavaDate(new java.sql.Date(parseToDate(s).getTime))
  }

  override def format(days: Int): String = {
    format(DateTimeUtils.toJavaDate(days))
  }

  override def format(localDate: LocalDate): String = {
    format(localDateToDays(localDate))
  }
}

/**
 * The legacy formatter is based on Apache Commons FastDateFormat. The formatter uses the default
 * JVM time zone intentionally for compatibility with Spark 2.4 and earlier versions.
 *
 * Note: Using of the default JVM time zone makes the formatter compatible with the legacy
 *       `DateTimeUtils` methods `toJavaDate` and `fromJavaDate` that are based on the default
 *       JVM time zone too.
 *
 * @param pattern `java.text.SimpleDateFormat` compatible pattern.
 * @param locale The locale overrides the system locale and is used in parsing/formatting.
 */
class LegacyFastDateFormatter(pattern: String, locale: Locale) extends LegacyDateFormatter {
  @transient
  private lazy val fdf = FastDateFormat.getInstance(pattern, locale)
  override def parseToDate(s: String): Date = fdf.parse(s)
  override def format(d: Date): String = fdf.format(d)
  override def validatePatternString(): Unit = fdf
}

// scalastyle:off line.size.limit
/**
 * The legacy formatter is based on `java.text.SimpleDateFormat`. The formatter uses the default
 * JVM time zone intentionally for compatibility with Spark 2.4 and earlier versions.
 *
 * Note: Using of the default JVM time zone makes the formatter compatible with the legacy
 *       `DateTimeUtils` methods `toJavaDate` and `fromJavaDate` that are based on the default
 *       JVM time zone too.
 *
 * @param pattern The pattern describing the date and time format.
 *                See <a href="https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html">
 *                Date and Time Patterns</a>
 * @param locale  The locale whose date format symbols should be used. It overrides the system
 *                locale in parsing/formatting.
 */
// scalastyle:on line.size.limit
class LegacySimpleDateFormatter(pattern: String, locale: Locale) extends LegacyDateFormatter {
  @transient
  private lazy val sdf = new SimpleDateFormat(pattern, locale)
  override def parseToDate(s: String): Date = sdf.parse(s)
  override def format(d: Date): String = sdf.format(d)
  override def validatePatternString(): Unit = sdf

}

object DateFormatter {
  import LegacyDateFormats._

  val defaultLocale: Locale = Locale.US

  val defaultPattern: String = "yyyy-MM-dd"

  private def getFormatter(
      format: Option[String],
      locale: Locale = defaultLocale,
      legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT,
      isParsing: Boolean): DateFormatter = {
    if (SQLConf.get.legacyTimeParserPolicy == LEGACY) {
      getLegacyFormatter(format.getOrElse(defaultPattern), locale, legacyFormat)
    } else {
      val df = format
        .map(new Iso8601DateFormatter(_, locale, legacyFormat, isParsing))
        .getOrElse(new DefaultDateFormatter(locale, legacyFormat, isParsing))
      df.validatePatternString()
      df
    }
  }

  def getLegacyFormatter(
      pattern: String,
      locale: Locale,
      legacyFormat: LegacyDateFormat): DateFormatter = {
    legacyFormat match {
      case FAST_DATE_FORMAT =>
        new LegacyFastDateFormatter(pattern, locale)
      case SIMPLE_DATE_FORMAT | LENIENT_SIMPLE_DATE_FORMAT =>
        new LegacySimpleDateFormatter(pattern, locale)
    }
  }

  def apply(
      format: Option[String],
      locale: Locale,
      legacyFormat: LegacyDateFormat,
      isParsing: Boolean): DateFormatter = {
    getFormatter(format, locale, legacyFormat, isParsing)
  }

  def apply(
      format: String,
      locale: Locale,
      legacyFormat: LegacyDateFormat,
      isParsing: Boolean): DateFormatter = {
    getFormatter(Some(format), locale, legacyFormat, isParsing)
  }

  def apply(format: String, isParsing: Boolean = false): DateFormatter = {
    getFormatter(Some(format), isParsing = isParsing)
  }

  def apply(): DateFormatter = {
    getFormatter(None, isParsing = false)
  }
}
