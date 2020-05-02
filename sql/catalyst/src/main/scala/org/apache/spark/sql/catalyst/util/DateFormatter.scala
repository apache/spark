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
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy._

sealed trait DateFormatter extends Serializable {
  def parse(s: String): Int // returns days since epoch
  def format(days: Int): String
}

class Iso8601DateFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale,
    legacyFormat: LegacyDateFormats.LegacyDateFormat)
  extends DateFormatter with DateTimeFormatterHelper {

  @transient
  private lazy val formatter = getOrCreateFormatter(pattern, locale)

  @transient
  private lazy val legacyFormatter = DateFormatter.getLegacyFormatter(
    pattern, zoneId, locale, legacyFormat)

  override def parse(s: String): Int = {
    val specialDate = convertSpecialDate(s.trim, zoneId)
    specialDate.getOrElse {
      try {
        val localDate = LocalDate.parse(s, formatter)
        localDateToDays(localDate)
      } catch checkDiffResult(s, legacyFormatter.parse)
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
    fromJavaDate(new java.sql.Date(parseToDate(s).getTime))
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

  val defaultPattern: String = "yyyy-MM-dd"

  private def getFormatter(
      format: Option[String],
      zoneId: ZoneId,
      locale: Locale = defaultLocale,
      legacyFormat: LegacyDateFormat = LENIENT_SIMPLE_DATE_FORMAT): DateFormatter = {
    val pattern = format.getOrElse(defaultPattern)
    if (SQLConf.get.legacyTimeParserPolicy == LEGACY) {
      getLegacyFormatter(pattern, zoneId, locale, legacyFormat)
    } else {
      new Iso8601DateFormatter(pattern, zoneId, locale, legacyFormat)
    }
  }

  def getLegacyFormatter(
      pattern: String,
      zoneId: ZoneId,
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
}
