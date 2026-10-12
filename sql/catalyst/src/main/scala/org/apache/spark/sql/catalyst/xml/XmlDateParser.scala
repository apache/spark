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

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.util.Try

import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}

private[xml] class XmlDateParser(options: XmlOptions) extends Serializable {
  private val legacyIsoFirst = options.dateFormatOption.isDefined &&
    SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY

  private lazy val formatter = DateFormatter(
    options.dateFormatInRead,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  def parse(value: String): Int = {
    val isoDate = if (legacyIsoFirst && value.nonEmpty && Character.isDigit(value.head)) {
      Try(LocalDate.parse(value, DateTimeFormatter.ISO_DATE)).toOption
    } else {
      None
    }
    isoDate.map { date =>
      // Match spark-xml's java.sql.Date conversion for historical dates.
      DateTimeUtils.fromJavaDate(Date.valueOf(date))
    }.getOrElse(formatter.parse(value))
  }
}
