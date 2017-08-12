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

package org.apache.spark.sql.execution.datasources.csv

import java.nio.charset.StandardCharsets
import java.util.{Locale, TimeZone}

import com.univocity.parsers.csv.{CsvParserSettings, CsvWriterSettings, UnescapedQuoteHandling}
import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util._

class CSVOptions(
    @transient private val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends Logging with Serializable {

  def this(
    parameters: Map[String, String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String = "") = {
      this(
        CaseInsensitiveMap(parameters),
        defaultTimeZoneId,
        defaultColumnNameOfCorruptRecord)
  }

  private def getChar(paramName: String, default: Char): Char = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) if value.length == 0 => '\u0000'
      case Some(value) if value.length == 1 => value.charAt(0)
      case _ => throw new RuntimeException(s"$paramName cannot be more than one character")
    }
  }

  private def getInt(paramName: String, default: Int): Int = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) => try {
        value.toInt
      } catch {
        case e: NumberFormatException =>
          throw new RuntimeException(s"$paramName should be an integer. Found $value")
      }
    }
  }

  private def getBool(paramName: String, default: Boolean = false): Boolean = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param == null) {
      default
    } else if (param.toLowerCase(Locale.ROOT) == "true") {
      true
    } else if (param.toLowerCase(Locale.ROOT) == "false") {
      false
    } else {
      throw new Exception(s"$paramName flag can be true or false")
    }
  }

  val delimiter = CSVUtils.toChar(
    parameters.getOrElse("sep", parameters.getOrElse("delimiter", ",")))
  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(PermissiveMode)
  val charset = parameters.getOrElse("encoding",
    parameters.getOrElse("charset", StandardCharsets.UTF_8.name()))

  val quote = getChar("quote", '\"')
  val escape = getChar("escape", '\\')
  val comment = getChar("comment", '\u0000')

  val headerFlag = getBool("header")
  val inferSchemaFlag = getBool("inferSchema")
  val ignoreLeadingWhiteSpaceInRead = getBool("ignoreLeadingWhiteSpace", default = false)
  val ignoreTrailingWhiteSpaceInRead = getBool("ignoreTrailingWhiteSpace", default = false)

  // For write, both options were `true` by default. We leave it as `true` for
  // backwards compatibility.
  val ignoreLeadingWhiteSpaceFlagInWrite = getBool("ignoreLeadingWhiteSpace", default = true)
  val ignoreTrailingWhiteSpaceFlagInWrite = getBool("ignoreTrailingWhiteSpace", default = true)

  val columnNameOfCorruptRecord =
    parameters.getOrElse("columnNameOfCorruptRecord", defaultColumnNameOfCorruptRecord)

  val nullValue = parameters.getOrElse("nullValue", "")

  val nanValue = parameters.getOrElse("nanValue", "NaN")

  val positiveInf = parameters.getOrElse("positiveInf", "Inf")
  val negativeInf = parameters.getOrElse("negativeInf", "-Inf")


  val compressionCodec: Option[String] = {
    val name = parameters.get("compression").orElse(parameters.get("codec"))
    name.map(CompressionCodecs.getCodecClassName)
  }

  val timeZone: TimeZone = DateTimeUtils.getTimeZone(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(
      parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), timeZone, Locale.US)

  val multiLine = parameters.get("multiLine").map(_.toBoolean).getOrElse(false)

  val maxColumns = getInt("maxColumns", 20480)

  val maxCharsPerColumn = getInt("maxCharsPerColumn", -1)

  val escapeQuotes = getBool("escapeQuotes", true)

  val quoteAll = getBool("quoteAll", false)

  val inputBufferSize = 128

  val isCommentSet = this.comment != '\u0000'

  def asWriterSettings: CsvWriterSettings = {
    val writerSettings = new CsvWriterSettings()
    val format = writerSettings.getFormat
    format.setDelimiter(delimiter)
    format.setQuote(quote)
    format.setQuoteEscape(escape)
    format.setComment(comment)
    writerSettings.setIgnoreLeadingWhitespaces(ignoreLeadingWhiteSpaceFlagInWrite)
    writerSettings.setIgnoreTrailingWhitespaces(ignoreTrailingWhiteSpaceFlagInWrite)
    writerSettings.setNullValue(nullValue)
    writerSettings.setEmptyValue(nullValue)
    writerSettings.setSkipEmptyLines(true)
    writerSettings.setQuoteAllFields(quoteAll)
    writerSettings.setQuoteEscapingEnabled(escapeQuotes)
    writerSettings
  }

  def asParserSettings: CsvParserSettings = {
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setDelimiter(delimiter)
    format.setQuote(quote)
    format.setQuoteEscape(escape)
    format.setComment(comment)
    settings.setIgnoreLeadingWhitespaces(ignoreLeadingWhiteSpaceInRead)
    settings.setIgnoreTrailingWhitespaces(ignoreTrailingWhiteSpaceInRead)
    settings.setReadInputOnSeparateThread(false)
    settings.setInputBufferSize(inputBufferSize)
    settings.setMaxColumns(maxColumns)
    settings.setNullValue(nullValue)
    settings.setMaxCharsPerColumn(maxCharsPerColumn)
    settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_DELIMITER)
    settings
  }
}
