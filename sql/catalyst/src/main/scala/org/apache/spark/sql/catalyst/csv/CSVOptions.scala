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

package org.apache.spark.sql.catalyst.csv

import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util.Locale

import com.univocity.parsers.csv.{CsvParserSettings, CsvWriterSettings, UnescapedQuoteHandling}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy

class CSVOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    val columnPruning: Boolean,
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends FileSourceOptions(parameters) with Logging {

  def this(
    parameters: Map[String, String],
    columnPruning: Boolean,
    defaultTimeZoneId: String) = {
    this(
      CaseInsensitiveMap(parameters),
      columnPruning,
      defaultTimeZoneId,
      SQLConf.get.columnNameOfCorruptRecord)
  }

  def this(
    parameters: Map[String, String],
    columnPruning: Boolean,
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String) = {
      this(
        CaseInsensitiveMap(parameters),
        columnPruning,
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
      case _ => throw QueryExecutionErrors.paramExceedOneCharError(paramName)
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
          throw QueryExecutionErrors.paramIsNotIntegerError(paramName, value)
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
      throw QueryExecutionErrors.paramIsNotBooleanValueError(paramName)
    }
  }

  val delimiter = CSVExprUtils.toDelimiterStr(
    parameters.getOrElse("sep", parameters.getOrElse("delimiter", ",")))
  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(PermissiveMode)
  val charset = parameters.getOrElse("encoding",
    parameters.getOrElse("charset", StandardCharsets.UTF_8.name()))

  val quote = getChar("quote", '\"')
  val escape = getChar("escape", '\\')
  val charToEscapeQuoteEscaping = parameters.get("charToEscapeQuoteEscaping") match {
    case None => None
    case Some(null) => None
    case Some(value) if value.length == 0 => None
    case Some(value) if value.length == 1 => Some(value.charAt(0))
    case _ => throw QueryExecutionErrors.paramExceedOneCharError("charToEscapeQuoteEscaping")
  }
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

  val zoneId: ZoneId = DateTimeUtils.getZoneId(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  // A language tag in IETF BCP 47 format
  val locale: Locale = parameters.get("locale").map(Locale.forLanguageTag).getOrElse(Locale.US)

  val dateFormatInRead: Option[String] = parameters.get("dateFormat")
  val dateFormatInWrite: String = parameters.getOrElse("dateFormat", DateFormatter.defaultPattern)

  val timestampFormatInRead: Option[String] =
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      Some(parameters.getOrElse("timestampFormat",
        s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"))
    } else {
      parameters.get("timestampFormat")
    }
  val timestampFormatInWrite: String = parameters.getOrElse("timestampFormat",
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"
    } else {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXX]"
    })

  val timestampNTZFormatInRead: Option[String] = parameters.get("timestampNTZFormat")
  val timestampNTZFormatInWrite: String = parameters.getOrElse("timestampNTZFormat",
    s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS]")

  val multiLine = parameters.get("multiLine").map(_.toBoolean).getOrElse(false)

  val maxColumns = getInt("maxColumns", 20480)

  val maxCharsPerColumn = getInt("maxCharsPerColumn", -1)

  val escapeQuotes = getBool("escapeQuotes", true)

  val quoteAll = getBool("quoteAll", false)

  /**
   * The max error content length in CSV parser/writer exception message.
   */
  val maxErrorContentLength = 1000

  val isCommentSet = this.comment != '\u0000'

  val samplingRatio =
    parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

  /**
   * Forcibly apply the specified or inferred schema to datasource files.
   * If the option is enabled, headers of CSV files will be ignored.
   */
  val enforceSchema = getBool("enforceSchema", default = true)


  /**
   * String representation of an empty value in read and in write.
   */
  val emptyValue = parameters.get("emptyValue")
  /**
   * The string is returned when CSV reader doesn't have any characters for input value,
   * or an empty quoted string `""`. Default value is empty string.
   */
  val emptyValueInRead = emptyValue.getOrElse("")
  /**
   * The value is used instead of an empty string in write. Default value is `""`
   */
  val emptyValueInWrite = emptyValue.getOrElse("\"\"")

  /**
   * A string between two consecutive JSON records.
   */
  val lineSeparator: Option[String] = parameters.get("lineSep").map { sep =>
    require(sep.nonEmpty, "'lineSep' cannot be an empty string.")
    // Intentionally allow it up to 2 for Window's CRLF although multiple
    // characters have an issue with quotes. This is intentionally undocumented.
    require(sep.length <= 2, "'lineSep' can contain only 1 character.")
    if (sep.length == 2) logWarning("It is not recommended to set 'lineSep' " +
      "with 2 characters due to the limitation of supporting multi-char 'lineSep' within quotes.")
    sep
  }

  val lineSeparatorInRead: Option[Array[Byte]] = lineSeparator.map { lineSep =>
    lineSep.getBytes(charset)
  }
  val lineSeparatorInWrite: Option[String] = lineSeparator

  val inputBufferSize: Option[Int] = parameters.get("inputBufferSize").map(_.toInt)
    .orElse(SQLConf.get.getConf(SQLConf.CSV_INPUT_BUFFER_SIZE))

  /**
   * The handling method to be used when unescaped quotes are found in the input.
   */
  val unescapedQuoteHandling: UnescapedQuoteHandling = UnescapedQuoteHandling.valueOf(parameters
    .getOrElse("unescapedQuoteHandling", "STOP_AT_DELIMITER").toUpperCase(Locale.ROOT))

  def asWriterSettings: CsvWriterSettings = {
    val writerSettings = new CsvWriterSettings()
    val format = writerSettings.getFormat
    format.setDelimiter(delimiter)
    format.setQuote(quote)
    format.setQuoteEscape(escape)
    charToEscapeQuoteEscaping.foreach(format.setCharToEscapeQuoteEscaping)
    if (isCommentSet) {
      format.setComment(comment)
    }
    lineSeparatorInWrite.foreach(format.setLineSeparator)

    writerSettings.setIgnoreLeadingWhitespaces(ignoreLeadingWhiteSpaceFlagInWrite)
    writerSettings.setIgnoreTrailingWhitespaces(ignoreTrailingWhiteSpaceFlagInWrite)
    writerSettings.setNullValue(nullValue)
    writerSettings.setEmptyValue(emptyValueInWrite)
    writerSettings.setSkipEmptyLines(true)
    writerSettings.setQuoteAllFields(quoteAll)
    writerSettings.setQuoteEscapingEnabled(escapeQuotes)
    writerSettings.setErrorContentLength(maxErrorContentLength)
    writerSettings
  }

  def asParserSettings: CsvParserSettings = {
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setDelimiter(delimiter)
    format.setQuote(quote)
    format.setQuoteEscape(escape)
    lineSeparator.foreach(format.setLineSeparator)
    charToEscapeQuoteEscaping.foreach(format.setCharToEscapeQuoteEscaping)
    if (isCommentSet) {
      format.setComment(comment)
    } else {
      settings.setCommentProcessingEnabled(false)
    }

    settings.setIgnoreLeadingWhitespaces(ignoreLeadingWhiteSpaceInRead)
    settings.setIgnoreTrailingWhitespaces(ignoreTrailingWhiteSpaceInRead)
    settings.setReadInputOnSeparateThread(false)
    inputBufferSize.foreach(settings.setInputBufferSize)
    settings.setMaxColumns(maxColumns)
    settings.setNullValue(nullValue)
    settings.setEmptyValue(emptyValueInRead)
    settings.setMaxCharsPerColumn(maxCharsPerColumn)
    settings.setUnescapedQuoteHandling(unescapedQuoteHandling)
    settings.setLineSeparatorDetectionEnabled(lineSeparatorInRead.isEmpty && multiLine)
    lineSeparatorInRead.foreach { _ =>
      settings.setNormalizeLineEndingsWithinQuotes(!multiLine)
    }
    settings.setErrorContentLength(maxErrorContentLength)

    settings
  }
}
