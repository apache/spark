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

  private def getChar(csvOption: CSVOptions.Value, default: Char): Char = {
    val paramValue = parameters.get(csvOption.toString)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) if value.length == 0 => '\u0000'
      case Some(value) if value.length == 1 => value.charAt(0)
      case _ => throw QueryExecutionErrors.paramExceedOneCharError(csvOption.toString)
    }
  }

  private def getInt(paramName: CSVOptions.Value, default: Int): Int = {
    val paramValue = parameters.get(paramName.toString)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) => try {
        value.toInt
      } catch {
        case e: NumberFormatException =>
          throw QueryExecutionErrors.paramIsNotIntegerError(paramName.toString, value)
      }
    }
  }

  private def getBool(paramName: CSVOptions.Value, default: Boolean = false): Boolean = {
    val param = parameters.getOrElse(paramName.toString, default.toString)
    if (param == null) {
      default
    } else if (param.toLowerCase(Locale.ROOT) == "true") {
      true
    } else if (param.toLowerCase(Locale.ROOT) == "false") {
      false
    } else {
      throw QueryExecutionErrors.paramIsNotBooleanValueError(paramName.toString)
    }
  }

  private def getString(paramName: CSVOptions.Value): Option[String] = {
    parameters.get(paramName.toString)
  }

  import org.apache.spark.sql.catalyst.csv.CSVOptions._

  val delimiter = CSVExprUtils.toDelimiterStr(
    getString(SEP).getOrElse(getString(DELIMITER).getOrElse(",")))
  val parseMode: ParseMode =
    getString(MODE).map(ParseMode.fromString).getOrElse(PermissiveMode)
  val charset = getString(ENCODING).getOrElse(
    getString(CHARSET).getOrElse(StandardCharsets.UTF_8.name()))

  val quote = getChar(QUOTE, '\"')
  val escape = getChar(ESCAPE, '\\')
  val charToEscapeQuoteEscaping = getString(CHAR_TO_ESCAPE_QUOTE_ESCAPING) match {
    case None => None
    case Some(null) => None
    case Some(value) if value.length == 0 => None
    case Some(value) if value.length == 1 => Some(value.charAt(0))
    case _ =>
      throw QueryExecutionErrors.paramExceedOneCharError(CHAR_TO_ESCAPE_QUOTE_ESCAPING.toString)
  }
  val comment = getChar(COMMENT, '\u0000')

  val headerFlag = getBool(HEADER)
  val inferSchemaFlag = getBool(INFER_SCHEMA)
  val ignoreLeadingWhiteSpaceInRead = getBool(IGNORE_LEADING_WHITESPACE, default = false)
  val ignoreTrailingWhiteSpaceInRead = getBool(IGNORE_TRAILING_WHITESPACE, default = false)

  // For write, both options were `true` by default. We leave it as `true` for
  // backwards compatibility.
  val ignoreLeadingWhiteSpaceFlagInWrite = getBool(IGNORE_LEADING_WHITESPACE, default = true)
  val ignoreTrailingWhiteSpaceFlagInWrite = getBool(IGNORE_TRAILING_WHITESPACE, default = true)

  val columnNameOfCorruptRecord =
    getString(COLUMN_NAME_OF_CORRUPT_RECORD).getOrElse(defaultColumnNameOfCorruptRecord)

  val nullValue = getString(NULL_VALUE).getOrElse("")

  val nanValue = getString(NAN_VALUE).getOrElse("NaN")

  val positiveInf = getString(POSITIVE_INF).getOrElse("Inf")
  val negativeInf = getString(NEGATIVE_INF).getOrElse("-Inf")


  val compressionCodec: Option[String] = {
    val name = getString(COMPRESSION).orElse(getString(CODEC))
    name.map(CompressionCodecs.getCodecClassName)
  }

  val zoneId: ZoneId = DateTimeUtils.getZoneId(
    getString(TIME_ZONE).getOrElse(defaultTimeZoneId))

  // A language tag in IETF BCP 47 format
  val locale: Locale = getString(LOCALE).map(Locale.forLanguageTag).getOrElse(Locale.US)

  /**
   * Infer columns with all valid date entries as date type (otherwise inferred as string or
   * timestamp type) if schema inference is enabled.
   *
   * Enabled by default.
   *
   * Not compatible with legacyTimeParserPolicy == LEGACY since legacy date parser will accept
   * extra trailing characters. Thus, disabled when legacyTimeParserPolicy == LEGACY
   */
  val prefersDate = {
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      false
    } else {
      getBool(PREFERS_DATE, true)
    }
  }

  val dateFormatOption: Option[String] = getString(DATE_FORMAT)
  // Provide a default value for dateFormatInRead when prefersDate. This ensures that the
  // Iso8601DateFormatter (with strict date parsing) is used for date inference
  val dateFormatInRead: Option[String] =
    if (prefersDate) {
      Option(dateFormatOption.getOrElse(DateFormatter.defaultPattern))
    } else {
      dateFormatOption
    }
  val dateFormatInWrite: String = getString(DATE_FORMAT).getOrElse(DateFormatter.defaultPattern)

  val timestampFormatInRead: Option[String] =
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      Some(getString(TIMESTAMP_FORMAT).getOrElse(
        s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"))
    } else {
      getString(TIMESTAMP_FORMAT)
    }
  val timestampFormatInWrite: String = getString(TIMESTAMP_FORMAT).getOrElse(
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"
    } else {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXX]"
    })

  val timestampNTZFormatInRead: Option[String] = getString(TIMESTAMP_NTZ_FORMAT)
  val timestampNTZFormatInWrite: String = getString(TIMESTAMP_NTZ_FORMAT).getOrElse(
    s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS]")

  // SPARK-39731: Enables the backward compatible parsing behavior.
  // Generally, this config should be set to false to avoid producing potentially incorrect results
  // which is the current default (see UnivocityParser).
  //
  // If enabled and the date cannot be parsed, we will fall back to `DateTimeUtils.stringToDate`.
  // If enabled and the timestamp cannot be parsed, `DateTimeUtils.stringToTimestamp` will be used.
  // Otherwise, depending on the parser policy and a custom pattern, an exception may be thrown and
  // the value will be parsed as null.
  val enableDateTimeParsingFallback: Option[Boolean] =
    getString(ENABLE_DATETIME_PARSING_FALLBACK).map(_.toBoolean)

  val multiLine = getString(MULTI_LINE).map(_.toBoolean).getOrElse(false)

  val maxColumns = getInt(MAX_COLUMNS, 20480)

  val maxCharsPerColumn = getInt(MAX_CHARS_PER_COLUMN, -1)

  val escapeQuotes = getBool(ESCAPE_QUOTES, true)

  val quoteAll = getBool(QUOTE_ALL, false)

  /**
   * The max error content length in CSV parser/writer exception message.
   */
  val maxErrorContentLength = 1000

  val isCommentSet = this.comment != '\u0000'

  val samplingRatio =
    getString(SAMPLING_RATIO).map(_.toDouble).getOrElse(1.0)

  /**
   * Forcibly apply the specified or inferred schema to datasource files.
   * If the option is enabled, headers of CSV files will be ignored.
   */
  val enforceSchema = getBool(ENFORCE_SCHEMA, default = true)

  /**
   * String representation of an empty value in read and in write.
   */
  val emptyValue = getString(EMPTY_VALUE)
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
  val lineSeparator: Option[String] = getString(LINE_SEP).map { sep =>
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

  val inputBufferSize: Option[Int] = getString(INPUT_BUFFER_SIZE).map(_.toInt)
    .orElse(SQLConf.get.getConf(SQLConf.CSV_INPUT_BUFFER_SIZE))

  /**
   * The handling method to be used when unescaped quotes are found in the input.
   */
  val unescapedQuoteHandling: UnescapedQuoteHandling = UnescapedQuoteHandling.valueOf(
    getString(UNESCAPED_QUOTE_HANDLING).getOrElse("STOP_AT_DELIMITER").toUpperCase(Locale.ROOT))

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

object CSVOptions extends Enumeration {
  val HEADER = Value("header")
  val DELIMITER = Value("delimiter")
  val ENCODING = Value("encoding")
  val INFER_SCHEMA = Value("inferSchema")
  val IGNORE_LEADING_WHITESPACE = Value("ignoreLeadingWhiteSpace")
  val IGNORE_TRAILING_WHITESPACE = Value("ignoreTrailingWhiteSpace")
  val PREFERS_DATE = Value("prefersDate")
  val ESCAPE_QUOTES = Value("escapeQuotes")
  val QUOTE_ALL = Value("quoteAll")
  val ENFORCE_SCHEMA = Value("enforceSchema")
  val QUOTE = Value("quote")
  val ESCAPE = Value("escape")
  val COMMENT = Value("comment")
  val MAX_COLUMNS = Value("maxColumns")
  val MAX_CHARS_PER_COLUMN = Value("maxCharsPerColumn")
  val MODE = Value("mode")
  val CHAR_TO_ESCAPE_QUOTE_ESCAPING = Value("charToEscapeQuoteEscaping")
  val CODEC = Value("codec")
  val LOCALE = Value("locale")
  val DATE_FORMAT = Value("dateFormat")
  val TIMESTAMP_FORMAT = Value("timestampFormat")
  val TIMESTAMP_NTZ_FORMAT = Value("timestampNTZFormat")
  val ENABLE_DATETIME_PARSING_FALLBACK = Value("enableDateTimeParsingFallback")
  val MULTI_LINE = Value("multiLine")
  val SAMPLING_RATIO = Value("samplingRatio")
  val EMPTY_VALUE = Value("emptyValue")
  val LINE_SEP = Value("lineSep")
  val INPUT_BUFFER_SIZE = Value("inputBufferSize")
  val SEP = Value("sep")
  val CHARSET = Value("charset")
  val COLUMN_NAME_OF_CORRUPT_RECORD = Value("columnNameOfCorruptRecord")
  val NULL_VALUE = Value("nullValue")
  val NAN_VALUE = Value("nanValue")
  val POSITIVE_INF = Value("positiveInf")
  val NEGATIVE_INF = Value("negativeInf")
  val COMPRESSION = Value("compression")
  val TIME_ZONE = Value("timeZone")
  val UNESCAPED_QUOTE_HANDLING = Value("unescapedQuoteHandling")
}
