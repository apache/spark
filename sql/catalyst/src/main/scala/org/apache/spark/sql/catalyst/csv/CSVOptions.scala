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
import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumnsUtils.EXISTS_DEFAULT_COLUMN_METADATA_KEY
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types.StructType

class CSVOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    val columnPruning: Boolean,
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends FileSourceOptions(parameters) with Logging {

  import CSVOptions._

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
    parameters.getOrElse(SEP, parameters.getOrElse(DELIMITER, ",")))
  val parseMode: ParseMode =
    parameters.get(MODE).map(ParseMode.fromString).getOrElse(PermissiveMode)
  val charset = parameters.get(ENCODING).orElse(parameters.get(CHARSET))
    .map(CharsetProvider.forName(_, SQLConf.get.legacyJavaCharsets, caller = "CSVOptions"))
    .getOrElse(StandardCharsets.UTF_8).name()

  val quote = getChar(QUOTE, '\"')
  val escape = getChar(ESCAPE, '\\')
  val charToEscapeQuoteEscaping = parameters.get(CHAR_TO_ESCAPE_QUOTE_ESCAPING) match {
    case None => None
    case Some(null) => None
    case Some(value) if value.length == 0 => None
    case Some(value) if value.length == 1 => Some(value.charAt(0))
    case _ => throw QueryExecutionErrors.paramExceedOneCharError(CHAR_TO_ESCAPE_QUOTE_ESCAPING)
  }
  val comment = getChar(COMMENT, '\u0000')

  val headerFlag = getBool(HEADER)
  val inferSchemaFlag = getBool(INFER_SCHEMA)
  val inferStringTypeForTimeOnlyColumnFlag =
    getBool(INFER_STRING_TYPE_FOR_TIME_ONLY_COLUMN, default = false)
  val ignoreLeadingWhiteSpaceInRead = getBool(IGNORE_LEADING_WHITESPACE, default = false)
  val ignoreTrailingWhiteSpaceInRead = getBool(IGNORE_TRAILING_WHITESPACE, default = false)

  // For write, both options were `true` by default. We leave it as `true` for
  // backwards compatibility.
  val ignoreLeadingWhiteSpaceFlagInWrite = getBool(IGNORE_LEADING_WHITESPACE, default = true)
  val ignoreTrailingWhiteSpaceFlagInWrite = getBool(IGNORE_TRAILING_WHITESPACE, default = true)

  val columnNameOfCorruptRecord =
    parameters.getOrElse(COLUMN_NAME_OF_CORRUPT_RECORD, defaultColumnNameOfCorruptRecord)

  val nullValue = parameters.getOrElse(NULL_VALUE, "")

  val nanValue = parameters.getOrElse(NAN_VALUE, "NaN")

  val positiveInf = parameters.getOrElse(POSITIVE_INF, "Inf")
  val negativeInf = parameters.getOrElse(NEGATIVE_INF, "-Inf")


  val compressionCodec: Option[String] = {
    val name = parameters.get(COMPRESSION).orElse(parameters.get(CODEC))
    name.map(CompressionCodecs.getCodecClassName)
  }

  val zoneId: ZoneId = DateTimeUtils.getZoneId(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  // A language tag in IETF BCP 47 format
  val locale: Locale = parameters.get(LOCALE).map(Locale.forLanguageTag).getOrElse(Locale.US)

  /**
   * Infer columns with all valid date entries as date type (otherwise inferred as string or
   * timestamp type) if schema inference is enabled.
   *
   * Enabled by default.
   *
   * Not compatible with legacyTimeParserPolicy == LEGACY since legacy date parser will accept
   * extra trailing characters. Thus, disabled when legacyTimeParserPolicy == LEGACY
   */
  val preferDate = {
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      false
    } else {
      getBool(PREFER_DATE, true)
    }
  }

  val dateFormatOption: Option[String] = parameters.get(DATE_FORMAT)
  // Provide a default value for dateFormatInRead when preferDate. This ensures that the
  // Iso8601DateFormatter (with strict date parsing) is used for date inference
  val dateFormatInRead: Option[String] =
    if (preferDate) {
      Option(dateFormatOption.getOrElse(DateFormatter.defaultPattern))
    } else {
      dateFormatOption
    }
  val dateFormatInWrite: String = parameters.getOrElse(DATE_FORMAT, DateFormatter.defaultPattern)

  val timestampFormatInRead: Option[String] =
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      Some(parameters.getOrElse(TIMESTAMP_FORMAT,
        s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"))
    } else {
      parameters.get(TIMESTAMP_FORMAT)
    }
  val timestampFormatInWrite: String =
    parameters.getOrElse(TIMESTAMP_FORMAT, commonTimestampFormat)

  val timestampNTZFormatInRead: Option[String] = parameters.get(TIMESTAMP_NTZ_FORMAT)
  val timestampNTZFormatInWrite: String = parameters.getOrElse(TIMESTAMP_NTZ_FORMAT,
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
    parameters.get(ENABLE_DATETIME_PARSING_FALLBACK).map(_.toBoolean)

  val multiLine = parameters.get(MULTI_LINE).map(_.toBoolean).getOrElse(false)

  val maxColumns = getInt(MAX_COLUMNS, 20480)

  val maxCharsPerColumn = getInt(MAX_CHARS_PER_COLUMN, -1)

  val escapeQuotes = getBool(ESCAPE_QUOTES, true)

  val quoteAll = getBool(QUOTE_ALL, false)

  /**
   * The max error content length in CSV parser/writer exception message.
   */
  val maxErrorContentLength = 1000

  val isCommentSet = parameters.get(COMMENT) match {
    case Some(value) if value.length == 1 => true
    case _ => false
  }

  val samplingRatio =
    parameters.get(SAMPLING_RATIO).map(_.toDouble).getOrElse(1.0)

  /**
   * Forcibly apply the specified or inferred schema to datasource files.
   * If the option is enabled, headers of CSV files will be ignored.
   */
  val enforceSchema = getBool(ENFORCE_SCHEMA, default = true)

  /**
   * String representation of an empty value in read and in write.
   */
  val emptyValue = parameters.get(EMPTY_VALUE)
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
  val lineSeparator: Option[String] = parameters.get(LINE_SEP).map { sep =>
    require(sep != null, "'lineSep' cannot be a null value.")
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

  val inputBufferSize: Option[Int] = parameters.get(INPUT_BUFFER_SIZE).map(_.toInt)
    .orElse(SQLConf.get.getConf(SQLConf.CSV_INPUT_BUFFER_SIZE))

  /**
   * The handling method to be used when unescaped quotes are found in the input.
   */
  val unescapedQuoteHandling: UnescapedQuoteHandling = UnescapedQuoteHandling.valueOf(parameters
    .getOrElse(UNESCAPED_QUOTE_HANDLING, "STOP_AT_DELIMITER").toUpperCase(Locale.ROOT))

  /**
   * Returns true if column pruning is enabled and there are no existence column default values in
   * the [[schema]].
   *
   * The column pruning feature can be enabled either via the CSV option `columnPruning` or
   * in non-multiline mode via initialization of CSV options by the SQL config:
   * `spark.sql.csv.parser.columnPruning.enabled`.
   * The feature is disabled in the `multiLine` mode because of the issue:
   * https://github.com/uniVocity/univocity-parsers/issues/529
   *
   * We disable column pruning when there are any column defaults, instead preferring to reach in
   * each row and then post-process it to substitute the default values after.
   */
  def isColumnPruningEnabled(schema: StructType): Boolean =
    isColumnPruningOptionEnabled &&
      !schema.exists(_.metadata.contains(EXISTS_DEFAULT_COLUMN_METADATA_KEY))

  private val isColumnPruningOptionEnabled: Boolean =
    getBool(COLUMN_PRUNING, !multiLine && columnPruning)

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

object CSVOptions extends DataSourceOptions {
  val HEADER = newOption("header")
  val INFER_SCHEMA = newOption("inferSchema")
  val IGNORE_LEADING_WHITESPACE = newOption("ignoreLeadingWhiteSpace")
  val IGNORE_TRAILING_WHITESPACE = newOption("ignoreTrailingWhiteSpace")
  val PREFER_DATE = newOption("preferDate")
  val ESCAPE_QUOTES = newOption("escapeQuotes")
  val QUOTE_ALL = newOption("quoteAll")
  val ENFORCE_SCHEMA = newOption("enforceSchema")
  val QUOTE = newOption("quote")
  val ESCAPE = newOption("escape")
  val COMMENT = newOption("comment")
  val MAX_COLUMNS = newOption("maxColumns")
  val MAX_CHARS_PER_COLUMN = newOption("maxCharsPerColumn")
  val MODE = newOption("mode")
  val CHAR_TO_ESCAPE_QUOTE_ESCAPING = newOption("charToEscapeQuoteEscaping")
  val LOCALE = newOption("locale")
  val DATE_FORMAT = newOption("dateFormat")
  val TIMESTAMP_FORMAT = newOption("timestampFormat")
  val TIMESTAMP_NTZ_FORMAT = newOption("timestampNTZFormat")
  val ENABLE_DATETIME_PARSING_FALLBACK = newOption("enableDateTimeParsingFallback")
  val MULTI_LINE = newOption("multiLine")
  val SAMPLING_RATIO = newOption("samplingRatio")
  val EMPTY_VALUE = newOption("emptyValue")
  val LINE_SEP = newOption("lineSep")
  val INPUT_BUFFER_SIZE = newOption("inputBufferSize")
  val COLUMN_NAME_OF_CORRUPT_RECORD = newOption("columnNameOfCorruptRecord")
  val NULL_VALUE = newOption("nullValue")
  val NAN_VALUE = newOption("nanValue")
  val POSITIVE_INF = newOption("positiveInf")
  val NEGATIVE_INF = newOption("negativeInf")
  val TIME_ZONE = newOption("timeZone")
  val UNESCAPED_QUOTE_HANDLING = newOption("unescapedQuoteHandling")
  // Options with alternative
  val ENCODING = "encoding"
  val CHARSET = "charset"
  newOption(ENCODING, CHARSET)
  val COMPRESSION = "compression"
  val CODEC = "codec"
  newOption(COMPRESSION, CODEC)
  val SEP = "sep"
  val DELIMITER = "delimiter"
  newOption(SEP, DELIMITER)
  val COLUMN_PRUNING = newOption("columnPruning")
  val INFER_STRING_TYPE_FOR_TIME_ONLY_COLUMN = newOption("inferStringTypeForTimeOnlyColumn")
}
