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

package org.apache.spark.sql.catalyst.json

import java.nio.charset.{Charset, StandardCharsets}
import java.time.ZoneId
import java.util.Locale

import com.fasterxml.jackson.core.{JsonFactory, JsonFactoryBuilder}
import com.fasterxml.jackson.core.json.JsonReadFeature

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy

/**
 * Options for parsing JSON data into Spark SQL rows.
 *
 * Most of these map directly to Jackson's internal options, specified in [[JsonReadFeature]].
 */
private[sql] class JSONOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends FileSourceOptions(parameters) with Logging  {

  import JSONOptions._

  def this(
    parameters: Map[String, String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String = "") = {
      this(
        CaseInsensitiveMap(parameters),
        defaultTimeZoneId,
        defaultColumnNameOfCorruptRecord)
  }

  val samplingRatio =
    parameters.get(SAMPLING_RATIO).map(_.toDouble).getOrElse(1.0)
  val primitivesAsString =
    parameters.get(PRIMITIVES_AS_STRING).map(_.toBoolean).getOrElse(false)
  val prefersDecimal =
    parameters.get(PREFERS_DECIMAL).map(_.toBoolean).getOrElse(false)
  val allowComments =
    parameters.get(ALLOW_COMMENTS).map(_.toBoolean).getOrElse(false)
  val allowUnquotedFieldNames =
    parameters.get(ALLOW_UNQUOTED_FIELD_NAMES).map(_.toBoolean).getOrElse(false)
  val allowSingleQuotes =
    parameters.get(ALLOW_SINGLE_QUOTES).map(_.toBoolean).getOrElse(true)
  val allowNumericLeadingZeros =
    parameters.get(ALLOW_NUMERIC_LEADING_ZEROS).map(_.toBoolean).getOrElse(false)
  val allowNonNumericNumbers =
    parameters.get(ALLOW_NON_NUMERIC_NUMBERS).map(_.toBoolean).getOrElse(true)
  val allowBackslashEscapingAnyCharacter =
    parameters.get(ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER).map(_.toBoolean).getOrElse(false)
  private val allowUnquotedControlChars =
    parameters.get(ALLOW_UNQUOTED_CONTROL_CHARS).map(_.toBoolean).getOrElse(false)
  val compressionCodec = parameters.get(COMPRESSION).map(CompressionCodecs.getCodecClassName)
  val parseMode: ParseMode =
    parameters.get(MODE).map(ParseMode.fromString).getOrElse(PermissiveMode)
  val columnNameOfCorruptRecord =
    parameters.getOrElse(COLUMN_NAME_OF_CORRUPTED_RECORD, defaultColumnNameOfCorruptRecord)

  // Whether to ignore column of all null values or empty array/struct during schema inference
  val dropFieldIfAllNull = parameters.get(DROP_FIELD_IF_ALL_NULL).map(_.toBoolean).getOrElse(false)

  // Whether to ignore null fields during json generating
  val ignoreNullFields = parameters.get(IGNORE_NULL_FIELDS).map(_.toBoolean)
    .getOrElse(SQLConf.get.jsonGeneratorIgnoreNullFields)

  // If this is true, when writing NULL values to columns of JSON tables with explicit DEFAULT
  // values, never skip writing the NULL values to storage, overriding 'ignoreNullFields' above.
  // This can be useful to enforce that inserted NULL values are present in storage to differentiate
  // from missing data.
  val writeNullIfWithDefaultValue = SQLConf.get.jsonWriteNullIfWithDefaultValue

  // A language tag in IETF BCP 47 format
  val locale: Locale = parameters.get(LOCALE).map(Locale.forLanguageTag).getOrElse(Locale.US)

  val zoneId: ZoneId = DateTimeUtils.getZoneId(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  val dateFormatInRead: Option[String] = parameters.get(DATE_FORMAT)
  val dateFormatInWrite: String = parameters.getOrElse(DATE_FORMAT, DateFormatter.defaultPattern)

  val timestampFormatInRead: Option[String] =
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      Some(parameters.getOrElse(TIMESTAMP_FORMAT,
        s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"))
    } else {
      parameters.get(TIMESTAMP_FORMAT)
    }
  val timestampFormatInWrite: String = parameters.getOrElse(TIMESTAMP_FORMAT,
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"
    } else {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXX]"
    })

  val timestampNTZFormatInRead: Option[String] = parameters.get(TIMESTAMP_NTZ_FORMAT)
  val timestampNTZFormatInWrite: String =
    parameters.getOrElse(TIMESTAMP_NTZ_FORMAT, s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS]")

  // SPARK-39731: Enables the backward compatible parsing behavior.
  // Generally, this config should be set to false to avoid producing potentially incorrect results
  // which is the current default (see JacksonParser).
  //
  // If enabled and the date cannot be parsed, we will fall back to `DateTimeUtils.stringToDate`.
  // If enabled and the timestamp cannot be parsed, `DateTimeUtils.stringToTimestamp` will be used.
  // Otherwise, depending on the parser policy and a custom pattern, an exception may be thrown and
  // the value will be parsed as null.
  val enableDateTimeParsingFallback: Option[Boolean] =
    parameters.get(ENABLE_DATETIME_PARSING_FALLBACK).map(_.toBoolean)

  val multiLine = parameters.get(MULTI_LINE).map(_.toBoolean).getOrElse(false)

  /**
   * A string between two consecutive JSON records.
   */
  val lineSeparator: Option[String] = parameters.get(LINE_SEP).map { sep =>
    require(sep.nonEmpty, "'lineSep' cannot be an empty string.")
    sep
  }

  protected def checkedEncoding(enc: String): String = enc

  /**
   * Standard encoding (charset) name. For example UTF-8, UTF-16LE and UTF-32BE.
   * If the encoding is not specified (None) in read, it will be detected automatically
   * when the multiLine option is set to `true`. If encoding is not specified in write,
   * UTF-8 is used by default.
   */
  val encoding: Option[String] = parameters.get(ENCODING)
    .orElse(parameters.get(CHARSET)).map(checkedEncoding)

  val lineSeparatorInRead: Option[Array[Byte]] = lineSeparator.map { lineSep =>
    lineSep.getBytes(encoding.getOrElse(StandardCharsets.UTF_8.name()))
  }
  val lineSeparatorInWrite: String = lineSeparator.getOrElse("\n")

  /**
   * Generating JSON strings in pretty representation if the parameter is enabled.
   */
  val pretty: Boolean = parameters.get(PRETTY).map(_.toBoolean).getOrElse(false)

  /**
   * Enables inferring of TimestampType and TimestampNTZType from strings matched to the
   * corresponding timestamp pattern defined by the timestampFormat and timestampNTZFormat options
   * respectively.
   */
  val inferTimestamp: Boolean = parameters.get(INFER_TIMESTAMP).map(_.toBoolean).getOrElse(false)

  /**
   * Generating \u0000 style codepoints for non-ASCII characters if the parameter is enabled.
   */
  val writeNonAsciiCharacterAsCodePoint: Boolean =
    parameters.get(WRITE_NON_ASCII_CHARACTER_AS_CODEPOINT).map(_.toBoolean).getOrElse(false)

  /** Build a Jackson [[JsonFactory]] using JSON options. */
  def buildJsonFactory(): JsonFactory = {
    new JsonFactoryBuilder()
      .configure(JsonReadFeature.ALLOW_JAVA_COMMENTS, allowComments)
      .configure(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES, allowUnquotedFieldNames)
      .configure(JsonReadFeature.ALLOW_SINGLE_QUOTES, allowSingleQuotes)
      .configure(JsonReadFeature.ALLOW_LEADING_ZEROS_FOR_NUMBERS, allowNumericLeadingZeros)
      .configure(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS, allowNonNumericNumbers)
      .configure(
        JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
        allowBackslashEscapingAnyCharacter)
      .configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS, allowUnquotedControlChars)
      .build()
  }
}

private[sql] class JSONOptionsInRead(
    @transient override val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends JSONOptions(parameters, defaultTimeZoneId, defaultColumnNameOfCorruptRecord) {

  def this(
    parameters: Map[String, String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String = "") = {
    this(
      CaseInsensitiveMap(parameters),
      defaultTimeZoneId,
      defaultColumnNameOfCorruptRecord)
  }

  protected override def checkedEncoding(enc: String): String = {
    val isDenied = JSONOptionsInRead.denyList.contains(Charset.forName(enc))
    require(multiLine || !isDenied,
      s"""The $enc encoding must not be included in the denyList when multiLine is disabled:
         |denylist: ${JSONOptionsInRead.denyList.mkString(", ")}""".stripMargin)

    val isLineSepRequired =
        multiLine || Charset.forName(enc) == StandardCharsets.UTF_8 || lineSeparator.nonEmpty
    require(isLineSepRequired, s"The lineSep option must be specified for the $enc encoding")

    enc
  }
}

private[sql] object JSONOptionsInRead {
  // The following encodings are not supported in per-line mode (multiline is false)
  // because they cause some problems in reading files with BOM which is supposed to
  // present in the files with such encodings. After splitting input files by lines,
  // only the first lines will have the BOM which leads to impossibility for reading
  // the rest lines. Besides of that, the lineSep option must have the BOM in such
  // encodings which can never present between lines.
  val denyList = Seq(
    Charset.forName("UTF-16"),
    Charset.forName("UTF-32")
  )
}

object JSONOptions extends DataSourceOptions {
  val SAMPLING_RATIO = newOption("samplingRatio")
  val PRIMITIVES_AS_STRING = newOption("primitivesAsString")
  val PREFERS_DECIMAL = newOption("prefersDecimal")
  val ALLOW_COMMENTS = newOption("allowComments")
  val ALLOW_UNQUOTED_FIELD_NAMES = newOption("allowUnquotedFieldNames")
  val ALLOW_SINGLE_QUOTES = newOption("allowSingleQuotes")
  val ALLOW_NUMERIC_LEADING_ZEROS = newOption("allowNumericLeadingZeros")
  val ALLOW_NON_NUMERIC_NUMBERS = newOption("allowNonNumericNumbers")
  val ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER = newOption("allowBackslashEscapingAnyCharacter")
  val ALLOW_UNQUOTED_CONTROL_CHARS = newOption("allowUnquotedControlChars")
  val COMPRESSION = newOption("compression")
  val MODE = newOption("mode")
  val DROP_FIELD_IF_ALL_NULL = newOption("dropFieldIfAllNull")
  val IGNORE_NULL_FIELDS = newOption("ignoreNullFields")
  val LOCALE = newOption("locale")
  val DATE_FORMAT = newOption("dateFormat")
  val TIMESTAMP_FORMAT = newOption("timestampFormat")
  val TIMESTAMP_NTZ_FORMAT = newOption("timestampNTZFormat")
  val ENABLE_DATETIME_PARSING_FALLBACK = newOption("enableDateTimeParsingFallback")
  val MULTI_LINE = newOption("multiLine")
  val LINE_SEP = newOption("lineSep")
  val PRETTY = newOption("pretty")
  val INFER_TIMESTAMP = newOption("inferTimestamp")
  val COLUMN_NAME_OF_CORRUPTED_RECORD = newOption("columnNameOfCorruptRecord")
  val TIME_ZONE = newOption("timeZone")
  val WRITE_NON_ASCII_CHARACTER_AS_CODEPOINT = newOption("writeNonAsciiCharacterAsCodePoint")
  // Options with alternative
  val ENCODING = "encoding"
  val CHARSET = "charset"
  newOption(ENCODING, CHARSET)
}
