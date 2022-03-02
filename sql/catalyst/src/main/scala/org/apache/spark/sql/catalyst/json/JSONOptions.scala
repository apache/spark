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
  extends Logging with Serializable  {

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
    parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
  val primitivesAsString =
    parameters.get("primitivesAsString").map(_.toBoolean).getOrElse(false)
  val prefersDecimal =
    parameters.get("prefersDecimal").map(_.toBoolean).getOrElse(false)
  val allowComments =
    parameters.get("allowComments").map(_.toBoolean).getOrElse(false)
  val allowUnquotedFieldNames =
    parameters.get("allowUnquotedFieldNames").map(_.toBoolean).getOrElse(false)
  val allowSingleQuotes =
    parameters.get("allowSingleQuotes").map(_.toBoolean).getOrElse(true)
  val allowNumericLeadingZeros =
    parameters.get("allowNumericLeadingZeros").map(_.toBoolean).getOrElse(false)
  val allowNonNumericNumbers =
    parameters.get("allowNonNumericNumbers").map(_.toBoolean).getOrElse(true)
  val allowBackslashEscapingAnyCharacter =
    parameters.get("allowBackslashEscapingAnyCharacter").map(_.toBoolean).getOrElse(false)
  private val allowUnquotedControlChars =
    parameters.get("allowUnquotedControlChars").map(_.toBoolean).getOrElse(false)
  val compressionCodec = parameters.get("compression").map(CompressionCodecs.getCodecClassName)
  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(PermissiveMode)
  val columnNameOfCorruptRecord =
    parameters.getOrElse("columnNameOfCorruptRecord", defaultColumnNameOfCorruptRecord)

  // Whether to ignore column of all null values or empty array/struct during schema inference
  val dropFieldIfAllNull = parameters.get("dropFieldIfAllNull").map(_.toBoolean).getOrElse(false)

  // Whether to ignore null fields during json generating
  val ignoreNullFields = parameters.get("ignoreNullFields").map(_.toBoolean)
    .getOrElse(SQLConf.get.jsonGeneratorIgnoreNullFields)

  // A language tag in IETF BCP 47 format
  val locale: Locale = parameters.get("locale").map(Locale.forLanguageTag).getOrElse(Locale.US)

  val zoneId: ZoneId = DateTimeUtils.getZoneId(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

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
  val timestampNTZFormatInWrite: String =
    parameters.getOrElse("timestampNTZFormat", s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS]")

  val multiLine = parameters.get("multiLine").map(_.toBoolean).getOrElse(false)

  /**
   * A string between two consecutive JSON records.
   */
  val lineSeparator: Option[String] = parameters.get("lineSep").map { sep =>
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
  val encoding: Option[String] = parameters.get("encoding")
    .orElse(parameters.get("charset")).map(checkedEncoding)

  val lineSeparatorInRead: Option[Array[Byte]] = lineSeparator.map { lineSep =>
    lineSep.getBytes(encoding.getOrElse(StandardCharsets.UTF_8.name()))
  }
  val lineSeparatorInWrite: String = lineSeparator.getOrElse("\n")

  /**
   * Generating JSON strings in pretty representation if the parameter is enabled.
   */
  val pretty: Boolean = parameters.get("pretty").map(_.toBoolean).getOrElse(false)

  /**
   * Enables inferring of TimestampType and TimestampNTZType from strings matched to the
   * corresponding timestamp pattern defined by the timestampFormat and timestampNTZFormat options
   * respectively.
   */
  val inferTimestamp: Boolean = parameters.get("inferTimestamp").map(_.toBoolean).getOrElse(false)

  /**
   * Generating \u0000 style codepoints for non-ASCII characters if the parameter is enabled.
   */
  val writeNonAsciiCharacterAsCodePoint: Boolean =
    parameters.get("writeNonAsciiCharacterAsCodePoint").map(_.toBoolean).getOrElse(false)

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
