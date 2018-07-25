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
import java.util.{Locale, TimeZone}

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util._

/**
 * Options for parsing JSON data into Spark SQL rows.
 *
 * Most of these map directly to Jackson's internal options, specified in [[JsonParser.Feature]].
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

  val timeZone: TimeZone = DateTimeUtils.getTimeZone(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(
      parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), timeZone, Locale.US)

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
    lineSep.getBytes(encoding.getOrElse("UTF-8"))
  }
  val lineSeparatorInWrite: String = lineSeparator.getOrElse("\n")

  /** Sets config options on a Jackson [[JsonFactory]]. */
  def setJacksonOptions(factory: JsonFactory): Unit = {
    factory.configure(JsonParser.Feature.ALLOW_COMMENTS, allowComments)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, allowUnquotedFieldNames)
    factory.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, allowSingleQuotes)
    factory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, allowNumericLeadingZeros)
    factory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, allowNonNumericNumbers)
    factory.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
      allowBackslashEscapingAnyCharacter)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, allowUnquotedControlChars)
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
    val isBlacklisted = JSONOptionsInRead.blacklist.contains(Charset.forName(enc))
    require(multiLine || !isBlacklisted,
      s"""The ${enc} encoding must not be included in the blacklist when multiLine is disabled:
         |Blacklist: ${JSONOptionsInRead.blacklist.mkString(", ")}""".stripMargin)

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
  val blacklist = Seq(
    Charset.forName("UTF-16"),
    Charset.forName("UTF-32")
  )
}
