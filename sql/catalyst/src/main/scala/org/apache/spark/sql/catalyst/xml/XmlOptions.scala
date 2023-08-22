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

import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util.Locale
import javax.xml.stream.XMLInputFactory

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs, DateFormatter, DateTimeUtils, ParseMode, PermissiveMode, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}

/**
 * Options for the XML data source.
 */
private[sql] class XmlOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends FileSourceOptions(parameters) with Logging {

  import XmlOptions._

  def this(
      parameters: Map[String, String] = Map.empty,
      defaultTimeZoneId: String = SQLConf.get.sessionLocalTimeZone,
      defaultColumnNameOfCorruptRecord: String = "") = {
    this(
      CaseInsensitiveMap(parameters),
      defaultTimeZoneId,
      defaultColumnNameOfCorruptRecord)
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

  val compressionCodec = parameters.get("compression").orElse(parameters.get("codec"))
    .map(CompressionCodecs.getCodecClassName)
  val rowTag = parameters.getOrElse("rowTag", XmlOptions.DEFAULT_ROW_TAG)
  require(rowTag.nonEmpty, "'rowTag' option should not be empty string.")
  require(!rowTag.startsWith("<") && !rowTag.endsWith(">"),
          "'rowTag' should not include angle brackets")
  val rootTag = parameters.getOrElse("rootTag", XmlOptions.DEFAULT_ROOT_TAG)
  require(!rootTag.startsWith("<") && !rootTag.endsWith(">"),
          "'rootTag' should not include angle brackets")
  val declaration = parameters.getOrElse("declaration", XmlOptions.DEFAULT_DECLARATION)
  require(!declaration.startsWith("<") && !declaration.endsWith(">"),
          "'declaration' should not include angle brackets")
  val arrayElementName = parameters.getOrElse("arrayElementName",
    XmlOptions.DEFAULT_ARRAY_ELEMENT_NAME)
  val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
  require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
  val excludeAttributeFlag = parameters.get("excludeAttribute").map(_.toBoolean).getOrElse(false)
  val treatEmptyValuesAsNulls =
    parameters.get("treatEmptyValuesAsNulls").map(_.toBoolean).getOrElse(false)
  val attributePrefix =
    parameters.getOrElse("attributePrefix", XmlOptions.DEFAULT_ATTRIBUTE_PREFIX)
  val valueTag = parameters.getOrElse("valueTag", XmlOptions.DEFAULT_VALUE_TAG)
  require(valueTag.nonEmpty, "'valueTag' option should not be empty string.")
  require(valueTag != attributePrefix,
    "'valueTag' and 'attributePrefix' options should not be the same.")
  val nullValue = parameters.getOrElse("nullValue", XmlOptions.DEFAULT_NULL_VALUE)
  val columnNameOfCorruptRecord =
    parameters.getOrElse("columnNameOfCorruptRecord", "_corrupt_record")
  val ignoreSurroundingSpaces =
    parameters.get("ignoreSurroundingSpaces").map(_.toBoolean).getOrElse(false)
  val parseMode = ParseMode.fromString(parameters.getOrElse("mode", PermissiveMode.name))
  val inferSchema = parameters.get("inferSchema").map(_.toBoolean).getOrElse(true)
  val rowValidationXSDPath = parameters.get("rowValidationXSDPath").orNull
  val wildcardColName =
    parameters.getOrElse("wildcardColName", XmlOptions.DEFAULT_WILDCARD_COL_NAME)
  val ignoreNamespace = parameters.get("ignoreNamespace").map(_.toBoolean).getOrElse(false)

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
  val timestampFormatInWrite: String = parameters.getOrElse(TIMESTAMP_FORMAT,
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"
    } else {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXX]"
    })

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

  val timezone = parameters.get("timezone")

  val zoneId: ZoneId = DateTimeUtils.getZoneId(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION,
      parameters.getOrElse(TIME_ZONE, defaultTimeZoneId)))

  // A language tag in IETF BCP 47 format
  val locale: Locale = parameters.get(LOCALE).map(Locale.forLanguageTag).getOrElse(Locale.US)

  val multiLine = parameters.get(MULTI_LINE).map(_.toBoolean).getOrElse(true)
  val charset = parameters.getOrElse(ENCODING,
    parameters.getOrElse(CHARSET, XmlOptions.DEFAULT_CHARSET))

  def buildXmlFactory(): XMLInputFactory = {
    XMLInputFactory.newInstance()
  }

  val timestampFormatter = TimestampFormatter(
    timestampFormatInRead,
    zoneId,
    locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  val timestampFormatterInWrite = TimestampFormatter(
    timestampFormatInWrite,
    zoneId,
    locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)

  val dateFormatter = DateFormatter(
    dateFormatInRead,
    locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  val dateFormatterInWrite = DateFormatter(
    dateFormatInWrite,
    locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)
}

private[sql] object XmlOptions extends DataSourceOptions {
  val DEFAULT_ATTRIBUTE_PREFIX = "_"
  val DEFAULT_VALUE_TAG = "_VALUE"
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_ROOT_TAG = "ROWS"
  val DEFAULT_DECLARATION = "version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\""
  val DEFAULT_ARRAY_ELEMENT_NAME = "item"
  val DEFAULT_CHARSET: String = StandardCharsets.UTF_8.name
  val DEFAULT_NULL_VALUE: String = null
  val DEFAULT_WILDCARD_COL_NAME = "xs_any"
  val PREFER_DATE = newOption("preferDate")
  val LOCALE = newOption("locale")
  val COMPRESSION = newOption("compression")
  val ENABLE_DATETIME_PARSING_FALLBACK = newOption("enableDateTimeParsingFallback")
  val MULTI_LINE = newOption("multiLine")
  val DATE_FORMAT = newOption("dateFormat")
  val TIMESTAMP_FORMAT = newOption("timestampFormat")
  // Options with alternative
  val ENCODING = "encoding"
  val CHARSET = "charset"
  newOption(ENCODING, CHARSET)
  val TIME_ZONE = "timezone"
  newOption(DateTimeUtils.TIMEZONE_OPTION, TIME_ZONE)

  def apply(parameters: Map[String, String]): XmlOptions =
    new XmlOptions(parameters, SQLConf.get.sessionLocalTimeZone)

  def apply(): XmlOptions =
    new XmlOptions(Map.empty, SQLConf.get.sessionLocalTimeZone)
}
