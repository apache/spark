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
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs, DateFormatter, DateTimeUtils, ParseMode, PermissiveMode}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}

/**
 * Options for the XML data source.
 */
class XmlOptions(
    val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String,
    rowTagRequired: Boolean)
  extends FileSourceOptions(parameters) with Logging {

  import XmlOptions._

  def this(
      parameters: Map[String, String] = Map.empty,
      defaultTimeZoneId: String = SQLConf.get.sessionLocalTimeZone,
      defaultColumnNameOfCorruptRecord: String = SQLConf.get.columnNameOfCorruptRecord,
      rowTagRequired: Boolean = false) = {
    this(
      CaseInsensitiveMap(parameters),
      defaultTimeZoneId,
      defaultColumnNameOfCorruptRecord,
      rowTagRequired)
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

  val compressionCodec = parameters.get(COMPRESSION).map(CompressionCodecs.getCodecClassName)
  val rowTagOpt = parameters.get(XmlOptions.ROW_TAG).map(_.trim)

  if (rowTagRequired && rowTagOpt.isEmpty) {
    throw QueryCompilationErrors.xmlRowTagRequiredError(XmlOptions.ROW_TAG)
  }

  val rowTag = rowTagOpt.getOrElse(XmlOptions.DEFAULT_ROW_TAG)
  require(rowTag.nonEmpty, s"'$ROW_TAG' option should not be an empty string.")
  require(!rowTag.startsWith("<") && !rowTag.endsWith(">"),
          s"'$ROW_TAG' should not include angle brackets")
  val rootTag = parameters.getOrElse(ROOT_TAG, XmlOptions.DEFAULT_ROOT_TAG)
  require(!rootTag.startsWith("<") && !rootTag.endsWith(">"),
          s"'$ROOT_TAG' should not include angle brackets")
  val declaration = parameters.getOrElse(DECLARATION, XmlOptions.DEFAULT_DECLARATION)
  require(!declaration.startsWith("<") && !declaration.endsWith(">"),
          s"'$DECLARATION' should not include angle brackets")
  val arrayElementName = parameters.getOrElse(ARRAY_ELEMENT_NAME,
    XmlOptions.DEFAULT_ARRAY_ELEMENT_NAME)
  val samplingRatio = parameters.get(SAMPLING_RATIO).map(_.toDouble).getOrElse(1.0)
  require(samplingRatio > 0, s"$SAMPLING_RATIO ($samplingRatio) should be greater than 0")
  val excludeAttributeFlag = getBool(EXCLUDE_ATTRIBUTE, false)
  val attributePrefix =
    parameters.getOrElse(ATTRIBUTE_PREFIX, XmlOptions.DEFAULT_ATTRIBUTE_PREFIX)
  val valueTag = parameters.getOrElse(VALUE_TAG, XmlOptions.DEFAULT_VALUE_TAG)
  require(valueTag.nonEmpty, s"'$VALUE_TAG' option should not be empty string.")
  require(valueTag != attributePrefix,
    s"'$VALUE_TAG' and '$ATTRIBUTE_PREFIX' options should not be the same.")
  val nullValue = parameters.getOrElse(NULL_VALUE, XmlOptions.DEFAULT_NULL_VALUE)
  val columnNameOfCorruptRecord =
    parameters.getOrElse(COLUMN_NAME_OF_CORRUPT_RECORD, defaultColumnNameOfCorruptRecord)
  val ignoreSurroundingSpaces = getBool(IGNORE_SURROUNDING_SPACES, true)
  val parseMode = ParseMode.fromString(parameters.getOrElse(MODE, PermissiveMode.name))
  val inferSchema = getBool(INFER_SCHEMA, true)
  val rowValidationXSDPath = parameters.get(ROW_VALIDATION_XSD_PATH).orNull
  val wildcardColName =
    parameters.getOrElse(WILDCARD_COL_NAME, XmlOptions.DEFAULT_WILDCARD_COL_NAME)
  val ignoreNamespace = getBool(IGNORE_NAMESPACE, false)
  val prefersDecimal =
    parameters.get(PREFERS_DECIMAL).map(_.toBoolean).getOrElse(false)
  // setting indent to "" disables indentation in the generated XML.
  // Each row will be written in a new line.
  val indent = parameters.getOrElse(INDENT, DEFAULT_INDENT)
  val validateName = getBool(VALIDATE_NAME, true)

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
  val timestampNTZFormatInWrite: String =
    parameters.getOrElse(TIMESTAMP_NTZ_FORMAT, s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS]")

  val timezone = parameters.get("timezone")

  val zoneId: ZoneId = DateTimeUtils.getZoneId(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION,
      parameters.getOrElse(TIME_ZONE, defaultTimeZoneId)))

  // A language tag in IETF BCP 47 format
  val locale: Locale = parameters.get(LOCALE).map(Locale.forLanguageTag).getOrElse(Locale.US)

  val multiLine = parameters.get(MULTI_LINE).map(_.toBoolean).getOrElse(true)
  val charset = parameters.getOrElse(ENCODING,
    parameters.getOrElse(CHARSET, XmlOptions.DEFAULT_CHARSET))

  // This option takes in a column name and specifies that the entire XML record should be stored
  // as a single VARIANT type column in the table with the given column name.
  // E.g. spark.read.format("xml").option("singleVariantColumn", "colName")
  val singleVariantColumn = parameters.get(SINGLE_VARIANT_COLUMN)

  def buildXmlFactory(): XMLInputFactory = {
    XMLInputFactory.newInstance()
  }
}

object XmlOptions extends DataSourceOptions {
  val DEFAULT_ATTRIBUTE_PREFIX = "_"
  val DEFAULT_VALUE_TAG = "_VALUE"
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_ROOT_TAG = "ROWS"
  val DEFAULT_DECLARATION = "version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\""
  val DEFAULT_ARRAY_ELEMENT_NAME = "item"
  val DEFAULT_CHARSET: String = StandardCharsets.UTF_8.name
  val DEFAULT_NULL_VALUE: String = null
  val DEFAULT_WILDCARD_COL_NAME = "xs_any"
  val DEFAULT_INDENT = "    "
  val ROW_TAG = newOption("rowTag")
  val ROOT_TAG = newOption("rootTag")
  val DECLARATION = newOption("declaration")
  val ARRAY_ELEMENT_NAME = newOption("arrayElementName")
  val EXCLUDE_ATTRIBUTE = newOption("excludeAttribute")
  val ATTRIBUTE_PREFIX = newOption("attributePrefix")
  val VALUE_TAG = newOption("valueTag")
  val NULL_VALUE = newOption("nullValue")
  val IGNORE_SURROUNDING_SPACES = newOption("ignoreSurroundingSpaces")
  val ROW_VALIDATION_XSD_PATH = newOption("rowValidationXSDPath")
  val WILDCARD_COL_NAME = newOption("wildcardColName")
  val IGNORE_NAMESPACE = newOption("ignoreNamespace")
  val INFER_SCHEMA = newOption("inferSchema")
  val PREFER_DATE = newOption("preferDate")
  val MODE = newOption("mode")
  val LOCALE = newOption("locale")
  val COMPRESSION = newOption("compression")
  val MULTI_LINE = newOption("multiLine")
  val SAMPLING_RATIO = newOption("samplingRatio")
  val COLUMN_NAME_OF_CORRUPT_RECORD = newOption(DataSourceOptions.COLUMN_NAME_OF_CORRUPT_RECORD)
  val DATE_FORMAT = newOption("dateFormat")
  val TIMESTAMP_FORMAT = newOption("timestampFormat")
  val TIMESTAMP_NTZ_FORMAT = newOption("timestampNTZFormat")
  val TIME_ZONE = newOption("timeZone")
  val INDENT = newOption("indent")
  val PREFERS_DECIMAL = newOption("prefersDecimal")
  val VALIDATE_NAME = newOption("validateName")
  val SINGLE_VARIANT_COLUMN = newOption("singleVariantColumn")
  // Options with alternative
  val ENCODING = "encoding"
  val CHARSET = "charset"
  newOption(ENCODING, CHARSET)

  def apply(parameters: Map[String, String]): XmlOptions =
    new XmlOptions(parameters)

  def apply(): XmlOptions =
    new XmlOptions(Map.empty)
}
