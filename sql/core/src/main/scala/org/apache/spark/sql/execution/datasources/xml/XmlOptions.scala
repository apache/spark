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
package org.apache.spark.sql.execution.datasources.xml

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.catalyst.util.{ParseMode, PermissiveMode}

/**
 * Options for the XML data source.
 */
private[xml] class XmlOptions(
    @transient private val parameters: Map[String, String])
  extends Serializable {

  def this() = this(Map.empty)

  val charset = parameters.getOrElse("charset", XmlOptions.DEFAULT_CHARSET)
  val codec = parameters.get("compression").orElse(parameters.get("codec")).orNull
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
  val timestampFormat = parameters.get("timestampFormat")
  val timezone = parameters.get("timezone")
  val dateFormat = parameters.get("dateFormat")
}

private[xml] object XmlOptions {
  val DEFAULT_ATTRIBUTE_PREFIX = "_"
  val DEFAULT_VALUE_TAG = "_VALUE"
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_ROOT_TAG = "ROWS"
  val DEFAULT_DECLARATION = "version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\""
  val DEFAULT_ARRAY_ELEMENT_NAME = "item"
  val DEFAULT_CHARSET: String = StandardCharsets.UTF_8.name
  val DEFAULT_NULL_VALUE: String = null
  val DEFAULT_WILDCARD_COL_NAME = "xs_any"

  def apply(parameters: Map[String, String]): XmlOptions = new XmlOptions(parameters)
}
