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
import java.text.SimpleDateFormat

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.{CompressionCodecs, ParseModes}

private[sql] class CSVOptions(@transient private val parameters: Map[String, String])
  extends Logging with Serializable {
  import org.apache.spark.sql.execution.datasources.ParameterUtils._

  val delimiter = CSVTypeCast.toChar(
    getNullSafeString(parameters, "sep", getNullSafeString(parameters, "delimiter", ",")))
  private val parseMode = getNullSafeString(parameters, "mode", "PERMISSIVE")
  val charset = getNullSafeString(parameters, "encoding",
    getNullSafeString(parameters, "charset", StandardCharsets.UTF_8.name()))

  val quote = getNullSafeChar(parameters, "quote", '\"')
  val escape = getNullSafeChar(parameters, "escape", '\\')
  val comment = getNullSafeChar(parameters, "comment", '\u0000')

  val headerFlag = getNullSafeBool(parameters, "header", default = false)
  val inferSchemaFlag = getNullSafeBool(parameters, "inferSchema", default = false)
  val ignoreLeadingWhiteSpaceFlag =
    getNullSafeBool(parameters, "ignoreLeadingWhiteSpace", default = false)
  val ignoreTrailingWhiteSpaceFlag =
    getNullSafeBool(parameters, "ignoreTrailingWhiteSpace", default = false)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logWarning(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  val failFast = ParseModes.isFailFastMode(parseMode)
  val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  val permissive = ParseModes.isPermissiveMode(parseMode)

  val nullValue = parameters.getOrElse("nullValue", "")

  val nanValue = parameters.getOrElse("nanValue", "NaN")

  val positiveInf = parameters.getOrElse("positiveInf", "Inf")
  val negativeInf = parameters.getOrElse("negativeInf", "-Inf")


  val compressionCodec: Option[String] = {
    val name = parameters.get("compression").orElse(parameters.get("codec"))
    name.map(CompressionCodecs.getCodecClassName)
  }

  // Share date format object as it is expensive to parse date pattern.
  val dateFormat: SimpleDateFormat = {
    val dateFormat = parameters.getOrElse("dateFormat", null)
    if (dateFormat != null) {
      new SimpleDateFormat(dateFormat)
    } else {
      null
    }
  }

  val maxColumns = getNullSafeInt(parameters, "maxColumns", 20480)

  val maxCharsPerColumn = getNullSafeInt(parameters, "maxCharsPerColumn", 1000000)

  val inputBufferSize = 128

  val isCommentSet = this.comment != '\u0000'

  val rowSeparator = "\n"
}

object CSVOptions {

  def apply(): CSVOptions = new CSVOptions(Map.empty)

  def apply(paramName: String, paramValue: String): CSVOptions = {
    new CSVOptions(Map(paramName -> paramValue))
  }
}
