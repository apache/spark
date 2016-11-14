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
import java.util.Locale

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs, ParseModes}

private[csv] class CSVOptions(@transient private val parameters: Map[String, String])
  extends Logging with Serializable {

  private val caseInsensitiveOptions = new CaseInsensitiveMap(parameters)

  private def getChar(paramName: String, default: Char): Char = {
    val paramValue = caseInsensitiveOptions.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) if value.length == 0 => '\u0000'
      case Some(value) if value.length == 1 => value.charAt(0)
      case _ => throw new RuntimeException(s"$paramName cannot be more than one character")
    }
  }

  private def getInt(paramName: String, default: Int): Int = {
    val paramValue = caseInsensitiveOptions.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) => try {
        value.toInt
      } catch {
        case e: NumberFormatException =>
          throw new RuntimeException(s"$paramName should be an integer. Found $value")
      }
    }
  }

  private def getBool(paramName: String, default: Boolean = false): Boolean = {
    val param = caseInsensitiveOptions.getOrElse(paramName, default.toString)
    if (param == null) {
      default
    } else if (param.toLowerCase == "true") {
      true
    } else if (param.toLowerCase == "false") {
      false
    } else {
      throw new Exception(s"$paramName flag can be true or false")
    }
  }

  val delimiter = CSVTypeCast.toChar(
    caseInsensitiveOptions.getOrElse("sep", caseInsensitiveOptions.getOrElse("delimiter", ",")))
  private val parseMode = caseInsensitiveOptions.getOrElse("mode", "PERMISSIVE")
  val charset = caseInsensitiveOptions.getOrElse("encoding",
    caseInsensitiveOptions.getOrElse("charset", StandardCharsets.UTF_8.name()))

  val quote = getChar("quote", '\"')
  val escape = getChar("escape", '\\')
  val comment = getChar("comment", '\u0000')

  val headerFlag = getBool("header")
  val inferSchemaFlag = getBool("inferSchema")
  val ignoreLeadingWhiteSpaceFlag = getBool("ignoreLeadingWhiteSpace")
  val ignoreTrailingWhiteSpaceFlag = getBool("ignoreTrailingWhiteSpace")

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logWarning(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  val failFast = ParseModes.isFailFastMode(parseMode)
  val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  val permissive = ParseModes.isPermissiveMode(parseMode)

  val nullValue = caseInsensitiveOptions.getOrElse("nullValue", "")

  val nanValue = caseInsensitiveOptions.getOrElse("nanValue", "NaN")

  val positiveInf = caseInsensitiveOptions.getOrElse("positiveInf", "Inf")
  val negativeInf = caseInsensitiveOptions.getOrElse("negativeInf", "-Inf")


  val compressionCodec: Option[String] = {
    val name = caseInsensitiveOptions.get("compression").orElse(caseInsensitiveOptions.get("codec"))
    name.map(CompressionCodecs.getCodecClassName)
  }

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(
      caseInsensitiveOptions.getOrElse("dateFormat", "yyyy-MM-dd"),
      Locale.US)

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(
      caseInsensitiveOptions.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"), Locale.US)

  val maxColumns = getInt("maxColumns", 20480)

  val maxCharsPerColumn = getInt("maxCharsPerColumn", -1)

  val escapeQuotes = getBool("escapeQuotes", true)

  val maxMalformedLogPerPartition = getInt("maxMalformedLogPerPartition", 10)

  val quoteAll = getBool("quoteAll", false)

  val inputBufferSize = 128

  val isCommentSet = this.comment != '\u0000'
}

object CSVOptions {

  def apply(): CSVOptions = new CSVOptions(Map.empty)

  def apply(paramName: String, paramValue: String): CSVOptions = {
    new CSVOptions(Map(paramName -> paramValue))
  }
}
