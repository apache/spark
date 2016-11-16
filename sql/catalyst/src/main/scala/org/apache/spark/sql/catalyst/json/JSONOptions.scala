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

import java.util.Locale

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs, ParseModes}

/**
 * Options for parsing JSON data into Spark SQL rows.
 *
 * Most of these map directly to Jackson's internal options, specified in [[JsonParser.Feature]].
 */
private[sql] class JSONOptions(
    @transient private val parameters: CaseInsensitiveMap)
  extends Logging with Serializable  {

  def this(parameters: Map[String, String]) = this(new CaseInsensitiveMap(parameters))

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
  val compressionCodec = parameters.get("compression").map(CompressionCodecs.getCodecClassName)
  private val parseMode = parameters.getOrElse("mode", "PERMISSIVE")
  val columnNameOfCorruptRecord = parameters.get("columnNameOfCorruptRecord")

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(
      parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"), Locale.US)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logWarning(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  val failFast = ParseModes.isFailFastMode(parseMode)
  val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  val permissive = ParseModes.isPermissiveMode(parseMode)

  /** Sets config options on a Jackson [[JsonFactory]]. */
  def setJacksonOptions(factory: JsonFactory): Unit = {
    factory.configure(JsonParser.Feature.ALLOW_COMMENTS, allowComments)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, allowUnquotedFieldNames)
    factory.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, allowSingleQuotes)
    factory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, allowNumericLeadingZeros)
    factory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, allowNonNumericNumbers)
    factory.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
      allowBackslashEscapingAnyCharacter)
  }
}
