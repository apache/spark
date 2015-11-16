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

package org.apache.spark.sql.execution.datasources.json

import com.fasterxml.jackson.core.{JsonParser, JsonFactory}

/**
 * Options for the JSON data source.
 *
 * Most of these map directly to Jackson's internal options, specified in [[JsonParser.Feature]].
 */
case class JSONOptions(
    samplingRatio: Double = 1.0,
    primitivesAsString: Boolean = false,
    allowComments: Boolean = false,
    allowUnquotedFieldNames: Boolean = false,
    allowSingleQuotes: Boolean = true,
    allowNumericLeadingZeros: Boolean = false,
    allowNonNumericNumbers: Boolean = false) {

  /** Sets config options on a Jackson [[JsonFactory]]. */
  def setJacksonOptions(factory: JsonFactory): Unit = {
    factory.configure(JsonParser.Feature.ALLOW_COMMENTS, allowComments)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, allowUnquotedFieldNames)
    factory.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, allowSingleQuotes)
    factory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, allowNumericLeadingZeros)
    factory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, allowNonNumericNumbers)
  }
}


object JSONOptions {
  def createFromConfigMap(parameters: Map[String, String]): JSONOptions = JSONOptions(
    samplingRatio =
      parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0),
    primitivesAsString =
      parameters.get("primitivesAsString").map(_.toBoolean).getOrElse(false),
    allowComments =
      parameters.get("allowComments").map(_.toBoolean).getOrElse(false),
    allowUnquotedFieldNames =
      parameters.get("allowUnquotedFieldNames").map(_.toBoolean).getOrElse(false),
    allowSingleQuotes =
      parameters.get("allowSingleQuotes").map(_.toBoolean).getOrElse(true),
    allowNumericLeadingZeros =
      parameters.get("allowNumericLeadingZeros").map(_.toBoolean).getOrElse(false),
    allowNonNumericNumbers =
      parameters.get("allowNonNumericNumbers").map(_.toBoolean).getOrElse(true)
  )
}
