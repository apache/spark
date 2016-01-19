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

import java.nio.charset.Charset

import org.apache.spark.Logging

private[sql] case class CSVParameters(@transient parameters: Map[String, String]) extends Logging {

  private def getChar(paramName: String, default: Char): Char = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(value) if value.length == 0 => '\u0000'
      case Some(value) if value.length == 1 => value.charAt(0)
      case _ => throw new RuntimeException(s"$paramName cannot be more than one character")
    }
  }

  private def getBool(paramName: String, default: Boolean = false): Boolean = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param.toLowerCase() == "true") {
      true
    } else if (param.toLowerCase == "false") {
      false
    } else {
      throw new Exception(s"$paramName flag can be true or false")
    }
  }

  val delimiter = CSVTypeCast.toChar(
    parameters.getOrElse("sep", parameters.getOrElse("delimiter", ",")))
  val parseMode = parameters.getOrElse("mode", "PERMISSIVE")
  val charset = parameters.getOrElse("encoding",
    parameters.getOrElse("charset", Charset.forName("UTF-8").name()))

  val quote = getChar("quote", '\"')
  val escape = getChar("escape", '\\')
  val comment = getChar("comment", '\u0000')

  val headerFlag = getBool("header")
  val inferSchemaFlag = getBool("inferSchema")
  val ignoreLeadingWhiteSpaceFlag = getBool("ignoreLeadingWhiteSpace")
  val ignoreTrailingWhiteSpaceFlag = getBool("ignoreTrailingWhiteSpace")

  // Limit the number of lines we'll search for a header row that isn't comment-prefixed
  val MAX_COMMENT_LINES_IN_HEADER = 10

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logWarning(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  val failFast = ParseModes.isFailFastMode(parseMode)
  val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  val permissive = ParseModes.isPermissiveMode(parseMode)

  val nullValue = parameters.getOrElse("nullValue", "")

  val maxColumns = 20480

  val maxCharsPerColumn = 100000

  val inputBufferSize = 128

  val isCommentSet = this.comment != '\u0000'

  val rowSeparator = "\n"
}

private[csv] object ParseModes {

  val PERMISSIVE_MODE = "PERMISSIVE"
  val DROP_MALFORMED_MODE = "DROPMALFORMED"
  val FAIL_FAST_MODE = "FAILFAST"

  val DEFAULT = PERMISSIVE_MODE

  def isValidMode(mode: String): Boolean = {
    mode.toUpperCase match {
      case PERMISSIVE_MODE | DROP_MALFORMED_MODE | FAIL_FAST_MODE => true
      case _ => false
    }
  }

  def isDropMalformedMode(mode: String): Boolean = mode.toUpperCase == DROP_MALFORMED_MODE
  def isFailFastMode(mode: String): Boolean = mode.toUpperCase == FAIL_FAST_MODE
  def isPermissiveMode(mode: String): Boolean = if (isValidMode(mode))  {
    mode.toUpperCase == PERMISSIVE_MODE
  } else {
    true // We default to permissive is the mode string is not valid
  }
}
