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

package org.apache.spark.sql.catalyst.csv

import org.apache.commons.lang3.StringUtils

import org.apache.spark.SparkIllegalArgumentException

object CSVExprUtils {
  /**
   * Filter ignorable rows for CSV iterator (lines empty and starting with `comment`).
   * This is currently being used in CSV reading path and CSV schema inference.
   */
  def filterCommentAndEmpty(iter: Iterator[String], options: CSVOptions): Iterator[String] = {
    if (options.isCommentSet) {
      val commentPrefix = options.comment.toString
      iter.filter { line =>
        line.trim.nonEmpty && !line.startsWith(commentPrefix)
      }
    } else {
      iter.filter(_.trim.nonEmpty)
    }
  }

  def skipComments(iter: Iterator[String], options: CSVOptions): Iterator[String] = {
    if (options.isCommentSet) {
      val commentPrefix = options.comment.toString
      iter.dropWhile { line =>
        line.trim.isEmpty || line.startsWith(commentPrefix)
      }
    } else {
      iter.dropWhile(_.trim.isEmpty)
    }
  }

  /**
   * Extracts header and moves iterator forward so that only data remains in it
   */
  def extractHeader(iter: Iterator[String], options: CSVOptions): Option[String] = {
    val nonEmptyLines = skipComments(iter, options)
    if (nonEmptyLines.hasNext) {
      Some(nonEmptyLines.next())
    } else {
      None
    }
  }

  /**
   * Helper method that converts string representation of a character to actual character.
   * It handles some Java escaped strings and throws exception if given string is longer than one
   * character.
   */
  @throws[SparkIllegalArgumentException]
  def toChar(str: String): Char = {
    (str: Seq[Char]) match {
      case Seq() => throw new SparkIllegalArgumentException("INVALID_DELIMITER_VALUE.EMPTY_STRING")
      case Seq('\\') =>
        throw new SparkIllegalArgumentException("INVALID_DELIMITER_VALUE.SINGLE_BACKSLASH")
      case Seq(c) => c
      case Seq('\\', 't') => '\t'
      case Seq('\\', 'r') => '\r'
      case Seq('\\', 'b') => '\b'
      case Seq('\\', 'f') => '\f'
      // In case user changes quote char and uses \" as delimiter in options
      case Seq('\\', '\"') => '\"'
      case Seq('\\', '\'') => '\''
      case Seq('\\', '\\') => '\\'
      case _ if str == "\u0000" => '\u0000'
      case Seq('\\', _) =>
        throw new SparkIllegalArgumentException(
          errorClass =
            "INVALID_DELIMITER_VALUE.UNSUPPORTED_SPECIAL_CHARACTER",
            messageParameters = Map("str" -> str))
      case _ =>
        throw new SparkIllegalArgumentException(
          errorClass =
            "INVALID_DELIMITER_VALUE.DELIMITER_LONGER_THAN_EXPECTED",
            messageParameters = Map("str" -> str))
    }
  }

  /**
   * Helper method that converts string representation of a character sequence to actual
   * delimiter characters. The input is processed in "chunks", and each chunk is converted
   * by calling [[CSVExprUtils.toChar()]].  A chunk is either:
   * <ul>
   *   <li>a backslash followed by another character</li>
   *   <li>a non-backslash character by itself</li>
   * </ul>
   * , in that order of precedence. The result of the converting all chunks is returned as
   * a [[String]].
   *
   * <br/><br/>Examples:
   * <ul><li>`\t` will result in a single tab character as the separator (same as before)
   * </li><li>`|||` will result in a sequence of three pipe characters as the separator
   * </li><li>`\\` will result in a single backslash as the separator (same as before)
   * </li><li>`\.` will result in an error (since a dot is not a character that needs escaped)
   * </li><li>`\\.` will result in a backslash, then dot, as the separator character sequence
   * </li><li>`.\t.` will result in a dot, then tab, then dot as the separator character sequence
   * </li>
   * </ul>
   *
   * @param str the string representing the sequence of separator characters
   * @return a [[String]] representing the multi-character delimiter
   * @throws SparkIllegalArgumentException if any of the individual input chunks are illegal
   */
  def toDelimiterStr(str: String): String = {
    var idx = 0

    var delimiter = ""

    while (idx < str.length()) {
      // if the current character is a backslash, check it plus the next char
      // in order to use existing escape logic
      val readAhead = if (str(idx) == '\\') 2 else 1
      // get the chunk of 1 or 2 input characters to convert to a single delimiter char
      val chunk = StringUtils.substring(str, idx, idx + readAhead)
      delimiter += toChar(chunk)
      // advance the counter by the length of input chunk processed
      idx += chunk.length()
    }

    delimiter.mkString("")
  }
}
