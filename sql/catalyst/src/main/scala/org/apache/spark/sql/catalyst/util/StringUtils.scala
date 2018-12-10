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

package org.apache.spark.sql.catalyst.util

import java.util.regex.{Pattern, PatternSyntaxException}

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.unsafe.types.UTF8String


object StringUtils {

  /**
   * Validate and convert SQL 'like' pattern to a Java regular expression.
   *
   * Underscores (_) are converted to '.' and percent signs (%) are converted to '.*', other
   * characters are quoted literally. Escaping is done according to the rules specified in
   * [[org.apache.spark.sql.catalyst.expressions.Like]] usage documentation. An invalid pattern will
   * throw an [[AnalysisException]].
   *
   * @param pattern the SQL pattern to convert
   * @return the equivalent Java regular expression of the pattern
   */
  def escapeLikeRegex(pattern: String): String = {
    val in = pattern.toIterator
    val out = new StringBuilder()

    def fail(message: String) = throw new AnalysisException(
      s"the pattern '$pattern' is invalid, $message")

    while (in.hasNext) {
      in.next match {
        case '\\' if in.hasNext =>
          val c = in.next
          c match {
            case '_' | '%' | '\\' => out ++= Pattern.quote(Character.toString(c))
            case _ => fail(s"the escape character is not allowed to precede '$c'")
          }
        case '\\' => fail("it is not allowed to end with the escape character")
        case '_' => out ++= "."
        case '%' => out ++= ".*"
        case c => out ++= Pattern.quote(Character.toString(c))
      }
    }
    "(?s)" + out.result() // (?s) enables dotall mode, causing "." to match new lines
  }

  private[this] val trueStrings = Set("t", "true", "y", "yes", "1").map(UTF8String.fromString)
  private[this] val falseStrings = Set("f", "false", "n", "no", "0").map(UTF8String.fromString)

  // scalastyle:off caselocale
  def isTrueString(s: UTF8String): Boolean = trueStrings.contains(s.toLowerCase)
  def isFalseString(s: UTF8String): Boolean = falseStrings.contains(s.toLowerCase)
  // scalastyle:on caselocale

  /**
   * This utility can be used for filtering pattern in the "Like" of "Show Tables / Functions" DDL
   * @param names the names list to be filtered
   * @param pattern the filter pattern, only '*' and '|' are allowed as wildcards, others will
   *                follow regular expression convention, case insensitive match and white spaces
   *                on both ends will be ignored
   * @return the filtered names list in order
   */
  def filterPattern(names: Seq[String], pattern: String): Seq[String] = {
    val funcNames = scala.collection.mutable.SortedSet.empty[String]
    pattern.trim().split("\\|").foreach { subPattern =>
      try {
        val regex = ("(?i)" + subPattern.replaceAll("\\*", ".*")).r
        funcNames ++= names.filter{ name => regex.pattern.matcher(name).matches() }
      } catch {
        case _: PatternSyntaxException =>
      }
    }
    funcNames.toSeq
  }

  def split(sql: String): Array[String] = {
    val dQuote: Char = '"'
    val sQuote: Char = '\''
    val semicolon: Char = ';'
    val escape: Char = '\\'
    val dot = '.'
    val inlineComment = "--"

    var cursor: Int = 0
    var inStr: Char = dot // dot means that the cursor is not in a quoted string
    val ret: mutable.ArrayBuffer[String] = mutable.ArrayBuffer()
    var currentSQL: mutable.StringBuilder = mutable.StringBuilder.newBuilder

    while (cursor < sql.length) {
      val current: Char = sql(cursor)
      sql.substring(cursor) match {
        // if it is comment, move cursor at the end of this line
        case remaining if inStr == dot && remaining.startsWith(inlineComment) =>
          cursor += remaining.takeWhile(x => x != '\n').length
        // end of the sql
        case remaining if inStr == dot && current == semicolon =>
          ret += currentSQL.toString.trim
          currentSQL.clear()
          cursor += 1
        // start of single/double quote
        case remaining if inStr == dot && (List(dQuote, sQuote) contains current) =>
          inStr = current
          currentSQL += current
          cursor += 1
        case remaining if remaining.length >= 2 && inStr != dot && current == escape =>
          currentSQL.append(remaining.take(2))
          cursor += 2
        // end of single/double quote
        case remaining if (inStr == dQuote || inStr == sQuote) && current == inStr =>
          inStr = '.'
          currentSQL += current
          cursor += 1
        case remaining =>
          currentSQL += current
          cursor += 1
      }
    }
    ret += currentSQL.toString.trim
    ret.filter(org.apache.commons.lang.StringUtils.isNotBlank).toArray
  }


  /**
   * Concatenation of sequence of strings to final string with cheap append method
   * and one memory allocation for the final string.
   */
  class StringConcat {
    private val strings = new mutable.ArrayBuffer[String]
    private var length: Int = 0

    /**
      * Appends a string and accumulates its length to allocate a string buffer for all
      * appended strings once in the toString method.
      */
    def append(s: String): Unit = {
      if (s != null) {
        strings.append(s)
        length += s.length
      }
    }

    /**
      * The method allocates memory for all appended strings, writes them to the memory and
      * returns concatenated string.
      */
    override def toString: String = {
      val result = new java.lang.StringBuilder(length)
      strings.foreach(result.append)
      result.toString
    }
  }
}
