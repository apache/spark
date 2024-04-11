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

import org.apache.commons.text.similarity.LevenshteinDistance

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

object StringUtils extends Logging {

  /**
   * Validate and convert SQL 'like' pattern to a Java regular expression.
   *
   * Underscores (_) are converted to '.' and percent signs (%) are converted to '.*', other
   * characters are quoted literally. Escaping is done according to the rules specified in
   * [[org.apache.spark.sql.catalyst.expressions.Like]] usage documentation. An invalid pattern will
   * throw an [[AnalysisException]].
   *
   * @param pattern the SQL pattern to convert
   * @param escapeChar the escape string contains one character.
   * @return the equivalent Java regular expression of the pattern
   */
  def escapeLikeRegex(pattern: String, escapeChar: Char): String = {
    val in = pattern.iterator
    val out = new StringBuilder()

    while (in.hasNext) {
      in.next() match {
        case c1 if c1 == escapeChar && in.hasNext =>
          val c = in.next()
          c match {
            case '_' | '%' => out ++= Pattern.quote(Character.toString(c))
            case c if c == escapeChar => out ++= Pattern.quote(Character.toString(c))
            case _ => throw QueryCompilationErrors.escapeCharacterInTheMiddleError(
              pattern, Character.toString(c))
          }
        case c if c == escapeChar =>
          throw QueryCompilationErrors.escapeCharacterAtTheEndError(pattern)
        case '_' => out ++= "."
        case '%' => out ++= ".*"
        case c => out ++= Pattern.quote(Character.toString(c))
      }
    }
    "(?s)" + out.result() // (?s) enables dotall mode, causing "." to match new lines
  }

  /**
   * Returns a pretty string of the byte array which prints each byte as a hex digit and add spaces
   * between them. For example, [1A C0].
   */
  def getHexString(bytes: Array[Byte]): String = bytes.map("%02X".format(_)).mkString("[", " ", "]")

  private[this] val trueStrings =
    Set("t", "true", "y", "yes", "1").map(UTF8String.fromString)

  private[this] val falseStrings =
    Set("f", "false", "n", "no", "0").map(UTF8String.fromString)

  private[spark] def orderSuggestedIdentifiersBySimilarity(
      baseString: String,
      candidates: Seq[Seq[String]]): Seq[String] = {
    val baseParts = UnresolvedAttribute.parseAttributeName(baseString)
    val strippedCandidates =
      // Group by the qualifier. If all identifiers have the same qualifier, strip it.
      // For example: Seq(`abc`.`def`.`t1`, `abc`.`def`.`t2`) => Seq(`t1`, `t2`)
      if (baseParts.size == 1 && candidates.groupBy(_.dropRight(1)).size == 1) {
        candidates.map(_.takeRight(1))
      // Group by the qualifier excluding table name. If all identifiers have the same prefix
      // (namespace) excluding table names, strip this prefix.
      // For example: Seq(`abc`.`def`.`t1`, `abc`.`xyz`.`t2`) => Seq(`def`.`t1`, `xyz`.`t2`)
      } else if (baseParts.size <= 2 && candidates.groupBy(_.dropRight(2)).size == 1) {
        candidates.map(_.takeRight(2))
      } else {
        // Some candidates have different qualifiers
        candidates
      }

    strippedCandidates
      .map(quoteNameParts)
      .sortBy(LevenshteinDistance.getDefaultInstance.apply(_, baseString))
  }

  // scalastyle:off caselocale
  def isTrueString(s: UTF8String): Boolean = trueStrings.contains(s.trimAll().toLowerCase)

  def isFalseString(s: UTF8String): Boolean = falseStrings.contains(s.trimAll().toLowerCase)
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

  /**
   * A string concatenator for plan strings.  Uses length from a configured value, and
   *  prints a warning the first time a plan is truncated.
   */
  class PlanStringConcat extends StringConcat(Math.max(0, SQLConf.get.maxPlanStringLength - 30)) {
    override def toString: String = {
      if (atLimit) {
        logWarning(
          log"Truncated the string representation of a plan since it was too long. The " +
            log"plan had length ${MDC(QUERY_PLAN_LENGTH_ACTUAL, length)} " +
            log"and the maximum is ${MDC(QUERY_PLAN_LENGTH_MAX, maxLength)}. This behavior " +
            log"can be adjusted by setting " +
            log"'${MDC(CONFIG, SQLConf.MAX_PLAN_STRING_LENGTH.key)}'.")
        val truncateMsg = if (maxLength == 0) {
          s"Truncated plan of $length characters"
        } else {
          s"... ${length - maxLength} more characters"
        }
        val result = new java.lang.StringBuilder(maxLength + truncateMsg.length)
        strings.forEach(s => result.append(s))
        result.append(truncateMsg)
        result.toString
      } else {
        super.toString
      }
    }
  }
}
