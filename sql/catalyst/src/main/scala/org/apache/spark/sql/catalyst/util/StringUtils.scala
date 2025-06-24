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

import java.util.Locale
import java.util.regex.{Pattern, PatternSyntaxException}

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.text.similarity.LevenshteinDistance

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
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

  /**
   * Removes comments from a SQL command. Visible for testing only.
   * @param command The SQL command to remove comments from.
   * @param replaceWithWhitespace If true, replaces the comment with whitespace instead of
   *                              stripping them in order to ensure query length and character
   *                              positions are preserved.
   */
  protected[util] def stripComment(
      command: String, replaceWithWhitespace: Boolean = false): String = {
    // Important characters
    val SINGLE_QUOTE = '\''
    val DOUBLE_QUOTE = '"'
    val BACKTICK = '`'
    val BACKSLASH = '\\'
    val HYPHEN = '-'
    val NEWLINE = '\n'
    val STAR = '*'
    val SLASH = '/'

    // Possible states
    object Quote extends Enumeration {
      type State = Value
      val InSingleQuote, InDoubleQuote, InComment, InBacktick, NoQuote, InBracketedComment = Value
    }
    import Quote._

    var curState = NoQuote
    var curIdx = 0
    val singleCommand = new StringBuilder()

    val skipNextCharacter = () => {
      curIdx += 1
      // Optionally append whitespace when skipping next character
      if (replaceWithWhitespace) {
        singleCommand.append(" ")
      }
    }

    while (curIdx < command.length) {
      var curChar = command.charAt(curIdx)
      var appendCharacter = true

      (curState, curChar) match {
        case (InBracketedComment, STAR) =>
          val nextIdx = curIdx + 1
          if (nextIdx < command.length && command.charAt(nextIdx) == SLASH) {
            curState = NoQuote
            skipNextCharacter()
          }
          appendCharacter = false
        case (InComment, NEWLINE) =>
          curState = NoQuote
        case (InComment, _) =>
          appendCharacter = false
        case (InBracketedComment, _) =>
          appendCharacter = false
        case (NoQuote, HYPHEN) =>
          val nextIdx = curIdx + 1
          if (nextIdx < command.length && command.charAt(nextIdx) == HYPHEN) {
            appendCharacter = false
            skipNextCharacter()
            curState = InComment
          }
        case (NoQuote, DOUBLE_QUOTE) => curState = InDoubleQuote
        case (NoQuote, SINGLE_QUOTE) => curState = InSingleQuote
        case (NoQuote, BACKTICK) => curState = InBacktick
        case (NoQuote, SLASH) =>
          val nextIdx = curIdx + 1
          if (nextIdx < command.length && command.charAt(nextIdx) == STAR) {
            appendCharacter = false
            skipNextCharacter()
            curState = InBracketedComment
          }
        case (InSingleQuote, SINGLE_QUOTE) => curState = NoQuote
        case (InDoubleQuote, DOUBLE_QUOTE) => curState = NoQuote
        case (InBacktick, BACKTICK) => curState = NoQuote
        case (InDoubleQuote | InSingleQuote, BACKSLASH) =>
          // This is to make sure we are handling \" or \' within "" or '' correctly.
          // For example, select "\"--hello--\""
          val nextIdx = curIdx + 1
          if (nextIdx < command.length) {
            singleCommand.append(curChar)
            curIdx = nextIdx
            curChar = command.charAt(curIdx)
          }
        case (_, _) => ()
      }

      if (appendCharacter) {
        singleCommand.append(curChar)
      } else if (replaceWithWhitespace) {
        singleCommand.append(" ")
      }
      curIdx += 1
    }

    singleCommand.toString()
  }

  /**
   * Check if query is SQL Script.
   *
   * @param query The query string.
   */
  def isSqlScript(query: String): Boolean = {
    val cleanText = stripComment(query).trim.toUpperCase(Locale.ROOT)
    // SQL Stored Procedure body, specified during procedure creation, is also a SQL Script.
    (cleanText.startsWith("BEGIN") && (cleanText.endsWith("END") ||
      cleanText.endsWith("END;"))) || isCreateSqlStoredProcedureText(cleanText)
  }

  /**
   * Check if text is create SQL Stored Procedure command.
   *
   * @param cleanText The query text, already stripped of comments and capitalized
   */
  private def isCreateSqlStoredProcedureText(cleanText: String): Boolean = {
    import scala.util.matching.Regex

    val pattern: Regex =
      """(?s)^CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\s+\w+\s*\(.*?\).*BEGIN.*END\s*;?\s*$""".r

    pattern.matches(cleanText)
  }

  private def containsNonWhiteSpaceCharacters(inputString: String): Boolean = {
    val pattern = "\\S".r
    pattern.findFirstIn(inputString).isDefined
  }

  // Implementation is grabbed from SparkSQLCLIDriver.splitSemiColon, the only difference is this
  // implementation handles backtick and treat it as single/double quote.
  // Below comments are from the source:
  // Adapted splitSemiColon from Hive 2.3's CliDriver.splitSemiColon.
  // Note: [SPARK-31595] if there is a `'` in a double quoted string, or a `"` in a single quoted
  // string, the origin implementation from Hive will not drop the trailing semicolon as expected,
  // hence we refined this function a little bit.
  // Note: [SPARK-33100] Ignore a semicolon inside a bracketed comment in spark-sql.
  def splitSemiColonWithIndex(line: String, enableSqlScripting: Boolean): List[String] = {
    var insideSingleQuote = false
    var insideDoubleQuote = false
    var insideBacktick = false
    var insideSimpleComment = false
    var bracketedCommentLevel = 0
    var escape = false
    var beginIndex = 0
    var leavingBracketedComment = false
    var hasPrecedingNonCommentString = false
    var isStatement = false
    val ret = new ArrayBuffer[String]()

    lazy val insideSqlScript: Boolean = isSqlScript(line)

    def insideBracketedComment: Boolean = bracketedCommentLevel > 0
    def insideComment: Boolean = insideSimpleComment || insideBracketedComment
    def statementInProgress(index: Int): Boolean =
      isStatement || (!insideComment &&
        index > beginIndex && !s"${line.charAt(index)}".trim.isEmpty)

    for (index <- 0 until line.length) {
      // Checks if we need to decrement a bracketed comment level; the last character '/' of
      // bracketed comments is still inside the comment, so `insideBracketedComment` must keep
      // true in the previous loop and we decrement the level here if needed.
      if (leavingBracketedComment) {
        bracketedCommentLevel -= 1
        leavingBracketedComment = false
      }

      if (line.charAt(index) == '\'' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideDoubleQuote && !insideBacktick) {
          // flip the boolean variable
          insideSingleQuote = !insideSingleQuote
        }
      } else if (line.charAt(index) == '\"' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideSingleQuote && !insideBacktick) {
          // flip the boolean variable
          insideDoubleQuote = !insideDoubleQuote
        }
      } else if (line.charAt(index) == '`' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideSingleQuote && !insideDoubleQuote) {
          // flip the boolean variable
          insideBacktick = !insideBacktick
        }
      } else if (line.charAt(index) == '-') {
        val hasNext = index + 1 < line.length
        if (insideDoubleQuote || insideSingleQuote || insideBacktick || insideComment) {
          // Ignores '-' in any case of quotes or comment.
          // Avoids to start a comment(--) within a quoted segment or already in a comment.
          // Sample query: select "quoted value --"
          //                                    ^^ avoids starting a comment if inside quotes.
        } else if (hasNext && line.charAt(index + 1) == '-') {
          // ignore quotes and ; in simple comment
          insideSimpleComment = true
        }
      } else if (line.charAt(index) == ';') {
        if (insideSingleQuote || insideDoubleQuote || insideBacktick || insideComment) {
          // do not split
        } else if (enableSqlScripting && insideSqlScript) {
          // do not split
        } else {
          if (isStatement) {
            // split, do not include ; itself
            ret.append(line.substring(beginIndex, index))
          }
          beginIndex = index + 1
          isStatement = false
        }
      } else if (line.charAt(index) == '\n') {
        // with a new line the inline simple comment should end.
        if (!escape) {
          insideSimpleComment = false
        }
      } else if (line.charAt(index) == '/' && !insideSimpleComment) {
        val hasNext = index + 1 < line.length
        if (insideSingleQuote || insideDoubleQuote || insideBacktick) {
          // Ignores '/' in any case of quotes
        } else if (insideBracketedComment && line.charAt(index - 1) == '*') {
          // Decrements `bracketedCommentLevel` at the beginning of the next loop
          leavingBracketedComment = true
        } else if (hasNext && line.charAt(index + 1) == '*') {
          bracketedCommentLevel += 1
          // Check if there's non-comment characters(non space, non newline characters) before
          // multiline comments.
          hasPrecedingNonCommentString = beginIndex != index && containsNonWhiteSpaceCharacters(
            line.substring(beginIndex, index)
          )
        }
      }
      // set the escape
      if (escape) {
        escape = false
      } else if (line.charAt(index) == '\\') {
        escape = true
      }

      isStatement = statementInProgress(index)
    }
    // Check the last char is end of nested bracketed comment.
    val endOfBracketedComment = leavingBracketedComment && bracketedCommentLevel == 1 &&
      !hasPrecedingNonCommentString
    // Spark SQL support simple comment and nested bracketed comment in query body.
    // But if Spark SQL receives a comment alone, it will throw parser exception.
    // In Spark SQL CLI, if there is a completed comment in the end of whole query,
    // since Spark SQL CLL use `;` to split the query, CLI will pass the comment
    // to the backend engine and throw exception. CLI should ignore this comment,
    // If there is an uncompleted statement or an uncompleted bracketed comment in the end,
    // CLI should also pass this part to the backend engine, which may throw an exception
    // with clear error message (for incomplete statement, if there's non comment characters,
    // we would still append the string).
    if (!endOfBracketedComment && (isStatement || insideBracketedComment)) {
      ret.append(line.substring(beginIndex))
    }
    ret.toList
  }
}
