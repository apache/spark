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

package org.apache.spark.sql.catalyst.trees

import org.apache.spark.{QueryContext, QueryContextType, SparkUnsupportedOperationException}

object SQLQueryContext {

  /**
   * Inclusive Unicode code-point slice `[from, to]` of `text`. Indexes are clamped to the text.
   * Parser origins store ANTLR code-point offsets, so this is the unit every SQL-text recovery
   * path must use.
   */
  private[sql] def sliceByCodePoints(text: String, from: Int, to: Int): String = {
    val count = text.codePointCount(0, text.length)
    val begin = math.max(0, math.min(from, count))
    val end = math.max(begin, math.min(to + 1, count))
    text.substring(text.offsetByCodePoints(0, begin), text.offsetByCodePoints(0, end))
  }

  /** True when `text` contains a supplementary character (two UTF-16 units). */
  private def hasSupplementary(text: String): Boolean =
    text.length != text.codePointCount(0, text.length)
}

/** The class represents error context of a SQL query. */
case class SQLQueryContext(
    line: Option[Int],
    startPosition: Option[Int],
    originStartIndex: Option[Int],
    originStopIndex: Option[Int],
    sqlText: Option[String],
    originObjectType: Option[String],
    originObjectName: Option[String])
    extends QueryContext {
  override val contextType = QueryContextType.SQL

  override val objectType = originObjectType.getOrElse("")
  override val objectName = originObjectName.getOrElse("")
  override val startIndex = originStartIndex.getOrElse(-1)
  override val stopIndex = originStopIndex.getOrElse(-1)

  /**
   * The SQL query context of current node. For example:
   * ==SQL of VIEW v1 (line 1, position 25)==
   * SELECT '' AS five, i.f1, i.f1 - int('2') AS x FROM INT4_TBL i ^^^^^^^^^^^^^^^
   */
  override lazy val summary: String = {
    // If the query context is missing or incorrect, simply return an empty string.
    if (!isValid) {
      ""
    } else {
      val positionContext = if (line.isDefined && startPosition.isDefined) {
        // Note that the line number starts from 1, while the start position starts from 0.
        // Here we increase the start position by 1 for consistency.
        s" (line ${line.get}, position ${startPosition.get + 1})"
      } else {
        ""
      }
      val objectContext = if (originObjectType.isDefined && originObjectName.isDefined) {
        s" of ${originObjectType.get} ${originObjectName.get}"
      } else {
        ""
      }
      val builder = new StringBuilder
      builder ++= s"== SQL$objectContext$positionContext ==\n"

      val text = sqlText.get
      // Parser origins are code-point indexes. Convert to UTF-16 for line walking
      // when the text has supplementary characters; otherwise the two units match.
      val startCp = math.max(originStartIndex.get, 0)
      val stopCp = originStopIndex.get
      val (start, stop) = if (SQLQueryContext.hasSupplementary(text)) {
        val count = text.codePointCount(0, text.length)
        val from = text.offsetByCodePoints(0, startCp)
        val to = text.offsetByCodePoints(0, math.min(stopCp + 1, count)) - 1
        (from, to)
      } else {
        (startCp, math.min(stopCp, text.length - 1))
      }
      // Ideally we should show all the lines which contains the SQL text context of the current
      // node:
      // [additional text] [current tree node] [additional text]
      // However, we need to truncate the additional text in case it is too long. The following
      // variable is to define the max length of additional text.
      val maxExtraContextLength = 32
      val truncatedText = "..."
      var lineStartIndex = start
      // Collect the SQL text within the starting line of current Node.
      // The text is truncated if it is too long.
      while (lineStartIndex >= 0 &&
        start - lineStartIndex <= maxExtraContextLength &&
        text.charAt(lineStartIndex) != '\n') {
        lineStartIndex -= 1
      }
      val startTruncated = start - lineStartIndex > maxExtraContextLength
      var currentIndex = lineStartIndex
      if (startTruncated) {
        currentIndex -= truncatedText.length
      }

      var lineStopIndex = stop
      // Collect the SQL text within the ending line of current Node.
      // The text is truncated if it is too long.
      while (lineStopIndex < text.length &&
        lineStopIndex - stop <= maxExtraContextLength &&
        text.charAt(lineStopIndex) != '\n') {
        lineStopIndex += 1
      }
      val stopTruncated = lineStopIndex - stop > maxExtraContextLength

      val truncatedSubText = (if (startTruncated) truncatedText else "") +
        text.substring(lineStartIndex + 1, lineStopIndex) +
        (if (stopTruncated) truncatedText else "")
      val lines = truncatedSubText.split("\n")
      val oneCaretPerCodePoint = SQLQueryContext.hasSupplementary(text)
      lines.foreach { lineText =>
        builder ++= lineText + "\n"
        currentIndex += 1
        if (oneCaretPerCodePoint) {
          var i = 0
          while (i < lineText.length) {
            val width = Character.charCount(lineText.codePointAt(i))
            if (currentIndex < start) {
              builder ++= " "
            } else if (currentIndex <= stop) {
              builder ++= "^"
            }
            currentIndex += width
            i += width
          }
        } else {
          (0 until lineText.length).foreach { _ =>
            if (currentIndex < start) {
              builder ++= " "
            } else if (currentIndex >= start && currentIndex <= stop) {
              builder ++= "^"
            }
            currentIndex += 1
          }
        }
        builder ++= "\n"
      }
      builder.result()
    }
  }

  /** Gets the textual fragment of a SQL query. */
  lazy val fragment: String = {
    if (!isValid) {
      ""
    } else if (!SQLQueryContext.hasSupplementary(sqlText.get)) {
      sqlText.get.substring(originStartIndex.get, originStopIndex.get + 1)
    } else {
      SQLQueryContext.sliceByCodePoints(sqlText.get, originStartIndex.get, originStopIndex.get)
    }
  }

  def isValid: Boolean = {
    sqlText.isDefined && originStartIndex.isDefined && originStopIndex.isDefined && {
      val text = sqlText.get
      val start = originStartIndex.get
      val stop = originStopIndex.get
      val limit =
        if (SQLQueryContext.hasSupplementary(text)) text.codePointCount(0, text.length)
        else text.length
      start >= 0 && stop < limit && start <= stop
    }
  }

  override def callSite: String = throw SparkUnsupportedOperationException()
}

case class DataFrameQueryContext(
    stackTrace: Seq[StackTraceElement],
    pysparkErrorContext: Option[(String, String)])
    extends QueryContext {
  override val contextType = QueryContextType.DataFrame

  override def objectType: String = throw SparkUnsupportedOperationException()
  override def objectName: String = throw SparkUnsupportedOperationException()
  override def startIndex: Int = throw SparkUnsupportedOperationException()
  override def stopIndex: Int = throw SparkUnsupportedOperationException()

  override val fragment: String = {
    pysparkErrorContext.map(_._1).getOrElse {
      stackTrace.headOption
        .map { firstElem =>
          val methodName = firstElem.getMethodName
          if (methodName.length > 1 && methodName(0) == '$') {
            methodName.substring(1)
          } else {
            methodName
          }
        }
        .getOrElse("")
    }
  }

  override val callSite: String =
    pysparkErrorContext.map(_._2).getOrElse(stackTrace.tail.mkString("\n"))

  override lazy val summary: String = {
    val builder = new StringBuilder
    builder ++= "== DataFrame ==\n"
    builder ++= "\""

    builder ++= fragment
    builder ++= "\""
    builder ++= " was called from\n"
    builder ++= callSite
    builder += '\n'

    builder.result()
  }
}
