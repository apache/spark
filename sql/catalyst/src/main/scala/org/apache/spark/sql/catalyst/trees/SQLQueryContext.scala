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

import org.apache.spark.QueryContext

/** The class represents error context of a SQL query. */
case class SQLQueryContext(
    line: Option[Int],
    startPosition: Option[Int],
    originStartIndex: Option[Int],
    originStopIndex: Option[Int],
    sqlText: Option[String],
    originObjectType: Option[String],
    originObjectName: Option[String]) extends QueryContext {

  override val objectType = originObjectType.getOrElse("")
  override val objectName = originObjectName.getOrElse("")
  override val startIndex = originStartIndex.getOrElse(-1)
  override val stopIndex = originStopIndex.getOrElse(-1)

  /**
   * The SQL query context of current node. For example:
   * == SQL of VIEW v1(line 1, position 25) ==
   * SELECT '' AS five, i.f1, i.f1 - int('2') AS x FROM INT4_TBL i
   *                          ^^^^^^^^^^^^^^^
   */
  lazy val summary: String = {
    // If the query context is missing or incorrect, simply return an empty string.
    if (!isValid) {
      ""
    } else {
      val positionContext = if (line.isDefined && startPosition.isDefined) {
        // Note that the line number starts from 1, while the start position starts from 0.
        // Here we increase the start position by 1 for consistency.
        s"(line ${line.get}, position ${startPosition.get + 1})"
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
      val start = math.max(originStartIndex.get, 0)
      val stop = math.min(originStopIndex.getOrElse(text.length - 1), text.length - 1)
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
      lines.foreach { lineText =>
        builder ++= lineText + "\n"
        currentIndex += 1
        (0 until lineText.length).foreach { _ =>
          if (currentIndex < start) {
            builder ++= " "
          } else if (currentIndex >= start && currentIndex <= stop) {
            builder ++= "^"
          }
          currentIndex += 1
        }
        builder ++= "\n"
      }
      builder.result()
    }
  }

  /** Gets the textual fragment of a SQL query. */
  override lazy val fragment: String = {
    if (!isValid) {
      ""
    } else {
      sqlText.get.substring(originStartIndex.get, originStopIndex.get + 1)
    }
  }

  def isValid: Boolean = {
    sqlText.isDefined && originStartIndex.isDefined && originStopIndex.isDefined &&
      originStartIndex.get >= 0 && originStopIndex.get < sqlText.get.length &&
      originStartIndex.get <= originStopIndex.get

  }
}
