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

package org.apache.spark.sql.catalyst.fwf

object FixedWidthUtils {

  def widthsToColspecs(widths: Seq[Int]): Seq[(Int, Int)] = {
    var col = 0
    widths.map { w =>
      val spec = (col, col + w)
      col += w
      spec
    }
  }

  private def delimiterChars(delimiter: Option[String]): Set[Char] = delimiter match {
    case Some(d) if d.nonEmpty => Set('\r', '\n') ++ d.toSet
    case _ => Set('\n', '\r', '\t', ' ')
  }

  private def stripDelims(s: String, delims: Set[Char]): String = {
    var start = 0
    var end = s.length
    while (start < end && delims.contains(s.charAt(start))) start += 1
    while (end > start && delims.contains(s.charAt(end - 1))) end -= 1
    s.substring(start, end)
  }

  def sliceLine(
      line: String,
      colspecs: Seq[(Int, Int)],
      delimiter: Option[String]): Array[String] = {
    val delims = delimiterChars(delimiter)
    val len = line.length
    colspecs.map { case (from, to) =>
      val start = math.max(0, math.min(from, len))
      val end = math.max(start, math.min(to, len))
      stripDelims(line.substring(start, end), delims)
    }.toArray
  }

  def detectColspecs(
      rows: Seq[String],
      comment: Option[Char],
      delimiter: Option[String]): Seq[(Int, Int)] = {
    if (rows.isEmpty) {
      return Seq.empty
    }
    val delims = delimiterChars(delimiter)
    val maxLen = rows.map(_.length).max
    val effectiveRows = comment match {
      case Some(c) =>
        rows.map { row =>
          val idx = row.indexOf(c)
          if (idx >= 0) row.substring(0, idx) else row
        }
      case None => rows
    }
    val mask = new Array[Boolean](maxLen + 1)
    effectiveRows.foreach { row =>
      var i = 0
      val n = row.length
      while (i < n) {
        if (!delims.contains(row.charAt(i))) mask(i) = true
        i += 1
      }
    }
    val edges = scala.collection.mutable.ArrayBuffer.empty[Int]
    var prev = false
    var i = 0
    while (i <= maxLen) {
      val cur = mask(i)
      if (cur != prev) edges += i
      prev = cur
      i += 1
    }
    (0 until edges.length - 1 by 2).map(i => (edges(i), edges(i + 1)))
  }

  // After column pruning use isLineBlank on the raw line.
  def isLineEffectivelyEmpty(tokens: Array[String]): Boolean = !tokens.exists(_.nonEmpty)

  def isLineBlank(line: String, delimiter: Option[String]): Boolean =
    stripDelims(line, delimiterChars(delimiter)).isEmpty

  def isCommentLine(line: String, comment: Option[Char]): Boolean =
    comment.exists(c => line.nonEmpty && line.charAt(0) == c)
}
