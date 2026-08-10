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

/**
 * Helpers for turning fixed-width text lines into fields: slicing a line by character position,
 * and inferring those positions from a sample of lines when they aren't given explicitly.
 */
object FixedWidthUtils {

  /**
   * Converts contiguous field widths into half-open colspecs, e.g. `Seq(5, 5)` -> `[(0,5),(5,10)]`.
   */
  def widthsToColspecs(widths: Seq[Int]): Seq[(Int, Int)] = {
    var col = 0
    widths.map { w =>
      val spec = (col, col + w)
      col += w
      spec
    }
  }

  /**
   * Characters treated as blank (not part of any field) when slicing a line or inferring
   * colspecs. A custom `delimiter` replaces the default whitespace set entirely rather than
   * extending it.
   */
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

  /**
   * Slices one line into fields according to `colspecs`, a sequence of half-open character
   * intervals. A colspec beyond the end of the line yields an empty string. Each field is then
   * stripped of the configured delimiter characters.
   */
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

  /**
   * Infers colspecs from a sample of rows: marks every character position covered by a
   * non-delimiter character, across all sampled rows (comment text stripped first), into a 0/1
   * occupancy mask one longer than the longest row, then pairs up each occupied run's start/end
   * edge as a colspec. Returns an empty `Seq` for empty input rather than raising, matching how
   * Spark's own file sources treat "no data to infer from" as an empty schema.
   */
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

  /**
   * Whether a tokenized row is effectively empty: every field is an empty string. A fixed-width
   * blank line slices into an array of empty strings rather than an empty array, so this needs
   * its own check.
   *
   * Only sound when `tokens` covers the row's full width (e.g. during schema inference, which
   * never column-prunes). Column-pruned tokens must use [[isLineBlank]] on the raw line instead:
   * a row can have blank *selected* columns while still holding real data in columns that
   * weren't selected, and a zero-column projection (e.g. a bare `count()`) always slices to an
   * empty `Array()`, which this method (vacuously) reports as blank.
   */
  def isLineEffectivelyEmpty(tokens: Array[String]): Boolean = !tokens.exists(_.nonEmpty)

  /**
   * Whether an entire raw line, before any column slicing/pruning, is blank under the configured
   * delimiter's definition of "blank". Unlike [[isLineEffectivelyEmpty]], this is unaffected by
   * column pruning since it never looks at sliced tokens.
   */
  def isLineBlank(line: String, delimiter: Option[String]): Boolean =
    stripDelims(line, delimiterChars(delimiter)).isEmpty

  /**
   * Whether a raw (not yet sliced) line is entirely a comment line, i.e. it starts with the
   * comment character, mirroring CSV's own whole-line comment filtering. Such lines must be
   * dropped before header/data-row selection: [[detectColspecs]] only strips a trailing comment
   * suffix for mask-building purposes, it doesn't drop the line, so a comment line left in the
   * row stream would otherwise get sliced like any other row.
   */
  def isCommentLine(line: String, comment: Option[Char]): Boolean =
    comment.exists(c => line.nonEmpty && line.charAt(0) == c)
}
