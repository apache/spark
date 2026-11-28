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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.trees.SQLQueryContext

/**
 * Case class representing a text substitution.
 */
case class Substitution(location: ParameterLocation, replacement: String) {
  // Convenience methods to access location fields directly
  def start: Int = location.start
  def end: Int = location.end
}

/**
 * Represents a range mapping from substituted positions to original positions. This is used for
 * efficient O(log k) position mapping where k = number of substitutions.
 *
 * @param substitutedStart
 *   Start position in substituted text (inclusive)
 * @param substitutedEnd
 *   End position in substituted text (exclusive)
 * @param originalStart
 *   Start position in original text
 * @param offsetDelta
 *   Offset difference between original and substituted positions
 */
case class PositionRange(
    substitutedStart: Int,
    substitutedEnd: Int,
    originalStart: Int,
    offsetDelta: Int)

/**
 * Maps positions between original SQL text and substituted SQL text using sparse ranges.
 *
 * Position lookup uses O(log k) time where k = number of substitutions. Line lookup uses a
 * compact index of the original text's line starts so assigning origins does not repeatedly scan
 * the SQL.
 *
 * @param originalText
 *   The original SQL text with parameter markers
 * @param substitutedText
 *   The SQL text after parameter substitution
 * @param substitutions
 *   List of substitutions that were applied
 */
case class PositionMapper(
    originalText: String,
    substitutedText: String,
    substitutions: List[Substitution]) {

  // Build sparse position ranges and cumulative offsets for efficient lookup.
  private val positionRanges = buildPositionRanges()
  private val cumulativeOffsets = {
    val offsets = new Array[Int](positionRanges.length)
    var index = 0
    var offset = 0
    while (index < positionRanges.length) {
      offset += positionRanges(index).offsetDelta
      offsets(index) = offset
      index += 1
    }
    offsets
  }
  private lazy val originalLineInfo = buildLineInfo(originalText)

  /**
   * Map a position in the substituted text back to the original text.
   *
   * @param substitutedPos
   *   Position in the substituted text
   * @return
   *   Position in the original text, or the same position if no mapping exists
   */
  def mapToOriginal(substitutedPos: Int): Int = {
    val rangeIndex = lastRangeStartingAtOrBefore(substitutedPos)
    if (rangeIndex >= 0 && substitutedPos < positionRanges(rangeIndex).substitutedEnd) {
      positionRanges(rangeIndex).originalStart
    } else {
      substitutedPos + cumulativeOffsetAt(rangeIndex)
    }
  }

  /** Map an inclusive stop position in the substituted text back to the original text. */
  def mapStopToOriginal(substitutedPos: Int): Int = {
    val rangeIndex = lastRangeStartingAtOrBefore(substitutedPos)
    if (rangeIndex >= 0 && substitutedPos < positionRanges(rangeIndex).substitutedEnd) {
      val range = positionRanges(rangeIndex)
      val substitutedLength = range.substitutedEnd - range.substitutedStart
      // substitutedLength + offsetDelta is the original marker length.
      range.originalStart + substitutedLength + range.offsetDelta - 1
    } else {
      substitutedPos + cumulativeOffsetAt(rangeIndex)
    }
  }

  /** Map a substituted code-point index to its one-based line and zero-based column. */
  def mapToOriginalLineAndPosition(substitutedPos: Int): (Int, Int) = {
    lineAndPosition(mapToOriginal(substitutedPos))
  }

  /** Map a substituted one-based line and zero-based column to the original text. */
  def mapLineAndPositionToOriginal(line: Int, startPosition: Int): (Int, Int) = {
    mapToOriginalLineAndPosition(indexForLineAndPosition(line, startPosition))
  }

  /**
   * Extract the original text fragment corresponding to a range in the substituted text. Maps
   * both start and stop positions back to the original text and returns the substring.
   *
   * An inclusive stop inside a replacement maps to the end of the original marker, while a start
   * inside the same replacement maps to the beginning of the marker.
   *
   * @param substitutedStart
   *   Inclusive start position in the substituted text
   * @param substitutedStop
   *   Inclusive stop position in the substituted text
   * @return
   *   The corresponding fragment from the original text
   */
  def originalFragment(substitutedStart: Int, substitutedStop: Int): String = {
    val mappedStart = mapToOriginal(substitutedStart)
    val mappedStop = mapStopToOriginal(substitutedStop)
    SQLQueryContext.sliceByCodePoints(originalText, mappedStart, mappedStop)
  }

  private def lineAndPosition(originalPos: Int): (Int, Int) = {
    val lineStarts = originalLineInfo._1
    val codePointCount = originalLineInfo._2
    val clampedPos = math.max(0, math.min(originalPos, codePointCount))
    val searchResult = java.util.Arrays.binarySearch(lineStarts, clampedPos)
    val lineIndex = if (searchResult >= 0) searchResult else -searchResult - 2
    (lineIndex + 1, clampedPos - lineStarts(lineIndex))
  }

  private def indexForLineAndPosition(line: Int, startPosition: Int): Int = {
    var currentLine = 1
    var lineStart = 0
    while (currentLine < line && lineStart < substitutedText.length) {
      val nextLine = substitutedText.indexOf('\n', lineStart)
      if (nextLine < 0) {
        lineStart = substitutedText.length
      } else {
        lineStart = nextLine + 1
      }
      currentLine += 1
    }
    substitutedText.codePointCount(0, lineStart) + startPosition
  }

  private def buildLineInfo(text: String): (Array[Int], Int) = {
    val starts = scala.collection.mutable.ArrayBuffer(0)
    var charIndex = 0
    var codePointIndex = 0
    while (charIndex < text.length) {
      val codePoint = text.codePointAt(charIndex)
      charIndex += Character.charCount(codePoint)
      codePointIndex += 1
      if (codePoint == '\n') starts += codePointIndex
    }
    (starts.toArray, codePointIndex)
  }

  private def lastRangeStartingAtOrBefore(substitutedPos: Int): Int = {
    var low = 0
    var high = positionRanges.length - 1
    while (low <= high) {
      val middle = (low + high) >>> 1
      if (positionRanges(middle).substitutedStart <= substitutedPos) {
        low = middle + 1
      } else {
        high = middle - 1
      }
    }
    high
  }

  private def cumulativeOffsetAt(rangeIndex: Int): Int = {
    if (rangeIndex < 0) 0 else cumulativeOffsets(rangeIndex)
  }

  /**
   * Build sparse position ranges in O(k) space where k = number of substitutions.
   *
   * @example
   *   For original "SELECT :name, :age" -> substituted "SELECT 'John', 25":
   *   - Substitution(7, 12, "'John'") replaces ":name" with "'John'"
   *   - Substitution(14, 18, "25") replaces ":age" with "25"
   *
   * Creates PositionRanges:
   *   - Range for "'John'": substituted[7,13) -> original[7,12), offset=-1
   *   - Range for "25": substituted[15,17) -> original[14,18), offset=2
   *
   * This allows mapping any position in "SELECT 'John', 25" back to "SELECT :name, :age".
   */
  private def buildPositionRanges(): Array[PositionRange] = {
    if (substitutions.isEmpty) {
      return Array.empty
    }

    val sortedSubstitutions = substitutions.sortBy(_.start)

    // Assert that substitutions don't overlap
    sortedSubstitutions.zip(sortedSubstitutions.tail).foreach { case (current, next) =>
      assert(
        current.end <= next.start,
        s"Overlapping substitutions detected: [${current.start}, ${current.end}) overlaps with " +
          s"[${next.start}, ${next.end}). This indicates a bug in parameter substitution logic.")
    }

    val ranges = new Array[PositionRange](sortedSubstitutions.length)
    var originalPos = 0
    var substitutedPos = 0
    sortedSubstitutions.zipWithIndex.foreach { case (substitution, index) =>
      val textBeforeLength = substitution.start - originalPos
      substitutedPos += textBeforeLength

      // Parser and mapper indexes are Unicode code points. A supplementary character is one code
      // point, not String.length's two UTF-16 units.
      val replacement = substitution.replacement
      val substitutedLength = replacement.codePointCount(0, replacement.length)
      val originalLength = substitution.end - substitution.start
      ranges(index) = PositionRange(
        substitutedStart = substitutedPos,
        substitutedEnd = substitutedPos + substitutedLength,
        originalStart = substitution.start,
        offsetDelta = originalLength - substitutedLength)

      originalPos = substitution.end
      substitutedPos += substitutedLength
    }
    ranges
  }
}

/**
 * Companion object for PositionMapper.
 */
object PositionMapper {

  /**
   * Create an identity PositionMapper for when no substitutions occurred. This is used as an
   * optimization when no parameter markers are present.
   *
   * @param text
   *   The SQL text (both original and substituted are the same)
   * @return
   *   A PositionMapper that maps positions to themselves
   */
  def identity(text: String): PositionMapper = {
    PositionMapper(text, text, List.empty)
  }
}
