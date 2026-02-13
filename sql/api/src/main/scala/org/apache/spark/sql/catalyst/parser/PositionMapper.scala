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
 * efficient O(k) position mapping where k = number of substitutions.
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
 * This implementation uses O(k) space and O(log k) lookup time where k = number of substitutions.
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

  // Build sparse position ranges for efficient lookup
  private val positionRanges = buildPositionRanges()

  /**
   * Map a position in the substituted text back to the original text. Uses binary search for
   * O(log k) lookup time.
   *
   * @param substitutedPos
   *   Position in the substituted text
   * @return
   *   Position in the original text, or the same position if no mapping exists
   */
  def mapToOriginal(substitutedPos: Int): Int = {
    // Binary search for the range containing this position
    positionRanges.find(range =>
      substitutedPos >= range.substitutedStart && substitutedPos < range.substitutedEnd) match {
      case Some(range) =>
        // Position is within a substitution range
        range.originalStart
      case None =>
        // Position is in an unmapped region - apply cumulative offset
        val cumulativeOffset = positionRanges
          .takeWhile(_.substitutedStart <= substitutedPos)
          .map(_.offsetDelta)
          .sum
        substitutedPos + cumulativeOffset
    }
  }

  /**
   * Build sparse position ranges using functional approach. O(k) space complexity where k =
   * number of substitutions.
   *
   * @example
   *   For original "SELECT :name, :age" -> substituted "SELECT 'John', 25":
   *   - Substitution(7, 12, "'John'") replaces ":name" with "'John'"
   *   - Substitution(14, 18, "25") replaces ":age" with "25"
   *
   * Creates PositionRanges:
   *   - Range for "'John'": substituted[7,13) -> original[7,12), offset=-1
   *   - Range for "25": substituted[15,17) -> original[14,18), offset=-3
   *
   * This allows mapping any position in "SELECT 'John', 25" back to "SELECT :name, :age".
   */
  private def buildPositionRanges(): List[PositionRange] = {
    if (substitutions.isEmpty) {
      return List.empty
    }

    val sortedSubstitutions = substitutions.sortBy(_.start)

    // Assert that substitutions don't overlap
    sortedSubstitutions.zip(sortedSubstitutions.tail).foreach { case (current, next) =>
      assert(
        current.end <= next.start,
        s"Overlapping substitutions detected: [${current.start}, ${current.end}) overlaps with " +
          s"[${next.start}, ${next.end}). This indicates a bug in parameter substitution logic.")
    }

    // Use scanLeft for functional accumulation of position state
    case class PositionState(originalPos: Int, substitutedPos: Int, ranges: List[PositionRange])

    val finalState = sortedSubstitutions.foldLeft(PositionState(0, 0, List.empty)) {
      case (state, substitution) =>
        val PositionState(originalPos, substitutedPos, ranges) = state

        // Calculate positions after accounting for text before this substitution
        val textBeforeLength = substitution.start - originalPos
        val newSubstitutedPos = substitutedPos + textBeforeLength

        // Create range for the substitution
        val substitutedLength = substitution.replacement.length
        val originalLength = substitution.end - substitution.start
        val offsetDelta = originalLength - substitutedLength

        val newRange = PositionRange(
          substitutedStart = newSubstitutedPos,
          substitutedEnd = newSubstitutedPos + substitutedLength,
          originalStart = substitution.start,
          offsetDelta = offsetDelta)

        PositionState(
          originalPos = substitution.end,
          substitutedPos = newSubstitutedPos + substitutedLength,
          ranges = ranges :+ newRange)
    }

    finalState.ranges
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
