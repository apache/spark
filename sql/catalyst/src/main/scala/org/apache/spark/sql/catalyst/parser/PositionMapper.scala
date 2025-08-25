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
 * Maps positions between original SQL text and substituted SQL text.
 *
 * When parameters are substituted in SQL text, the character positions change.
 * This class maintains a mapping that allows us to translate error positions
 * from the substituted text back to the original text that the user submitted.
 *
 * @param originalText The original SQL text with parameter markers
 * @param substitutedText The SQL text after parameter substitution
 * @param substitutions List of substitutions that were applied
 */
class PositionMapper(
    val originalText: String,
    val substitutedText: String,
    val substitutions: List[Substitution]) {

  // Build position mapping from substituted positions to original positions
  private val positionMap = buildPositionMap()

  /**
   * Map a position in the substituted text back to the original text.
   *
   * @param substitutedPos Position in the substituted text
   * @return Position in the original text, or the same position if no mapping exists
   */
  def mapToOriginal(substitutedPos: Int): Int = {
    positionMap.getOrElse(substitutedPos, substitutedPos)
  }

  /**
   * Map a range in the substituted text back to the original text.
   *
   * @param startPos Start position in substituted text
   * @param endPos End position in substituted text
   * @return (originalStart, originalEnd) tuple
   */
  def mapRangeToOriginal(startPos: Int, endPos: Int): (Int, Int) = {
    (mapToOriginal(startPos), mapToOriginal(endPos))
  }

  /**
   * Build a mapping from each position in substituted text to original text.
   */
  private def buildPositionMap(): Map[Int, Int] = {
    val mapping = scala.collection.mutable.Map[Int, Int]()

    // Sort substitutions by start position
    val sortedSubstitutions = substitutions.sortBy(_.start)

    var originalPos = 0
    var substitutedPos = 0

    for (substitution <- sortedSubstitutions) {
      // Map positions before this substitution (1:1 mapping)
      while (originalPos < substitution.start) {
        mapping(substitutedPos) = originalPos
        originalPos += 1
        substitutedPos += 1
      }

      // Handle the substitution region
      val originalLength = substitution.end - substitution.start
      val substitutedLength = substitution.replacement.length

      // Map all positions in the substituted region back to the start of the original parameter
      for (i <- 0 until substitutedLength) {
        mapping(substitutedPos + i) = substitution.start
      }

      // Advance positions
      originalPos += originalLength  // Skip over the original parameter
      substitutedPos += substitutedLength  // Skip over the substituted value
    }

    // Map remaining positions after the last substitution (1:1 mapping)
    while (originalPos < originalText.length && substitutedPos < substitutedText.length) {
      mapping(substitutedPos) = originalPos
      originalPos += 1
      substitutedPos += 1
    }

    mapping.toMap
  }
}

/**
 * Companion object for PositionMapper.
 */
object PositionMapper {

  /**
   * Create a PositionMapper from substitution information.
   *
   * @param originalText The original SQL text
   * @param substitutedText The substituted SQL text
   * @param substitutions The substitutions that were applied
   * @return A PositionMapper instance
   */
  def apply(
      originalText: String,
      substitutedText: String,
      substitutions: List[Substitution]): PositionMapper = {
    new PositionMapper(originalText, substitutedText, substitutions)
  }
}
