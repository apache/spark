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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.lang.StringBuilder
import java.util.ArrayDeque

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Utility object for finding differences between logical plans and generating truncated
 * representations showing context around the first mismatch.
 */
object LogicalPlanDifference {

  /**
   * Generate truncated versions of two logical plans showing context around the first mismatch.
   *
   * This method finds the first line where the string representations of the two plans differ,
   * then returns truncated versions of both plans containing:
   * - Up to contextSize lines before the first mismatch
   * - The mismatched line itself
   * - Up to contextSize lines after the mismatch
   *
   * If the plans are identical, returns empty strings for both plans.
   *
   * This method performs a simple line-by-line comparison of the string representations of the
   * plans. It does not perform any semantic analysis or structural comparison. The semantic
   * comparison should be done beforehand.
   *
   * @param lhsPlan the left-hand side logical plan to compare
   * @param rhsPlan the right-hand side logical plan to compare
   * @param contextSize the number of lines to show before and after the mismatch
   * @return a tuple of (truncatedLhsPlan, truncatedRhsPlan)
   */
  def apply(lhsPlan: LogicalPlan, rhsPlan: LogicalPlan, contextSize: Int): (String, String) = {
    val lhsPlanString = lhsPlan.toString
    val rhsPlanString = rhsPlan.toString

    val lhsIterator = new LineIterator(lhsPlanString)
    val rhsIterator = new LineIterator(rhsPlanString)

    val lhsBuffer = new CircularBuffer(contextSize + 1)
    val rhsBuffer = new CircularBuffer(contextSize + 1)

    var mismatchFound = false

    while (!mismatchFound && (lhsIterator.hasNext || rhsIterator.hasNext)) {
      val lhsLine = if (lhsIterator.hasNext) Some(lhsIterator.next()) else None
      val rhsLine = if (rhsIterator.hasNext) Some(rhsIterator.next()) else None

      lhsLine.foreach(lhsBuffer.add)
      rhsLine.foreach(rhsBuffer.add)

      mismatchFound = checkLineDifference(
        lhsLine = lhsLine,
        rhsLine = rhsLine,
        lhsSource = lhsPlanString,
        rhsSource = rhsPlanString
      )
    }

    if (!mismatchFound) {
      ("", "")
    } else {
      val lhsResult = new StringBuilder()
      val rhsResult = new StringBuilder()

      lhsBuffer.foreach { position =>
        lhsResult.append(lhsPlanString, position.start, position.end).append('\n')
      }
      rhsBuffer.foreach { position =>
        rhsResult.append(rhsPlanString, position.start, position.end).append('\n')
      }

      var linesAfter = 0
      while (linesAfter < contextSize && (lhsIterator.hasNext || rhsIterator.hasNext)) {
        if (lhsIterator.hasNext) {
          val position = lhsIterator.next()
          lhsResult.append(lhsPlanString, position.start, position.end).append('\n')
        }
        if (rhsIterator.hasNext) {
          val position = rhsIterator.next()
          rhsResult.append(rhsPlanString, position.start, position.end).append('\n')
        }

        linesAfter += 1
      }

      (lhsResult.toString(), rhsResult.toString())
    }
  }

  /**
   * Checks if two line positions represent different content by comparing the corresponding
   * regions in the source strings without materializing the line strings.
   *
   * @param lhsLine the left-hand side line position (optional)
   * @param rhsLine the right-hand side line position (optional)
   * @param lhsSource the source string for the left-hand side line
   * @param rhsSource the source string for the right-hand side line
   * @return true if the lines differ, false if they are the same
   */
  private def checkLineDifference(
      lhsLine: Option[LinePosition],
      rhsLine: Option[LinePosition],
      lhsSource: String,
      rhsSource: String): Boolean = {
    (lhsLine, rhsLine) match {
      case (Some(lhsPosition), Some(rhsPosition)) =>
        val lhsLength = lhsPosition.end - lhsPosition.start
        val rhsLength = rhsPosition.end - rhsPosition.start
        lhsLength != rhsLength || !lhsSource.regionMatches(
          lhsPosition.start,
          rhsSource,
          rhsPosition.start,
          lhsLength
        )
      case (None, None) => false
      case _ => true
    }
  }

  /**
   * Represents a line position in a string by its start and end indices, avoiding string
   * allocation until the line content is actually needed.
   *
   * @param start the starting index of the line (inclusive)
   * @param end the ending index of the line (exclusive)
   */
  private case class LinePosition(start: Int, end: Int)

  /**
   * Iterator that iterates over lines in a string, returning line positions (start and end
   * indices) instead of materialized strings to avoid allocations.
   *
   * @param str the string to iterate over
   */
  private class LineIterator(str: String) extends Iterator[LinePosition] {
    private var position = 0
    private val stringLength = str.length

    /**
     * Returns true if there are more lines to read from the string.
     *
     * @return true if the iterator has more elements, false otherwise
     */
    override def hasNext: Boolean = position < stringLength

    /**
     * Returns the position (start and end indices) of the next line from the string. A line is
     * defined as a sequence of characters ending with a newline character or the end of the
     * string. The newline character itself is not included in the line position.
     *
     * @return the position of the next line as (start, end) indices
     * @throws NoSuchElementException if there are no more lines to read
     */
    override def next(): LinePosition = {
      if (!hasNext) throw new NoSuchElementException("next on empty iterator")

      val start = position
      while (position < stringLength && str.charAt(position) != '\n') {
        position += 1
      }

      val linePosition = LinePosition(start, position)
      if (position < stringLength) position += 1

      linePosition
    }
  }

  /**
   * Circular buffer that maintains the last N line positions added.
   *
   * @param capacity the maximum number of line positions to retain in the buffer
   */
  private class CircularBuffer(capacity: Int) {
    private val buffer = new ArrayDeque[LinePosition](capacity)

    /**
     * Adds a line position to the buffer. If the buffer is at capacity, the oldest element is
     * removed before adding the new element. If capacity is 0, the line position is not added.
     *
     * @param linePosition the line position to add to the buffer
     */
    def add(linePosition: LinePosition): Unit = {
      if (capacity > 0) {
        if (buffer.size() >= capacity) {
          buffer.removeFirst()
        }
        buffer.addLast(linePosition)
      }
    }

    /**
     * Applies a function to each line position in the buffer in the order they were added.
     *
     * @param f the function to apply to each line position
     */
    def foreach(f: LinePosition => Unit): Unit = {
      val iterator = buffer.iterator()
      while (iterator.hasNext) {
        f(iterator.next())
      }
    }
  }
}
