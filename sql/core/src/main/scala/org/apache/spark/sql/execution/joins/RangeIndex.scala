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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.SizeEstimator

/**
 * Broadcast payload of a range join. An index only names candidate rows.
 * [[BroadcastRangeJoinExec]] evaluates the original join condition to filter candidates.
 *
 *  - [[IntervalIndex]] answers "which ranges overlap this window?"
 *  - [[PointIndex]] answers "which points lie on this side of a bound?"
 */
private[execution] sealed trait RangeRelation extends Serializable {
  def estimatedSize(): Long
  def keyType: DataType
}

private[execution] object RangeIndex {

  /** Accessor reading a single field from `InternalRow` as a boxed value (or null). */
  def getValue(dt: DataType, ordinal: Int): InternalRow => Any = {
    val accessor = InternalRow.getAccessor(dt)
    (input: InternalRow) => accessor(input, ordinal)
  }
}

/**
 * Augmented interval tree of build-side intervals, stored as flat arrays.
 *
 * Intervals are sorted by low bound. For any span `[start, end)`, its midpoint
 * is the subtree root, and `maxHighs(mid)` records the maximum high bound in
 * that span. Subtrees whose max high is below the probe window, or whose low
 * starts after it, are pruned during search.
 */
private[execution] object IntervalIndex {

  def build(
      keyType: DataType,
      intervals: Array[(Any, Any, InternalRow)]): IntervalIndex = {
    val ordering = PhysicalDataType.ordering(keyType)
    val valid = intervals.iterator
      .collect {
        case (lo, hi, r) if lo != null && hi != null =>
          if (ordering.lteq(lo, hi)) (lo, hi, r) else (hi, lo, r)
      }
      .toArray
    java.util.Arrays.sort(valid, Ordering.by[(Any, Any, InternalRow), Any](_._1)(ordering))

    val length = valid.length
    val maxHighs = new Array[Any](length)

    def fillMaxHigh(start: Int, end: Int): Any = {
      if (start >= end) return null
      val mid = (start + end) >>> 1
      var max = valid(mid)._2
      val left = fillMaxHigh(start, mid)
      val right = fillMaxHigh(mid + 1, end)
      if (left != null) max = ordering.max(max, left)
      if (right != null) max = ordering.max(max, right)
      maxHighs(mid) = max
      max
    }

    fillMaxHigh(0, length)
    new IntervalIndex(keyType, valid, maxHighs)
  }
}

private[execution] class IntervalIndex private[joins] (
    val keyType: DataType,
    private[this] val intervals: Array[(Any, Any, InternalRow)],
    private[this] val maxHighs: Array[Any])
  extends RangeRelation {

  @transient
  private[this] lazy val ordering: Ordering[Any] = PhysicalDataType.ordering(keyType)

  override def estimatedSize(): Long =
    SizeEstimator.estimate(intervals) + SizeEstimator.estimate(maxHighs)

  def overlapping(low: Any, high: Any): Iterator[InternalRow] = {
    if (intervals.isEmpty || low == null || high == null) return Iterator.empty
    val (probeLow, probeHigh) = if (ordering.lteq(low, high)) (low, high) else (high, low)

    new Iterator[InternalRow] {
      private[this] var stackStart = new Array[Int](128)
      private[this] var stackEnd = new Array[Int](128)
      private[this] var stackTop = 0
      private[this] var nextRow: InternalRow = null

      push(0, intervals.length)

      private def push(s: Int, e: Int): Unit = {
        if (stackTop >= stackStart.length) {
          stackStart = java.util.Arrays.copyOf(stackStart, stackStart.length * 2)
          stackEnd = java.util.Arrays.copyOf(stackEnd, stackEnd.length * 2)
        }
        stackStart(stackTop) = s
        stackEnd(stackTop) = e
        stackTop += 1
      }

      private def advance(): InternalRow = {
        while (stackTop > 0) {
          stackTop -= 1
          val start = stackStart(stackTop)
          val end = stackEnd(stackTop)
          if (start < end) {
            val mid = (start + end) >>> 1
            if (ordering.gteq(maxHighs(mid), probeLow)) {
              if (ordering.lteq(intervals(mid)._1, probeHigh)) {
                push(mid + 1, end)
              }
              push(mid, -1)
              push(start, mid)
            }
          } else if (end == -1) {
            val (lo, hi, row) = intervals(start)
            if (ordering.gteq(hi, probeLow) && ordering.lteq(lo, probeHigh)) {
              return row
            }
          }
        }
        null
      }

      override def hasNext: Boolean = {
        if (nextRow != null) true
        else {
          nextRow = advance()
          nextRow != null
        }
      }

      override def next(): InternalRow = {
        if (!hasNext) throw new NoSuchElementException("empty iterator")
        val r = nextRow
        nextRow = null
        r
      }
    }
  }
}

/**
 * Sorted build-side points for single cross-side inequalities (`<`, `<=`, `>`, `>=`).
 */
private[execution] object PointIndex {

  def build(keyType: DataType, keyedRows: Array[(Any, InternalRow)]): PointIndex = {
    val ordering = PhysicalDataType.ordering(keyType)
    val present = keyedRows.iterator.filter(_._1 != null).toArray
    java.util.Arrays.sort(present, Ordering.by[(Any, InternalRow), Any](_._1)(ordering))
    new PointIndex(keyType, present)
  }
}

private[execution] class PointIndex private[joins] (
    val keyType: DataType,
    private[this] val points: Array[(Any, InternalRow)])
  extends RangeRelation {

  @transient
  private[this] lazy val ordering: Ordering[Any] = PhysicalDataType.ordering(keyType)

  private[this] val length = points.length

  override def estimatedSize(): Long = SizeEstimator.estimate(points)

  /** Points whose key is less than or equal to `value`. */
  def upTo(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(0, searchBound(value, skipEquals = true))

  /** Points whose key is greater than or equal to `value`. */
  def from(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(searchBound(value, skipEquals = false), length)

  private def searchBound(value: Any, skipEquals: Boolean): Int = {
    var lo = 0
    var hi = length
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      val c = ordering.compare(points(mid)._1, value)
      if (c < 0 || (skipEquals && c == 0)) lo = mid + 1 else hi = mid
    }
    lo
  }

  private def slice(from: Int, until: Int): Iterator[InternalRow] =
    (from until until).iterator.map(i => points(i)._2)
}
