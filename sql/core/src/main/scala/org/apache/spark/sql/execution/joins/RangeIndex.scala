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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
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
      ordering: Ordering[Any],
      intervals: Array[(Any, Any, InternalRow)]): IntervalIndex = {
    val valid = intervals
      .filter(t => t._1 != null && t._2 != null)
      .map { case (lo, hi, r) =>
        if (ordering.lteq(lo, hi)) (lo, hi, r) else (hi, lo, r)
      }
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
    new IntervalIndex(ordering, valid, maxHighs)
  }
}

private[execution] class IntervalIndex private[joins] (
    private[this] val ordering: Ordering[Any],
    private[this] val intervals: Array[(Any, Any, InternalRow)],
    private[this] val maxHighs: Array[Any])
  extends RangeRelation {

  override def estimatedSize(): Long =
    SizeEstimator.estimate(intervals) + SizeEstimator.estimate(maxHighs)

  def overlapping(low: Any, high: Any): Iterator[InternalRow] = {
    if (intervals.isEmpty || low == null || high == null) return Iterator.empty
    val (probeLow, probeHigh) = if (ordering.lteq(low, high)) (low, high) else (high, low)

    val buffer = ArrayBuffer.empty[InternalRow]
    def search(start: Int, end: Int): Unit = {
      if (start >= end) return
      val mid = (start + end) >>> 1
      if (ordering.gteq(maxHighs(mid), probeLow)) {
        search(start, mid)
        val (lo, hi, row) = intervals(mid)
        if (ordering.gteq(hi, probeLow) && ordering.lteq(lo, probeHigh)) {
          buffer += row
        }
        if (ordering.lteq(lo, probeHigh)) {
          search(mid + 1, end)
        }
      }
    }

    search(0, intervals.length)
    buffer.iterator
  }
}

/**
 * Sorted build-side points for single cross-side inequalities (`<`, `<=`, `>`, `>=`).
 */
private[execution] object PointIndex {

  def build(ordering: Ordering[Any], keyedRows: Array[(Any, InternalRow)]): PointIndex = {
    val present = keyedRows.filter(_._1 != null)
    java.util.Arrays.sort(present, Ordering.by[(Any, InternalRow), Any](_._1)(ordering))
    new PointIndex(ordering, present)
  }
}

private[execution] class PointIndex private[joins] (
    private[this] val ordering: Ordering[Any],
    private[this] val points: Array[(Any, InternalRow)])
  extends RangeRelation {

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
