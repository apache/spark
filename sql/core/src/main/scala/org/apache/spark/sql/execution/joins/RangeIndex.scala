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
import org.apache.spark.sql.catalyst.expressions.{Expression, InterpretedProjection}
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
      buildKeys: Seq[Expression],
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): IntervalIndex = {
    val initialCapacity = sizeHint.map(n => math.min(n, Int.MaxValue).toInt).getOrElse(64)
    val buffer = new ArrayBuffer[(Any, Any, InternalRow)](initialCapacity)
    val projection = new InterpretedProjection(buildKeys)
    val lowOf = RangeIndex.getValue(buildKeys.head.dataType, 0)
    val highOf =
      if (buildKeys.length == 1) lowOf else RangeIndex.getValue(buildKeys(1).dataType, 1)
    val ordering = PhysicalDataType.ordering(keyType)

    while (rows.hasNext) {
      val r = rows.next()
      val p = projection(r)
      normalizedInterval(ordering, lowOf(p), highOf(p), r, buffer)
    }

    sortAndBuild(keyType, ordering, buffer.toArray)
  }

  def build(
      keyType: DataType,
      intervals: Array[(Any, Any, InternalRow)]): IntervalIndex = {
    val ordering = PhysicalDataType.ordering(keyType)
    val buffer = new ArrayBuffer[(Any, Any, InternalRow)](intervals.length)
    var i = 0
    while (i < intervals.length) {
      val (rawLo, rawHi, r) = intervals(i)
      normalizedInterval(ordering, rawLo, rawHi, r, buffer)
      i += 1
    }
    sortAndBuild(keyType, ordering, buffer.toArray)
  }

  /** Drops a null-bounded interval; otherwise orders (lo, hi) and copies the key and row. */
  private def normalizedInterval(
      ordering: Ordering[Any],
      rawLo: Any,
      rawHi: Any,
      r: InternalRow,
      buffer: ArrayBuffer[(Any, Any, InternalRow)]): Unit = {
    if (rawLo != null && rawHi != null) {
      val lo = InternalRow.copyValue(rawLo)
      val hi = InternalRow.copyValue(rawHi)
      val (normLo, normHi) = if (ordering.lteq(lo, hi)) (lo, hi) else (hi, lo)
      buffer += ((normLo, normHi, r.copy()))
    }
  }

  private def sortAndBuild(
      keyType: DataType,
      ordering: Ordering[Any],
      valid: Array[(Any, Any, InternalRow)]): IntervalIndex = {
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
  @volatile private[this] var _ordering: Ordering[Any] = _

  private[joins] def ordering: Ordering[Any] = {
    var ord = _ordering
    if (ord == null) {
      ord = PhysicalDataType.ordering(keyType)
      _ordering = ord
    }
    ord
  }

  override def estimatedSize(): Long =
    SizeEstimator.estimate(Array[AnyRef](intervals, maxHighs))

  def overlapping(low: Any, high: Any): Iterator[InternalRow] = {
    if (intervals.isEmpty || low == null || high == null) return Iterator.empty
    val ord = ordering
    val (probeLow, probeHigh) = if (ord.lteq(low, high)) (low, high) else (high, low)
    new OverlapIterator(intervals, maxHighs, ord, probeLow, probeHigh)
  }
}

private[joins] class OverlapIterator(
    intervals: Array[(Any, Any, InternalRow)],
    maxHighs: Array[Any],
    ord: Ordering[Any],
    probeLow: Any,
    probeHigh: Any) extends Iterator[InternalRow] {

  private[this] val depth = 32 - java.lang.Integer.numberOfLeadingZeros(intervals.length)
  private[joins] var stackStart = new Array[Int](math.max(8, depth * 3))
  private[joins] var stackEnd = new Array[Int](math.max(8, depth * 3))
  private[joins] var stackTop = 0
  private[joins] var steps = 0
  private[this] var nextRow: InternalRow = null

  push(0, intervals.length)

  private[joins] def stackCapacity: Int = stackStart.length

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
      steps += 1
      stackTop -= 1
      val start = stackStart(stackTop)
      val end = stackEnd(stackTop)
      if (start < end) {
        val mid = (start + end) >>> 1
        if (ord.gteq(maxHighs(mid), probeLow)) {
          if (ord.lteq(intervals(mid)._1, probeHigh)) {
            push(mid + 1, end)
          }
          push(mid, -1)
          push(start, mid)
        }
      } else if (end == -1) {
        val (lo, hi, row) = intervals(start)
        if (ord.gteq(hi, probeLow) && ord.lteq(lo, probeHigh)) {
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

/**
 * Sorted build-side points for single cross-side inequalities (`<`, `<=`, `>`, `>=`).
 */
private[execution] object PointIndex {

  def build(
      keyType: DataType,
      buildKeys: Seq[Expression],
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): PointIndex = {
    val initialCapacity = sizeHint.map(n => math.min(n, Int.MaxValue).toInt).getOrElse(64)
    val buffer = new ArrayBuffer[(Any, InternalRow)](initialCapacity)
    val projection = new InterpretedProjection(buildKeys)
    val lowOf = RangeIndex.getValue(buildKeys.head.dataType, 0)
    val ordering = PhysicalDataType.ordering(keyType)

    while (rows.hasNext) {
      val r = rows.next()
      val p = projection(r)
      val rawKey = lowOf(p)
      if (rawKey != null) {
        val key = InternalRow.copyValue(rawKey)
        buffer += ((key, r.copy()))
      }
    }

    sortAndBuild(keyType, ordering, buffer.toArray)
  }

  def build(keyType: DataType, keyedRows: Array[(Any, InternalRow)]): PointIndex = {
    val ordering = PhysicalDataType.ordering(keyType)
    val buffer = new ArrayBuffer[(Any, InternalRow)](keyedRows.length)
    var i = 0
    while (i < keyedRows.length) {
      val (rawKey, row) = keyedRows(i)
      if (rawKey != null) {
        val key = InternalRow.copyValue(rawKey)
        buffer += ((key, row.copy()))
      }
      i += 1
    }
    sortAndBuild(keyType, ordering, buffer.toArray)
  }

  private def sortAndBuild(
      keyType: DataType,
      ordering: Ordering[Any],
      present: Array[(Any, InternalRow)]): PointIndex = {
    java.util.Arrays.sort(present, Ordering.by[(Any, InternalRow), Any](_._1)(ordering))
    new PointIndex(keyType, present)
  }
}

private[execution] class PointIndex private[joins] (
    val keyType: DataType,
    private[this] val points: Array[(Any, InternalRow)])
  extends RangeRelation {

  @transient
  @volatile private[this] var _ordering: Ordering[Any] = _

  private[joins] def ordering: Ordering[Any] = {
    var ord = _ordering
    if (ord == null) {
      ord = PhysicalDataType.ordering(keyType)
      _ordering = ord
    }
    ord
  }

  private[this] val length = points.length

  override def estimatedSize(): Long = SizeEstimator.estimate(points)

  /** Points whose key is less than or equal to `value`. */
  def upTo(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(0, searchBound(value, skipEquals = true))

  /** Points whose key is greater than or equal to `value`. */
  def from(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(searchBound(value, skipEquals = false), length)

  private def searchBound(value: Any, skipEquals: Boolean): Int = {
    val ord = ordering
    var lo = 0
    var hi = length
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      val c = ord.compare(points(mid)._1, value)
      if (c < 0 || (skipEquals && c == 0)) lo = mid + 1 else hi = mid
    }
    lo
  }

  private def slice(from: Int, until: Int): Iterator[InternalRow] =
    (from until until).iterator.map(i => points(i)._2)
}
