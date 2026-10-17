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
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, IntegerType,
  LongType, ShortType, TimestampNTZType, TimestampType}
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator}

/**
 * Broadcast payload of a range join. An index only names candidate rows.
 * [[BroadcastRangeJoinExec]] evaluates the original join condition to filter
 * candidates. Keys that map to one order-preserving long are stored raw and
 * compared unboxed ([[LongIntervalIndex]], [[LongPointIndex]]); every other
 * supported type is compared as boxed values ([[IntervalIndex]],
 * [[PointIndex]]). [[RangeBroadcastMode]] picks the family from the build
 * keys' data types.
 *
 *  - The interval indexes answer "which ranges overlap this window?"
 *  - The point indexes answer "which points lie on this side of a bound?"
 */
private[execution] sealed trait RangeRelation extends KnownSizeEstimation with Serializable {
  def keyType: DataType
}

/**
 * The boxed-key index family's shared ordering cache, derived from `keyType`
 * on first comparison and kept out of the serialized form, so each executor
 * re-derives its own instance instead of receiving one per broadcast. The
 * long index family compares raw keys and has no ordering to cache.
 */
private[joins] sealed trait BoxedKeyOrdering extends RangeRelation {
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
}

/**
 * The long index family's shared key conversion, derived from `keyType` on
 * first probe and kept out of the serialized form, so each executor derives
 * its own converter after deserialization. The interpreted probe path calls
 * [[toLong]] once per streamed key; generated probe code keeps its own
 * conversion, emitted at plan time.
 */
private[joins] sealed trait LongKeyConversion extends RangeRelation {
  @transient
  @volatile private[this] var _toLong: Any => Long = _

  /**
   * One evaluated streamed key to its order-preserving long. `RangePredicate`
   * gives both sides of a range predicate one type after analysis, so the
   * streamed key type is this index's `keyType`.
   */
  private[joins] def toLong(key: Any): Long = {
    var f = _toLong
    if (f == null) {
      f = RangeIndex.requiredLongKey(keyType).value
      _toLong = f
    }
    f(key)
  }
}

private[joins] object RangeIndex {

  /** Accessor reading a single field from `InternalRow` as a boxed value (or null). */
  def getValue(dt: DataType, ordinal: Int): InternalRow => Any = {
    val accessor = InternalRow.getAccessor(dt)
    (input: InternalRow) => accessor(input, ordinal)
  }

  /**
   * One key type's bridge to an order-preserving long: reading a projected build
   * field, converting an evaluated streamed value, and turning generated Java for
   * a typed key into generated Java for a long. The three agree, so a build key
   * and a probe key of the same type become the same long.
   */
  final case class LongKey(
      field: (InternalRow, Int) => Long,
      value: Any => Long,
      code: String => String)

  /**
   * An order-preserving long for a fixed-width key, or None when the type must
   * stay on the generic boxed index: variable-length keys, and floating point,
   * whose SQL equality for -0.0 and NaN has no bit-preserving order. This is
   * the only place a key type is specialized; the long indexes and the generated
   * probe code both consult it.
   */
  def longKey(dt: DataType): Option[LongKey] = {
    def key(field: (InternalRow, Int) => Long, value: Any => Long): LongKey =
      LongKey(field, value, v => s"((long) $v)")
    dt match {
      case _: BooleanType =>
        Some(LongKey(
          (r, o) => if (r.getBoolean(o)) 1L else 0L,
          v => if (v.asInstanceOf[Boolean]) 1L else 0L,
          v => s"($v ? 1L : 0L)"))
      case _: ByteType =>
        Some(key((r, o) => r.getByte(o).toLong, v => v.asInstanceOf[Byte].toLong))
      case _: ShortType =>
        Some(key((r, o) => r.getShort(o).toLong, v => v.asInstanceOf[Short].toLong))
      case _: IntegerType | _: DateType =>
        Some(key((r, o) => r.getInt(o).toLong, v => v.asInstanceOf[Int].toLong))
      case _: LongType | _: TimestampType | _: TimestampNTZType =>
        Some(key((r, o) => r.getLong(o), v => v.asInstanceOf[Long]))
      case _ => None
    }
  }

  /**
   * Every key maps to one order-preserving long. [[RangeBroadcastMode]] picks
   * the index family by this test and [[BroadcastRangeJoinExec]] picks its probe
   * code by the same one, so the two cannot drift apart. `RangePredicate` gives
   * both sides of a range predicate one type after analysis, so testing the
   * build keys also settles the streamed keys.
   */
  private[execution] def canMapKeysToLong(keys: Seq[Expression]): Boolean =
    keys.forall(k => longKey(k.dataType).isDefined)

  /** longKey for a type the caller has routed to the long family. */
  private[execution] def requiredLongKey(dt: DataType): LongKey = {
    longKey(dt).getOrElse(
      throw new IllegalStateException(s"$dt has no order-preserving long key."))
  }
}

/**
 * Array-backed candidate scan. The index hands rows to the join in array order,
 * without a closure or a tuple hop: the per-pair overhead then matches a bare
 * loop over a broadcast row array. One instance per probe; never serialized.
 */
private[joins] final class RowSlice(
    rows: Array[InternalRow],
    from: Int,
    until: Int) extends Iterator[InternalRow] {
  private var i = from
  def hasNext: Boolean = i < until
  def next(): InternalRow = {
    val r = rows(i)
    i += 1
    r
  }
}

private[joins] object RowSlice {
  /** A shared empty scan: a null probe bound matches nothing. */
  val empty: RowSlice = new RowSlice(new Array[InternalRow](0), 0, 0)
}

// ===== Boxed-key indexes ====================================================
//
// Keys are the boxed values `RangeIndex.getValue` reads, compared through
// `BoxedKeyOrdering`. Floating point, strings, binary, and decimal keys stay
// here: their comparison has no order-preserving single-long form.

/**
 * Augmented interval tree of build-side intervals. Entries are (low, high, row)
 * tuples sorted by low bound, with a parallel row array feeding the scan path
 * and `maxHighs(mid)` recording the maximum high bound in the span rooted at
 * mid. Subtrees whose max high is below the probe window, or whose low starts
 * after it, are pruned during search, and a window meeting the common
 * intersection skips the walk entirely.
 */
private[joins] class IntervalIndex private (
    val keyType: DataType,
    private[this] val intervals: Array[(Any, Any, InternalRow)],
    private[this] val maxHighs: Array[Any],
    private[this] val rows: Array[InternalRow])
  extends BoxedKeyOrdering {

  // 1-D Helly: the stored intervals are null-free with lo <= hi, so they share
  // the intersection [coreLow, coreHigh] iff coreLow <= coreHigh. `intervals`
  // is sorted by low, so the last low bounds the core from below.
  private[this] val coreLow: Any =
    if (intervals.isEmpty) null else intervals(intervals.length - 1)._1
  private[this] val coreHigh: Any =
    if (intervals.isEmpty) null
    else intervals.iterator.map(_._2).reduce[Any](ordering.min(_, _))
  private[this] val hasCommonCore: Boolean =
    coreLow != null && ordering.lteq(coreLow, coreHigh)

  override def estimatedSize: Long =
    SizeEstimator.estimate(Array[AnyRef](intervals, maxHighs, rows))

  def overlapping(low: Any, high: Any): Iterator[InternalRow] = {
    if (intervals.isEmpty || low == null || high == null) return Iterator.empty
    val ord = ordering
    val (probeLow, probeHigh) = if (ord.lteq(low, high)) (low, high) else (high, low)
    // A window that meets the core overlaps every interval, and the walk below
    // would return every row anyway: return them without visiting the tree.
    if (hasCommonCore && ord.lteq(probeLow, coreHigh) && ord.gteq(probeHigh, coreLow)) {
      return new RowSlice(rows, 0, rows.length)
    }
    new OverlapIterator(intervals, rows, maxHighs, ord, probeLow, probeHigh)
  }
}

private[joins] object IntervalIndex {

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
    val rows = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      rows(i) = valid(i)._3
      i += 1
    }
    new IntervalIndex(keyType, valid, maxHighs, rows)
  }
}

private[joins] class OverlapIterator(
    intervals: Array[(Any, Any, InternalRow)],
    rows: Array[InternalRow],
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
        val lo = intervals(start)._1
        val hi = intervals(start)._2
        if (ord.gteq(hi, probeLow) && ord.lteq(lo, probeHigh)) {
          return rows(start)
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
private[joins] class PointIndex private (
    val keyType: DataType,
    private[this] val points: Array[(Any, InternalRow)],
    private[this] val rows: Array[InternalRow])
  extends BoxedKeyOrdering {

  private[this] val length = points.length

  override def estimatedSize: Long = SizeEstimator.estimate(Array[AnyRef](points, rows))

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

  private def slice(from: Int, until: Int): Iterator[InternalRow] = new RowSlice(rows, from, until)
}

private[joins] object PointIndex {

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

  private def sortAndBuild(
      keyType: DataType,
      ordering: Ordering[Any],
      present: Array[(Any, InternalRow)]): PointIndex = {
    java.util.Arrays.sort(present, Ordering.by[(Any, InternalRow), Any](_._1)(ordering))
    val rows = new Array[InternalRow](present.length)
    var i = 0
    while (i < present.length) {
      rows(i) = present(i)._2
      i += 1
    }
    new PointIndex(keyType, present, rows)
  }
}

// ===== Long-specialized indexes for fixed-width keys =======================
//
// Indexes for fixed-width keys, stored as order-preserving longs converted at the
// boundary by `RangeIndex.longKey`. Once built, these structures carry no Spark
// types: the tree walk and the binary searches compare raw longs, and generated
// probe code passes long keys without boxing. Types that cannot be one long, or
// whose SQL equality has no bit mapping (floating point), stay on `IntervalIndex`
// and `PointIndex`. `RangeBroadcastMode` picks between the two families once,
// from the build keys' data types.

private[joins] class LongIntervalIndex private (
    val keyType: DataType,
    private[this] val lows: Array[Long],
    private[this] val highs: Array[Long],
    private[this] val maxHighs: Array[Long],
    private[this] val rows: Array[InternalRow])
  extends LongKeyConversion {

  // 1-D Helly on raw keys: the stored intervals are null-free with lo <= hi,
  // so they share the intersection [coreLow, coreHigh] iff coreLow <= coreHigh.
  // A window that meets the core overlaps every interval, and the walk below
  // would return every row anyway.
  private[this] val coreLow: Long = if (lows.isEmpty) Long.MinValue else lows(lows.length - 1)
  private[this] val coreHigh: Long = {
    var min = Long.MaxValue
    var i = 0
    while (i < highs.length) {
      if (highs(i) < min) min = highs(i)
      i += 1
    }
    if (highs.isEmpty) Long.MinValue else min
  }
  private[this] val hasCommonCore: Boolean = !lows.isEmpty && coreLow <= coreHigh

  override def estimatedSize: Long =
    SizeEstimator.estimate(Array[AnyRef](lows, highs, maxHighs, rows))

  def overlapping(low: Long, high: Long): Iterator[InternalRow] = {
    if (lows.isEmpty) return Iterator.empty
    var probeLow = low
    var probeHigh = high
    if (probeLow > probeHigh) {
      val t = probeLow
      probeLow = probeHigh
      probeHigh = t
    }
    if (hasCommonCore && probeLow <= coreHigh && probeHigh >= coreLow) {
      return new RowSlice(rows, 0, rows.length)
    }
    new LongOverlapIterator(lows, highs, rows, maxHighs, probeLow, probeHigh)
  }
}

private[joins] object LongIntervalIndex {

  def build(
      keyType: DataType,
      buildKeys: Seq[Expression],
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): LongIntervalIndex = {
    val toLongs: Seq[(InternalRow, Int) => Long] =
      buildKeys.map(k => RangeIndex.requiredLongKey(k.dataType).field)
    val initialCapacity = sizeHint.map(n => math.min(n, Int.MaxValue).toInt).getOrElse(64)
    val valid = new ArrayBuffer[(Long, Long, InternalRow)](initialCapacity)
    val projection = new InterpretedProjection(buildKeys)

    while (rows.hasNext) {
      val r = rows.next()
      val p = projection(r)
      if (!p.isNullAt(0) && (buildKeys.length == 1 || !p.isNullAt(1))) {
        // A point build key is one key used as both ends, so it is read once.
        var lo = toLongs.head(p, 0)
        var hi = if (buildKeys.length == 1) lo else toLongs(1)(p, 1)
        if (lo > hi) {
          val t = lo
          lo = hi
          hi = t
        }
        valid += ((lo, hi, r.copy()))
      }
    }

    val staged = valid.toArray
    java.util.Arrays.sort(staged, Ordering.by[(Long, Long, InternalRow), Long](_._1))
    val length = staged.length
    val lows = new Array[Long](length)
    val highs = new Array[Long](length)
    val rowArr = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      lows(i) = staged(i)._1
      highs(i) = staged(i)._2
      rowArr(i) = staged(i)._3
      i += 1
    }

    val maxHighs = new Array[Long](length)

    def fillMaxHigh(start: Int, end: Int): Long = {
      if (start >= end) return Long.MinValue
      val mid = (start + end) >>> 1
      var max = highs(mid)
      val left = fillMaxHigh(start, mid)
      val right = fillMaxHigh(mid + 1, end)
      if (left > max) max = left
      if (right > max) max = right
      maxHighs(mid) = max
      max
    }

    fillMaxHigh(0, length)
    new LongIntervalIndex(keyType, lows, highs, maxHighs, rowArr)
  }
}

private[joins] class LongOverlapIterator(
    lows: Array[Long],
    highs: Array[Long],
    rows: Array[InternalRow],
    maxHighs: Array[Long],
    probeLow: Long,
    probeHigh: Long) extends Iterator[InternalRow] {

  private[this] val depth = 32 - java.lang.Integer.numberOfLeadingZeros(lows.length)
  private[this] var stackStart = new Array[Int](math.max(8, depth * 3))
  private[this] var stackEnd = new Array[Int](math.max(8, depth * 3))
  private[this] var stackTop = 0
  private[this] var nextRow: InternalRow = null

  push(0, lows.length)

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
        if (maxHighs(mid) >= probeLow) {
          if (lows(mid) <= probeHigh) {
            push(mid + 1, end)
          }
          push(mid, -1)
          push(start, mid)
        }
      } else if (end == -1) {
        if (highs(start) >= probeLow && lows(start) <= probeHigh) {
          return rows(start)
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

private[joins] class LongPointIndex private (
    val keyType: DataType,
    private[this] val keys: Array[Long],
    private[this] val rows: Array[InternalRow])
  extends LongKeyConversion {

  private[this] val length = keys.length

  override def estimatedSize: Long = SizeEstimator.estimate(Array[AnyRef](keys, rows))

  /** Points whose key is less than or equal to `value`. */
  def upTo(value: Long): Iterator[InternalRow] =
    new RowSlice(rows, 0, searchBound(value, skipEquals = true))

  /** Points whose key is greater than or equal to `value`. */
  def from(value: Long): Iterator[InternalRow] =
    new RowSlice(rows, searchBound(value, skipEquals = false), length)

  // java.util.Arrays.binarySearch returns an arbitrary hit; a bound scan needs
  // the first or last position, so lower and upper bounds are hand-rolled.
  private def searchBound(value: Long, skipEquals: Boolean): Int = {
    var lo = 0
    var hi = length
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      val c = java.lang.Long.compare(keys(mid), value)
      if (c < 0 || (skipEquals && c == 0)) lo = mid + 1 else hi = mid
    }
    lo
  }
}

private[joins] object LongPointIndex {

  def build(
      keyType: DataType,
      buildKeys: Seq[Expression],
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): LongPointIndex = {
    require(buildKeys.length == 1, "A long point index expects one build key.")
    val keyAt = RangeIndex.requiredLongKey(buildKeys.head.dataType).field
    val initialCapacity = sizeHint.map(n => math.min(n, Int.MaxValue).toInt).getOrElse(64)
    val present = new ArrayBuffer[(Long, InternalRow)](initialCapacity)
    val projection = new InterpretedProjection(buildKeys)

    while (rows.hasNext) {
      val r = rows.next()
      val p = projection(r)
      if (!p.isNullAt(0)) {
        present += ((keyAt(p, 0), r.copy()))
      }
    }

    val staged = present.toArray
    java.util.Arrays.sort(staged, Ordering.by[(Long, InternalRow), Long](_._1))
    val length = staged.length
    val keys = new Array[Long](length)
    val rowArr = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      keys(i) = staged(i)._1
      rowArr(i) = staged(i)._2
      i += 1
    }
    new LongPointIndex(keyType, keys, rowArr)
  }
}
