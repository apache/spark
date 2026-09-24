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

import scala.collection.immutable.HashMap
import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

/**
 * Broadcast payload of a range join. An index only names candidate rows.
 * [[BroadcastRangeJoinExec]] keeps a candidate when the original join condition
 * is true, so inclusivity never lives in the index.
 *
 *  - [[IntervalIndex]] answers "which ranges overlap this window?"
 *  - [[PointIndex]] answers "which points lie on this side of a bound?"
 */
private[execution] sealed trait RangeRelation extends Serializable {
  def sizeInBytes(): Long
}

private[execution] object RangeIndex {

  /** In-memory size of one indexed row. Non-unsafe rows are a test-only estimate. */
  def rowSize(row: InternalRow): Long = row match {
    case u: UnsafeRow => u.getSizeInBytes.toLong
    case _ => row.numFields * 8L
  }

  /**
   * One sweep-line event used to build an [[IntervalIndex]].
   *
   * @param key   the bound value this event occurs at
   * @param flow  [[RangeEvent.Start]], [[RangeEvent.Point]], or [[RangeEvent.End]]
   * @param row   the build-side row that produced this event
   * @param index a unique id for `row` so deactivation is O(1)
   */
  case class RangeEvent(key: Any, flow: Int, row: InternalRow, index: Int)

  object RangeEvent {
    /** Interval start. Sorts after Point at the same key. */
    val Start = 1
    /** Degenerate interval (low == high). */
    val Point = 0
    /** Interval end. Sorts before Point at the same key. */
    val End = -1
  }

  /**
   * A type-specialized accessor that reads a single field from an `InternalRow` as a boxed
   * value (or `null` if the field is null). Shared by the exchange (build-side key extraction)
   * and the join (stream-side key extraction) so the dispatch table lives in one place.
   */
  def getValue(dt: DataType, ordinal: Int): InternalRow => Any = {
    // Same set as `RangePredicate.supportedType`. Anything else is rejected at planning
    val accessor = InternalRow.getAccessor(dt)
    (input: InternalRow) => accessor(input, ordinal)
  }

  /**
   * First index in `keys` whose key compares greater than `value` under `cmp`, or, when
   * `orEqual` is set, greater than or equal to `value`. Shared by the lowerBound/upperBound
   * pair on both [[IntervalIndex]] and [[PointIndex]]; `cmp` is where they differ, since
   * `IntervalIndex` treats a leading null key as sorting before every value.
   */
  def searchBound(keys: Array[Any], value: Any, orEqual: Boolean, cmp: (Any, Any) => Int): Int = {
    var lo = 0
    var hi = keys.length
    while (lo < hi) {
      val mid = lo + ((hi - lo) >>> 1)
      val c = cmp(keys(mid), value)
      if (if (orEqual) c <= 0 else c < 0) lo = mid + 1 else hi = mid
    }
    lo
  }

  /**
   * Turn a projected `(low, high)` row into sweep events. A null bound emits none.
   *
   * An inverted interval (`low > high`) is normalized to `(high, low)` rather than dropped.
   * The join condition still accepts a row the predicate accepts, so dropping would lose it:
   * with build `(3, 1)` and stream `(0, 5)`, `a.lo < b.hi AND b.lo < a.hi` holds and the row
   * has to be a candidate. Normalizing only widens the window. `start <= end` also keeps the
   * sweep's invariant, so `IntervalIndex` never sees a reversed pair.
   */
  def toRangeEvent(
      buildSideKeyValueGetter: List[InternalRow => Any],
      lowHighExtr: Projection,
      cmp: Ordering[Any]): (InternalRow, Int) => Seq[RangeEvent] = {
    (row: InternalRow, index: Int) => {
      val lowHigh: InternalRow = lowHighExtr(row)
      val low = buildSideKeyValueGetter(0)(lowHigh)
      val high = buildSideKeyValueGetter(1)(lowHigh)
      if (low != null && high != null) {
        val result = cmp.compare(low, high)
        if (result == 0) {
          RangeEvent(low, RangeEvent.Point, row, index) :: Nil
        } else {
          val (start, end) = if (result < 0) (low, high) else (high, low)
          RangeEvent(start, RangeEvent.Start, row, index) ::
            RangeEvent(end, RangeEvent.End, row, index) :: Nil
        }
      } else {
        Nil
      }
    }
  }
}

/**
 * Sweep-line index of build-side intervals. `overlapping` returns every row whose
 * interval may overlap `[low, high]`. The window is a superset: the low search is
 * exclusive and the high search is inclusive, and the caller drops false positives.
 */
private[execution] object IntervalIndex {

  /** Build an interval index from unsorted events. */
  def build(ordering: Ordering[Any], events: Array[RangeIndex.RangeEvent]): IntervalIndex = {
    val eventComparator = new java.util.Comparator[RangeIndex.RangeEvent] {
      override def compare(a: RangeIndex.RangeEvent, b: RangeIndex.RangeEvent): Int = {
        val keyCmp = ordering.compare(a.key, b.key)
        if (keyCmp != 0) keyCmp else Integer.compare(a.flow, b.flow)
      }
    }
    java.util.Arrays.sort(events, eventComparator)
    buildFromSorted(ordering, events)
  }

  private def buildFromSorted(
      ordering: Ordering[Any],
      events: Array[RangeIndex.RangeEvent]): IntervalIndex = {
    // A leading null key makes the binary search uniform, including an empty index.
    val keys = mutable.Buffer[Any](null)
    val offsets = mutable.Buffer[Int](0)
    val activatedRows = mutable.Buffer.empty[InternalRow]
    val activeOld = mutable.Buffer.empty[HashMap[Int, InternalRow]]
    val activeAll = mutable.Buffer.empty[HashMap[Int, InternalRow]]

    // Keyed by the event id so a row leaves the active set in O(1). Two snapshots
    // are recorded per distinct key: `activeOld` (before this key's own Start
    // events) and `activeAll` (after them); `overlapping` below explains why both
    // are needed. A build side of heavily-overlapping intervals (e.g. n intervals
    // all spanning the same window) has O(n) distinct keys each with an O(n)
    // active set, so a plain map copied at every key would make the snapshots
    // alone O(n^2). `HashMap` is persistent: `updated`/`removed` share structure
    // with the previous version instead of copying it, so recording a snapshot is
    // O(log n) and the total cost across all keys stays O(n log n). It is also
    // `Serializable` for the broadcast, unlike the order-preserving persistent
    // maps in the same package (`TreeSeqMap`, `VectorMap`) -- iteration order
    // does not matter here since old/all are tracked as separate snapshots.
    var currentKey: Any = null
    var currentActiveRows = HashMap.empty[Int, InternalRow]
    var currentOldActiveRows = currentActiveRows
    var oldActiveRowsCaptured = false

    def writeActiveRows(): Unit = {
      activeOld += (if (oldActiveRowsCaptured) currentOldActiveRows else currentActiveRows)
      activeAll += currentActiveRows
    }

    events.foreach { event =>
      // Group by the index ordering. Array equality is identity, and
      // UTF8String equality is binary, so `!=` would split one sweep key.
      if (currentKey == null || ordering.compare(currentKey, event.key) != 0) {
        writeActiveRows()
        currentKey = event.key
        oldActiveRowsCaptured = false
        keys += event.key
        offsets += activatedRows.size
      }
      // The active set before any row starts at this key. Ends at this key have
      // already been removed by the time the first Point/Start event is seen.
      if (event.flow >= RangeIndex.RangeEvent.Point && !oldActiveRowsCaptured) {
        currentOldActiveRows = currentActiveRows
        oldActiveRowsCaptured = true
      }
      event.flow match {
        case RangeIndex.RangeEvent.Start =>
          activatedRows += event.row
          currentActiveRows = currentActiveRows.updated(event.index, event.row)
        case RangeIndex.RangeEvent.Point =>
          activatedRows += event.row
        case RangeIndex.RangeEvent.End =>
          currentActiveRows = currentActiveRows.removed(event.index)
      }
    }
    writeActiveRows()

    new IntervalIndex(ordering, keys.toArray, offsets.toArray, activeOld.toArray,
      activeAll.toArray, activatedRows.toArray)
  }
}

private[execution] class IntervalIndex(
    private[this] val ordering: Ordering[Any],
    private[this] val keys: Array[Any],
    private[this] val offsets: Array[Int],
    private[this] val activeOld: Array[HashMap[Int, InternalRow]],
    private[this] val activeAll: Array[HashMap[Int, InternalRow]],
    private[this] val activated: Array[InternalRow])
  extends RangeRelation {

  override def sizeInBytes(): Long = activated.map(RangeIndex.rowSize).sum

  private[this] val maxKeyIndex = keys.length - 1

  /** The leading null key sorts before every real key. */
  private def keyCmp(key: Any, value: Any): Int =
    if (key == null) -1 else ordering.compare(key, value)

  /** First index whose key is greater than or equal to `value`. */
  private def lowerBound(value: Any): Int =
    RangeIndex.searchBound(keys, value, orEqual = false, keyCmp)

  /** First index whose key is greater than `value`. Walks a whole equal run. */
  private def upperBound(value: Any): Int =
    RangeIndex.searchBound(keys, value, orEqual = true, keyCmp)

  /**
   * Rows whose interval may overlap `[low, high]`.
   *
   * `first` is the last key strictly below `low`, so active rows there include
   * intervals that end at `low`. `last` is the last key at or below `high`.
   * Both bounds walk a whole run of equal keys. Three layouts fall out:
   *  - `first < last`: active rows at `first` (including its own Starts, since
   *    those rows are still active going into `(first, last]`), then rows
   *    activated on `(first, last]`. This is `activeAll(first)`.
   *  - `first == last`: only those active rows, same set. The probe did not
   *    land on a key.
   *  - `first > last`: `low > high` and a key lies strictly between them. Only
   *    rows already active before `keys(first)` can match, excluding rows that
   *    start there. This is `activeOld(first)`.
   */
  def overlapping(low: Any, high: Any): Iterator[InternalRow] = {
    if (keys.length == 1 || low == null || high == null) return Iterator.empty

    val first = lowerBound(low) - 1
    val last = upperBound(high) - 1

    new Iterator[InternalRow] {
      // Active rows are a persistent snapshot, walked with an iterator instead of
      // `active(first)(rowIndex)`; `activated` is a plain array, so it stays indexed.
      var usingActive = true
      val activeIter: Iterator[InternalRow] =
        (if (first <= last) activeAll(first) else activeOld(first)).valuesIterator
      var activatedAvailable = first < last
      var rowIndex = 0
      var rowLength = 0

      override final def hasNext: Boolean = {
        if (usingActive && activeIter.hasNext) return true
        usingActive = false
        var result = rowIndex < rowLength
        if (!result && activatedAvailable) {
          activatedAvailable = false
          rowIndex = offsets(first + 1)
          rowLength = if (last == maxKeyIndex) activated.length else offsets(last + 1)
          result = rowIndex < rowLength
        }
        result
      }

      override final def next(): InternalRow = {
        if (usingActive) {
          activeIter.next()
        } else {
          val row = activated(rowIndex)
          rowIndex += 1
          row
        }
      }
    }
  }
}

/**
 * Sorted build-side points. `upTo` and `from` both include equals, so `<` and
 * `<=` (and the two greater-than forms) share one broadcast. The join condition
 * drops the bound that does not belong.
 */
private[execution] object PointIndex {
  def build(ordering: Ordering[Any], keyedRows: Array[(Any, InternalRow)]): PointIndex = {
    val present = keyedRows.filter(_._1 != null)
    val comparator = new java.util.Comparator[(Any, InternalRow)] {
      override def compare(a: (Any, InternalRow), b: (Any, InternalRow)): Int =
        ordering.compare(a._1, b._1)
    }
    java.util.Arrays.sort(present, comparator)
    val keys = new Array[Any](present.length)
    val rows = new Array[InternalRow](present.length)
    var i = 0
    while (i < present.length) {
      keys(i) = present(i)._1
      rows(i) = present(i)._2
      i += 1
    }
    new PointIndex(ordering, keys, rows)
  }
}

private[execution] class PointIndex(
    private[this] val ordering: Ordering[Any],
    private[this] val keys: Array[Any],
    private[this] val rows: Array[InternalRow])
  extends RangeRelation {

  override def sizeInBytes(): Long = rows.map(RangeIndex.rowSize).sum

  /** Points whose key is less than or equal to `value`. */
  def upTo(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(0, upperBound(value))

  /** Points whose key is greater than or equal to `value`. */
  def from(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(lowerBound(value), keys.length)

  private def slice(from: Int, until: Int): Iterator[InternalRow] = new Iterator[InternalRow] {
    private var i = from
    override def hasNext: Boolean = i < until
    override def next(): InternalRow = {
      val row = rows(i)
      i += 1
      row
    }
  }

  /** First index whose key is greater than or equal to `value`. */
  private def lowerBound(value: Any): Int =
    RangeIndex.searchBound(keys, value, orEqual = false, ordering.compare)

  /** First index whose key is greater than `value`. */
  private def upperBound(value: Any): Int =
    RangeIndex.searchBound(keys, value, orEqual = true, ordering.compare)
}
