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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Projection}
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Unit tests for the two range-join indexes. [[IntervalIndex]] is the sweep used by
 * point-in-range. [[PointIndex]] is the sorted array used by a single inequality.
 * Operator-level behavior is covered by `RangeJoinSuite`.
 */
class RangeIndexSuite extends SparkFunSuite {

  private val intOrdering: Ordering[Any] = PhysicalDataType.ordering(IntegerType)
  private val doubleOrdering: Ordering[Any] = PhysicalDataType.ordering(DoubleType)

  private def row(id: Int): InternalRow = InternalRow(id)

  private def interval(lo: Int, hi: Int, id: Int): Seq[RangeIndex.RangeEvent] = {
    val stored = row(id)
    RangeIndex.RangeEvent(lo, RangeIndex.RangeEvent.Start, stored, id) ::
      RangeIndex.RangeEvent(hi, RangeIndex.RangeEvent.End, stored, id) :: Nil
  }

  private def point(v: Int, id: Int): Seq[RangeIndex.RangeEvent] =
    RangeIndex.RangeEvent(v, RangeIndex.RangeEvent.Point, row(id), id) :: Nil

  private def buildIntervals(events: Seq[RangeIndex.RangeEvent]): IntervalIndex =
    IntervalIndex.build(intOrdering, events.toArray)

  private def ids(rows: Iterator[InternalRow]): List[Int] = rows.map(_.getInt(0)).toList

  private def probeIds(index: IntervalIndex, low: Any, high: Any): List[Int] =
    ids(index.overlapping(low, high))

  test("toRangeEvent: point, interval, reversed, and null") {
    val getters = List(RangeIndex.getValue(IntegerType, 0), RangeIndex.getValue(IntegerType, 1))
    val identity = new Projection { override def apply(input: InternalRow): InternalRow = input }
    val eventifier = RangeIndex.toRangeEvent(getters, identity, intOrdering)

    assert(eventifier(InternalRow(5, 5), 0) ==
      Seq(RangeIndex.RangeEvent(5, RangeIndex.RangeEvent.Point, InternalRow(5, 5), 0)))
    assert(eventifier(InternalRow(1, 3), 1) ==
      Seq(
        RangeIndex.RangeEvent(1, RangeIndex.RangeEvent.Start, InternalRow(1, 3), 1),
        RangeIndex.RangeEvent(3, RangeIndex.RangeEvent.End, InternalRow(1, 3), 1)))
    // Normalized, not dropped: the join condition still accepts a pair whose sweep
    // events would otherwise never exist.
    assert(eventifier(InternalRow(3, 1), 2) ==
      Seq(
        RangeIndex.RangeEvent(1, RangeIndex.RangeEvent.Start, InternalRow(3, 1), 2),
        RangeIndex.RangeEvent(3, RangeIndex.RangeEvent.End, InternalRow(3, 1), 2)))
    assert(eventifier(InternalRow(null, 1), 3).isEmpty)
  }

  test("an inverted build interval reaches the windows the condition accepts") {
    // (3, 1) is normalized to [1, 3]. A window with low < 1 and high > 3 accepts it,
    // and dropping the row would lose the pair the join condition then rejects.
    val index = RangeBroadcastMode(
      Seq(BoundReference(0, IntegerType, nullable = true),
        BoundReference(1, IntegerType, nullable = true)),
      IntervalIndexKind)
      .transform(Array(InternalRow(3, 1), InternalRow(0, 5)))
      .asInstanceOf[IntervalIndex]
    assert(index.overlapping(0, 4).map(_.getInt(0)).toSet == Set(3, 0))
    assert(index.overlapping(4, 9).map(_.getInt(0)).toList == List(0))
  }

  test("interval sweep returns each overlapping row once") {
    // Events arrive out of order: [10, 20] then [0, 5].
    val unsorted = buildIntervals(Seq(
      RangeIndex.RangeEvent(10, RangeIndex.RangeEvent.Start, row(0), 0),
      RangeIndex.RangeEvent(0, RangeIndex.RangeEvent.Start, row(1), 1),
      RangeIndex.RangeEvent(5, RangeIndex.RangeEvent.End, row(1), 1),
      RangeIndex.RangeEvent(20, RangeIndex.RangeEvent.End, row(0), 0)))
    assert(probeIds(unsorted, 3, 3) == List(1))
    assert(probeIds(unsorted, 15, 15) == List(0))

    // Probe 50 is a build key. Row 0 spans it; row 1 starts there.
    val spanning = buildIntervals(interval(0, 100, 0) ++ interval(50, 200, 1))
    assert(probeIds(spanning, 50, 50).toSet == Set(0, 1))
    assert(probeIds(spanning, 25, 25) == List(0))
    assert(probeIds(spanning, 150, 150) == List(1))

    val duplicated = buildIntervals(interval(0, 100, 0) ++ interval(0, 100, 1))
    assert(probeIds(duplicated, 50, 50).sorted == List(0, 1))

    val nested = buildIntervals(
      interval(0, 100, 0) ++ interval(10, 20, 1) ++
        interval(10, 50, 2) ++ interval(90, 110, 3))
    def assertOnce(low: Int, expected: Set[Int]): Unit = {
      val actual = probeIds(nested, low, low)
      assert(actual.toSet == expected, s"probe $low -> $actual")
      assert(actual.size == expected.size, s"probe $low duplicated $actual")
    }
    assertOnce(0, Set(0))
    assertOnce(10, Set(0, 1, 2))
    assertOnce(15, Set(0, 1, 2))
    assertOnce(30, Set(0, 2))
    assertOnce(95, Set(0, 3))
    assertOnce(105, Set(3))
    assertOnce(200, Set.empty)

    assert(probeIds(buildIntervals(Seq.empty), 1, 1).isEmpty)

    val withNull = buildIntervals(interval(0, 10, 0) ++ point(5, 1))
    assert(probeIds(withNull, null, 5).isEmpty)
    assert(probeIds(withNull, 5, null).isEmpty)
    assert(withNull.sizeInBytes() == 16L)

    // At an interior endpoint, the range that ends and the one that starts
    // are each a candidate once.
    val chained = buildIntervals(interval(0, 1, 0) ++ interval(1, 2, 1) ++ interval(2, 3, 2))
    assert(probeIds(chained, 0, 0) == List(0))
    assert(probeIds(chained, 1, 1).sorted == List(0, 1))
    assert(probeIds(chained, 2, 2).sorted == List(1, 2))
    assert(probeIds(chained, 3, 3) == List(2))

    // [25, 10] matches [0, 30], not a row that starts strictly between the keys.
    val inverted = buildIntervals(interval(0, 30, 0) ++ interval(15, 40, 1) ++ point(15, 2))
    assert(probeIds(inverted, 25, 10) == List(0))
    assert(probeIds(inverted, 10, 20).sorted == List(0, 1, 2))
  }

  test("interval sweep is correct under heavy build-side overlap") {
    // Every interval nests inside the one before it, so at the middle keys roughly
    // half of all rows are active at once: the shape that forced an O(n^2) memory
    // blowup when each of the n distinct keys copied its own active-rows array.
    val n = 300
    val events = (0 until n).flatMap(i => interval(i, 2 * n - i, i))
    val nested = buildIntervals(events)

    def expected(probe: Int): Set[Int] =
      (0 until n).filter(i => i <= probe && probe <= 2 * n - i).toSet

    Seq(0, 1, n / 2, n, 2 * n - 1, 2 * n).foreach { probe =>
      assert(probeIds(nested, probe, probe).toSet == expected(probe), s"probe $probe")
    }
  }

  test("interval sweep treats NaN as its own point and Infinity as an open bound") {
    // PhysicalDoubleType's ordering (SQLOrderingUtil.compareDoubles) totally orders
    // -Inf < ... < +Inf < NaN, with NaN comparing equal only to itself. So [1.0, +Inf)
    // behaves like "at or above 1.0" with no finite ceiling, and a NaN key overlaps
    // only a probe that also lands exactly on NaN.
    val events =
      RangeIndex.RangeEvent(1.0, RangeIndex.RangeEvent.Start, row(1), 1) ::
      RangeIndex.RangeEvent(Double.PositiveInfinity, RangeIndex.RangeEvent.End, row(1), 1) ::
      RangeIndex.RangeEvent(Double.NegativeInfinity, RangeIndex.RangeEvent.Start, row(2), 2) ::
      RangeIndex.RangeEvent(-1.0, RangeIndex.RangeEvent.End, row(2), 2) ::
      RangeIndex.RangeEvent(Double.NaN, RangeIndex.RangeEvent.Point, row(3), 3) :: Nil
    val index = IntervalIndex.build(doubleOrdering, events.toArray)

    assert(ids(index.overlapping(5.0, 5.0)) == List(1))
    assert(ids(index.overlapping(Double.PositiveInfinity, Double.PositiveInfinity)) == List(1))
    assert(ids(index.overlapping(-5.0, -5.0)) == List(2))
    assert(ids(index.overlapping(Double.NegativeInfinity, Double.NegativeInfinity)) == List(2))
    assert(ids(index.overlapping(Double.NaN, Double.NaN)) == List(3))
    assert(ids(index.overlapping(0.0, 0.0)).isEmpty)
  }

  test("interval index merges keys that compare equal but are not equal") {
    // Array equals is identity, and UTF8String equals is binary, so these keys
    // compare equal under the index ordering without being equal. Splitting
    // them into two sweep keys drops a row the probe should return. Signed
    // zero is already one key: Scala equality treats -0.0 and 0.0 as equal.
    def assertMerged(
        ordering: Ordering[Any],
        low: Any,
        leftKey: Any,
        rightKey: Any,
        high: Any,
        earlier: Seq[Any]): Unit = {
      assert(leftKey != rightKey)
      assert(ordering.compare(leftKey, rightKey) == 0)
      val points = Array(
        RangeIndex.RangeEvent(leftKey, RangeIndex.RangeEvent.Point, row(0), 0),
        RangeIndex.RangeEvent(rightKey, RangeIndex.RangeEvent.Point, row(1), 1))
      val pointIdx = IntervalIndex.build(ordering, points)
      assert(ids(pointIdx.overlapping(leftKey, leftKey)).toSet == Set(0, 1))
      assert(ids(pointIdx.overlapping(rightKey, rightKey)).toSet == Set(0, 1))

      val events = earlier.zipWithIndex.map { case (key, i) =>
        RangeIndex.RangeEvent(key, RangeIndex.RangeEvent.Point, row(10 + i), 10 + i)
      } ++ Seq(
        RangeIndex.RangeEvent(low, RangeIndex.RangeEvent.Start, row(0), 0),
        RangeIndex.RangeEvent(leftKey, RangeIndex.RangeEvent.End, row(0), 0),
        RangeIndex.RangeEvent(rightKey, RangeIndex.RangeEvent.Start, row(1), 1),
        RangeIndex.RangeEvent(high, RangeIndex.RangeEvent.End, row(1), 1))
      val index = IntervalIndex.build(ordering, events.toArray)
      assert(ids(index.overlapping(leftKey, leftKey)).sorted == List(0, 1))
      assert(ids(index.overlapping(rightKey, rightKey)).sorted == List(0, 1))
    }

    val bytes = Array[Byte](1, 2)
    val sameBytes = Array[Byte](1, 2)
    assertMerged(
      PhysicalDataType.ordering(BinaryType),
      Array[Byte](1, 0),
      bytes,
      sameBytes,
      Array[Byte](2),
      Seq(Array[Byte](0), Array[Byte](0, 1)))

    val upper = UTF8String.fromString("M")
    val lower = UTF8String.fromString("m")
    assertMerged(
      PhysicalDataType.ordering(StringType("UNICODE_CI")),
      UTF8String.fromString("c"),
      upper,
      lower,
      UTF8String.fromString("z"),
      Seq(UTF8String.fromString("a"), UTF8String.fromString("b")))
  }

  test("interval search uses the last key that compares equal") {
    // Build groups equal keys, so the bound is exercised on an index whose
    // key array still has two slots for 5. Stopping at the first equal slot
    // returns only row 0.
    val empty = HashMap.empty[Int, InternalRow]
    val index = new IntervalIndex(
      intOrdering,
      Array[Any](null, 5, 5),
      Array(0, 0, 1),
      Array(empty, empty, empty),
      Array(empty, empty, empty),
      Array(row(0), row(1)))
    assert(ids(index.overlapping(5, 5)).sorted == List(0, 1))
    assert(ids(index.overlapping(4, 4)).isEmpty)
    assert(ids(index.overlapping(6, 6)).isEmpty)
  }

  test("point index answers both sides of a bound, including equals") {
    // Unsorted, with two rows on key 5. Equals stay, in input order.
    val keyed = Array[(Any, InternalRow)](
      (8, row(8)),
      (1, row(1)),
      (5, row(5)),
      (5, row(50)),
      (3, row(3)),
      (null, row(-1)))
    val index = PointIndex.build(intOrdering, keyed)
    assert(ids(index.upTo(5)) == List(1, 3, 5, 50))
    assert(ids(index.upTo(4)) == List(1, 3))
    assert(ids(index.upTo(0)).isEmpty)
    assert(ids(index.upTo(8)) == List(1, 3, 5, 50, 8))
    assert(ids(index.from(5)) == List(5, 50, 8))
    assert(ids(index.from(6)) == List(8))
    assert(ids(index.from(9)).isEmpty)
    assert(ids(index.from(1)) == List(1, 3, 5, 50, 8))
    assert(index.upTo(null).isEmpty)
    assert(index.from(null).isEmpty)
    assert(index.sizeInBytes() == 40L)

    val empty = PointIndex.build(intOrdering, Array.empty)
    assert(empty.upTo(1).isEmpty)
    assert(empty.from(1).isEmpty)
    assert(empty.sizeInBytes() == 0L)
  }

  test("point index orders NaN above +Infinity and -Infinity below every finite value") {
    // Total order: -Inf < -1.0 < 0.0 < 1.0 < +Inf < NaN, with NaN the unique maximum.
    val keyed = Array[(Any, InternalRow)](
      (Double.NaN, row(90)),
      (Double.PositiveInfinity, row(80)),
      (1.0, row(1)),
      (Double.NegativeInfinity, row(-80)),
      (-1.0, row(-1)),
      (0.0, row(0)))
    val index = PointIndex.build(doubleOrdering, keyed)
    assert(ids(index.upTo(0.0)) == List(-80, -1, 0))
    assert(ids(index.from(1.0)) == List(1, 80, 90))
    // NaN is the unique maximum: only a NaN key is at or above it.
    assert(ids(index.from(Double.NaN)) == List(90))
    assert(ids(index.upTo(Double.NaN)) == List(-80, -1, 0, 1, 80, 90))
    // +Infinity is below NaN but above every finite value.
    assert(ids(index.upTo(Double.PositiveInfinity)) == List(-80, -1, 0, 1, 80))
    assert(ids(index.from(Double.NegativeInfinity)) == List(-80, -1, 0, 1, 80, 90))
  }
}
