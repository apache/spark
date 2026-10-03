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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Unit tests for the two range-join indexes. [[IntervalIndex]] is the interval tree
 * used by point-in-range and interval overlap. [[PointIndex]] is the sorted array
 * used by a single inequality. Operator-level behavior is covered by `RangeJoinSuite`.
 */
class RangeIndexSuite extends SparkFunSuite {

  private val intOrdering: Ordering[Any] = PhysicalDataType.ordering(IntegerType)
  private val doubleOrdering: Ordering[Any] = PhysicalDataType.ordering(DoubleType)

  private def row(id: Int): InternalRow = InternalRow(id)

  private def span(lo: Any, hi: Any, id: Int): (Any, Any, InternalRow) = (lo, hi, row(id))

  private def buildIntervals(spans: Seq[(Any, Any, InternalRow)]): IntervalIndex =
    IntervalIndex.build(intOrdering, spans.toArray)

  private def ids(rows: Iterator[InternalRow]): List[Int] = rows.map(_.getInt(0)).toList

  private def probeIds(index: IntervalIndex, low: Any, high: Any): List[Int] =
    ids(index.overlapping(low, high))

  private def javaRoundTrip(index: IntervalIndex): (Array[Byte], IntervalIndex) = {
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    out.writeObject(index)
    out.close()
    val raw = bytes.toByteArray
    val in = new ObjectInputStream(new ByteArrayInputStream(raw))
    try {
      (raw, in.readObject().asInstanceOf[IntervalIndex])
    } finally {
      in.close()
    }
  }

  test("build keeps a point, orders an inverted interval, and drops a null bound") {
    val index = IntervalIndex.build(intOrdering, Array(
      (5, 5, InternalRow(5, 5)),
      (1, 3, InternalRow(1, 3)),
      (3, 1, InternalRow(3, 1)),
      (null, 1, InternalRow(null, 1))))
    // (3, 1) is stored as [1, 3]. The null bound is absent.
    assert(ids(index.overlapping(2, 2)).sorted == List(1, 3))
    assert(ids(index.overlapping(5, 5)) == List(5))
    assert(ids(index.overlapping(4, 4)).isEmpty)
  }

  test("an inverted build interval reaches the windows the condition accepts") {
    // (3, 1) is normalized to [1, 3]. A window with low < 1 and high > 3 accepts it,
    // and dropping the row would lose the pair the join condition accepts.
    val index = RangeBroadcastMode(
      Seq(BoundReference(0, IntegerType, nullable = true),
        BoundReference(1, IntegerType, nullable = true)),
      IntervalIndexKind)
      .transform(Array(InternalRow(3, 1), InternalRow(0, 5)))
      .asInstanceOf[IntervalIndex]
    assert(ids(index.overlapping(0, 4)).toSet == Set(3, 0))
    assert(ids(index.overlapping(4, 9)) == List(0))
  }

  test("interval tree returns each overlapping row once") {
    // Spans arrive out of low order: [10, 20] then [0, 5].
    val unsorted = buildIntervals(Seq(span(10, 20, 0), span(0, 5, 1)))
    assert(probeIds(unsorted, 3, 3) == List(1))
    assert(probeIds(unsorted, 15, 15) == List(0))

    // Probe 50 is a build key. Row 0 spans it; row 1 starts there.
    val spanning = buildIntervals(Seq(span(0, 100, 0), span(50, 200, 1)))
    assert(probeIds(spanning, 50, 50).toSet == Set(0, 1))
    assert(probeIds(spanning, 25, 25) == List(0))
    assert(probeIds(spanning, 150, 150) == List(1))

    val duplicated = buildIntervals(Seq(span(0, 100, 0), span(0, 100, 1)))
    assert(probeIds(duplicated, 50, 50).sorted == List(0, 1))

    val nested = buildIntervals(Seq(
      span(0, 100, 0), span(10, 20, 1), span(10, 50, 2), span(90, 110, 3)))
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

    val withNull = buildIntervals(Seq(span(0, 10, 0), span(5, 5, 1), (null, 4, row(9))))
    assert(probeIds(withNull, null, 5).isEmpty)
    assert(probeIds(withNull, 5, null).isEmpty)
    // The null bound is dropped, so two intervals remain and the estimate covers both.
    val one = buildIntervals(Seq(span(0, 10, 0)))
    assert(withNull.estimatedSize() > one.estimatedSize())

    // At an interior endpoint, the range that ends and the one that starts
    // are each a candidate once.
    val chained = buildIntervals(Seq(span(0, 1, 0), span(1, 2, 1), span(2, 3, 2)))
    assert(probeIds(chained, 0, 0) == List(0))
    assert(probeIds(chained, 1, 1).sorted == List(0, 1))
    assert(probeIds(chained, 2, 2).sorted == List(1, 2))
    assert(probeIds(chained, 3, 3) == List(2))

    // A probe whose endpoints are reversed is the closed window between them.
    // [10, 25] meets [0, 30], [15, 40], and the point at 15.
    val inverted = buildIntervals(Seq(span(0, 30, 0), span(15, 40, 1), span(15, 15, 2)))
    assert(probeIds(inverted, 25, 10).sorted == List(0, 1, 2))
    assert(probeIds(inverted, 10, 20).sorted == List(0, 1, 2))
  }

  test("interval tree is correct under heavy build-side overlap") {
    // Every interval nests inside the one before it, so a point in the middle
    // hits about half of the rows. The tree still stores one node per row.
    val n = 300
    val nested = buildIntervals((0 until n).map(i => span(i, 2 * n - i, i)))

    def expected(probe: Int): Set[Int] =
      (0 until n).filter(i => i <= probe && probe <= 2 * n - i).toSet

    Seq(0, 1, n / 2, n, 2 * n - 1, 2 * n).foreach { probe =>
      assert(probeIds(nested, probe, probe).toSet == expected(probe), s"probe $probe")
    }
  }

  test("broadcast of heavily overlapping intervals stays linear") {
    // [i, i + n] for i in 0 until n. Every interval contains n. The broadcast
    // is the flat arrays, one slot per interval, so serialization stays linear.
    val n = 200
    val index = buildIntervals((0 until n).map(i => span(i, i + n, i)))
    val (raw, restored) = javaRoundTrip(index)
    assert(raw.length < n * 1024, s"serialized ${raw.length} bytes")
    val probe = n
    assert(probeIds(restored, probe, probe).toSet == probeIds(index, probe, probe).toSet)
    assert(probeIds(restored, probe, probe).size == n)
  }

  test("estimatedSize depends on the interval count, not the overlap depth") {
    val n = 200
    val one = buildIntervals(Seq(span(0, 1, 0)))
    val disjoint = buildIntervals((0 until n).map(i => span(2 * i, 2 * i + 1, i)))
    val overlap = buildIntervals((0 until n).map(i => span(i, i + n, i)))
    val again = buildIntervals((0 until n).map(i => span(2 * i, 2 * i + 1, i)))
    assert(disjoint.estimatedSize() > one.estimatedSize())
    assert(overlap.estimatedSize() > one.estimatedSize())
    assert(disjoint.estimatedSize() == again.estimatedSize())
    // Nesting stores no extra copy of the rows. Key objects differ, so the two
    // indexes need not be equal, but overlap stays the same order of magnitude.
    assert(overlap.estimatedSize() < disjoint.estimatedSize() * 3)
  }

  test("estimatedSize includes distinct key objects") {
    val ints = buildIntervals(Seq(span(0, 1, 0)))
    val wide = UTF8String.fromString("x" * 64)
    val strings = IntervalIndex.build(
      PhysicalDataType.ordering(StringType),
      Array((wide, wide, row(0))))
    assert(strings.estimatedSize() > ints.estimatedSize())
  }

  test("interval tree treats NaN as its own point and Infinity as an open bound") {
    // PhysicalDoubleType's ordering (SQLOrderingUtil.compareDoubles) totally orders
    // -Inf < ... < +Inf < NaN, with NaN comparing equal only to itself. So [1.0, +Inf)
    // behaves like "at or above 1.0" with no finite ceiling, and a NaN key overlaps
    // only a probe that also lands exactly on NaN.
    val index = IntervalIndex.build(doubleOrdering, Array(
      (1.0, Double.PositiveInfinity, row(1)),
      (Double.NegativeInfinity, -1.0, row(2)),
      (Double.NaN, Double.NaN, row(3))))

    assert(ids(index.overlapping(5.0, 5.0)) == List(1))
    assert(ids(index.overlapping(Double.PositiveInfinity, Double.PositiveInfinity)) == List(1))
    assert(ids(index.overlapping(-5.0, -5.0)) == List(2))
    assert(ids(index.overlapping(Double.NegativeInfinity, Double.NegativeInfinity)) == List(2))
    assert(ids(index.overlapping(Double.NaN, Double.NaN)) == List(3))
    assert(ids(index.overlapping(0.0, 0.0)).isEmpty)
  }

  test("interval index merges keys that compare equal but are not equal") {
    // Array equals is identity, and UTF8String equals is binary, so these keys
    // compare equal under the index ordering without being equal. The tree
    // orders them with that comparator, so a probe on either key returns both.
    // Signed zero is already one key: Scala equality treats -0.0 and 0.0 as equal.
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
        (leftKey, leftKey, row(0)),
        (rightKey, rightKey, row(1)))
      val pointIdx = IntervalIndex.build(ordering, points)
      assert(ids(pointIdx.overlapping(leftKey, leftKey)).toSet == Set(0, 1))
      assert(ids(pointIdx.overlapping(rightKey, rightKey)).toSet == Set(0, 1))

      val intervals = earlier.zipWithIndex.map { case (key, i) =>
        (key, key, row(10 + i))
      }.toArray ++ Array(
        (low, leftKey, row(0)),
        (rightKey, high, row(1)))
      val index = IntervalIndex.build(ordering, intervals)
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

  test("a nested probe does not drop rows from the outer iterator") {
    val index = buildIntervals((0 until 32).map(i => span(i, i + 10, i)))
    val outer = index.overlapping(0, 100)
    val seen = scala.collection.mutable.ArrayBuffer.empty[Int]
    // The inner probe runs while the outer iterator still has rows, the same
    // shape as two interval joins fused in one whole-stage.
    while (outer.hasNext) {
      seen += outer.next().getInt(0)
      assert(ids(index.overlapping(0, 5)).sorted == (0 to 5).toList)
    }
    assert(seen.sorted == (0 until 32).toList)
  }

  test("a point probe of disjoint intervals returns only the containing row") {
    val n = 1000
    val index = buildIntervals((0 until n).map(i => span(i * 2, i * 2 + 1, i)))
    assert(probeIds(index, 2, 2) == List(1))
    assert(probeIds(index, 3, 3) == List(1))
    assert(probeIds(index, 4, 4) == List(2))
    assert(probeIds(index, -1, -1).isEmpty)
    assert(probeIds(index, 100000, 100000).isEmpty)
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

    val empty = PointIndex.build(intOrdering, Array.empty)
    assert(empty.upTo(1).isEmpty)
    assert(empty.from(1).isEmpty)
    // SizeEstimator charges the empty array shell, and the five kept points on top of it.
    assert(empty.estimatedSize() > 0)
    assert(index.estimatedSize() > empty.estimatedSize())
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
