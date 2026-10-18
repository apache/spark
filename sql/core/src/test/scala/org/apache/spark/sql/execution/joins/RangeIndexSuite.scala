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
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

/**
 * Unit tests for the two range-join indexes. [[IntervalIndex]] is the interval tree
 * used by point-in-range and interval overlap. [[PointIndex]] is the sorted array
 * used by a single inequality. Operator-level behavior is covered by `RangeJoinSuite`.
 */
class RangeIndexSuite extends SparkFunSuite {

  private def row(id: Int): InternalRow = InternalRow(id)

  private def span(lo: Any, hi: Any, id: Int): (Any, Any, InternalRow) = (lo, hi, row(id))

  private def buildIntervals(spans: Seq[(Any, Any, InternalRow)]): IntervalIndex =
    buildIntervals(IntegerType, spans)

  /**
   * Builds the boxed interval index through the production build. The row
   * layout puts the asserted id at ordinal 0, exactly where a real build row
   * carries its columns; the keys bind to ordinals 1 and 2.
   */
  private def buildIntervals(
      keyType: DataType,
      spans: Seq[(Any, Any, InternalRow)]): IntervalIndex = {
    val keys = Seq(
      BoundReference(1, keyType, nullable = true),
      BoundReference(2, keyType, nullable = true))
    IntervalIndex.build(keyType, keys,
      spans.iterator.map(t => InternalRow(t._3.getInt(0), t._1, t._2)),
      Some(spans.size.toLong))
  }

  /** The point-index build with the same row layout: id at 0, key at 1. */
  private def buildPoints(keyType: DataType, keyed: Seq[(Any, InternalRow)]): PointIndex = {
    val key = Seq(BoundReference(1, keyType, nullable = true))
    PointIndex.build(keyType, key,
      keyed.iterator.map(t => InternalRow(t._2.getInt(0), t._1)),
      Some(keyed.size.toLong))
  }

  private def ids(rows: Iterator[InternalRow]): List[Int] = rows.map(_.getInt(0)).toList

  private def probeIds(index: IntervalIndex, low: Any, high: Any): List[Int] =
    ids(index.overlapping(low, high))

  /**
   * An iterator that hands back the same mutable row on every `next()` call, each field
   * set to the current index. Mirrors how Spark's row iterators commonly reuse a single
   * buffer (e.g. `UnsafeRow` deserialization) instead of allocating a fresh row per record.
   */
  private def reusingRowIterator(
      fields: Int,
      n: Int): (org.apache.spark.sql.catalyst.expressions.GenericInternalRow,
      Iterator[InternalRow]) = {
    val shared = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(fields)
    val iter = new Iterator[InternalRow] {
      private var i = 0
      override def hasNext: Boolean = i < n
      override def next(): InternalRow = {
        var c = 0
        while (c < fields) { shared.update(c, i); c += 1 }
        i += 1
        shared
      }
    }
    (shared, iter)
  }

  private def reusingByteBufferStringIterator(
      n: Int): (Array[Byte], Iterator[InternalRow]) = {
    val sharedBytes = new Array[Byte](4)
    val sharedRow = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(2)
    val iter = new Iterator[InternalRow] {
      private var i = 0
      override def hasNext: Boolean = i < n
      override def next(): InternalRow = {
        val s = f"$i%04d"
        java.lang.System.arraycopy(s.getBytes("UTF-8"), 0, sharedBytes, 0, 4)
        val utf8 = UTF8String.fromAddress(sharedBytes, Platform.BYTE_ARRAY_OFFSET, 4)
        sharedRow.update(0, utf8)
        sharedRow.update(1, i)
        i += 1
        sharedRow
      }
    }
    (sharedBytes, iter)
  }

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
    val index = buildIntervals(Seq(
      span(5, 5, 5),
      span(1, 3, 1),
      span(3, 1, 3),
      (null, 1, row(1))))
    // (3, 1) is stored as [1, 3]. The null bound is absent.
    assert(ids(index.overlapping(2, 2)).sorted == List(1, 3))
    assert(ids(index.overlapping(5, 5)) == List(5))
    assert(ids(index.overlapping(4, 4)).isEmpty)
  }

  test("IntervalIndex.build defends against a build-side iterator reusing its row buffer") {
    // If build() captured keys or rows by reference instead of copying them, every stored
    // entry would collapse to the iterator's last value once the shared buffer is mutated
    // again after the build-side iterator is drained -- exactly what a broadcast's row
    // iterator does once BroadcastExchangeExec finishes feeding it to RangeBroadcastMode.
    val n = 50
    val buildKeys = Seq(
      BoundReference(0, IntegerType, nullable = true),
      BoundReference(1, IntegerType, nullable = true))
    val (shared, rows) = reusingRowIterator(fields = 3, n)
    val index = IntervalIndex.build(IntegerType, buildKeys, rows, Some(n))

    // Simulate the caller reusing the buffer after the build-side iterator is exhausted.
    shared.update(0, -1)
    shared.update(1, -1)
    shared.update(2, -1)

    (0 until n).foreach { i =>
      val payloads = index.overlapping(i, i).map(_.getInt(2)).toList
      assert(payloads == List(i), s"interval $i was corrupted by later reuse of the row buffer")
    }
  }

  test("IntervalIndex.build defends against string keys sharing a reused byte buffer") {
    val n = 50
    val buildKeys = Seq(
      BoundReference(0, StringType, nullable = true),
      BoundReference(0, StringType, nullable = true))
    val (sharedBytes, rows) = reusingByteBufferStringIterator(n)
    val index = IntervalIndex.build(StringType, buildKeys, rows, Some(n))

    java.util.Arrays.fill(sharedBytes, 'X'.toByte)

    (0 until n).foreach { i =>
      val key = UTF8String.fromString(f"$i%04d")
      val matching = index.overlapping(key, key).toList
      assert(matching.size == 1, s"interval $i was corrupted by shared buffer reuse")
      assert(matching.head.getInt(1) == i)
    }
  }

  test("an inverted build interval reaches the windows the condition accepts") {
    // Int keys route to the long index, so this also pins swap normalization there.
    val index = RangeBroadcastMode(
      Seq(BoundReference(0, IntegerType, nullable = true),
        BoundReference(1, IntegerType, nullable = true)),
      IntervalIndexKind)
      .transform(Array(InternalRow(3, 1), InternalRow(0, 5)))
      .asInstanceOf[LongIntervalIndex]
    assert(index.overlapping(0L, 4L).map(_.getInt(0)).toSet == Set(3, 0))
    assert(index.overlapping(4L, 9L).map(_.getInt(0)).toList == List(0))
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
    assert(withNull.estimatedSize > one.estimatedSize)

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
    assert(disjoint.estimatedSize > one.estimatedSize)
    assert(overlap.estimatedSize > one.estimatedSize)
    assert(disjoint.estimatedSize == again.estimatedSize)
    // Nesting stores no extra copy of the rows. Key objects differ, so the two
    // indexes need not be equal, but overlap stays the same order of magnitude.
    assert(overlap.estimatedSize < disjoint.estimatedSize * 3)
  }

  test("estimatedSize includes distinct key objects") {
    val ints = buildIntervals(Seq(span(0, 1, 0)))
    val wide = UTF8String.fromString("x" * 64)
    val strings = buildIntervals(StringType, Seq((wide, wide, row(0))))
    assert(strings.estimatedSize > ints.estimatedSize)
  }

  test("interval tree treats NaN as its own point and Infinity as a bound") {
    // PhysicalDoubleType's ordering (SQLOrderingUtil.compareDoubles) totally orders
    // -Inf < ... < +Inf < NaN, with NaN comparing equal only to itself. So [1.0, +Inf]
    // includes +Inf under inclusive probe, and a NaN key overlaps only a probe that
    // also lands exactly on NaN.
    val index = buildIntervals(DoubleType, Seq(
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
        dataType: DataType,
        low: Any,
        leftKey: Any,
        rightKey: Any,
        high: Any,
        earlier: Seq[Any]): Unit = {
      val ordering = PhysicalDataType.ordering(dataType)
      assert(leftKey != rightKey)
      assert(ordering.compare(leftKey, rightKey) == 0)
      val points = Array(
        (leftKey, leftKey, row(0)),
        (rightKey, rightKey, row(1)))
      val pointIdx = buildIntervals(dataType, points.toSeq)
      assert(ids(pointIdx.overlapping(leftKey, leftKey)).toSet == Set(0, 1))
      assert(ids(pointIdx.overlapping(rightKey, rightKey)).toSet == Set(0, 1))

      val intervals = earlier.zipWithIndex.map { case (key, i) =>
        (key, key, row(10 + i))
      }.toArray ++ Array(
        (low, leftKey, row(0)),
        (rightKey, high, row(1)))
      val index = buildIntervals(dataType, intervals.toSeq)
      assert(ids(index.overlapping(leftKey, leftKey)).sorted == List(0, 1))
      assert(ids(index.overlapping(rightKey, rightKey)).sorted == List(0, 1))
    }

    val bytes = Array[Byte](1, 2)
    val sameBytes = Array[Byte](1, 2)
    assertMerged(
      BinaryType,
      Array[Byte](1, 0),
      bytes,
      sameBytes,
      Array[Byte](2),
      Seq(Array[Byte](0), Array[Byte](0, 1)))

    val upper = UTF8String.fromString("M")
    val lower = UTF8String.fromString("m")
    assertMerged(
      StringType("UNICODE_CI"),
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
    val index = buildPoints(IntegerType, keyed.toSeq)
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

    val empty = buildPoints(IntegerType, Seq.empty)
    assert(empty.upTo(1).isEmpty)
    assert(empty.from(1).isEmpty)
    // SizeEstimator charges the empty array shell, and the five kept points on top of it.
    assert(empty.estimatedSize > 0)
    assert(index.estimatedSize > empty.estimatedSize)
  }

  test("PointIndex.build defends against a build-side iterator reusing its row buffer") {
    // Same hazard as the IntervalIndex case above: a point index built straight off a
    // reused row buffer must copy both the key and the row, not just reference them.
    val n = 50
    val buildKeys = Seq(BoundReference(0, IntegerType, nullable = true))
    val (shared, rows) = reusingRowIterator(fields = 2, n)
    val index = PointIndex.build(IntegerType, buildKeys, rows, Some(n))

    shared.update(0, -1)
    shared.update(1, -1)

    (0 until n).foreach { i =>
      val payload = index.from(i).next().getInt(1)
      assert(payload == i, s"point $i was corrupted by later reuse of the row buffer")
    }
  }

  test("PointIndex.build defends against string keys sharing a reused byte buffer") {
    val n = 50
    val buildKeys = Seq(BoundReference(0, StringType, nullable = true))
    val (sharedBytes, rows) = reusingByteBufferStringIterator(n)
    val index = PointIndex.build(StringType, buildKeys, rows, Some(n))

    java.util.Arrays.fill(sharedBytes, 'X'.toByte)

    (0 until n).foreach { i =>
      val key = UTF8String.fromString(f"$i%04d")
      val matching = index.from(key).take(1).toList
      assert(matching.nonEmpty && matching.head.getInt(1) == i,
        s"point $i was corrupted by shared buffer reuse")
    }
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
    val index = buildPoints(DoubleType, keyed.toSeq)
    assert(ids(index.upTo(0.0)) == List(-80, -1, 0))
    assert(ids(index.from(1.0)) == List(1, 80, 90))
    // NaN is the unique maximum: only a NaN key is at or above it.
    assert(ids(index.from(Double.NaN)) == List(90))
    assert(ids(index.upTo(Double.NaN)) == List(-80, -1, 0, 1, 80, 90))
    // +Infinity is below NaN but above every finite value.
    assert(ids(index.upTo(Double.PositiveInfinity)) == List(-80, -1, 0, 1, 80))
    assert(ids(index.from(Double.NegativeInfinity)) == List(-80, -1, 0, 1, 80, 90))
  }

  test("upTo/from matches a brute-force reference across randomized point sets") {
    // PointIndex's counterpart to "overlapping matches a brute-force reference
    // across randomized interval sets": cross-checks the binary-search bound
    // scan against an independent O(n) scan, across many random key sets and
    // probe values, including duplicate keys.
    val rand = new scala.util.Random(42)
    val bound = 50
    for (_ <- 0 until 30) {
      val n = 1 + rand.nextInt(300)
      val raw = (0 until n).map(i => (rand.nextInt(bound), i))
      val index = buildPoints(IntegerType, raw.map { case (k, i) => (k, row(i)) })

      for (_ <- 0 until 20) {
        val probe = rand.nextInt(bound)
        val expectedUpTo = raw.collect { case (k, i) if k <= probe => i }.toSet
        val expectedFrom = raw.collect { case (k, i) if k >= probe => i }.toSet
        assert(ids(index.upTo(probe)).toSet == expectedUpTo,
          s"n=$n probe=$probe expected=$expectedUpTo")
        assert(ids(index.from(probe)).toSet == expectedFrom,
          s"n=$n probe=$probe expected=$expectedFrom")
      }
    }
  }

  test("indexes round-trip through Java and strict Kryo serialization") {
    val serializers = Seq(
      "Java" -> new org.apache.spark.serializer.JavaSerializer(
        new org.apache.spark.SparkConf()).newInstance(),
      "Kryo" -> {
        val conf = new org.apache.spark.SparkConf()
          .set(org.apache.spark.internal.config.Kryo.KRYO_REGISTRATION_REQUIRED, true)
        new org.apache.spark.serializer.KryoSerializer(conf).newInstance()
      })

    // (key type, lo, hi, probe) covering each supported key family: fixed
    // width, floating point, decimal, binary, and collated strings. The
    // collated probe mixes cases, so a restored index only answers it when
    // its collation ordering survived the round trip. CharType/VarcharType
    // carry a StringConstraint (FixedLength/MaxLength) that Kryo must have
    // registered separately from plain StringType.
    val cases: Seq[(DataType, Any, Any, Any)] = Seq(
      (ByteType, 0.toByte, 10.toByte, 5.toByte),
      (ShortType, 0.toShort, 10.toShort, 5.toShort),
      (IntegerType, 0, 10, 5),
      (LongType, 0L, 10L, 5L),
      (FloatType, 0.0f, 10.0f, 5.0f),
      (DoubleType, 0.0, 10.0, 5.0),
      (DecimalType(10, 2), Decimal(0.0), Decimal(10.0), Decimal(5.0)),
      (StringType("UTF8_LCASE"),
        UTF8String.fromString("abc"), UTF8String.fromString("zzz"), UTF8String.fromString("MMM")),
      (CharType(3),
        UTF8String.fromString("abc"), UTF8String.fromString("zzz"), UTF8String.fromString("mmm")),
      (VarcharType(10),
        UTF8String.fromString("abc"), UTF8String.fromString("zzz"), UTF8String.fromString("mmm")),
      (BinaryType, Array[Byte](0), Array[Byte](10), Array[Byte](5)),
      (DateType, 100, 200, 150),
      (TimestampType, 1000L, 2000L, 1500L),
      (TimestampNTZType, 1000L, 2000L, 1500L),
      (BooleanType, false, true, true))

    def roundTrip[A <: AnyRef: scala.reflect.ClassTag](
        ser: org.apache.spark.serializer.SerializerInstance, index: A): A =
      ser.deserialize[A](ser.serialize(index))

    // One reader for the sealed family, mirroring candidates() dispatch:
    // the interval kinds answer a window, the point kinds answer a
    // lower-bound scan, and every relation this test builds answers at
    // row ordinal 0.
    def answer(rel: RangeRelation, low: Any, high: Any): List[Int] = rel match {
      case r: LongIntervalIndex => ids(r.overlapping(r.toLong(low), r.toLong(high)))
      case r: IntervalIndex => ids(r.overlapping(low, high))
      case r: LongPointIndex => ids(r.from(r.toLong(low)))
      case r: PointIndex => ids(r.from(low))
    }

    cases.foreach { case (dt, lo, hi, probe) =>
      // One straight span and one inverted span that normalizes to the same
      // shape, plus the family the broadcast build routes the type to. Rows
      // carry the asserted id at ordinal 0 and the keys at 1 and 2, so one
      // reader answers every family.
      val interval = buildIntervals(dt, Seq((lo, hi, row(5)), (hi, lo, row(6))))
      val point = buildPoints(dt, Seq((probe, row(7))))
      val routedInterval = RangeBroadcastMode(
        Seq(BoundReference(1, dt, nullable = true), BoundReference(2, dt, nullable = true)),
        IntervalIndexKind)
        .transform(Array(InternalRow(5, lo, hi), InternalRow(6, hi, lo)))
      val routedPoint = RangeBroadcastMode(
        Seq(BoundReference(1, dt, nullable = true)), PointIndexKind)
        .transform(Array(InternalRow(7, probe)))

      val intervalWindow = answer(interval, probe, probe)
      val pointScan = answer(point, probe, probe)
      assert(intervalWindow.sorted == List(5, 6), s"$dt window")
      assert(pointScan.sorted == List(7), s"$dt bound")
      // The routed family answers the same query on the same rows.
      assert(answer(routedInterval, probe, probe).sorted == intervalWindow.sorted, s"$dt routed")
      assert(answer(routedPoint, probe, probe).sorted == pointScan.sorted, s"$dt routed point")

      serializers.foreach { case (name, ser) =>
        Seq(interval, routedInterval).foreach { rel =>
          assert(answer(roundTrip(ser, rel), probe, probe).sorted == intervalWindow.sorted,
            s"$dt interval via $name")
        }
        Seq(point, routedPoint).foreach { rel =>
          assert(answer(roundTrip(ser, rel), probe, probe).sorted == pointScan.sorted,
            s"$dt point via $name")
        }
      }
    }
  }

  test("overlap iterator right-sizes scratch stack to tree depth") {
    // A single-row index has a trivial core equal to its one interval, so the
    // probe deliberately lands outside it to exercise the walk's scratch stack.
    val oneRowIndex = buildIntervals(Seq(span(0, 10, 0)))
    val oneRowIter = oneRowIndex.overlapping(15, 15).asInstanceOf[OverlapIterator]
    assert(oneRowIter.stackCapacity == 8,
      s"one-row index should allocate minimal scratch (8), got ${oneRowIter.stackCapacity}")

    val largeIndex = buildIntervals((0 until 10000).map(i => span(i, i + 100, i)))
    val largeIter = largeIndex.overlapping(50, 50).asInstanceOf[OverlapIterator]
    // 10,000 rows has tree depth 14, requiring 3 * 14 = 42 capacity.
    assert(largeIter.stackCapacity == 42,
      s"10,000-row index should size scratch to tree depth (42), got ${largeIter.stackCapacity}")
  }

  test("overlapping traversal is lazy and supports early exit under high overlap") {
    // The pending-work stack tracks tree depth, not match count: draining a handful
    // of candidates out of ~100 matches among n=10,000 intervals must not grow it
    // toward n. Staggered spans also avoid the common-core short cut, which would
    // skip the walk entirely.
    val n = 10000
    val overlappingSpans = (0 until n).map(i => span(i, i + 100, i))
    val index = buildIntervals(overlappingSpans)
    val iter = index.overlapping(5000, 5000).asInstanceOf[OverlapIterator]
    val initialCapacity = iter.stackCapacity

    assert(iter.take(3).size == 3)
    assert(iter.steps < 50,
      s"lazy traversal must perform bounded steps ($n total intervals), but took ${iter.steps}")
    assert(iter.stackCapacity == initialCapacity,
      s"stack grew from $initialCapacity to ${iter.stackCapacity} after taking 3 of $n matches")
  }

  test("overlapping uses the common-core short cut without changing the answer") {
    // [0, 10], [5, 15], and [8, 20] share the core [8, 10] (1-D Helly). Any
    // window that reaches it overlaps every interval, so the index returns
    // all rows directly; windows outside keep the walk's selective answer.
    val index = buildIntervals(Seq(span(0, 10, 0), span(5, 15, 1), span(8, 20, 2)))
    assert(probeIds(index, 9, 9).sorted == List(0, 1, 2))
    assert(probeIds(index, 8, 10).sorted == List(0, 1, 2))
    assert(probeIds(index, 6, 12).sorted == List(0, 1, 2))
    // Endpoints are normalized first, so a reversed window reaches the core too.
    assert(probeIds(index, 12, 6).sorted == List(0, 1, 2))
    // The short cut skips the walk's iterator for a core window, and only there.
    assert(!index.overlapping(9, 9).isInstanceOf[OverlapIterator])
    assert(probeIds(index, 0, 2) == List(0))
    assert(probeIds(index, 6, 7).sorted == List(0, 1))
    assert(probeIds(index, 11, 12).sorted == List(1, 2))
    assert(probeIds(index, 16, 30) == List(2))
  }

  test("overlapping matches a brute-force reference across randomized interval sets") {
    // Cross-checks the tree-pruning search in overlapping() against an independent
    // O(n) scan, across many random interval sets and probe windows. Hand-written
    // cases above cover specific shapes; this covers combinations they would miss.
    val rand = new scala.util.Random(42)
    val bound = 50
    for (_ <- 0 until 30) {
      val n = 1 + rand.nextInt(300)
      // Endpoints are not pre-sorted, exercising build()'s lo/hi-swap normalization.
      val raw = (0 until n).map(i => (rand.nextInt(bound), rand.nextInt(bound), i))
      val index = buildIntervals(raw.map { case (a, b, i) => span(a, b, i) })
      val normalized = raw.map { case (a, b, i) => (math.min(a, b), math.max(a, b), i) }

      for (_ <- 0 until 20) {
        val a = rand.nextInt(bound)
        val b = rand.nextInt(bound)
        val (low, high) = (math.min(a, b), math.max(a, b))
        val expected = normalized.collect {
          case (lo, hi, i) if lo <= high && low <= hi => i
        }.toSet
        assert(probeIds(index, low, high).toSet == expected,
          s"n=$n low=$low high=$high expected=$expected")
      }
    }
  }

  test("broadcast modes route fixed-width keys to the long indexes") {
    // Both build keys are long-convertible: the long interval index.
    assert(RangeBroadcastMode(
      Seq(BoundReference(0, IntegerType, nullable = true),
        BoundReference(1, IntegerType, nullable = true)),
      IntervalIndexKind)
      .transform(Array(InternalRow(0, 10, 1))).isInstanceOf[LongIntervalIndex])
    assert(RangeBroadcastMode(
      Seq(BoundReference(0, IntegerType, nullable = true)), PointIndexKind)
      .transform(Array(InternalRow(5, 1))).isInstanceOf[LongPointIndex])
    // Floating point has no bit-preserving SQL order, and strings are not one long.
    assert(RangeBroadcastMode(
      Seq(BoundReference(0, FloatType, nullable = true)), PointIndexKind)
      .transform(Array(InternalRow(1.0f, 1))).isInstanceOf[PointIndex])
    assert(RangeBroadcastMode(
      Seq(BoundReference(0, StringType, nullable = true),
        BoundReference(1, StringType, nullable = true)),
      IntervalIndexKind)
      .transform(Array(InternalRow(
        UTF8String.fromString("a"), UTF8String.fromString("b"), 1))).isInstanceOf[IntervalIndex])
  }

  test("the long interval index answers the same windows as the generic one") {
    def longIndex(spans: Seq[(Any, Any, InternalRow)]): LongIntervalIndex =
      RangeBroadcastMode(
        Seq(BoundReference(0, IntegerType, nullable = true),
          BoundReference(1, IntegerType, nullable = true)),
        IntervalIndexKind)
        .transform(spans.map(t => InternalRow(t._1, t._2, t._3.getInt(0))).toArray)
        .asInstanceOf[LongIntervalIndex]
    def assertSameWindow(
        generic: IntervalIndex,
        longIdx: LongIntervalIndex,
        lo: Int,
        hi: Int): Unit = {
      val expected = probeIds(generic, lo, hi).sorted
      val actual = longIdx.overlapping(lo.toLong, hi.toLong).map(_.getInt(2)).toList.sorted
      assert(actual == expected, s"window ($lo, $hi): expected $expected, got $actual")
    }

    // Includes an inverted span and a null bound, and shares the core [8, 10].
    val spans = Seq(span(0, 10, 0), span(5, 15, 1), (null, 4, row(9)),
      span(8, 20, 2), span(15, 5, 3))
    val generic = buildIntervals(spans)
    val longIdx = longIndex(spans)
    assert(longIdx.estimatedSize > 0)
    // A window that meets the core skips the walk and still answers everything.
    assert(!longIdx.overlapping(9L, 9L).isInstanceOf[LongOverlapIterator])
    assert(longIdx.overlapping(9L, 9L).map(_.getInt(2)).toList.sorted == List(0, 1, 2, 3))
    Seq((2, 2), (5, 7), (9, 9), (13, 12), (11, 11), (15, 15), (20, 30))
      .foreach { case (lo, hi) => assertSameWindow(generic, longIdx, lo, hi) }

    // Randomized sets cross-check the families independently: the generic index
    // is itself brute-force verified, so this pins the walk, the lo/hi swap,
    // and null drops without recomputing expectations by hand.
    val rand = new scala.util.Random(42)
    val bound = 50
    for (_ <- 0 until 20) {
      val n = 1 + rand.nextInt(300)
      val raw = (0 until n).map(i => (rand.nextInt(bound), rand.nextInt(bound), i))
      val randomSpans = raw.map { case (a, b, i) => span(a, b, i) }
      val randomGeneric = buildIntervals(randomSpans)
      val randomLong = longIndex(randomSpans)
      for (_ <- 0 until 15) {
        val a = rand.nextInt(bound)
        val b = rand.nextInt(bound)
        assertSameWindow(randomGeneric, randomLong, math.min(a, b), math.max(a, b))
      }
    }
  }

  test("boolean keys take the long path and answer the same windows") {
    // false < true maps to 0 < 1. Intervals: [false, true], [true, true], one
    // null-bound row that both families drop.
    val spans = Array(
      (false: Any, true: Any, row(0)),
      (true: Any, true: Any, row(1)),
      (null, true: Any, row(2)))
    val generic = buildIntervals(BooleanType, spans.toSeq)
    val longIdx = RangeBroadcastMode(
      Seq(BoundReference(0, BooleanType, nullable = true),
        BoundReference(1, BooleanType, nullable = true)),
      IntervalIndexKind)
      .transform(Array(InternalRow(false, true, 0), InternalRow(true, true, 1),
        InternalRow(null, true, 2)))
      .asInstanceOf[LongIntervalIndex]
    assert(longIdx.estimatedSize > 0)
    Seq((false, false), (true, true), (false, true)).foreach { case (lo, hi) =>
      val expected = probeIds(generic, lo, hi).sorted
      val actual = longIdx
        .overlapping(if (lo) 1L else 0L, if (hi) 1L else 0L)
        .map(_.getInt(2)).toList.sorted
      assert(actual == expected, s"window ($lo, $hi): expected $expected, got $actual")
    }
  }

  test("the long point index answers the same bounds as the generic one") {
    def longIndex(keyed: Seq[(Any, InternalRow)]): LongPointIndex =
      RangeBroadcastMode(
        Seq(BoundReference(0, IntegerType, nullable = true)), PointIndexKind)
        .transform(keyed.map(t => InternalRow(t._1, t._2.getInt(0))).toArray)
        .asInstanceOf[LongPointIndex]
    def assertSameBounds(generic: PointIndex, longIdx: LongPointIndex, v: Int): Unit = {
      val upTo = longIdx.upTo(v.toLong).map(_.getInt(1)).toList.sorted
      assert(upTo == ids(generic.upTo(v)).sorted, s"upTo($v)")
      val from = longIdx.from(v.toLong).map(_.getInt(1)).toList.sorted
      assert(from == ids(generic.from(v)).sorted, s"from($v)")
    }

    // Unsorted, with two rows on key 5, and a null key that both paths drop.
    val keyed = Array[(Any, InternalRow)](
      (8, row(8)), (1, row(1)), (5, row(5)), (5, row(50)), (3, row(3)), (null, row(-1)))
    val generic = buildPoints(IntegerType, keyed.toSeq)
    val longIdx = longIndex(keyed.toSeq)
    Seq(0, 1, 4, 5, 6, 8, 9).foreach(v => assertSameBounds(generic, longIdx, v))

    // Randomized sets cross-check the families independently: the generic index
    // is itself brute-force verified, so this pins the long path's bound search
    // and null handling without recomputing expectations by hand.
    val rand = new scala.util.Random(42)
    val bound = 50
    for (_ <- 0 until 20) {
      val n = 1 + rand.nextInt(300)
      val raw = (0 until n).map(i => (rand.nextInt(bound), i))
      val randomKeyed = raw.map { case (k, i) => (k: Any, row(i)) }
      val randomGeneric = buildPoints(IntegerType, randomKeyed)
      val randomLong = longIndex(randomKeyed)
      for (_ <- 0 until 15) {
        assertSameBounds(randomGeneric, randomLong, rand.nextInt(bound))
      }
    }
  }

  test("LongIntervalIndex.build defends against a build-side iterator reusing its buffer") {
    // Same hazard as the generic case: keys and rows are copied out, so a
    // reused row buffer cannot corrupt the finished index.
    val n = 50
    val buildKeys = Seq(
      BoundReference(0, IntegerType, nullable = true),
      BoundReference(1, IntegerType, nullable = true))
    val (shared, rows) = reusingRowIterator(fields = 3, n)
    val index = RangeBroadcastMode(buildKeys, IntervalIndexKind)
      .transform(rows, Some(n)).asInstanceOf[LongIntervalIndex]

    shared.update(0, -1)
    shared.update(1, -1)
    shared.update(2, -1)

    (0 until n).foreach { i =>
      val payloads = index.overlapping(i.toLong, i.toLong).map(_.getInt(2)).toList
      assert(payloads == List(i), s"interval $i was corrupted by later reuse of the row buffer")
    }
  }

}
