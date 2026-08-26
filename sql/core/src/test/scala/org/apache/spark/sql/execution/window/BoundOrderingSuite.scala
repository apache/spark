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

package org.apache.spark.sql.execution.window

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, IdentityProjection, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.IntegerType

/**
 * Tests for the peer-group machinery behind GROUPS frames: [[PeerGroupCursor]], which counts
 * peer groups incrementally along a monotonically advancing row position, and
 * [[GroupBoundOrdering]], which turns two such cursors into a frame bound.
 */
class BoundOrderingSuite extends SparkFunSuite {

  private val ordering: Ordering[InternalRow] = Ordering.by[InternalRow, Int](_.getInt(0))

  private def row(value: Int): UnsafeRow = {
    val r = new UnsafeRow(1)
    r.pointTo(new Array[Byte](64), 16)
    r.setInt(0, value)
    r
  }

  /**
   * Walks `values` through a cursor one position at a time and returns the group number
   * reported at each position.
   */
  private def walk(values: Seq[Int]): Seq[Int] = {
    val cursor = new PeerGroupCursor(ordering, IdentityProjection)
    values.zipWithIndex.map { case (v, i) => cursor.groupOf(row(v), i) }
  }

  test("empty partition never advances the cursor") {
    assert(walk(Seq.empty) === Seq.empty)
  }

  test("single row is a single group") {
    assert(walk(Seq(5)) === Seq(0))
  }

  test("all rows in one group") {
    assert(walk(Seq.fill(50)(7)) === Seq.fill(50)(0))
  }

  test("every row is its own group") {
    assert(walk(0 until 50) === (0 until 50))
  }

  test("mixed group sizes") {
    // 80 singleton groups (values 0 until 80) followed by 3 groups of 20 rows each.
    val singletons = 0 until 80
    val repeated = (1000 until 1003).flatMap(v => Seq.fill(20)(v))
    val expected = (0 until 80) ++ Seq.fill(20)(80) ++ Seq.fill(20)(81) ++ Seq.fill(20)(82)
    assert(walk(singletons ++ repeated) === expected)
  }

  test("re-asking for the current position does not advance the cursor") {
    val cursor = new PeerGroupCursor(ordering, IdentityProjection)
    assert(cursor.groupOf(row(1), 0) === 0)
    // The row is ignored while the position is unchanged: frames pass whichever row they
    // have on hand when an edge has not moved.
    assert(cursor.groupOf(row(99), 0) === 0)
    assert(cursor.groupOf(row(99), 0) === 0)
    assert(cursor.groupOf(row(1), 1) === 0)
    assert(cursor.groupOf(row(2), 2) === 1)
    assert(cursor.groupOf(row(7), 2) === 1)
  }

  test("skipping a position fails loudly instead of miscounting groups") {
    val cursor = new PeerGroupCursor(ordering, IdentityProjection)
    assert(cursor.groupOf(row(1), 0) === 0)
    val e = intercept[AssertionError] {
      cursor.groupOf(row(3), 2)
    }
    assert(e.getMessage.contains("cannot skip from position 0 to 2"))
  }

  test("reset rewinds the cursor for a new partition") {
    val cursor = new PeerGroupCursor(ordering, IdentityProjection)
    assert(cursor.groupOf(row(1), 0) === 0)
    assert(cursor.groupOf(row(2), 1) === 1)
    cursor.reset()
    // Without the rewind, position 0 would be a skip backwards and the group count would carry
    // over from the previous partition.
    assert(cursor.groupOf(row(2), 0) === 0)
    assert(cursor.groupOf(row(2), 1) === 0)
    assert(cursor.groupOf(row(9), 2) === 1)
  }

  test("cursor retains a copy of the group representative") {
    // Iterators over a spilled partition hand out one mutable row buffer over and over.
    val cursor = new PeerGroupCursor(ordering, IdentityProjection)
    val buffer = row(0)
    val groups = Seq(1, 1, 2, 2, 2, 3).zipWithIndex.map { case (v, i) =>
      buffer.setInt(0, v)
      cursor.groupOf(buffer, i)
    }
    assert(groups === Seq(0, 0, 1, 1, 1, 2))
  }

  test("peer cursors retain only projected keys when input and projection buffers are reused") {
    val projection = UnsafeProjection.create(Seq(BoundReference(1, IntegerType, nullable = false)))
    val keyOrdering = new Ordering[InternalRow] {
      override def compare(left: InternalRow, right: InternalRow): Int = {
        Seq(left, right).foreach { key =>
          assert(key.numFields == 1)
          assert(key.asInstanceOf[UnsafeRow].getSizeInBytes == 16)
        }
        ordering.compare(left, right)
      }
    }
    val first = new PeerGroupCursor(keyOrdering, projection)
    val second = new PeerGroupCursor(keyOrdering, projection)
    val input = InternalRow(new Array[Byte](64 * 1024), 1, 10)
    assert(first.groupOf(input, 0) == 0)
    input.setInt(1, 9)
    assert(second.groupOf(input, 0) == 0)
    input.setInt(1, 1)
    input.setInt(2, 20)
    assert(first.groupOf(input, 1) == 0)
    input.setInt(1, 2)
    assert(first.groupOf(input, 2) == 1)
    input.setInt(1, 9)
    assert(second.groupOf(input, 1) == 0)
  }

  /**
   * Drives a bound the way a frame does: `edge` gives the input position (this bound's frame
   * edge) for each output position, and both positions are backed by `values`.
   */
  private def compareAlong(
      bound: GroupBoundOrdering,
      values: Seq[Int],
      edge: Seq[Int]): Seq[Int] = {
    edge.zipWithIndex.map { case (inputIndex, outputIndex) =>
      bound.compare(row(values(inputIndex)), inputIndex, row(values(outputIndex)), outputIndex)
    }
  }

  test("single bound compares peer-group distance against its offset") {
    // Peer groups: {0,1} -> 0, {2,3} -> 1, {4} -> 2.
    val values = Seq(1, 1, 2, 2, 3)
    // Edge parked on the output row itself: distance is always 0, so the comparison is the
    // negated offset.
    val currentRowEdge = values.indices
    assert(compareAlong(
      GroupBoundOrdering(ordering, IdentityProjection, 0), values, currentRowEdge) ===
      Seq(0, 0, 0, 0, 0))
    assert(compareAlong(
      GroupBoundOrdering(ordering, IdentityProjection, -1), values, currentRowEdge) ===
      Seq(1, 1, 1, 1, 1))
    assert(compareAlong(
      GroupBoundOrdering(ordering, IdentityProjection, 2), values, currentRowEdge) ===
      Seq(-2, -2, -2, -2, -2))
  }

  test("single bound tracks an edge that advances independently of the output row") {
    val values = Seq(1, 1, 2, 2, 3)
    // Edge trailing one row behind the output row: groups 0, 0, 0, 1, 1 against output groups
    // 0, 0, 1, 1, 2, with a 1 PRECEDING offset.
    val edge = Seq(0, 0, 1, 2, 3)
    assert(compareAlong(GroupBoundOrdering(ordering, IdentityProjection, -1), values, edge) ===
      Seq(1, 1, 0, 1, 0))
  }

  test("prepare rewinds both cursors between partitions") {
    val bound = GroupBoundOrdering(ordering, IdentityProjection, 0)
    val partition1 = Seq(1, 1, 2)
    assert(compareAlong(bound, partition1, partition1.indices) === Seq(0, 0, 0))
    bound.prepare()
    val partition2 = Seq(5, 6, 7, 8)
    assert(compareAlong(bound, partition2, partition2.indices) === Seq(0, 0, 0, 0))
  }

  test("paired bounds share the output cursor so neither has to see every output row") {
    // GROUPS BETWEEN 3 PRECEDING AND 2 PRECEDING over five singleton groups. The upper bound is
    // consulted for every output row; the lower bound is not consulted until the frame first
    // becomes non-empty, and must still see the correct output group when it is.
    val values = Seq(10, 20, 30, 40, 50)
    val (lower, upper) = GroupBoundOrdering.paired(
      ordering, IdentityProjection, lowerOffset = -3, upperOffset = -2)

    def upperAt(index: Int, edge: Int): Int =
      upper.compare(row(values(edge)), edge, row(values(index)), index)
    def lowerAt(index: Int, edge: Int): Int =
      lower.compare(row(values(edge)), edge, row(values(index)), index)

    // Output rows 0 and 1: the upper edge cannot leave position 0; the lower bound is not asked.
    assert(upperAt(0, 0) === 2)
    assert(upperAt(1, 0) === 1)
    // Output row 2: the lower bound is consulted for the first time, against output group 2,
    // which only the shared cursor knows.
    assert(upperAt(2, 0) === 0)
    assert(upperAt(2, 1) === 1)
    assert(lowerAt(2, 0) === 1)
    // Output row 3: frame is groups 0..1.
    assert(upperAt(3, 1) === 0)
    assert(upperAt(3, 2) === 1)
    assert(lowerAt(3, 0) === 0)
  }

  test("prepare on either half of a pair rewinds the shared output cursor") {
    val values = Seq(1, 2, 3)
    val (lower, upper) = GroupBoundOrdering.paired(
      ordering, IdentityProjection, lowerOffset = 0, upperOffset = 0)
    assert(compareAlong(upper, values, values.indices) === Seq(0, 0, 0))
    // Only the upper half is re-prepared; the reset must still take effect for both.
    upper.prepare()
    assert(compareAlong(lower, values, values.indices) === Seq(0, 0, 0))
  }
}
