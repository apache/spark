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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Projection

/**
 * Function for comparing boundary values.
 */
private[window] abstract class BoundOrdering {
  def compare(inputRow: InternalRow, inputIndex: Int, outputRow: InternalRow, outputIndex: Int): Int

  /**
   * Prepares state for a partition before any calls to [[compare]].
   */
  def prepare(): Unit = {}
}

/**
 * Compare the input index to the bound of the output index.
 */
private[window] final case class RowBoundOrdering(offset: Int) extends BoundOrdering {
  override def compare(
      inputRow: InternalRow,
      inputIndex: Int,
      outputRow: InternalRow,
      outputIndex: Int): Int =
    inputIndex - (outputIndex + offset)
}

/**
 * Compare the value of the input index to the value bound of the output index.
 */
private[window] final case class RangeBoundOrdering(
    ordering: Ordering[InternalRow],
    current: Projection,
    bound: Projection)
  extends BoundOrdering {

  override def compare(
      inputRow: InternalRow,
      inputIndex: Int,
      outputRow: InternalRow,
      outputIndex: Int): Int =
    ordering.compare(current(inputRow), bound(outputRow))
}

/**
 * Counts peer groups -- maximal runs of rows equal under `ordering` -- along a position that
 * advances monotonically through a partition. Only the current group's ordering key is retained,
 * so state is O(1) rather than O(#groups).
 */
private[window] final class PeerGroupCursor(
    ordering: Ordering[InternalRow],
    projection: Projection) {

  /** Ordering key of the current peer group; null before the first row. */
  private[this] var groupKey: InternalRow = null
  private[this] var position: Int = -1
  private[this] var group: Int = 0

  /** Rewinds the cursor for a new partition. */
  def reset(): Unit = {
    groupKey = null
    position = -1
    group = 0
  }

  /**
   * Returns the peer-group number of `index`, whose row is `row`. `index` must be the current
   * position or the one immediately after it.
   */
  def groupOf(row: InternalRow, index: Int): Int = {
    if (index > position) {
      assert(index == position + 1,
        s"peer-group cursor cannot skip from position $position to $index")
      // Copy only the ordering key: both the projection and spilled iterators reuse buffers.
      val key = projection(row)
      if (groupKey == null) {
        groupKey = key.copy()
      } else if (ordering.compare(groupKey, key) != 0) {
        group += 1
        groupKey = key.copy()
      }
      position = index
    }
    group
  }
}

/**
 * Compares the peer-group indices of input and output rows, adjusted by `offset` groups.
 * Negative offsets represent `PRECEDING` and positive offsets represent `FOLLOWING`.
 *
 * Group numbers come from two [[PeerGroupCursor]]s riding positions the frame already visits:
 * the input cursor follows this bound's frame edge, the output cursor the output row. Nothing
 * proportional to the partition is materialized.
 *
 * Neither bound of a two-sided frame sees every output row on its own -- a frame consults a
 * bound only while its edge can still move, so under `GROUPS BETWEEN 3 PRECEDING AND 2
 * PRECEDING` the lower bound goes unconsulted until the frame first becomes non-empty -- so
 * the two share an output cursor and are built together by [[GroupBoundOrdering.paired]].
 *
 * Sharing is safe only because no output index is skipped, which [[PeerGroupCursor]] asserts
 * on. Edges advance monotonically and stall only at the end of the partition, so each output
 * index either has some bound consulted -- carrying the shared cursor forward one step -- or
 * finds both edges already at the end, after which no bound is consulted again.
 */
private[window] final class GroupBoundOrdering private (
    ordering: Ordering[InternalRow],
    projection: Projection,
    outputCursor: PeerGroupCursor,
    offset: Int)
  extends BoundOrdering {

  private[this] val inputCursor = new PeerGroupCursor(ordering, projection)

  override def prepare(): Unit = {
    inputCursor.reset()
    outputCursor.reset()
  }

  override def compare(
      inputRow: InternalRow,
      inputIndex: Int,
      outputRow: InternalRow,
      outputIndex: Int): Int = {
    val inputGroup = inputCursor.groupOf(inputRow, inputIndex)
    val outputGroup = outputCursor.groupOf(outputRow, outputIndex)
    // Any wrap in `outputGroup + offset` cancels, as in `RowBoundOrdering`: the frame only
    // compares an edge against a bound that can reach it.
    inputGroup - (outputGroup + offset)
  }
}

private[window] object GroupBoundOrdering {

  // Cursors may share a projection: calls are sequential, and each retained key is copied
  // before another cursor can reuse the projection's result buffer.

  /**
   * Creates the single bound of a one-sided frame, which owns its output cursor: being the only
   * bound, it is consulted on every output row.
   */
  def apply(
      ordering: Ordering[InternalRow],
      projection: Projection,
      offset: Int): GroupBoundOrdering =
    new GroupBoundOrdering(ordering, projection, new PeerGroupCursor(ordering, projection), offset)

  /** Creates both bounds of a two-sided frame, sharing an output cursor. */
  def paired(
      ordering: Ordering[InternalRow],
      projection: Projection,
      lowerOffset: Int,
      upperOffset: Int): (GroupBoundOrdering, GroupBoundOrdering) = {
    val outputCursor = new PeerGroupCursor(ordering, projection)
    (new GroupBoundOrdering(ordering, projection, outputCursor, lowerOffset),
      new GroupBoundOrdering(ordering, projection, outputCursor, upperOffset))
  }
}
