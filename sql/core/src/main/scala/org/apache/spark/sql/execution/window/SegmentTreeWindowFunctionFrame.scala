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

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, FrameType, MutableProjection, RowFrame, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.internal.SQLConf

/**
 * Moving-frame window function frame backed by [[WindowSegmentTree]]. Produces
 * the same outputs as [[SlidingWindowFunctionFrame]] for RowFrame moving
 * frames whose aggregate functions are all [[DeclarativeAggregate]] with
 * no FILTER/DISTINCT. For partitions below
 * `spark.sql.window.segmentTree.minPartitionRows`, delegates to a wrapped
 * [[SlidingWindowFunctionFrame]].
 *
 * See `docs/frame-integration-contract.md` Section 1 for the full contract.
 *
 * @note Not thread-safe.
 */
private[window] final class SegmentTreeWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    functions: Array[DeclarativeAggregate],
    inputSchema: Seq[Attribute],
    frameType: FrameType,
    lbound: BoundOrdering,
    ubound: BoundOrdering,
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    conf: SQLConf,
    maxCachedBlocks: Option[Int])
  extends WindowFunctionFrame with AutoCloseable {

  require(frameType == RowFrame,
    "SegmentTreeWindowFunctionFrame currently supports RowFrame only " +
      "(RangeFrame tracked in backlog.md#range-frame-support).")

  private[this] val fallback =
    new SlidingWindowFunctionFrame(target, processor, lbound, ubound)
  private[this] var tree: WindowSegmentTree = _
  private[this] var boundIter: Iterator[UnsafeRow] = _
  private[this] var nextRow: UnsafeRow = _
  private[this] var lowerBound: Int = 0
  private[this] var upperBound: Int = 0

  /** Test hook; the frame integration.5 wires this to a SQLMetric (see backlog). */
  private[window] var fallbackUsed: Boolean = false

  // Register close() once per frame instance so the tree's block cache is
  // released when the task completes. Keeping the registration here (vs.
  // inside the factory closure) avoids duplicate listeners when the factory
  // is invoked multiple times per task.
  {
    val tc = TaskContext.get()
    if (tc != null) {
      tc.addTaskCompletionListener[Unit](_ => close())
    }
  }

  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    // `prepare` is idempotent: the partition evaluator calls it once per
    // partition, but a second call (rebuild) must not reuse stale state
    // from the previous partition. Clear the tree and advance-loop cursor
    // fields (boundIter / nextRow) before rebuilding.
    if (tree != null) {
      tree.close()
      tree = null
    }
    boundIter = null
    nextRow = null
    lowerBound = 0
    upperBound = 0
    if (rows.length < conf.windowSegmentTreeMinPartitionRows) {
      fallbackUsed = true
      fallback.prepare(rows)
      return
    }
    fallbackUsed = false
    tree = new WindowSegmentTree(
      functions,
      inputSchema,
      newMutableProjection,
      fanout = conf.windowSegmentTreeFanout,
      blockSize = conf.windowSegmentTreeBlockSize,
      maxCachedBlocks = maxCachedBlocks)
    // Build first (drains rows into the tree's internal row array), then
    // open a fresh iterator for per-row bound advancement.
    tree.build(rows.generateIterator())
    boundIter = rows.generateIterator()
    nextRow = WindowFunctionFrame.getNextOrNull(boundIter)
  }

  override def write(index: Int, current: InternalRow): Unit = {
    if (fallbackUsed) {
      fallback.write(index, current)
      return
    }
    var boundsChanged = index == 0

    // advance loop: extend upperBound; if a candidate is already below the
    // lower bound, advance lowerBound in lock-step to preserve invariant A
    // (0 <= lowerBound <= upperBound <= tree.size).
    while (nextRow != null &&
        ubound.compare(nextRow, upperBound, current, index) <= 0) {
      if (lbound.compare(nextRow, lowerBound, current, index) < 0) {
        lowerBound += 1
      }
      nextRow = WindowFunctionFrame.getNextOrNull(boundIter)
      upperBound += 1
      boundsChanged = true
    }
    // drop loop: advance lowerBound to the frame's left edge. RowFrame's
    // `lbound.compare` is pure index arithmetic so the input row is not
    // read; the `lowerBound < upperBound` guard is the second defense.
    while (lowerBound < upperBound &&
        lbound.compare(null, lowerBound, current, index) < 0) {
      lowerBound += 1
      boundsChanged = true
    }

    if (boundsChanged) {
      tree.queryInto(lowerBound, upperBound, processor, target)
    }
  }

  override def currentLowerBound(): Int =
    if (fallbackUsed) fallback.currentLowerBound() else lowerBound

  override def currentUpperBound(): Int =
    if (fallbackUsed) fallback.currentUpperBound() else upperBound

  override def close(): Unit = {
    if (tree != null) {
      tree.close()
      tree = null
    }
  }
}
