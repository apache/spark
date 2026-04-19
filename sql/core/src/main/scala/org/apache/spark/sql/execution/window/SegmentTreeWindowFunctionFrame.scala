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
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, FrameType, MutableProjection, RangeFrame, RowFrame, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf

/**
 * Moving-frame window function frame backed by [[WindowSegmentTree]]. Produces
 * the same outputs as [[SlidingWindowFunctionFrame]] for RowFrame or
 * single-column RangeFrame moving frames whose aggregate functions are all
 * [[DeclarativeAggregate]] with no FILTER/DISTINCT. For partitions below
 * `spark.sql.window.segmentTree.minPartitionRows`, delegates to a wrapped
 * [[SlidingWindowFunctionFrame]].
 *
 * See `docs/frame-integration-contract.md` Section 1 for the full contract.
 *
 * RANGE support (RANGE frame support): when `frameType == RangeFrame`, the per-row
 * driver uses two forward-only cursors (`lowerIter` / `upperIter`) over the
 * materialized `rowArray` to advance `lowerBound` / `upperBound`. Endpoints
 * are monotone per partition (SQL RANGE semantics + sorted input), so total
 * cursor work is O(n). The segment tree then answers `[lowerBound,
 * upperBound)` in O(log n).
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
    maxCachedBlocks: Option[Int],
    taskMemoryManager: TaskMemoryManager,
    numSegmentTreeFrames: Option[SQLMetric] = None,
    numSegmentTreeFallbackFrames: Option[SQLMetric] = None)
  extends WindowFunctionFrame with AutoCloseable {

  require(frameType == RowFrame || frameType == RangeFrame,
    s"SegmentTreeWindowFunctionFrame supports RowFrame or RangeFrame, got $frameType")

  private[this] val fallback =
    new SlidingWindowFunctionFrame(target, processor, lbound, ubound)
  private[this] var tree: WindowSegmentTree = _

  // ---- RowFrame-only driver state ----
  // `boundIter` advances `upperBound` one row at a time (sliding admit loop);
  // the lower bound is inferred from pure index arithmetic under RowFrame.
  private[this] var boundIter: Iterator[UnsafeRow] = _
  private[this] var nextRow: UnsafeRow = _

  // ---- RangeFrame-only driver state ----
  // Two cursors over `rowArray`; `lowerRow` / `upperRow` hold the currently-
  // buffered head of each cursor (pre-fetched in `prepare` -- fix so
  // `RangeBoundOrdering.compare` is never called with a null row on round 0).
  //
  // Spill-safety invariant: when `rowArray` spills, its iterator reuses a
  // single `UnsafeRow` object whose pointer is rebound on each `next()`.
  // That is tolerated here because the cursor is **read-before-advance**:
  // each `writeRange` loop iteration reads `lowerRow`/`upperRow` for
  // comparison before calling `getNextOrNull(...)`. Between `write()`
  // calls the pointer's content is stable (this frame is the sole consumer
  // of each iterator). DO NOT cache a historical row into a separate field
  // without an explicit `.copy()`; the shared reusable UnsafeRow would
  // silently mutate.
  private[this] var lowerIter: Iterator[UnsafeRow] = _
  private[this] var upperIter: Iterator[UnsafeRow] = _
  private[this] var lowerRow: UnsafeRow = _
  private[this] var upperRow: UnsafeRow = _

  // Shared endpoints: monotone across `write()` calls within a partition.
  private[this] var lowerBound: Int = 0
  private[this] var upperBound: Int = 0

  /**
   * Runtime dispatch flag: when `true`, `write()`, `currentLowerBound()`,
   * and `currentUpperBound()` delegate to the wrapped
   * [[SlidingWindowFunctionFrame]] (small-partition path). Set by
   * `prepare()` based on partition size vs.
   * `spark.sql.window.segmentTree.minPartitionRows`.
   */
  private[window] var fallbackUsed: Boolean = false

  // Idempotency guard for the metric counters. `prepare()` is allowed to be
  // called more than once on the same partition (see comment in prepare()),
  // so we key on (identityHashCode(rows), rows.length) to dedupe repeat
  // invocations without requiring an explicit reset hook. See
  // `the PR description (SQLMetrics exposure)` section 2.5.
  private[this] var lastPreparedKey: Long = -1L

  // Register close() once per frame instance so the tree's block cache and
  // any open row-array iterators are released when the task completes.
  // Keeping the registration here (vs. inside the factory closure) avoids
  // duplicate listeners when the factory is invoked multiple times per task.
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
    // fields before rebuilding.
    if (tree != null) {
      tree.close()
      tree = null
    }
    closeIters()
    nextRow = null
    lowerRow = null
    upperRow = null
    lowerBound = 0
    upperBound = 0
    val key = (System.identityHashCode(rows).toLong << 32) | (rows.length & 0xffffffffL)
    val alreadyCounted = key == lastPreparedKey
    if (rows.length < conf.windowSegmentTreeMinPartitionRows) {
      fallbackUsed = true
      fallback.prepare(rows)
      if (!alreadyCounted) numSegmentTreeFallbackFrames.foreach(_ += 1)
      lastPreparedKey = key
      return
    }
    fallbackUsed = false
    tree = new WindowSegmentTree(
      functions,
      inputSchema,
      newMutableProjection,
      fanout = conf.windowSegmentTreeFanout,
      blockSize = conf.windowSegmentTreeBlockSize,
      maxCachedBlocks = maxCachedBlocks,
      taskMemoryManager = taskMemoryManager)
    // Build first (drains rows into the tree's internal row array), then
    // open fresh iterator(s) for per-row bound advancement.
    tree.build(rows.generateIterator())
    // Count only on the successful segtree path: if `tree.build` throws
    // (e.g. OOM during block allocation), the counter is not bumped.
    if (!alreadyCounted) numSegmentTreeFrames.foreach(_ += 1)
    lastPreparedKey = key
    frameType match {
      case RowFrame =>
        boundIter = rows.generateIterator()
        nextRow = WindowFunctionFrame.getNextOrNull(boundIter)
      case RangeFrame =>
        lowerIter = rows.generateIterator()
        upperIter = rows.generateIterator()
        // Pre-seed cursor heads so `RangeBoundOrdering.compare` (which
        // projects the input row to a scalar) never dereferences null on
        // round 0. Either may legitimately be null if `rows` is
        // empty; the advance loops' `!= null` / `< upperBound` guards
        // handle that.
        lowerRow = WindowFunctionFrame.getNextOrNull(lowerIter)
        upperRow = WindowFunctionFrame.getNextOrNull(upperIter)
    }
  }

  override def write(index: Int, current: InternalRow): Unit = {
    if (fallbackUsed) {
      fallback.write(index, current)
      return
    }
    frameType match {
      case RowFrame => writeRow(index, current)
      case RangeFrame => writeRange(index, current)
    }
  }

  private def writeRow(index: Int, current: InternalRow): Unit = {
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

  private def writeRange(index: Int, current: InternalRow): Unit = {
    var boundsChanged = index == 0

    // admit loop (upper edge): advance upperBound while the next row's
    // order-key value is <= the upper-bound-projected value of `current`.
    // `RangeBoundOrdering.compare` ignores its index arguments; we pass
    // `upperBound` purely for API symmetry with RowBoundOrdering.
    while (upperRow != null &&
        ubound.compare(upperRow, upperBound, current, index) <= 0) {
      upperBound += 1
      upperRow = WindowFunctionFrame.getNextOrNull(upperIter)
      boundsChanged = true
    }

    // drop loop (lower edge): strict `< 0`. Mirrors
    // `SlidingWindowFunctionFrame.write` line ~471, guarded by
    // `lowerBound < upperBound` so drop can never overrun admit --
    // this also ensures `lowerRow` is non-null when reached (if the
    // iterator exhausts, `lowerBound` has already caught up to
    // `numRows >= upperBound`, so the guard stops the loop first).
    while (lowerBound < upperBound &&
        lbound.compare(lowerRow, lowerBound, current, index) < 0) {
      lowerBound += 1
      lowerRow = WindowFunctionFrame.getNextOrNull(lowerIter)
      boundsChanged = true
    }

    if (boundsChanged) {
      // Empty frame (`lowerBound == upperBound`) is handled inside
      // `queryInto`, which initializes the processor and emits the
      // aggregate's zero/identity value -- same behavior as
      // `SlidingWindowFunctionFrame` with an empty buffer.
      tree.queryInto(lowerBound, upperBound, processor, target)
    }
  }

  override def currentLowerBound(): Int =
    if (fallbackUsed) fallback.currentLowerBound() else lowerBound

  override def currentUpperBound(): Int =
    if (fallbackUsed) fallback.currentUpperBound() else upperBound

  /**
   * Drop references to open rowArray iterators. Idempotent.
   *
   * Note: `ExternalAppendOnlyUnsafeRowArrayIterator.closeIfNeeded()` is
   * `protected`, so we cannot invoke it from here. Spark's own
   * `SlidingWindowFunctionFrame` also does not close its iterator; the
   * backing `UnsafeExternalSorter` is released by the enclosing
   * `WindowExec`'s `TaskCompletionListener`. Mirror that contract.
   */
  private def closeIters(): Unit = {
    boundIter = null
    lowerIter = null
    upperIter = null
  }

  override def close(): Unit = {
    if (tree != null) {
      tree.close()
      tree = null
    }
    closeIters()
  }
}
