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

import java.util.{LinkedHashMap => JLinkedHashMap, Map => JMap}

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Count, DeclarativeAggregate, Max, Min, StddevPop, StddevSamp, Sum, VariancePop, VarianceSamp}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.ArrayImplicits._

/**
 * Block-chunked segment tree for range aggregate queries over window partitions.
 *
 * === Why a segment tree ===
 *
 * For moving-frame window aggregates over a sorted partition, the classic
 * alternatives are:
 *
 *   - '''Add/remove with inverse''' (a.k.a. SlidingWindowFunctionFrame's O(1)
 *     path, and the approach SPARK-36844 explored): maintain a single running
 *     buffer, `add` on the advancing edge, `remove` on the retreating edge.
 *     Only works for '''invertible''' aggregates (SUM, COUNT, AVG) -- MIN/MAX
 *     have no inverse. Numerically problematic for floating-point STDDEV/VAR/
 *     FP-SUM: error '''accumulates across the whole partition''' and drifts
 *     monotonically over long partitions.
 *   - '''Monotonic deque''': O(1) amortized for MIN/MAX only; does not
 *     generalize.
 *
 * Segment-tree merges give up O(1) per row for O(log W) per row but preserve
 * two load-bearing properties:
 *
 *   1. '''No inverse required''' -- any aggregate whose `mergeExpressions`
 *      is associative works, including MIN/MAX and other non-invertible cases.
 *      The segment-tree path is therefore the single moving-frame strategy
 *      applicable to MIN/MAX at scale.
 *   2. '''Bounded FP error per query''' -- each output row performs
 *      O(log W) merges over independent block pre-aggregates with no
 *      cumulative state carried across rows. Rounding error is bounded per
 *      row rather than accumulating over the partition. This is the reason to
 *      prefer segtree for STDDEV/VAR/FP-SUM even though an inverse-based O(1)
 *      scheme looks cheaper on paper.
 *
 * Any future author considering an inverse-based optimization for the
 * aggregates in [[EligibleAggregates]] should verify both properties still
 * hold before replacing this path.
 *
 * === Layout ===
 *
 * The data layer references the caller-owned `ExternalAppendOnlyUnsafeRowArray`
 * (spillable) passed to `build`; the tree does not copy input rows. Each block
 * materializes its own small segment tree (levels 0..h). Internal nodes are
 * cached in an LRU keyed by block index; block root aggregates (block
 * pre-aggregates) stay resident for all blocks.
 * Leaves are materialized inside the per-block internal node arrays; a
 * future revision may drop leaf materialization and recompute leaves from
 * the spillable array on demand to reduce memory at the cost of CPU.
 *
 * === Memory accounting invariants ===
 *
 *  - I1: `SegTreeSpiller.spill()` must not call `acquireMemory` on its own
 *        [[MemoryConsumer]]. Doing so would deadlock against TMM's
 *        consumer-priority sort. All acquires happen on the hot path
 *        ([[ensureBlockLevels]]).
 *  - I2: `SegTreeSpiller.spill(_, trigger)` returns 0 when `trigger eq this`
 *        (self-trigger short-circuit) to prevent re-entrant eviction.
 *  - I3: The LRU's `removeEldestEntry` is disabled; eviction is driven
 *        explicitly from [[ensureBlockLevels]] (capacity overflow) or
 *        [[SegTreeSpiller.spill]] (TMM pressure).
 *  - I4: Every successful [[acquireBlockMemory]] is paired with exactly one
 *        [[releaseBlockMemory]] -- on eviction, on [[close]], and on
 *        exception paths where block construction fails. [[close]] is
 *        idempotent.
 *  - I5: Bytes accounted per block are a conservative upper bound
 *        (`cachedSlotsPerBlock * bufferWidthBytes`): assumes a full block
 *        and per-field 16 byte cost. Tail blocks and cheaper types leave
 *        headroom rather than under-report to TMM.
 *  - I8: If the underlying `rowArray` has already spilled to disk,
 *        `SegTreeSpiller.spill` returns 0 (rebuild after eviction would
 *        O(blockStart)-scan the spill file); TMM falls through to another
 *        consumer.
 *
 * @note Instances are not thread-safe.
 */
private[window] class WindowSegmentTree(
    functions: Array[DeclarativeAggregate],
    inputSchema: Seq[Attribute],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    fanout: Int = WindowSegmentTree.DefaultFanout,
    blockSize: Int = WindowSegmentTree.DefaultBlockSize,
    maxCachedBlocks: Option[Int] = None,
    taskMemoryManager: TaskMemoryManager = null)
  extends AutoCloseable {

  require(fanout >= 2, s"fanout must be >= 2, got $fanout")
  require(blockSize >= 1, s"blockSize must be >= 1, got $blockSize")
  require(functions.nonEmpty, "WindowSegmentTree requires at least one aggregate function")
  maxCachedBlocks.foreach { n =>
    require(n >= 1, s"maxCachedBlocks must be >= 1 when specified, got $n")
  }
  require(taskMemoryManager != null,
    "WindowSegmentTree requires a non-null TaskMemoryManager; " +
      "in tests use `new TaskMemoryManager(new TestMemoryManager(conf), 0)`")

  // ---------- Schemas & projections ----------

  private val bufferAttrs: Seq[AttributeReference] =
    functions.flatMap(_.aggBufferAttributes).toImmutableArraySeq
  private val rightAttrs: Seq[AttributeReference] =
    functions.flatMap(_.inputAggBufferAttributes).toImmutableArraySeq
  private val bufferDataTypes: IndexedSeq[DataType] =
    bufferAttrs.map(_.dataType).toIndexedSeq

  private val initialValues: Seq[Expression] = functions.flatMap(_.initialValues).toIndexedSeq
  private val updateExpressions: Seq[Expression] =
    functions.flatMap(_.updateExpressions).toIndexedSeq
  private val mergeExpressions: Seq[Expression] =
    functions.flatMap(_.mergeExpressions).toIndexedSeq

  private[this] val initProj: MutableProjection = newMutableProjection(initialValues, Nil)
  private[this] val updateProj: MutableProjection =
    newMutableProjection(updateExpressions, bufferAttrs ++ inputSchema)
  private[this] val mergeProj: MutableProjection =
    newMutableProjection(mergeExpressions, bufferAttrs ++ rightAttrs)

  private[this] val joinedRow = new JoinedRow()

  // ---------- State ----------

  private var numRows: Int = 0
  private var numBlocks: Int = 0
  private var rowArray: ExternalAppendOnlyUnsafeRowArray = _
  private var closed: Boolean = false

  /** Always-resident per-block root aggregates. `blockAggregates(i)` =
   *  merged buffer over all rows in block i. */
  private var blockAggregates: Array[InternalRow] = Array.empty

  /** Rough byte width of one aggregate buffer row. Chosen at 16 B/field as a
   *  conservative heap-overhead-aware lower bound for a
   *  `SpecificInternalRow` slot: primitive `MutableValue` is 8 B, boxed
   *  references and object headers push the effective footprint higher.
   *  Tighter per-type sizing (real boxing cost, variable-length fields) is
   *  intentionally out of scope here; TaskMemoryManager remains the hard
   *  backstop via spill / OOM. */
  private val bufferWidthBytes: Long = {
    val bytesPerField = 16L
    math.max(1L, bufferDataTypes.size.toLong * bytesPerField)
  }

  /** Number of aggregate-buffer slots cached per block (see I5 in class doc).
   *
   *  Invariant: equals `sum over levels L of levels(L).length` for any
   *  block materialized by [[buildBlockLevels]]. Level 0 holds `blockSize`
   *  leaf buffers and each subsequent level holds `ceil(prev / fanout)`
   *  parent buffers until a single root remains. The iterative ceiling
   *  matches the allocation in [[buildBlockLevels]] for every
   *  `(blockSize, fanout)` pair, including non-power-of-`fanout` cases.
   *  For `blockSize == 1` the block is a single leaf with no parent
   *  levels, so this returns 1. */
  private val cachedSlotsPerBlock: Long = {
    var n = blockSize.toLong
    var sum = n
    while (n > 1L) {
      n = (n + fanout - 1) / fanout
      sum += n
    }
    sum
  }

  /** Bytes accounted per cached block (see I5 in class doc). Conservative: assumes
   *  every block is full; tail block (`numRows % blockSize != 0`) will
   *  hold fewer leaves, giving a small headroom. */
  private[this] val blockBytes: Long =
    math.max(1L, cachedSlotsPerBlock * bufferWidthBytes)

  /** `spans(L)` = number of leaves covered by a single node at level L. Depends
   *  only on fanout + blockSize, so precomputed once. */
  private val spans: Array[Int] = {
    val maxLevel = {
      var lvl = 0
      var span = 1L
      while (span < blockSize) { span *= fanout; lvl += 1 }
      lvl
    }
    val arr = new Array[Int](maxLevel + 1)
    var s = 1L
    var i = 0
    while (i <= maxLevel) {
      arr(i) = if (s > Int.MaxValue) Int.MaxValue else s.toInt
      s *= fanout
      i += 1
    }
    arr
  }

  /** LRU cache of per-block internal node arrays. Key = blockIdx.
   *  Value = `Array[Array[InternalRow]]` with levels(0..h). Auto-eviction via
   *  `removeEldestEntry` is disabled (see I3 in class doc) -- eviction is driven
   *  explicitly from [[ensureBlockLevels]] (capacity overflow) or
   *  [[SegTreeSpiller.spill]] (TMM pressure). Each cache entry maps 1:1 to
   *  one [[acquireBlockMemory]] accounting. Callers (e.g.
   *  `SegmentTreeWindowFunctionFrame`) should pass a W-aware
   *  value like `ceil(W / blockSize) + 2`. */
  private val blockLevelsCache: JLinkedHashMap[Integer, Array[Array[InternalRow]]] =
    new JLinkedHashMap[Integer, Array[Array[InternalRow]]](16, 0.75f, true) {
      override def removeEldestEntry(
          eldest: JMap.Entry[Integer, Array[Array[InternalRow]]]): Boolean = false
    }

  // ---------- Memory consumer ----------

  /**
   * Private MemoryConsumer tracking cached block levels under TMM.
   *
   * Heap accounting only (no Tungsten pages): uses
   * [[MemoryConsumer.acquireMemory]] / [[MemoryConsumer.freeMemory]]. The
   * [[MemoryConsumer]] base class records `used` via an `AtomicLong` when
   * we call these -- so TMM's consumer-priority sort in
   * `acquireExecutionMemory` sees our pressure accurately.
   *
   * @note `spill()` MUST NOT call `acquireMemory` (see I1 in class doc).
   */
  private final class SegTreeSpiller extends MemoryConsumer(
      taskMemoryManager,
      taskMemoryManager.pageSizeBytes(),
      taskMemoryManager.getTungstenMemoryMode()) {
    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      // Self-trigger short-circuit (I2): prevents re-entrant eviction when
      // our own acquireMemory() triggers TMM to poll us.
      if (trigger eq this) return 0L
      // rowArray spilled-to-disk detection (I8). If the rowArray has already
      // spilled, evicting our cache is counter-productive (rebuild would
      // O(blockStart)-scan the spill file). Return 0L to let TMM fall
      // through to the next consumer. The current check uses `spillSize > 0`
      // as the "has spilled" signal; a more precise check would consult
      // `UnsafeExternalSorter.getSpillWriters` state, which is not public.
      if (rowArray != null && rowArray.spillSize > 0) return 0L
      evictUntil(size)
    }
  }

  private[this] val spiller: SegTreeSpiller = new SegTreeSpiller

  // ---------- Public API ----------

  def size: Int = numRows

  /**
   * Build the tree against a caller-owned row array.
   *
   * Ownership contract: the tree holds a reference to `rows` for its lifetime
   * but does NOT own it -- the caller (typically
   * `WindowPartitionEvaluator.buffer`) is responsible for `clear()`/lifetime
   * management at partition boundaries. The tree's `close()` drops its
   * reference without mutating the array.
   *
   * Exception-safe: if aggregation throws, the previously built state (if
   * any) is preserved.
   */
  def build(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    // rows.length is Int (bounded by Int.MaxValue by design); keep the
    // check for clarity against any future widening of that contract.
    val n = rows.length
    if (n < 0) {
      throw SparkException.internalError(
        s"WindowSegmentTree cannot hold more than Int.MaxValue rows, got $n")
    }
    val nBlocks = if (n == 0) 0 else (n + blockSize - 1) / blockSize
    val newBlockAggs = computeBlockAggregates(rows, n, nBlocks)

    // Commit.
    rowArray = rows
    numRows = n
    numBlocks = nBlocks
    blockAggregates = newBlockAggs
    // Rebuild invalidates cached block levels; release all acquired memory
    // before dropping the cache entries (I4 pairing).
    releaseAllCachedBlocks()
  }

  /**
   * Query [lo, hi) and directly evaluate the result via `processor.evaluate`
   * into `target`. Uses an internal pre-allocated buffer so no per-call
   * allocation is needed.
   */
  private[window] def queryInto(
      lo: Int, hi: Int, processor: AggregateProcessor, target: InternalRow): Unit = {
    query(lo, hi, internalQueryBuffer)
    processor.evaluate(internalQueryBuffer, target)
  }

  private[this] val internalQueryBuffer: InternalRow = newBuffer()

  def query(lo: Int, hi: Int, outBuffer: InternalRow): Unit = {
    if (lo < 0 || hi > numRows || lo > hi) {
      throw SparkException.internalError(
        s"Invalid range [lo=$lo, hi=$hi) for size=$numRows")
    }
    // Reset outBuffer to identity only after bounds validation.
    initProj.target(outBuffer)(InternalRow.empty)
    if (lo == hi) return

    val blo = lo / blockSize
    val bhi = (hi - 1) / blockSize

    if (blo == bhi) {
      val blockStart = blo * blockSize
      mergeBlockRange(blo, lo - blockStart, hi - blockStart, outBuffer)
    } else {
      // left partial
      val loStart = blo * blockSize
      val loBlockRows = math.min(blockSize, numRows - loStart)
      mergeBlockRange(blo, lo - loStart, loBlockRows, outBuffer)
      // full blocks
      var b = blo + 1
      while (b < bhi) {
        mergeInto(outBuffer, blockAggregates(b))
        b += 1
      }
      // right partial
      val hiStart = bhi * blockSize
      mergeBlockRange(bhi, 0, hi - hiStart, outBuffer)
    }
  }

  /** Terminal: releases all state. Idempotent (I4). */
  override def close(): Unit = {
    if (closed) return
    // Free all cached-block accounting before dropping references.
    releaseAllCachedBlocks()
    closeRowArray()
    blockAggregates = Array.empty
    numRows = 0
    numBlocks = 0
    closed = true
  }

  // ---------- Test hooks (package-private) ----------

  private[window] def peekBlockCount: Int = numBlocks

  private[window] def testOnlySpiller(): MemoryConsumer = spiller

  /** Test-only accessor for the per-block memory accounting value. */
  private[window] def peekBlockBytes: Long = blockBytes

  /** NOTE: test-only; promotes block to MRU in the LRU cache as a side effect. */
  private[window] def peekLevelSize(blockIdx: Int, level: Int): Int = {
    val levels = ensureBlockLevels(blockIdx)
    levels(level).length
  }

  /** NOTE: test-only; promotes block to MRU in the LRU cache as a side effect. */
  private[window] def peekLevelCount(blockIdx: Int): Int = {
    val levels = ensureBlockLevels(blockIdx)
    levels.length
  }

  // ---------- Internals ----------

  private def computeBlockAggregates(
      array: ExternalAppendOnlyUnsafeRowArray,
      n: Int,
      nBlocks: Int): Array[InternalRow] = {
    if (n == 0) return Array.empty
    val result = new Array[InternalRow](nBlocks)
    val iter = array.generateIterator()
    var b = 0
    while (b < nBlocks) {
      val buf = newBuffer()
      initProj.target(buf)(InternalRow.empty)
      val start = b * blockSize
      val end = math.min(start + blockSize, n)
      var i = start
      while (i < end) {
        if (!iter.hasNext) {
          throw SparkException.internalError("rowArray iterator exhausted unexpectedly")
        }
        val row = iter.next()
        updateProj.target(buf)(joinedRow(buf, row))
        i += 1
      }
      result(b) = buf
      b += 1
    }
    result
  }

  /** Merge `src` buffer into `dst` buffer using mergeProj. */
  private def mergeInto(dst: InternalRow, src: InternalRow): Unit = {
    mergeProj.target(dst)(joinedRow(dst, src))
  }

  private def newBuffer(): InternalRow =
    new SpecificInternalRow(bufferDataTypes)

  /** Merge the given leaf range [lo, hi) inside `blockIdx` into `out`. */
  private def mergeBlockRange(
      blockIdx: Int, lo: Int, hi: Int, out: InternalRow): Unit = {
    if (lo >= hi) return
    val levels = ensureBlockLevels(blockIdx)
    val blockRows = levels(0).length
    val topLevel = levels.length - 1
    queryDescend(levels, blockRows, topLevel, 0, lo, hi, out)
  }

  /** Descend the (per-block) segment tree merging any node fully contained
   *  in [queryLo, queryHi) into `out`. A node at (level L, index idx) covers
   *  leaves `[idx * span, min((idx+1)*span, blockRows))` where span = F^L. */
  private def queryDescend(
      levels: Array[Array[InternalRow]],
      blockRows: Int,
      level: Int,
      idx: Int,
      queryLo: Int,
      queryHi: Int,
      out: InternalRow): Unit = {
    val span = spans(level)
    val nodeLo = idx * span
    val nodeHi = math.min(nodeLo + span, blockRows)
    if (queryLo >= nodeHi || queryHi <= nodeLo) return
    if (queryLo <= nodeLo && nodeHi <= queryHi) {
      mergeInto(out, levels(level)(idx))
      return
    }
    val childLevel = level - 1
    val childLevelSize = levels(childLevel).length
    var c = 0
    while (c < fanout) {
      val childIdx = idx * fanout + c
      if (childIdx < childLevelSize) {
        queryDescend(levels, blockRows, childLevel, childIdx, queryLo, queryHi, out)
      }
      c += 1
    }
  }

  /** Build (or fetch from LRU) the full per-block levels array.
   *  Protocol: acquire memory -> build -> cache. Eviction on capacity
   *  overflow or on TMM spill request. */
  private def ensureBlockLevels(blockIdx: Int): Array[Array[InternalRow]] = {
    val cached = blockLevelsCache.get(Integer.valueOf(blockIdx))
    if (cached != null) return cached

    // Enforce LRU capacity before building a new entry (I3).
    val cap = maxCachedBlocks.getOrElse(Int.MaxValue)
    while (blockLevelsCache.size() >= cap) {
      if (!evictEldest()) return throwCacheEvictFailed(blockIdx)
    }

    // Acquire memory accounting for this block before building. If TMM
    // can't grant blockBytes up-front, try one manual evict-and-retry cycle
    // before giving up.
    if (!acquireBlockMemory(blockIdx)) {
      if (!evictEldest() || !acquireBlockMemory(blockIdx)) {
        // scalastyle:off throwerror
        throw QueryExecutionErrors.cannotAcquireMemoryForWindowAggregateError(
          blockBytes, 0L)
        // scalastyle:on throwerror
      }
    }

    // If buildBlockLevels throws, release the memory we just acquired (I4).
    val levels =
      try buildBlockLevels(blockIdx)
      catch { case t: Throwable => releaseBlockMemory(); throw t }
    blockLevelsCache.put(Integer.valueOf(blockIdx), levels)
    levels
  }

  private def buildBlockLevels(blockIdx: Int): Array[Array[InternalRow]] = {
    val blockStart = blockIdx * blockSize
    val blockRows = math.min(blockSize, numRows - blockStart)

    // Level 0: one aggregate per row in the block.
    val leaves = new Array[InternalRow](blockRows)
    val iter = rowArray.generateIterator(blockStart)
    var i = 0
    while (i < blockRows) {
      if (!iter.hasNext) {
        throw SparkException.internalError(
          s"rowArray iterator exhausted at block $blockIdx row $i")
      }
      val row = iter.next()
      val buf = newBuffer()
      initProj.target(buf)(InternalRow.empty)
      updateProj.target(buf)(joinedRow(buf, row))
      leaves(i) = buf
      i += 1
    }

    val allLevels = mutable.ArrayBuffer[Array[InternalRow]](leaves)
    var prev = leaves
    while (prev.length > 1) {
      val parentCount = (prev.length + fanout - 1) / fanout
      val parents = new Array[InternalRow](parentCount)
      var p = 0
      while (p < parentCount) {
        val buf = newBuffer()
        initProj.target(buf)(InternalRow.empty)
        val childStart = p * fanout
        val childEnd = math.min(childStart + fanout, prev.length)
        var c = childStart
        while (c < childEnd) {
          mergeInto(buf, prev(c))
          c += 1
        }
        parents(p) = buf
        p += 1
      }
      allLevels += parents
      prev = parents
    }
    allLevels.toArray
  }

  private def throwCacheEvictFailed(blockIdx: Int): Nothing = {
    throw SparkException.internalError(
      s"LRU cache eviction failed for block $blockIdx (size=${blockLevelsCache.size})")
  }

  // ---------- Memory accounting helpers ----------

  /** Try to acquire `blockBytes` for one cached block. Returns true on full
   *  grant, false on partial (after rolling the partial grant back). Must
   *  not be called from within [[SegTreeSpiller.spill]] (I1). */
  private def acquireBlockMemory(blockIdx: Int): Boolean = {
    val granted = spiller.acquireMemory(blockBytes)
    if (granted < blockBytes) {
      if (granted > 0) spiller.freeMemory(granted)
      false
    } else {
      true
    }
  }

  /** Release the accounting for one block. Caller ensures pairing with a
   *  prior successful [[acquireBlockMemory]] (I4). */
  private def releaseBlockMemory(): Unit = {
    spiller.freeMemory(blockBytes)
  }

  /** Evict LRU blocks until `target` bytes have been freed (or cache is
   *  empty). Returns freed bytes. Called from [[SegTreeSpiller.spill]]. */
  private def evictUntil(target: Long): Long = {
    var freed = 0L
    while (freed < target && !blockLevelsCache.isEmpty) {
      freed += evictEldestReturnBytes()
    }
    freed
  }

  /** Evict one LRU block. Returns true if a block was evicted. */
  private def evictEldest(): Boolean = {
    if (blockLevelsCache.isEmpty) return false
    evictEldestReturnBytes()
    true
  }

  private def evictEldestReturnBytes(): Long = {
    val it = blockLevelsCache.entrySet().iterator()
    if (!it.hasNext) return 0L
    val head = it.next()
    it.remove()
    releaseBlockMemory()
    blockBytes
  }

  /** Release accounting for all cached blocks and clear the cache. */
  private def releaseAllCachedBlocks(): Unit = {
    val n = blockLevelsCache.size()
    if (n > 0) {
      blockLevelsCache.clear()
      spiller.freeMemory(n.toLong * blockBytes)
    }
  }

  private def closeRowArray(): Unit = {
    // rowArray is caller-owned (see `build` docstring). Drop the reference
    // only; the caller (WindowPartitionEvaluator.buffer) manages clear().
    rowArray = null
  }
}

private[window] object WindowSegmentTree {
  val DefaultFanout: Int = 16
  val DefaultBlockSize: Int = 65536

  /**
   * Explicit allowlist of [[DeclarativeAggregate]] subclasses that are safe to evaluate
   * with the block-chunked segment tree.
   *
   * An aggregate is safe for segment-tree execution iff its combine semantics form a
   * commutative monoid on the partial-buffer representation (associativity +
   * compatibility with `mergeExpressions`). All nine entries below have been audited:
   *
   *   - [[Min]], [[Max]]: idempotent semilattice.
   *   - [[Sum]], [[Count]]: additive monoid.
   *   - [[Average]]: sum + count, both additive monoids.
   *   - [[StddevPop]], [[StddevSamp]], [[VariancePop]], [[VarianceSamp]]:
   *     Welford online-update form (count, mean, M2) is associative -- see
   *     CentralMomentAgg.mergeExpressions.
   *
   * Aggregates intentionally NOT in the allowlist (tracked as follow-up):
   *   - HyperLogLogPlusPlus / ApproxCountDistinct: sketch merge is associative but
   *     buffer representation carries register arrays whose segment-tree interaction
   *     has not been audited; kept fail-closed until dedicated coverage lands.
   *   - First / Last: order-dependent, not associative.
   *   - CollectList / CollectSet: unbounded buffer growth breaks segtree memory model.
   *   - Percentile / ApproxPercentile: buffer is a sorted sketch; merge cost and
   *     shape does not fit per-block pre-aggregates.
   *   - Any ImperativeAggregate: excluded by the `DeclarativeAggregate` type check.
   *
   * Callers should use [[isEligible]] rather than inlining `contains` on this set so
   * that future extensions (for example, eligibility predicates beyond the class) can
   * be centralized here.
   */
  val EligibleAggregates: Set[Class[_ <: DeclarativeAggregate]] = Set(
    classOf[Min],
    classOf[Max],
    classOf[Sum],
    classOf[Count],
    classOf[Average],
    classOf[StddevPop],
    classOf[StddevSamp],
    classOf[VariancePop],
    classOf[VarianceSamp]
  )

  /**
   * Returns true iff `f` is a [[DeclarativeAggregate]] on the explicit segment-tree
   * allowlist. See [[EligibleAggregates]] for the rationale and excluded aggregates.
   */
  def isEligible(f: Expression): Boolean = f match {
    case agg: DeclarativeAggregate => EligibleAggregates.contains(agg.getClass)
    case _ => false
  }
}
