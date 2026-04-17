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

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.memory.{MemoryConsumer, SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.ArrayImplicits._

/**
 * Block-chunked segment tree for range aggregate queries over window partitions.
 *
 * See `the class documentation`
 * for the full design (API contract Section 2, block-chunked memory layout Section 3,
 * DeclarativeAggregate binding Section 4, error handling Section 5, test hooks Section 6).
 *
 * initial implementation scope: correctness only. The data layer uses
 * `ExternalAppendOnlyUnsafeRowArray` to hold input rows (spillable). Each
 * block materializes its own small segment tree (levels 0..h). Internal
 * nodes are cached in an LRU keyed by block index; block root aggregates
 * (block pre-aggregates) stay resident for all blocks.
 *
 * Note: the design doc Section 3.3 specifies leaves are NOT materialized and
 * recomputed from the spillable array on demand. For initial implementation simplicity
 * we materialize leaves inside the per-block internal node arrays.
 * // TODO(SPARK-XXXXX) re-assess after Frame integration.
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
    spillThreshold: Int = Int.MaxValue,
    inMemoryThreshold: Int = Int.MaxValue,
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

  private val inputUnsafeProj: UnsafeProjection =
    UnsafeProjection.create(inputSchema.map(_.dataType).toArray)

  private[this] val joinedRow = new JoinedRow()

  // ---------- State ----------

  private var numRows: Int = 0
  private var numBlocks: Int = 0
  private var rowArray: ExternalAppendOnlyUnsafeRowArray = _
  private var closed: Boolean = false

  /** Always-resident per-block root aggregates. `blockAggregates(i)` =
   *  merged buffer over all rows in block i. */
  private var blockAggregates: Array[InternalRow] = Array.empty

  /** Rough byte width of one aggregate buffer row. Derived from buffer schema
   *  types (defaults to 8 bytes / field, matching `SpecificInternalRow`
   *  word-aligned slots). Used by [[blockBytes]] (contract I5). */
  private val bufferWidthBytes: Long = {
    // SpecificInternalRow stores each field in a MutableValue (8 bytes for
    // most primitives, object header for complex). 8 bytes/field is the
    // minimum lower bound; we pick 16 as a conservative heap-overhead-aware
    // default so memory accounting is not a gross under-estimate. This is
    // consistent with UnsafeRow's word-aligned record layout.
    val bytesPerField = 16L
    math.max(1L, bufferDataTypes.size.toLong * bytesPerField)
  }

  /** Bytes accounted per cached block's levels (contract Section 2.4 / I5). Covers
   *  a factor-of-F over-estimate of the geometric series of level sizes. */
  private[this] val blockBytes: Long = fanout.toLong * bufferWidthBytes

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
   *  `removeEldestEntry` is disabled (contract I3) -- eviction is driven
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

  // ---------- Memory consumer (contract Section 2.2) ----------

  /**
   * Private MemoryConsumer tracking cached block levels under TMM.
   *
   * Heap accounting only (no Tungsten pages): uses
   * [[MemoryConsumer.acquireMemory]] / [[MemoryConsumer.freeMemory]]. The
   * [[MemoryConsumer]] base class records `used` via an `AtomicLong` when
   * we call these -- so TMM's consumer-priority sort in
   * `acquireExecutionMemory` sees our pressure accurately.
   *
   * @note `spill()` MUST NOT call `acquireMemory` (contract I1).
   */
  private final class SegTreeSpiller extends MemoryConsumer(
      taskMemoryManager,
      taskMemoryManager.pageSizeBytes(),
      taskMemoryManager.getTungstenMemoryMode()) {
    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      // I2: self-trigger short-circuit. Prevents re-entrant eviction when
      // our own acquireMemory() triggers TMM to poll us.
      if (trigger eq this) return 0L
      // I8: rowArray spilled-to-disk detection. If the rowArray has already
      // spilled, evicting our cache is counter-productive (rebuild would
      // O(blockStart)-scan the spill file). Return 0L to let TMM fall
      // through to the next consumer.
      //
      // TODO(SPARK-XXXXX) #segtree-spill-priority (contract Section 7 O4): current
      // heuristic uses `spillSize > 0` as the "has spilled" signal. A more
      // precise check would consult `UnsafeExternalSorter.getSpillWriters`
      // state, but that API is not public. Re-evaluate after benchmark.
      // FIXME(kentyao): upstream a public "hasSpilled" hook on the array.
      if (rowArray != null && rowArray.spillSize > 0) return 0L
      evictUntil(size)
    }
  }

  private[this] val spiller: SegTreeSpiller = new SegTreeSpiller

  // Task kill / completion fall-back: ensure cache is freed even if caller
  // forgets close() (contract Section 2.5). Frame already registers a
  // listener too; double close is idempotent (contract I4).
  {
    val tc = TaskContext.get()
    if (tc != null) {
      tc.addTaskCompletionListener[Unit](_ => close())
    }
  }

  // ---------- Public API ----------

  def size: Int = numRows

  /**
   * Drain `rows` into this tree, replacing any previously built state.
   * Exception-safe: if iteration or aggregation throws, the previously built
   * state (if any) is preserved.
   */
  def build(rows: Iterator[InternalRow]): Unit = {
    val oldArray = rowArray
    var newArray: ExternalAppendOnlyUnsafeRowArray = null
    try {
      newArray = newRowArray()
      while (rows.hasNext) {
        val r = rows.next()
        val u = inputUnsafeProj(r)
        newArray.add(u)
      }
      // newArray.length is Int (bounded by Int.MaxValue by design); keep the
      // check for clarity against any future widening of that contract.
      val n = newArray.length
      if (n < 0) {
        throw new IllegalArgumentException(
          s"WindowSegmentTree cannot hold more than Int.MaxValue rows, got $n")
      }
      val nBlocks = if (n == 0) 0 else (n + blockSize - 1) / blockSize
      val newBlockAggs = computeBlockAggregates(newArray, n, nBlocks)

      // Commit.
      rowArray = newArray
      numRows = n
      numBlocks = nBlocks
      blockAggregates = newBlockAggs
      // Rebuild invalidates cached block levels; release all acquired memory
      // before dropping the cache entries (I4).
      releaseAllCachedBlocks()
    } catch {
      case t: Throwable =>
        if (newArray != null) newArray.clear()
        throw t
    }
    if (oldArray != null) oldArray.clear()
  }

  /**
   * Query [lo, hi) and directly evaluate the result via `processor.evaluate`
   * into `target`. Uses an internal pre-allocated buffer so no per-call
   * allocation is needed. See
   * `docs/frame-integration-contract.md` Section 2.
   */
  private[window] def queryInto(
      lo: Int, hi: Int, processor: AggregateProcessor, target: InternalRow): Unit = {
    query(lo, hi, internalQueryBuffer)
    processor.evaluate(internalQueryBuffer, target)
  }

  private[this] val internalQueryBuffer: InternalRow = newBuffer()

  def query(lo: Int, hi: Int, outBuffer: InternalRow): Unit = {
    if (lo < 0 || hi > numRows || lo > hi) {
      throw new IllegalArgumentException(
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

  /** Terminal: releases all state. Idempotent (contract I4). */
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

  /** Build (or fetch from LRU) the full per-block levels array. See contract
   *  Section 3 (acquire -> build -> cache) and Section 6 pseudocode. */
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
    // before giving up (contract Section 6).
    if (!acquireBlockMemory(blockIdx)) {
      if (!evictEldest() || !acquireBlockMemory(blockIdx)) {
        // scalastyle:off throwerror
        throw new SparkOutOfMemoryError(
          "UNABLE_TO_ACQUIRE_MEMORY",
          new java.util.HashMap[String, String]() {
            put("requestedBytes", blockBytes.toString)
            put("receivedBytes", "0")
          })
        // scalastyle:on throwerror
      }
    }

    // I4: if buildBlockLevels throws, release the memory we just acquired.
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

  // ---------- Memory accounting helpers (contract Section 2.3) ----------

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
   *  prior successful [[acquireBlockMemory]] (contract I4). */
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

  private def newRowArray(): ExternalAppendOnlyUnsafeRowArray = {
    val taskContext = TaskContext.get()
    if (taskContext == null) {
      throw new IllegalStateException(
        "WindowSegmentTree.build requires an active TaskContext")
    }
    val env = SparkEnv.get
    new ExternalAppendOnlyUnsafeRowArray(
      taskContext.taskMemoryManager(),
      env.blockManager,
      env.serializerManager,
      taskContext,
      1024,
      env.memoryManager.pageSizeBytes,
      inMemoryThreshold,
      Long.MaxValue,
      spillThreshold,
      Long.MaxValue)
  }

  private def closeRowArray(): Unit = {
    if (rowArray != null) {
      rowArray.clear()
      rowArray = null
    }
  }
}

private[window] object WindowSegmentTree {
  val DefaultFanout: Int = 16
  val DefaultBlockSize: Int = 65536
}
