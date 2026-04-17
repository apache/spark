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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
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
 * we materialize leaves inside the per-block internal node arrays; this
 * is a known deviation documented in PROGRESS to be revisited post-POC.
 */
class WindowSegmentTree(
    functions: Array[DeclarativeAggregate],
    inputSchema: Seq[Attribute],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    fanout: Int = WindowSegmentTree.DEFAULT_FANOUT,
    blockSize: Int = WindowSegmentTree.DEFAULT_BLOCK_SIZE,
    maxCachedBlocks: Int = -1)
  extends AutoCloseable {

  require(fanout >= 2, s"fanout must be >= 2, got $fanout")
  require(blockSize >= 1, s"blockSize must be >= 1, got $blockSize")
  require(functions.nonEmpty, "WindowSegmentTree requires at least one aggregate function")

  // ---------- Schemas & projections ----------

  private val bufferAttrs: Seq[AttributeReference] =
    functions.flatMap(_.aggBufferAttributes).toImmutableArraySeq
  private val rightAttrs: Seq[AttributeReference] =
    functions.flatMap(_.inputAggBufferAttributes).toImmutableArraySeq
  private val bufferDataTypes = bufferAttrs.map(_.dataType).toArray

  private val initialValues: Seq[Expression] = functions.flatMap(_.initialValues).toIndexedSeq
  private val updateExpressions: Seq[Expression] =
    functions.flatMap(_.updateExpressions).toIndexedSeq
  private val mergeExpressionsAll: Seq[Expression] =
    functions.flatMap(_.mergeExpressions).toIndexedSeq

  private val initProj: MutableProjection = newMutableProjection(initialValues, Nil)
  private val updateProj: MutableProjection =
    newMutableProjection(updateExpressions, bufferAttrs ++ inputSchema)
  private val mergeProj: MutableProjection =
    newMutableProjection(mergeExpressionsAll, bufferAttrs ++ rightAttrs)

  private val inputUnsafeProj: UnsafeProjection =
    UnsafeProjection.create(inputSchema.map(_.dataType).toArray)

  private val joinedRow = new JoinedRow()

  // ---------- State ----------

  private var numRows: Int = 0
  private var numBlocks: Int = 0
  private var rowArray: ExternalAppendOnlyUnsafeRowArray = _

  /** Always-resident per-block root aggregates. `blockAggregates(i)` =
   *  merged buffer over all rows in block i. */
  private var blockAggregates: Array[InternalRow] = Array.empty

  /** LRU cache of per-block internal node arrays. Key = blockIdx.
   *  Value = `Array[Array[InternalRow]]` with levels(0..h). */
  private val blockLevelsCache: JLinkedHashMap[Integer, Array[Array[InternalRow]]] = {
    val cap = if (maxCachedBlocks > 0) maxCachedBlocks else 0
    new JLinkedHashMap[Integer, Array[Array[InternalRow]]](16, 0.75f, true) {
      override def removeEldestEntry(
          eldest: JMap.Entry[Integer, Array[Array[InternalRow]]]): Boolean = {
        cap > 0 && this.size() > cap
      }
    }
  }

  // ---------- Public API ----------

  def size: Int = numRows

  def build(rows: Iterator[InternalRow], numRowsHint: Int): Unit = {
    closeRowArray()
    blockLevelsCache.clear()

    numRows = numRowsHint
    numBlocks = if (numRows == 0) 0 else (numRows + blockSize - 1) / blockSize

    if (numRows == 0) {
      blockAggregates = Array.empty
      return
    }

    rowArray = newRowArray()
    var count = 0
    while (rows.hasNext) {
      val r = rows.next()
      val u = inputUnsafeProj(r)
      rowArray.add(u)
      count += 1
    }
    if (count != numRows) {
      throw new IllegalArgumentException(
        s"numRows hint ($numRows) does not match actual row count ($count)")
    }

    // Compute per-block pre-aggregates in one streaming pass.
    blockAggregates = new Array[InternalRow](numBlocks)
    var b = 0
    val iter = rowArray.generateIterator()
    while (b < numBlocks) {
      val buf = newBuffer()
      initProj.target(buf)(InternalRow.empty)
      val start = b * blockSize
      val end = math.min(start + blockSize, numRows)
      var i = start
      while (i < end) {
        if (!iter.hasNext) {
          throw new IllegalStateException("rowArray iterator exhausted unexpectedly")
        }
        val row = iter.next()
        updateProj.target(buf)(joinedRow(buf, row))
        i += 1
      }
      blockAggregates(b) = buf
      b += 1
    }
  }

  def query(lo: Int, hi: Int, outBuffer: InternalRow): Unit = {
    if (lo < 0 || hi > numRows || lo > hi) {
      throw new IllegalArgumentException(
        s"Invalid range [lo=$lo, hi=$hi) for size=$numRows")
    }
    // Reset outBuffer to identity.
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

  override def close(): Unit = {
    closeRowArray()
    blockLevelsCache.clear()
    blockAggregates = Array.empty
    numRows = 0
    numBlocks = 0
  }

  // ---------- Test hooks (package-private) ----------

  private[window] def peekBlockCount: Int = numBlocks

  private[window] def peekLevelSize(blockIdx: Int, level: Int): Int = {
    val levels = ensureBlockLevels(blockIdx)
    levels(level).length
  }

  private[window] def peekLevelCount(blockIdx: Int): Int = {
    val levels = ensureBlockLevels(blockIdx)
    levels.length
  }

  // ---------- Internals ----------

  /** Merge `src` buffer into `dst` buffer using mergeProj. */
  private def mergeInto(dst: InternalRow, src: InternalRow): Unit = {
    mergeProj.target(dst)(joinedRow(dst, src))
  }

  private def newBuffer(): InternalRow =
    new SpecificInternalRow(bufferDataTypes.toIndexedSeq)

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
    val span = spanAtLevel(level)
    val nodeLo = idx * span
    val nodeHi = math.min(nodeLo + span, blockRows)
    if (queryLo >= nodeHi || queryHi <= nodeLo) return
    if (queryLo <= nodeLo && nodeHi <= queryHi) {
      mergeInto(out, levels(level)(idx))
      return
    }
    if (level == 0) return // partial overlap on a single leaf = empty intersection
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

  /** Number of leaves covered by a single node at the given level. */
  private def spanAtLevel(level: Int): Int = {
    var s = 1L
    var i = 0
    while (i < level) { s *= fanout; i += 1 }
    if (s > Int.MaxValue) Int.MaxValue else s.toInt
  }

  /** Build (or fetch from LRU) the full per-block levels array. */
  private def ensureBlockLevels(blockIdx: Int): Array[Array[InternalRow]] = {
    val cached = blockLevelsCache.get(Integer.valueOf(blockIdx))
    if (cached != null) return cached

    val blockStart = blockIdx * blockSize
    val blockRows = math.min(blockSize, numRows - blockStart)

    // Level 0: one aggregate per row in the block.
    val leaves = new Array[InternalRow](blockRows)
    val iter = rowArray.generateIterator(blockStart)
    var i = 0
    while (i < blockRows) {
      if (!iter.hasNext) {
        throw new IllegalStateException(
          s"rowArray iterator exhausted at block $blockIdx row $i")
      }
      val row = iter.next()
      val buf = newBuffer()
      initProj.target(buf)(InternalRow.empty)
      updateProj.target(buf)(joinedRow(buf, row))
      leaves(i) = buf
      i += 1
    }

    val allLevels = new java.util.ArrayList[Array[InternalRow]]()
    allLevels.add(leaves)

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
      allLevels.add(parents)
      prev = parents
    }

    val levels = allLevels.toArray(new Array[Array[InternalRow]](0))
    blockLevelsCache.put(Integer.valueOf(blockIdx), levels)
    levels
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
      Int.MaxValue,
      Long.MaxValue,
      Int.MaxValue,
      Long.MaxValue)
  }

  private def closeRowArray(): Unit = {
    if (rowArray != null) {
      rowArray.clear()
      rowArray = null
    }
  }
}

object WindowSegmentTree {
  val DEFAULT_FANOUT: Int = 16
  val DEFAULT_BLOCK_SIZE: Int = 65536
}
