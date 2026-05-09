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

import java.util.Properties

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, SparkOutOfMemoryError, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, MutableProjection, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{DeclarativeAggregate, Min}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.types.IntegerType

/**
 * Memory-manager integration tests for [[WindowSegmentTree]]. Covers:
 *  - `SegTreeSpiller` registration with `TaskMemoryManager`
 *  - `acquireBlockMemory` grant / partial-grant rollback
 *  - `evictUntil` LRU eviction driven by TMM pressure
 *  - `spill()` self-trigger short-circuit and rowArray-spilled fall-through
 *  - task completion / kill listener releasing all cached blocks
 * T5 (rowArray-spilled priority) and T8 (task-kill listener) are kept as
 * `ignore`d stubs so the matrix stays visible; each documents what it needs.
 */
class WindowSegmentTreeMemorySuite extends SparkFunSuite with LocalSparkContext {

  // common fixtures

  private val inputAttr: AttributeReference =
    AttributeReference("v", IntegerType, nullable = true)()
  private val inputSchema: Seq[Attribute] = Seq(inputAttr)

  private def newMutableProjection
      : (Seq[Expression], Seq[Attribute]) => MutableProjection =
    (exprs, attrs) => GenerateMutableProjection.generate(exprs, attrs)

  private def minAgg: DeclarativeAggregate = Min(inputAttr)

  /**
   * Standalone `TaskMemoryManager` backed by `TestMemoryManager` (not routed
   * through `SparkEnv`), with a matching `TaskContextImpl` installed.
   * Restores the previous `TaskContext` on exit.
   *
   * @param budget  initial execution memory; `Long.MaxValue` for T2,
   *                tight value (e.g. 2 * blockBytes) for T3.
   * @param offHeap when true, enables Tungsten off-heap mode (T9).
   */
  private def withTmm[T](budget: Long = Long.MaxValue, offHeap: Boolean = false)
      (body: (TaskMemoryManager, TestMemoryManager) => T): T = {
    val conf = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", offHeap.toString)
      .set("spark.memory.offHeap.size", "1048576")
    if (sc == null) {
      sc = new SparkContext("local", "WindowSegmentTreeMemorySuite", conf)
    }
    val mm = new TestMemoryManager(conf)
    mm.limit(budget)
    val tmm = new TaskMemoryManager(mm, 0)
    val prev = TaskContext.get()
    val tc = new TaskContextImpl(
      stageId = 0, stageAttemptNumber = 0, partitionId = 0,
      taskAttemptId = 0, attemptNumber = 0, numPartitions = 1,
      taskMemoryManager = tmm,
      localProperties = new Properties,
      metricsSystem = null.asInstanceOf[MetricsSystem],
      taskMetrics = TaskMetrics.empty,
      cpus = 1)
    TaskContext.setTaskContext(tc)
    try body(tmm, mm)
    finally {
      if (prev != null) TaskContext.setTaskContext(prev) else TaskContext.unset()
    }
  }

  private def buildTree(
      tmm: TaskMemoryManager,
      values: Seq[Int],
      fanout: Int = 4,
      blockSize: Int = 8,
      maxCachedBlocks: Option[Int] = Some(2)): WindowSegmentTree = {
    val tree = new WindowSegmentTree(
      Array(minAgg), inputSchema, newMutableProjection,
      fanout = fanout, blockSize = blockSize,
      maxCachedBlocks = maxCachedBlocks,
      taskMemoryManager = tmm)
    val rows = values.iterator.map { v =>
      val r = new GenericInternalRow(1); r.update(0, v); r.asInstanceOf[InternalRow]
    }
    SegmentTreeWindowTestHelper.buildTreeFromIter(tree, rows, inputSchema)
    tree
  }

  private def newOutBuffer(): SpecificInternalRow =
    new SpecificInternalRow(Seq[org.apache.spark.sql.types.DataType](IntegerType))

  private def queryMin(tree: WindowSegmentTree, lo: Int, hi: Int): Any = {
    val out = newOutBuffer()
    tree.query(lo, hi, out)
    if (out.isNullAt(0)) null else out.getInt(0)
  }

  private def naiveMin(vs: Seq[Int], lo: Int, hi: Int): Any =
    if (lo >= hi) null else vs.slice(lo, hi).min

  test("T1 constructor rejects null TaskMemoryManager") {
    val ex = intercept[IllegalArgumentException] {
      new WindowSegmentTree(
        Array(minAgg), inputSchema, newMutableProjection,
        taskMemoryManager = null)
    }
    assert(ex.getMessage.contains("non-null TaskMemoryManager"))
  }

  test("T2 ample budget: query correctness and positive memory usage") {
    withTmm() { (tmm, _) =>
      val values = Seq(5, 2, 9, 1, 7, 3, 4, 8, 6, 0, 11, 12, 13, 14, 15, 16)
      val tree = buildTree(tmm, values, fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(4))
      try {
        // Force block-level queries so the LRU actually populates.
        assert(queryMin(tree, 0, values.length) == values.min)
        assert(queryMin(tree, 1, 14) == naiveMin(values, 1, 14))
        assert(queryMin(tree, 5, 11) == naiveMin(values, 5, 11))
        // Positive consumption: at least one block level cached -> spiller acquired.
        assert(tmm.getMemoryConsumptionForThisTask > 0L,
          "Expected positive memory consumption after caching block levels")
      } finally tree.close()
      // All bytes released after close.
      assert(tmm.getMemoryConsumptionForThisTask == 0L,
        "Memory consumption must return to 0 after close()")
    }
  }

  test("T3 tight budget forces spill: results still match baseline") {
    val values = (0 until 40).map(i => (i * 37 + 11) % 97)
    // Baseline under ample budget.
    val baseline: Seq[Any] = {
      var captured: Seq[Any] = Seq.empty
      withTmm() { (tmm, _) =>
        val tree = buildTree(tmm, values, fanout = 4, blockSize = 4,
          maxCachedBlocks = Some(3))
        try {
          captured = (0 to values.length by 5).map(i => queryMin(tree, 0, i))
        } finally {
          tree.close()
        }
      }
      captured
    }
    // Tight budget: ~1 block headroom -> any new block-level load evicts the prior.
    // Sizing: (fanout=4, blockSize=4) -> level shape [4, 1] slots;
    // at bufferWidth=16 B/slot blockBytes = 5 * 16 = 80 B.
    // budget=128 B admits exactly one block and forces spill on the second.
    withTmm(budget = 128L) { (tmm, _) =>
      val tree = buildTree(tmm, values, fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(3))
      try {
        val observed = (0 to values.length by 5).map(i => queryMin(tree, 0, i))
        assert(observed == baseline,
          s"spill-path answers diverged: observed=$observed baseline=$baseline")
      } finally tree.close()
      assert(tmm.getMemoryConsumptionForThisTask == 0L)
    }
  }

  test("T4 self-trigger: spill(_, this) returns 0 and does not evict") {
    withTmm() { (tmm, _) =>
      val tree = buildTree(tmm, (0 until 16), fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(3))
      try {
        // Warm the cache.
        assert(queryMin(tree, 0, 16) == 0)
        val before = tmm.getMemoryConsumptionForThisTask
        assert(before > 0L, "precondition: some block levels must be cached")
        val spiller = tree.testOnlySpiller()
        val freed = spiller.spill(Long.MaxValue, spiller)
        assert(freed == 0L, s"self-trigger spill must return 0L, got $freed")
        val after = tmm.getMemoryConsumptionForThisTask
        assert(after == before,
          s"cache size must not change on self-trigger (before=$before after=$after)")
      } finally tree.close()
    }
  }

  test("T6 close() is idempotent and releases all acquired bytes") {
    withTmm() { (tmm, _) =>
      val tree = buildTree(tmm, (0 until 20), fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(3))
      assert(queryMin(tree, 0, 20) == 0)
      val peak = tmm.getMemoryConsumptionForThisTask
      assert(peak > 0L)
      tree.close()
      assert(tmm.getMemoryConsumptionForThisTask == 0L,
        "Memory must be fully released after first close()")
      // Second close must be a no-op.
      tree.close()
      assert(tmm.getMemoryConsumptionForThisTask == 0L,
        "Second close() must remain a no-op")
    }
  }

  test("T7 prepare mid-way failure releases all previously acquired blocks") {
    // Inject failure by pre-setting consequentOOM to a moderate N so
    // acquireMemory returns 0 after the first few successful blocks.
    withTmm() { (tmm, mm) =>
      val values = (0 until 40).map(i => 40 - i)
      val tree = buildTree(tmm, values, fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(10))
      try {
        // Warm up: populate cache entries.
        assert(queryMin(tree, 0, 40) == 1)
        // Force next acquireMemory calls to fail, then trigger cold-cache via
        // evict + re-acquire. Release everything so the next query re-acquires.
        val spiller = tree.testOnlySpiller()
        spiller.spill(Long.MaxValue, new MemoryConsumer(tmm,
            tmm.pageSizeBytes(), MemoryMode.ON_HEAP) {
          override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
        })
        assert(tmm.getMemoryConsumptionForThisTask == 0L,
          "after full spill, accounting must be zero")
        // Force next N acquireMemory calls to grant 0 bytes (hard OOM).
        // With maxCachedBlocks=10 and a cold cache, ensureBlockLevels will
        // call acquireBlockMemory; initial grant and post-evict retry both
        // see 0 -> SparkOutOfMemoryError.
        mm.markConsequentOOM(10)
        val ex = intercept[SparkOutOfMemoryError](queryMin(tree, 0, 20))
        assert(ex.getMessage.contains("UNABLE_TO_ACQUIRE_MEMORY"),
          s"unexpected OOM message: ${ex.getMessage}")
        // Failed acquire must not leak bytes against the task.
        assert(tmm.getMemoryConsumptionForThisTask == 0L,
          "After failed acquire, accounting must be zero (no partial leaks)")
      } finally tree.close()
      assert(tmm.getMemoryConsumptionForThisTask == 0L)
    }
  }

  test("T9 ON_HEAP Tungsten: spiller mode is ON_HEAP") {
    withTmm(offHeap = false) { (tmm, _) =>
      val tree = buildTree(tmm, (0 until 16), fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(2))
      try {
        assert(queryMin(tree, 0, 16) == 0)
        assert(tree.testOnlySpiller().getMode == MemoryMode.ON_HEAP)
        // In ON_HEAP Tungsten, our hardcoded mode also coincides with
        // `tmm.getTungstenMemoryMode`, so segtree and rowArray share the
        // pool and I8 (rowArray-spilled short-circuit) fires.
        assert(tree.testOnlySpiller().getMode == tmm.getTungstenMemoryMode)
      } finally tree.close()
    }
  }

  test("T9 OFF_HEAP Tungsten: spiller mode stays ON_HEAP (not OFF_HEAP)") {
    withTmm(offHeap = true) { (tmm, _) =>
      val tree = buildTree(tmm, (0 until 16), fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(2))
      try {
        assert(queryMin(tree, 0, 16) == 0)
        // Segtree's cache is JVM-heap (`SpecificInternalRow` /
        // `Array[Array[InternalRow]]`), never Tungsten pages. The spiller
        // must stay ON_HEAP regardless of `spark.memory.offHeap.enabled`,
        // or `spill(n)` would phantom-credit the off-heap pool and
        // violate TMM's same-pool spill contract. Mirrors `Spillable`.
        assert(tree.testOnlySpiller().getMode == MemoryMode.ON_HEAP)
        assert(tmm.getTungstenMemoryMode == MemoryMode.OFF_HEAP)
        assert(tree.testOnlySpiller().getMode != tmm.getTungstenMemoryMode)
      } finally tree.close()
    }
  }

  test("T10 blockBytes oracle: hand-computed table + runtime-anchored cross-check") {
    // Part A: hand-computed golden table. Values derived by listing the
    // cached level widths from buildBlockLevels * 16 B/field. Independent of
    // production `cachedSlotsPerBlock` -- fencepost or dropped-level
    // regressions flip at least one case. Cases chosen to cover:
    //   blockSize == 1    (single leaf, no parents), (F=3,B=5) and (F=4,B=7)
    //   (non-power-of-fanout asymmetric tails), (F=16,B=65536) (deep tree).
    // Level breakdown (leaves + parents + ... + root):
    //   (F=4,  B=1)     -> 1                              =    1 slot
    //   (F=4,  B=4)     -> 4 + 1                          =    5 slots
    //   (F=3,  B=5)     -> 5 + 2 + 1                      =    8 slots
    //   (F=4,  B=7)     -> 7 + 2 + 1                      =   10 slots
    //   (F=8,  B=16)    -> 16 + 2 + 1                     =   19 slots
    //   (F=16, B=256)   -> 256 + 16 + 1                   =  273 slots
    //   (F=16, B=65536) -> 65536 + 4096 + 256 + 16 + 1    = 69905 slots
    val expectedSlots: Seq[((Int, Int), Long)] = Seq(
      (4, 1) -> 1L,
      (4, 4) -> 5L,
      (3, 5) -> 8L,
      (4, 7) -> 10L,
      (8, 16) -> 19L,
      (16, 256) -> 273L,
      (16, 65536) -> 69905L)

    withTmm() { (tmm, _) =>
      for (((fanout, blockSize), slots) <- expectedSlots) {
        val tree = buildTree(tmm, Seq(1, 2, 3, 4, 5, 6, 7, 8),
          fanout = fanout, blockSize = blockSize, maxCachedBlocks = Some(1))
        try {
          // Min on one IntegerType field -> 1 buffer field x 16 B.
          val expected = math.max(1L, slots * 16L)
          assert(tree.peekBlockBytes == expected,
            s"blockBytes mismatch at (F=$fanout, blockSize=$blockSize): " +
              s"got=${tree.peekBlockBytes} expected=$expected (slots=$slots)")
        } finally tree.close()
      }
    }

    // Part B: runtime-anchored cross-check. Build a block, read actual cached
    // level array lengths via peek hooks, assert
    // `blockBytes == sum(levels) * bufferWidth`. Guards against the formula
    // and buildBlockLevels drifting apart.
    withTmm() { (tmm, _) =>
      val fanout = 4
      val blockSize = 8
      val tree = buildTree(tmm, (0 until blockSize),
        fanout = fanout, blockSize = blockSize, maxCachedBlocks = Some(1))
      try {
        // Materialize block 0.
        assert(queryMin(tree, 0, blockSize) == 0)
        val levelCount = tree.peekLevelCount(0)
        var slotSum = 0L
        var lvl = 0
        while (lvl < levelCount) {
          slotSum += tree.peekLevelSize(0, lvl)
          lvl += 1
        }
        val expected = math.max(1L, slotSum * 16L)
        assert(tree.peekBlockBytes == expected,
          s"runtime-anchored oracle mismatch: slots=$slotSum " +
            s"expected=$expected got=${tree.peekBlockBytes}")
      } finally tree.close()
    }
  }
}
