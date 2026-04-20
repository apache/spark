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
 * Memory-manager integration tests for [[WindowSegmentTree]] (memory-manager integration).
 * Exercises the matrix defined in
 * `the PR description` Section 5.
 *
 * Tests T5 (rowArray-spilled priority) and T8 (task-kill listener) are
 * intentionally left as `ignore`d stubs so the matrix stays visible;
 * each stub documents what additional infrastructure it needs before
 * it can run.
 */
class WindowSegmentTreeMemorySuite extends SparkFunSuite with LocalSparkContext {

  // ---- common fixtures ----

  private val inputAttr: AttributeReference =
    AttributeReference("v", IntegerType, nullable = true)()
  private val inputSchema: Seq[Attribute] = Seq(inputAttr)

  private def newMutableProjection
      : (Seq[Expression], Seq[Attribute]) => MutableProjection =
    (exprs, attrs) => GenerateMutableProjection.generate(exprs, attrs)

  private def minAgg: DeclarativeAggregate = Min(inputAttr)

  /**
   * Construct a standalone `TaskMemoryManager` backed by `TestMemoryManager`
   * (not routed through `SparkEnv`), install a matching `TaskContextImpl`,
   * and run `body`. Restores the previous `TaskContext` on exit.
   *
   * @param budget  initial available execution memory; use `Long.MaxValue`
   *                for T2 and a tight value (e.g. 2 * blockBytes) for T3.
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
    tree.build(rows)
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

  // ---- T1 ----
  test("T1 constructor rejects null TaskMemoryManager") {
    val ex = intercept[IllegalArgumentException] {
      new WindowSegmentTree(
        Array(minAgg), inputSchema, newMutableProjection,
        taskMemoryManager = null)
    }
    assert(ex.getMessage.contains("non-null TaskMemoryManager"))
  }

  // ---- T2 ----
  test("T2 ample budget: query correctness and positive memory usage") {
    withTmm() { (tmm, _) =>
      val values = Seq(5, 2, 9, 1, 7, 3, 4, 8, 6, 0, 11, 12, 13, 14, 15, 16)
      val tree = buildTree(tmm, values, fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(4))
      try {
        // Force a couple of block-level queries so the LRU actually populates.
        assert(queryMin(tree, 0, values.length) == values.min)
        assert(queryMin(tree, 1, 14) == naiveMin(values, 1, 14))
        assert(queryMin(tree, 5, 11) == naiveMin(values, 5, 11))
        // Memory consumption strictly positive because at least one block
        // level has been cached and the spiller acquired bytes for it.
        assert(tmm.getMemoryConsumptionForThisTask > 0L,
          "Expected positive memory consumption after caching block levels")
      } finally tree.close()
      // After close: all acquired bytes must be released.
      assert(tmm.getMemoryConsumptionForThisTask == 0L,
        "Memory consumption must return to 0 after close()")
    }
  }

  // ---- T3 ----
  test("T3 tight budget forces spill: results still match baseline") {
    val values = (0 until 40).map(i => (i * 37 + 11) % 97)
    // Collect the baseline under an ample budget.
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
    // Tight budget: only ~1 block's worth of headroom. Any new block that
    // needs levels will force the spiller to evict the prior block.
    //
    // Sizing: for (fanout=4, blockSize=4) the cached level shape is
    // [4, 1] slots; at bufferWidth=16 B per slot blockBytes = 5 * 16 = 80 B.
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

  // ---- T4 ----
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

  // ---- T6 ----
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
      // Second close must be a no-op (no double-free, no throw).
      tree.close()
      assert(tmm.getMemoryConsumptionForThisTask == 0L,
        "Second close() must remain a no-op")
    }
  }

  // ---- T7 ----
  test("T7 prepare mid-way failure releases all previously acquired blocks") {
    // Inject failure by pre-setting consequentOOM to a moderate N so the
    // spiller's acquireMemory returns 0 after the first few successful blocks.
    withTmm() { (tmm, mm) =>
      val values = (0 until 40).map(i => 40 - i)
      val tree = buildTree(tmm, values, fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(10))
      try {
        // Warm up: query to populate a couple of cache entries.
        assert(queryMin(tree, 0, 40) == 1)
        // Now force the *next* acquireMemory calls to fail, then trigger a
        // cold-cache path by forcing evict + re-acquire. We release everything
        // first so a subsequent query path must re-acquire.
        val spiller = tree.testOnlySpiller()
        spiller.spill(Long.MaxValue, new MemoryConsumer(tmm,
            tmm.pageSizeBytes(), MemoryMode.ON_HEAP) {
          override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
        })
        assert(tmm.getMemoryConsumptionForThisTask == 0L,
          "after full spill, accounting must be zero")
        // Force the next N acquireMemory calls to grant 0 bytes (hard OOM).
        // With maxCachedBlocks=10 and a cold cache, ensureBlockLevels will
        // call acquireBlockMemory; both initial grant and post-evict retry
        // will see 0 -> SparkOutOfMemoryError.
        mm.markConsequentOOM(10)
        val ex = intercept[SparkOutOfMemoryError](queryMin(tree, 0, 20))
        assert(ex.getMessage.contains("UNABLE_TO_ACQUIRE_MEMORY") ||
          ex.getMessage.contains("unable"),
          s"unexpected OOM message: ${ex.getMessage}")
        // Critically: the failed acquire path must not have left any bytes
        // accounted against the task.
        assert(tmm.getMemoryConsumptionForThisTask == 0L,
          "After failed acquire, accounting must be zero (no partial leaks)")
      } finally tree.close()
      assert(tmm.getMemoryConsumptionForThisTask == 0L)
    }
  }

  // ---- T9 ----
  test("T9 ON_HEAP path: spiller mode follows TMM tungsten mode") {
    withTmm(offHeap = false) { (tmm, _) =>
      val tree = buildTree(tmm, (0 until 16), fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(2))
      try {
        assert(queryMin(tree, 0, 16) == 0)
        assert(tree.testOnlySpiller().getMode == tmm.getTungstenMemoryMode)
        assert(tree.testOnlySpiller().getMode == MemoryMode.ON_HEAP)
      } finally tree.close()
    }
  }

  test("T9 OFF_HEAP path: spiller mode follows TMM tungsten mode") {
    withTmm(offHeap = true) { (tmm, _) =>
      val tree = buildTree(tmm, (0 until 16), fanout = 4, blockSize = 4,
        maxCachedBlocks = Some(2))
      try {
        assert(queryMin(tree, 0, 16) == 0)
        assert(tree.testOnlySpiller().getMode == tmm.getTungstenMemoryMode)
        assert(tree.testOnlySpiller().getMode == MemoryMode.OFF_HEAP)
      } finally tree.close()
    }
  }

  // ---- T10 ----
  test("T10 blockBytes oracle: hand-computed table + runtime-anchored cross-check") {
    // Part A: hand-computed golden table. These values were derived by
    // listing the cached level widths produced by buildBlockLevels and
    // multiplying by the 16 B/field buffer width used below. They do NOT
    // share a code path with production `cachedSlotsPerBlock` -- if the
    // production loop regresses (e.g., fencepost off-by-one or drops a
    // level) at least one of these cases flips, including:
    //   - blockSize == 1   : single leaf, no parent levels (edge case).
    //   - (F=3, B=5)       : non-power-of-fanout, asymmetric top levels.
    //   - (F=4, B=7)       : non-power-of-fanout, typical tail-shape.
    //   - (F=16, B=65536)  : deep tree, catches compounding fencepost.
    // Level breakdown per case (leaves + parents + ... + root):
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
          // Min on a single IntegerType field -> 1 buffer field x 16 B.
          val expected = math.max(1L, slots * 16L)
          assert(tree.peekBlockBytes == expected,
            s"blockBytes mismatch at (F=$fanout, blockSize=$blockSize): " +
              s"got=${tree.peekBlockBytes} expected=$expected (slots=$slots)")
        } finally tree.close()
      }
    }

    // Part B: runtime-anchored cross-check. Build a block, read back the
    // *actual* cached level array lengths via peek hooks, and assert
    // `blockBytes == sum(levels) * bufferWidth`. This guards against the
    // formula and buildBlockLevels drifting apart without sharing a code
    // path with either side.
    withTmm() { (tmm, _) =>
      val fanout = 4
      val blockSize = 8
      val tree = buildTree(tmm, (0 until blockSize),
        fanout = fanout, blockSize = blockSize, maxCachedBlocks = Some(1))
      try {
        // Force block 0 to be materialized.
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

  // ---- T5 (ignored stub) ----
  ignore("T5 rowArray-spilled short-circuit in SegTreeSpiller.spill -- ignored stub") {
    // Requires a controllable `hasSpilled` hook on
    // ExternalAppendOnlyUnsafeRowArray, which the current public API does
    // not expose. The production path uses a `spillSize > 0` heuristic in
    // WindowSegmentTree; this cell is kept as an ignored stub so the T5
    // row of the memory-manager matrix stays visible.
  }

  // ---- T8 (ignored stub) ----
  ignore("T8 task-kill completion listener triggers close -- ignored stub") {
    // Requires a SparkContext/DAGScheduler-driven task-kill path; covered
    // implicitly by the frame-layer listener wiring. Kept as an ignored
    // stub so the T8 row of the memory-manager matrix stays visible.
  }
}
