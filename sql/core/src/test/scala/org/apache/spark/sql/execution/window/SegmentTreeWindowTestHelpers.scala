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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

/**
 * Shared helpers for segment-tree window-frame tests. Kept in the same
 * package so tests can reach `private[window]` hooks on
 * [[SegmentTreeWindowFunctionFrame]] (see contract Section 1.3).
 *
 * Extracted from `SegmentTreeWindowFunctionSuite` in helper extraction so the
 * upcoming property-based suite can share the same fake-TaskContext /
 * fixture-frame plumbing without duplication.
 */
private[window] object SegmentTreeWindowTestHelpers {

  /**
   * Build a small-partition [[SegmentTreeWindowFunctionFrame]] (Sum over a
   * single IntegerType column, `rows` values = 0..rows-1, frame = -1..+1),
   * drive it through all rows, and hand the frame to `body`. Manages a
   * fake TaskContext if the caller didn't set one, and always closes the
   * frame on exit.
   */
  def withSmallPartitionFrame(conf: SQLConf, rows: Int)
      (body: SegmentTreeWindowFunctionFrame => Unit): Unit = {
    val existing = TaskContext.get()
    val owned = existing == null
    val tc = if (owned) {
      val fake = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
      TaskContext.setTaskContext(fake)
      fake
    } else existing
    try {
      val frame = buildSmallPartitionFrame(conf, rows, tc)
      try body(frame) finally frame.close()
    } finally {
      if (owned) TaskContext.unset()
    }
  }

  private def buildSmallPartitionFrame(
      conf: SQLConf, rows: Int, tc: TaskContext): SegmentTreeWindowFunctionFrame = {
    val attr = AttributeReference("v", IntegerType, nullable = false)()
    val input = Seq[Attribute](attr)
    val fn = Sum(attr)
    val bufAttrs = fn.aggBufferAttributes
    val processor = AggregateProcessor(
      Array[Expression](fn),
      0,
      input,
      (es, s) => GenerateMutableProjection.generate(es, s),
      Array[Option[Expression]](None))
    val target = new SpecificInternalRow(Seq(bufAttrs.head.dataType))
    val array = new ExternalAppendOnlyUnsafeRowArray(
      tc.taskMemoryManager(),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      tc,
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes,
      Int.MaxValue,
      Long.MaxValue,
      Int.MaxValue,
      Long.MaxValue)
    val unsafeProj = UnsafeProjection.create(Array(attr.dataType))
    var i = 0
    while (i < rows) {
      val r = new GenericInternalRow(Array[Any](i))
      array.add(unsafeProj(r))
      i += 1
    }
    val frame = new SegmentTreeWindowFunctionFrame(
      target,
      processor,
      Array(fn),
      input,
      RowFrame,
      RowBoundOrdering(-1),
      RowBoundOrdering(1),
      (es, s) => GenerateMutableProjection.generate(es, s),
      conf,
      None,
      tc.taskMemoryManager())
    frame.prepare(array)
    var j = 0
    while (j < rows) {
      frame.write(j, InternalRow.empty)
      j += 1
    }
    frame
  }

  /**
   * Build the given [[WindowSegmentTree]] from a caller-supplied row
   * iterator. Tests historically called `tree.build(iter)` when the tree
   * itself owned an internal buffer; after the ownership flip the caller
   * (here, the helper) materialises an [[ExternalAppendOnlyUnsafeRowArray]],
   * projects rows with [[UnsafeProjection]], and hands the backing array to
   * the tree. The tree retains a reference to the array; the helper does
   * not close it (close is idempotent via `tree.close()` / partition
   * teardown).
   *
   * `inMemoryThreshold` / `spillThreshold` tune the backing array so tests
   * can exercise the spill path (see `WindowSegmentTreeSuite` D9).
   */
  def buildTreeFromIter(
      tree: WindowSegmentTree,
      rows: Iterator[InternalRow],
      inputSchema: Seq[Attribute],
      inMemoryThreshold: Int = Int.MaxValue,
      spillThreshold: Int = Int.MaxValue): Unit = {
    val tc = TaskContext.get()
    require(tc != null, "buildTreeFromIter requires an active TaskContext")
    val env = SparkEnv.get
    val array = new ExternalAppendOnlyUnsafeRowArray(
      tc.taskMemoryManager(),
      env.blockManager,
      env.serializerManager,
      tc,
      1024,
      env.memoryManager.pageSizeBytes,
      inMemoryThreshold,
      Long.MaxValue,
      spillThreshold,
      Long.MaxValue)
    val proj = UnsafeProjection.create(inputSchema.map(_.dataType).toArray)
    rows.foreach(r => array.add(proj(r)))
    tree.build(array)
  }
}
