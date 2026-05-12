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
 */
private[window] object SegmentTreeWindowTestHelper {

  /**
   * Fake-TaskContext plumbing for lifecycle tests. Gives `body` a factory
   * producing `newArray(rows)` and `newFrame()` (unprepared — caller drives
   * prepare/write/close). Tracks frames so teardown always closes them.
   */
  def withFrameFactory(conf: SQLConf)
      (body: FrameFactory => Unit): Unit = {
    val existing = TaskContext.get()
    val owned = existing == null
    val tc = if (owned) {
      val fake = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
      TaskContext.setTaskContext(fake)
      fake
    } else existing
    val factory = new FrameFactory(conf, tc)
    try body(factory) finally {
      factory.closeAll()
      if (owned) TaskContext.unset()
    }
  }

  final class FrameFactory(conf: SQLConf, tc: TaskContext) {
    private val attr = AttributeReference("v", IntegerType, nullable = false)()
    private val input = Seq[Attribute](attr)
    private val fn = Sum(attr)
    private val bufAttrs = fn.aggBufferAttributes
    private val processor = AggregateProcessor(
      Array[Expression](fn),
      0,
      input,
      (es, s) => GenerateMutableProjection.generate(es, s),
      Array[Option[Expression]](None))
    private val unsafeProj = UnsafeProjection.create(Array(attr.dataType))
    private val opened = scala.collection.mutable.Buffer[AutoCloseable]()

    def newArray(rows: Int): ExternalAppendOnlyUnsafeRowArray = {
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
      var i = 0
      while (i < rows) {
        array.add(unsafeProj(new GenericInternalRow(Array[Any](i))))
        i += 1
      }
      array
    }

    /** Create a new frame. Caller owns lifecycle unless tracked via `track()`. */
    def newFrame(): SegmentTreeWindowFunctionFrame = {
      val target = new SpecificInternalRow(Seq(bufAttrs.head.dataType))
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
      track(frame)
      frame
    }

    def track[T <: AutoCloseable](c: T): T = { opened += c; c }

    def closeAll(): Unit = {
      opened.foreach { c =>
        try c.close() catch { case _: Throwable => () }
      }
      opened.clear()
    }
  }

  /**
   * Build a small-partition [[SegmentTreeWindowFunctionFrame]] (Sum over one
   * IntegerType column, values 0..rows-1, frame -1..+1), drive it through
   * all rows, then hand it to `body`. Manages a fake TaskContext if needed
   * and always closes the frame.
   */
  def withSmallPartitionFrame(conf: SQLConf, rows: Int)
      (body: SegmentTreeWindowFunctionFrame => Unit): Unit = {
    withFrameFactory(conf) { factory =>
      val frame = factory.newFrame()
      val array = factory.newArray(rows)
      frame.prepare(array)
      var j = 0
      while (j < rows) {
        frame.write(j, InternalRow.empty)
        j += 1
      }
      body(frame)
    }
  }

  /**
   * Build the given [[WindowSegmentTree]] from a row iterator. After the
   * buffer-ownership flip the caller (this helper) materialises an
   * [[ExternalAppendOnlyUnsafeRowArray]], projects rows via
   * [[UnsafeProjection]], and hands it to the tree. The tree retains the
   * array; close is idempotent via `tree.close()` / partition teardown.
   *
   * `inMemoryThreshold` / `spillThreshold` let tests exercise the spill
   * path (see `WindowSegmentTreeSuite` D9).
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
