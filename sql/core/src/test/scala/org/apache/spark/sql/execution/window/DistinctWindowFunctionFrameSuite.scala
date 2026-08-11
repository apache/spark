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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, TaskContext, TaskContextImpl}
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, GenericInternalRow}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType}

class DistinctWindowFunctionFrameSuite extends QueryTest with SharedSparkSession {

  private final class TestDistinctWindowFunctionFrame(
      inputAttribute: Attribute,
      spillSize: SQLMetric)
    extends DistinctWindowFunctionFrame(
      target = new GenericInternalRow(1),
      processor = null,
      distinctExpressions = Seq(inputAttribute),
      filter = None,
      inputSchema = Seq(inputAttribute),
      spillSize = spillSize,
      hashFallbackThreshold = Int.MaxValue,
      spillThreshold = Int.MaxValue,
      spillSizeThreshold = Long.MaxValue) {

    def deduplicate(rows: Seq[(UnsafeRow, UnsafeRow)]): Seq[(Int, Int)] = {
      val builder = new FirstVisibleRowsBuilder
      try {
        rows.foreach { case (key, position) => builder.add(key, position) }
        val result = ArrayBuffer.empty[(Int, Int)]
        builder.foreachDistinctRow { (key, position) =>
          result.append((key.getInt(0), position.getInt(1)))
        }
        result.toSeq
      } finally {
        builder.close()
      }
    }

    override protected def populateFirstVisibleRows(
        rows: ExternalAppendOnlyUnsafeRowArray,
        firstVisibleRows: FirstVisibleRowsBuilder): Unit = {}

    override protected def processDistinctRows(
        firstVisibleRows: FirstVisibleRowsBuilder): Unit = {}

    override protected def prepareFrame(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {}

    override def write(index: Int, current: InternalRow): Unit = {}

    override def currentUpperBound(): Int = 0
  }

  private def withAllocationFailures[T](numFailures: Int)(
      body: (TestDistinctWindowFunctionFrame, Seq[(UnsafeRow, UnsafeRow)]) => T): T = {
    val memoryManager = new TestMemoryManager(
      new SparkConf(false).set("spark.buffer.pageSize", "1m"))
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    val previousContext = TaskContext.get()
    TaskContext.setTaskContext(new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      taskAttemptId = 0,
      attemptNumber = 0,
      numPartitions = 1,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = null))

    var frame: TestDistinctWindowFunctionFrame = null
    try {
      val inputAttribute = AttributeReference("key", IntegerType, nullable = false)()
      frame = new TestDistinctWindowFunctionFrame(
        inputAttribute,
        SQLMetrics.createSizeMetric(sparkContext, "spill size"))
      val keyProjection = UnsafeProjection.create(Array[DataType](IntegerType))
      val positionProjection =
        UnsafeProjection.create(Array[DataType](IntegerType, IntegerType))
      def key(value: Int): UnsafeRow =
        keyProjection(new GenericInternalRow(Array[Any](value))).copy()
      def position(firstVisibleIndex: Int, inputIndex: Int): UnsafeRow =
        positionProjection(
          new GenericInternalRow(Array[Any](firstVisibleIndex, inputIndex))).copy()
      val rows = Seq(
        key(1) -> position(0, 0),
        key(1) -> position(1, 1),
        key(2) -> position(2, 2))

      memoryManager.markConsequentOOM(numFailures)
      body(frame, rows)
    } finally {
      if (frame != null) {
        frame.close()
      }
      assert(taskMemoryManager.cleanUpAllAllocatedMemory() === 0L)
      if (previousContext != null) {
        TaskContext.setTaskContext(previousContext)
      } else {
        TaskContext.unset()
      }
    }
  }

  test("fall back to the sorter when BytesToBytesMap construction runs out of memory") {
    withAllocationFailures(1) { (frame, rows) =>
      assert(frame.deduplicate(rows) === Seq(1 -> 0, 2 -> 2))
    }
  }

  test("propagate an OOM when the fallback sorter also cannot allocate memory") {
    withAllocationFailures(2) { (frame, rows) =>
      intercept[SparkOutOfMemoryError] {
        frame.deduplicate(rows)
      }
    }
  }
}
