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
import org.apache.spark.sql.catalyst.expressions.{
  Attribute, AttributeReference, Expression, GenericInternalRow, MutableProjection}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType}

class DistinctWindowFunctionFrameSuite extends QueryTest with SharedSparkSession {

  private final class TestDistinctWindowFunctionFrame(
      inputAttribute: Attribute,
      spillSize: SQLMetric,
      spillSizeThreshold: Long = Long.MaxValue)
    extends DistinctWindowFunctionFrame(
      target = new GenericInternalRow(1),
      processor = null,
      distinctExpressions = Seq(inputAttribute),
      filter = None,
      inputSchema = Seq(inputAttribute),
      spillSize = spillSize,
      hashFallbackThreshold = Int.MaxValue,
      spillThreshold = Int.MaxValue,
      spillSizeThreshold = spillSizeThreshold) {

    def deduplicate(rows: Seq[(UnsafeRow, UnsafeRow)]): Seq[(Int, Int)] = {
      deduplicateAndGetSpillSize(rows)._1
    }

    def deduplicateAndGetSpillSize(
        rows: Seq[(UnsafeRow, UnsafeRow)]): (Seq[(Int, Int)], Long) = {
      val builder = new FirstVisibleRowsBuilder
      try {
        rows.foreach { case (key, position) => builder.add(key, position) }
        val result = ArrayBuffer.empty[(Int, Int)]
        builder.foreachDistinctRow { (key, position) =>
          result.append((key.getInt(0), position.getInt(1)))
        }
        result.toSeq -> builder.getSpillSize
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

  private final class CountingCurrentRowBoundOrdering extends BoundOrdering {
    var numComparisons: Int = 0

    def reset(): Unit = numComparisons = 0

    override def compare(
        inputRow: InternalRow,
        inputIndex: Int,
        outputRow: InternalRow,
        outputIndex: Int): Int = {
      numComparisons += 1
      Integer.compare(inputRow.getInt(0), outputRow.getInt(0))
    }
  }

  private def withTaskMemoryManager[T](
      body: (TestMemoryManager, TaskMemoryManager) => T): T = {
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

    try {
      body(memoryManager, taskMemoryManager)
    } finally {
      assert(taskMemoryManager.cleanUpAllAllocatedMemory() === 0L)
      if (previousContext != null) {
        TaskContext.setTaskContext(previousContext)
      } else {
        TaskContext.unset()
      }
    }
  }

  private def withAllocationFailures[T](numFailures: Int)(
      body: (TestDistinctWindowFunctionFrame, Seq[(UnsafeRow, UnsafeRow)]) => T): T = {
    withTaskMemoryManager { (memoryManager, _) =>
      val inputAttribute = AttributeReference("key", IntegerType, nullable = false)()
      val frame = new TestDistinctWindowFunctionFrame(
        inputAttribute,
        SQLMetrics.createSizeMetric(sparkContext, "spill size"))
      try {
        memoryManager.markConsequentOOM(numFailures)
        body(frame, distinctRows())
      } finally {
        frame.close()
      }
    }
  }

  private def distinctRows(): Seq[(UnsafeRow, UnsafeRow)] = {
    val keyProjection = UnsafeProjection.create(Array[DataType](IntegerType))
    val positionProjection = UnsafeProjection.create(Array[DataType](IntegerType, IntegerType))
    def key(value: Int): UnsafeRow =
      keyProjection(new GenericInternalRow(Array[Any](value))).copy()
    def position(firstVisibleIndex: Int, inputIndex: Int): UnsafeRow =
      positionProjection(
        new GenericInternalRow(Array[Any](firstVisibleIndex, inputIndex))).copy()
    Seq(
      key(1) -> position(0, 0),
      key(1) -> position(1, 1),
      key(2) -> position(2, 2))
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

  test("spill the distinct-key sorter after size-based fallback") {
    withTaskMemoryManager { (_, _) =>
      val inputAttribute = AttributeReference("key", IntegerType, nullable = false)()
      val frame = new TestDistinctWindowFunctionFrame(
        inputAttribute,
        SQLMetrics.createSizeMetric(sparkContext, "spill size"),
        spillSizeThreshold = 1L)
      try {
        val (result, spillSize) = frame.deduplicateAndGetSpillSize(distinctRows())
        assert(result === Seq(1 -> 0, 2 -> 2))
        assert(spillSize > 0L)
      } finally {
        frame.close()
      }
    }
  }

  test("track range upper bounds lazily across partitions") {
    withTaskMemoryManager { (_, _) =>
      val inputAttribute = AttributeReference("value", IntegerType, nullable = false)()
      val target = new GenericInternalRow(1)
      val processor = AggregateProcessor(
        Array[Expression](Count(Seq(inputAttribute))),
        ordinal = 0,
        inputAttributes = Seq(inputAttribute),
        (expressions, schema) => MutableProjection.create(expressions, schema),
        filters = Array[Option[Expression]](None))
      val upperBound = new CountingCurrentRowBoundOrdering
      val frame = new UnboundedPrecedingDistinctWindowFunctionFrame(
        target,
        processor,
        distinctExpressions = Seq(inputAttribute),
        filter = None,
        inputSchema = Seq(inputAttribute),
        upperBound,
        SQLMetrics.createSizeMetric(sparkContext, "spill size"),
        hashFallbackThreshold = Int.MaxValue,
        spillThreshold = Int.MaxValue,
        spillSizeThreshold = Long.MaxValue)
      val rows = new ExternalAppendOnlyUnsafeRowArray(
        Int.MaxValue,
        Long.MaxValue,
        Int.MaxValue,
        Long.MaxValue)
      val projection = UnsafeProjection.create(Array[DataType](IntegerType))

      def addRows(values: Seq[Int]): Unit = {
        values.foreach { value =>
          rows.add(projection(new GenericInternalRow(Array[Any](value))))
        }
      }

      try {
        addRows(Seq(1, 1, 3))
        frame.prepare(rows)
        upperBound.reset()

        val firstPartition = rows.generateIterator()
        frame.write(0, firstPartition.next())
        assert(target.getLong(0) === 1L)
        assert(upperBound.numComparisons === 0)
        assert(frame.currentUpperBound() === 2)
        assert(upperBound.numComparisons === 3)

        frame.write(1, firstPartition.next())
        assert(target.getLong(0) === 1L)
        assert(upperBound.numComparisons === 3)
        assert(frame.currentUpperBound() === 2)
        assert(upperBound.numComparisons === 4)

        frame.write(2, firstPartition.next())
        assert(target.getLong(0) === 2L)
        assert(upperBound.numComparisons === 4)
        assert(frame.currentUpperBound() === 3)
        assert(upperBound.numComparisons === 5)

        rows.clear()
        addRows(Seq(2, 4))
        frame.prepare(rows)
        upperBound.reset()

        val secondPartition = rows.generateIterator()
        frame.write(0, secondPartition.next())
        assert(target.getLong(0) === 1L)
        assert(upperBound.numComparisons === 0)
        assert(frame.currentUpperBound() === 1)
        assert(upperBound.numComparisons === 2)

        frame.write(1, secondPartition.next())
        assert(target.getLong(0) === 2L)
        assert(upperBound.numComparisons === 2)
        assert(frame.currentUpperBound() === 2)
        assert(upperBound.numComparisons === 3)
      } finally {
        frame.close()
        rows.clear()
      }
    }
  }
}
