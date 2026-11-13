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

package org.apache.spark.sql.execution.aggregate

import java.util.Properties

import scala.util.Random

import org.mockito.Mockito._

import org.apache.spark.{SparkConf, SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.internal.config.MEMORY_OFFHEAP_ENABLED
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{FixedLengthRowBasedKeyValueBatch, Literal, RowBasedKeyValueBatch, VariableLengthRowBasedKeyValueBatch}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodegenContext, CodeGenerator}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{UnsafeFixedWidthAggregationMap, UnsafeKVExternalSorter}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test suite for the static helpers of [[HashAggregateExec]] that its generated code calls into.
 */
class HashAggregateExecSuite extends SparkFunSuite with SharedSparkSession {

  private val groupKeySchema = StructType(StructField("product", StringType) :: Nil)
  private val aggBufferSchema = StructType(StructField("salePrice", IntegerType) :: Nil)
  private val PAGE_SIZE_BYTES: Long = 1L << 26 // 64 megabytes

  /**
   * Runs `f` against a fresh aggregation map, with a task context in place because both the map and
   * the sorter it is destructed into allocate through the task memory manager. Asserts on the way
   * out that the spilled sorters left nothing behind.
   */
  private def withAggregationMap(f: UnsafeFixedWidthAggregationMap => Unit): Unit = {
    val conf = new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false")
    val taskMemoryManager = new TaskMemoryManager(new TestMemoryManager(conf), 0)
    // The map registers a completion listener, which a mock swallows: this test drives the spills
    // directly rather than running a task to completion. The sorter, in turn, reads the task memory
    // manager off the thread-local context, so a real one has to be in place as well.
    val taskContext = mock(classOf[TaskContext])
    when(taskContext.taskMemoryManager()).thenReturn(taskMemoryManager)
    TaskContext.setTaskContext(new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      numPartitions = 1,
      taskAttemptId = Random.nextInt(10000),
      attemptNumber = 0,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = null))

    val map = new UnsafeFixedWidthAggregationMap(
      InternalRow(0), // empty aggregation buffer
      aggBufferSchema,
      groupKeySchema,
      taskContext,
      128, // initial capacity
      PAGE_SIZE_BYTES)

    try {
      f(map)
    } finally {
      map.free()
      TaskContext.unset()
    }
    assert(taskMemoryManager.cleanUpAllAllocatedMemory() === 0)
  }

  /** Inserts `keys` into the map, using each key's length as its aggregation buffer value. */
  private def insert(map: UnsafeFixedWidthAggregationMap, keys: Seq[String]): Unit = {
    keys.foreach { key =>
      val buffer = map.getAggregationBuffer(InternalRow(UTF8String.fromString(key)))
      assert(buffer != null)
      buffer.setInt(0, key.length)
    }
  }

  /**
   * Drains the sorter and returns the keys it held, checking along the way that each key still
   * carries its own aggregation buffer -- draining to exhaustion also releases the sorter's memory.
   */
  private def drainKeys(sorter: UnsafeKVExternalSorter): Seq[String] = {
    val keys = Seq.newBuilder[String]
    val iter = sorter.sortedIterator()
    while (iter.next()) {
      assert(iter.getKey.getString(0).length === iter.getValue.getInt(0))
      keys += iter.getKey.getString(0)
    }
    keys.result()
  }

  test("spillHashMapToSorter destructs the map into a new sorter on the first spill") {
    withAggregationMap { map =>
      val keys = Seq("apple", "banana", "cherry")
      insert(map, keys)

      val sorter = HashAggregateExec.spillHashMapToSorter(map, null)

      assert(sorter != null)
      assert(drainKeys(sorter) === keys.sorted)
    }
  }

  test("spillHashMapToSorter merges into the existing sorter and returns it") {
    withAggregationMap { map =>
      val firstKeys = Seq("apple", "banana", "cherry")
      val secondKeys = Seq("damson", "elderberry", "fig")
      insert(map, firstKeys)
      val sorter = HashAggregateExec.spillHashMapToSorter(map, null)

      // The map keeps accepting keys after being destructed, so the second spill hits the merge
      // branch with a sorter already in hand.
      insert(map, secondKeys)
      val merged = HashAggregateExec.spillHashMapToSorter(map, sorter)

      // The same sorter comes back, now holding the keys of both spills.
      assert(merged eq sorter)
      assert(drainKeys(merged) === (firstKeys ++ secondKeys).sorted)
    }
  }

  test("SPARK-61647: closing a fast hash map drops its buckets") {
    val conf = new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false")
    // A fixed-length key, and a string key, whose batch also keeps key offsets.
    Seq(IntegerType -> classOf[FixedLengthRowBasedKeyValueBatch],
        StringType -> classOf[VariableLengthRowBasedKeyValueBatch]).foreach {
      case (keyType, batchClass) =>
        val taskMemoryManager = new TaskMemoryManager(new TestMemoryManager(conf), 0)
        val keySchema = StructType(StructField("k", keyType) :: Nil)
        val map = new TestFastHashMap(keySchema, aggBufferSchema, taskMemoryManager)
        assert(map.currentBuckets.length === 2048)
        assert(map.currentBatch.getClass === batchClass)
        // The task-completion hook that also closes the map keeps it reachable until the task
        // ends, where a task can run many partitions, each with its own map.
        map.close()
        assert(map.currentBuckets === null)
        assert(taskMemoryManager.cleanUpAllAllocatedMemory() === 0L)
        // The hook closes the map again after its output has closed it.
        map.close()
    }
  }

  test("SPARK-61647: closing a generated vectorized fast hash map drops its buckets") {
    val ctx = new CodegenContext
    val count = Count(Literal(1)).toAggregateExpression()
    val mapCode = new VectorizedHashMapGenerator(ctx, Seq(count), "FastHashMap",
      StructType(StructField("k", IntegerType) :: Nil),
      DataTypeUtils.fromAttributes(count.aggregateFunction.aggBufferAttributes),
      bitMaxCapacity = 10).generate()
    val code = new CodeAndComment(
      s"""
         |public Object generate(Object[] references) {
         |  return new Maps(references);
         |}
         |
         |class Maps {
         |  private Object[] references;
         |  ${ctx.declareMutableStates()}
         |
         |  public Maps(Object[] references) {
         |    this.references = references;
         |    ${ctx.initMutableStates()}
         |  }
         |
         |  public java.lang.AutoCloseable newMap() {
         |    return new FastHashMap();
         |  }
         |
         |  ${ctx.declareAddedFunctions()}
         |
         |  $mapCode
         |}
       """.stripMargin, ctx.getPlaceHolderToComments())
    val maps = CodeGenerator.compile(code)._1.generate(ctx.references.toArray)
    val newMap = maps.getClass.getMethod("newMap")
    newMap.setAccessible(true)
    val map = newMap.invoke(maps).asInstanceOf[AutoCloseable]
    val buckets = map.getClass.getDeclaredField("buckets")
    buckets.setAccessible(true)
    assert(buckets.get(map).asInstanceOf[Array[Int]].length === 2048)
    map.close()
    assert(buckets.get(map) === null)
    // The hook closes the map again after its output has closed it.
    map.close()
  }
}

/** A fast hash map of 1,024 rows that exposes its buckets and batch. */
private class TestFastHashMap(
    keySchema: StructType,
    valueSchema: StructType,
    taskMemoryManager: TaskMemoryManager)
  extends RowBasedAggregateHashMap(
    keySchema, valueSchema, taskMemoryManager, InternalRow(0), 1024, keySchema.length, 0) {
  def currentBuckets: Array[Int] = buckets
  def currentBatch: RowBasedKeyValueBatch = batch
}
