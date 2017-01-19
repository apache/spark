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

package org.apache.spark.sql.execution

import java.util.Properties

import scala.collection.mutable
import scala.util.{Random, Try}
import scala.util.control.NonFatal

import org.scalatest.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test suite for [[UnsafeFixedWidthAggregationMap]].
 *
 * Use [[testWithMemoryLeakDetection]] rather than [[test]] to construct test cases.
 */
class UnsafeFixedWidthAggregationMapSuite
  extends SparkFunSuite
  with Matchers
  with SharedSQLContext {

  import UnsafeFixedWidthAggregationMap._

  private val groupKeySchema = StructType(StructField("product", StringType) :: Nil)
  private val aggBufferSchema = StructType(StructField("salePrice", IntegerType) :: Nil)
  private def emptyAggregationBuffer: InternalRow = InternalRow(0)
  private val PAGE_SIZE_BYTES: Long = 1L << 26; // 64 megabytes

  private var memoryManager: TestMemoryManager = null
  private var taskMemoryManager: TaskMemoryManager = null

  def testWithMemoryLeakDetection(name: String)(f: => Unit) {
    def cleanup(): Unit = {
      if (taskMemoryManager != null) {
        assert(taskMemoryManager.cleanUpAllAllocatedMemory() === 0)
        taskMemoryManager = null
      }
      TaskContext.unset()
    }

    test(name) {
      val conf = new SparkConf().set("spark.memory.offHeap.enabled", "false")
      memoryManager = new TestMemoryManager(conf)
      taskMemoryManager = new TaskMemoryManager(memoryManager, 0)

      TaskContext.setTaskContext(new TaskContextImpl(
        stageId = 0,
        partitionId = 0,
        taskAttemptId = Random.nextInt(10000),
        attemptNumber = 0,
        taskMemoryManager = taskMemoryManager,
        localProperties = new Properties,
        metricsSystem = null))

      try {
        f
      } catch {
        case NonFatal(e) =>
          Try(cleanup())
          throw e
      }
      cleanup()
    }
  }

  private def randomStrings(n: Int): Seq[String] = {
    val rand = new Random(42)
    Seq.fill(512) {
      Seq.fill(rand.nextInt(100))(rand.nextPrintableChar()).mkString
    }.distinct
  }

  testWithMemoryLeakDetection("supported schemas") {
    assert(supportsAggregationBufferSchema(
      StructType(StructField("x", DecimalType.USER_DEFAULT) :: Nil)))
    assert(supportsAggregationBufferSchema(
      StructType(StructField("x", DecimalType.SYSTEM_DEFAULT) :: Nil)))
    assert(!supportsAggregationBufferSchema(StructType(StructField("x", StringType) :: Nil)))
    assert(
      !supportsAggregationBufferSchema(StructType(StructField("x", ArrayType(IntegerType)) :: Nil)))
  }

  testWithMemoryLeakDetection("empty map") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      1024, // initial capacity,
      PAGE_SIZE_BYTES,
      false // disable perf metrics
    )
    assert(!map.iterator().next())
    map.free()
  }

  testWithMemoryLeakDetection("updating values for a single key") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      1024, // initial capacity
      PAGE_SIZE_BYTES,
      false // disable perf metrics
    )
    val groupKey = InternalRow(UTF8String.fromString("cats"))

    // Looking up a key stores a zero-entry in the map (like Python Counters or DefaultDicts)
    assert(map.getAggregationBuffer(groupKey) != null)
    val iter = map.iterator()
    assert(iter.next())
    iter.getKey.getString(0) should be ("cats")
    iter.getValue.getInt(0) should be (0)
    assert(!iter.next())

    // Modifications to rows retrieved from the map should update the values in the map
    iter.getValue.setInt(0, 42)
    map.getAggregationBuffer(groupKey).getInt(0) should be (42)

    map.free()
  }

  testWithMemoryLeakDetection("inserting large random keys") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      128, // initial capacity
      PAGE_SIZE_BYTES,
      false // disable perf metrics
    )
    val rand = new Random(42)
    val groupKeys: Set[String] = Seq.fill(512)(rand.nextString(1024)).toSet
    groupKeys.foreach { keyString =>
      assert(map.getAggregationBuffer(InternalRow(UTF8String.fromString(keyString))) != null)
    }

    val seenKeys = new mutable.HashSet[String]
    val iter = map.iterator()
    while (iter.next()) {
      seenKeys += iter.getKey.getString(0)
    }
    assert(seenKeys.size === groupKeys.size)
    assert(seenKeys === groupKeys)
    map.free()
  }

  testWithMemoryLeakDetection("test external sorting") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      128, // initial capacity
      PAGE_SIZE_BYTES,
      false // disable perf metrics
    )

    val keys = randomStrings(1024).take(512)
    keys.foreach { keyString =>
      val buf = map.getAggregationBuffer(InternalRow(UTF8String.fromString(keyString)))
      buf.setInt(0, keyString.length)
      assert(buf != null)
    }
    val sorter = map.destructAndCreateExternalSorter()

    // Add more keys to the sorter and make sure the results come out sorted.
    val additionalKeys = randomStrings(1024)
    additionalKeys.zipWithIndex.foreach { case (str, i) =>
      val buf = map.getAggregationBuffer(InternalRow(UTF8String.fromString(str)))
      buf.setInt(0, str.length)

      if ((i % 100) == 0) {
        val sorter2 = map.destructAndCreateExternalSorter()
        sorter.merge(sorter2)
      }
    }
    val sorter2 = map.destructAndCreateExternalSorter()
    sorter.merge(sorter2)

    val out = new scala.collection.mutable.ArrayBuffer[String]
    val iter = sorter.sortedIterator()
    while (iter.next()) {
      // At here, we also test if copy is correct.
      val key = iter.getKey.copy()
      val value = iter.getValue.copy()
      assert(key.getString(0).length === value.getInt(0))
      out += key.getString(0)
    }

    assert(out === (keys ++ additionalKeys).sorted)
    map.free()
  }

  testWithMemoryLeakDetection("test external sorting with an empty map") {

    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      128, // initial capacity
      PAGE_SIZE_BYTES,
      false // disable perf metrics
    )
    val sorter = map.destructAndCreateExternalSorter()

    // Add more keys to the sorter and make sure the results come out sorted.
    val additionalKeys = randomStrings(1024)
    additionalKeys.zipWithIndex.foreach { case (str, i) =>
      val buf = map.getAggregationBuffer(InternalRow(UTF8String.fromString(str)))
      buf.setInt(0, str.length)

      if ((i % 100) == 0) {
        val sorter2 = map.destructAndCreateExternalSorter()
        sorter.merge(sorter2)
      }
    }
    val sorter2 = map.destructAndCreateExternalSorter()
    sorter.merge(sorter2)

    val out = new scala.collection.mutable.ArrayBuffer[String]
    val iter = sorter.sortedIterator()
    while (iter.next()) {
      // At here, we also test if copy is correct.
      val key = iter.getKey.copy()
      val value = iter.getValue.copy()
      assert(key.getString(0).length === value.getInt(0))
      out += key.getString(0)
    }

    assert(out === additionalKeys.sorted)
    map.free()
  }

  testWithMemoryLeakDetection("test external sorting with empty records") {

    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      StructType(Nil),
      StructType(Nil),
      taskMemoryManager,
      128, // initial capacity
      PAGE_SIZE_BYTES,
      false // disable perf metrics
    )
    (1 to 10).foreach { i =>
      val buf = map.getAggregationBuffer(UnsafeRow.createFromByteArray(0, 0))
      assert(buf != null)
    }

    // Convert the map into a sorter. Right now, it contains one record.
    val sorter = map.destructAndCreateExternalSorter()

    // Add more keys to the sorter and make sure the results come out sorted.
    (1 to 4096).foreach { i =>
      map.getAggregationBufferFromUnsafeRow(UnsafeRow.createFromByteArray(0, 0))

      if ((i % 100) == 0) {
        val sorter2 = map.destructAndCreateExternalSorter()
        sorter.merge(sorter2)
      }
    }
    val sorter2 = map.destructAndCreateExternalSorter()
    sorter.merge(sorter2)

    var count = 0
    val iter = sorter.sortedIterator()
    while (iter.next()) {
      // At here, we also test if copy is correct.
      iter.getKey.copy()
      iter.getValue.copy()
      count += 1
    }

    // 1 record per map, spilled 42 times.
    assert(count === 42)
    map.free()
  }

  testWithMemoryLeakDetection("convert to external sorter under memory pressure (SPARK-10474)") {
    val pageSize = 4096
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      128, // initial capacity
      pageSize,
      false // disable perf metrics
    )

    val rand = new Random(42)
    for (i <- 1 to 100) {
      val str = rand.nextString(1024)
      val buf = map.getAggregationBuffer(InternalRow(UTF8String.fromString(str)))
      buf.setInt(0, str.length)
    }
    // Simulate running out of space
    memoryManager.limit(0)
    val str = rand.nextString(1024)
    val buf = map.getAggregationBuffer(InternalRow(UTF8String.fromString(str)))
    assert(buf == null)

    // Convert the map into a sorter. This used to fail before the fix for SPARK-10474
    // because we would try to acquire space for the in-memory sorter pointer array before
    // actually releasing the pages despite having spilled all of them.
    var sorter: UnsafeKVExternalSorter = null
    try {
      sorter = map.destructAndCreateExternalSorter()
      map.free()
    } finally {
      if (sorter != null) {
        sorter.cleanupResources()
      }
    }
  }

}
