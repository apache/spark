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

import org.scalatest.{BeforeAndAfterEach, Matchers}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.shuffle.ShuffleMemoryManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.memory.{ExecutorMemoryManager, MemoryAllocator, TaskMemoryManager}
import org.apache.spark.unsafe.types.UTF8String


class UnsafeFixedWidthAggregationMapSuite
  extends SparkFunSuite
  with Matchers
  with BeforeAndAfterEach {

  import UnsafeFixedWidthAggregationMap._

  private val groupKeySchema = StructType(StructField("product", StringType) :: Nil)
  private val aggBufferSchema = StructType(StructField("salePrice", IntegerType) :: Nil)
  private def emptyAggregationBuffer: InternalRow = InternalRow(0)
  private val PAGE_SIZE_BYTES: Long = 1L << 26; // 64 megabytes

  private var taskMemoryManager: TaskMemoryManager = null
  private var shuffleMemoryManager: ShuffleMemoryManager = null

  override def beforeEach(): Unit = {
    taskMemoryManager = new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP))
    shuffleMemoryManager = new ShuffleMemoryManager(Long.MaxValue)
  }

  override def afterEach(): Unit = {
    if (taskMemoryManager != null) {
      val leakedShuffleMemory = shuffleMemoryManager.getMemoryConsumptionForThisTask()
      assert(taskMemoryManager.cleanUpAllAllocatedMemory() === 0)
      assert(leakedShuffleMemory === 0)
      taskMemoryManager = null
    }
  }

  test("supported schemas") {
    assert(supportsAggregationBufferSchema(
      StructType(StructField("x", DecimalType.USER_DEFAULT) :: Nil)))
    assert(!supportsAggregationBufferSchema(
      StructType(StructField("x", DecimalType.SYSTEM_DEFAULT) :: Nil)))
    assert(!supportsAggregationBufferSchema(StructType(StructField("x", StringType) :: Nil)))
    assert(
      !supportsAggregationBufferSchema(StructType(StructField("x", ArrayType(IntegerType)) :: Nil)))
  }

  test("empty map") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      shuffleMemoryManager,
      1024, // initial capacity,
      PAGE_SIZE_BYTES,
      false // disable perf metrics
    )
    assert(!map.iterator().next())
    map.free()
  }

  test("updating values for a single key") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      shuffleMemoryManager,
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

  test("inserting large random keys") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      shuffleMemoryManager,
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

}
