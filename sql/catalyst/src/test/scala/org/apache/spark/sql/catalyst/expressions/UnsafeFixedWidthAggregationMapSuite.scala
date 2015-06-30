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

package org.apache.spark.sql.catalyst.expressions

import scala.collection.JavaConverters._
import scala.util.Random

import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateProjection
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.memory.{ExecutorMemoryManager, MemoryAllocator, TaskMemoryManager}
import org.apache.spark.unsafe.types.UTF8String


class UnsafeFixedWidthAggregationMapSuite
  extends SparkFunSuite
  with Matchers
  with BeforeAndAfterEach {

  private val groupKeySchema = StructType(StructField("product", StringType) :: Nil)
  private val aggBufferSchema = StructType(StructField("salePrice", IntegerType) :: Nil)
  private def emptyProjection: Projection =
    GenerateProjection.generate(Seq(Literal(0)), Seq(AttributeReference("price", IntegerType)()))
  private def emptyAggregationBuffer: InternalRow = InternalRow(0)

  private var memoryManager: TaskMemoryManager = null

  override def beforeEach(): Unit = {
    memoryManager = new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP))
  }

  override def afterEach(): Unit = {
    if (memoryManager != null) {
      memoryManager.cleanUpAllAllocatedMemory()
      memoryManager = null
    }
  }

  test("empty map") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyProjection,
      new UnsafeRowConverter(groupKeySchema),
      new UnsafeRowConverter(aggBufferSchema),
      memoryManager,
      1024, // initial capacity
      false // disable perf metrics
    )
    assert(!map.iterator().hasNext)
    map.free()
  }

  test("updating values for a single key") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyProjection,
      new UnsafeRowConverter(groupKeySchema),
      new UnsafeRowConverter(aggBufferSchema),
      memoryManager,
      1024, // initial capacity
      false // disable perf metrics
    )
    val groupKey = InternalRow(UTF8String.fromString("cats"))

    // Looking up a key stores a zero-entry in the map (like Python Counters or DefaultDicts)
    map.getAggregationBuffer(groupKey)
    val iter = map.iterator()
    val entry = iter.next()
    assert(!iter.hasNext)
    entry.key.getString(0) should be ("cats")
    entry.value.getInt(0) should be (0)

    // Modifications to rows retrieved from the map should update the values in the map
    entry.value.setInt(0, 42)
    map.getAggregationBuffer(groupKey).getInt(0) should be (42)

    map.free()
  }

  test("inserting large random keys") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyProjection,
      new UnsafeRowConverter(groupKeySchema),
      new UnsafeRowConverter(aggBufferSchema),
      memoryManager,
      128, // initial capacity
      false // disable perf metrics
    )
    val rand = new Random(42)
    val groupKeys: Set[String] = Seq.fill(512)(rand.nextString(1024)).toSet
    groupKeys.foreach { keyString =>
      map.getAggregationBuffer(InternalRow(UTF8String.fromString(keyString)))
    }
    val seenKeys: Set[String] = map.iterator().asScala.map { entry =>
      entry.key.getString(0)
    }.toSet
    seenKeys.size should be (groupKeys.size)
    seenKeys should be (groupKeys)

    map.free()
  }

  test("with decimal in the key and values") {
    val groupKeySchema = StructType(StructField("price", DecimalType(10, 0)) :: Nil)
    val aggBufferSchema = StructType(StructField("amount", DecimalType.Unlimited) :: Nil)
    val emptyProjection = GenerateProjection.generate(Seq(Literal(Decimal(0))),
      Seq(AttributeReference("price", DecimalType.Unlimited)()))
    val map = new UnsafeFixedWidthAggregationMap(
      emptyProjection,
      new UnsafeRowConverter(groupKeySchema),
      new UnsafeRowConverter(aggBufferSchema),
      memoryManager,
      1, // initial capacity
      false // disable perf metrics
    )

    (0 until 100).foreach { i =>
      val groupKey = InternalRow(Decimal(i % 10))
      val row = map.getAggregationBuffer(groupKey)
      row.update(0, Decimal(i))
    }
    val seenKeys: Set[Int] = map.iterator().asScala.map { entry =>
      entry.key.getAs[Decimal](0).toInt
    }.toSet
    seenKeys.size should be (10)
    seenKeys should be ((0 until 10).toSet)

    map.free()
  }

}
