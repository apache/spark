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

import org.apache.spark.unsafe.memory.{MemoryManager, MemoryAllocator}
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

import org.apache.spark.sql.types._

class UnsafeFixedWidthAggregationMapSuite extends FunSuite with Matchers with BeforeAndAfterEach {

  import UnsafeFixedWidthAggregationMap._

  private val groupKeySchema = StructType(StructField("product", StringType) :: Nil)
  private val aggBufferSchema = StructType(StructField("salePrice", IntegerType) :: Nil)
  private def emptyAggregationBuffer: Row = new GenericRow(Array[Any](0))

  private var memoryManager: MemoryManager = null

  override def beforeEach(): Unit = {
    memoryManager = new MemoryManager(true)
  }

  override def afterEach(): Unit = {
    if (memoryManager != null) {
      memoryManager.cleanUpAllPages()
      memoryManager = null
    }
  }

  test("supported schemas") {
    assert(!supportsAggregationBufferSchema(StructType(StructField("x", StringType) :: Nil)))
    assert(supportsGroupKeySchema(StructType(StructField("x", StringType) :: Nil)))

    assert(
      !supportsAggregationBufferSchema(StructType(StructField("x", ArrayType(IntegerType)) :: Nil)))
    assert(
      !supportsGroupKeySchema(StructType(StructField("x", ArrayType(IntegerType)) :: Nil)))
  }

  test("empty map") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      memoryManager,
      1024,
      false
    )
    assert(!map.iterator().hasNext)
    map.free()
  }

  test("updating values for a single key") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      memoryManager,
      1024,
      false
    )
    val groupKey = new GenericRow(Array[Any](UTF8String("cats")))

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

}
