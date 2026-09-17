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

package org.apache.spark.sql.execution.python

import org.apache.arrow.c.{ArrowArray, ArrowSchema}
import org.apache.arrow.vector.IntVector

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ArrowColumnVector

class InProcessArrowBridgeSuite extends SparkFunSuite {
  test("CDI import leaves caller-owned array and schema storage open") {
    val allocator = ArrowUtils.rootAllocator
    val before = allocator.getAllocatedMemory
    val input = new IntVector("value", allocator)
    val array = ArrowArray.allocateNew(allocator)
    val schema = ArrowSchema.allocateNew(allocator)
    var result: ArrowColumnVector = null
    try {
      input.allocateNew(1)
      input.setSafe(0, 7)
      input.setValueCount(1)
      val arrayAddress = array.memoryAddress()
      val schemaAddress = schema.memoryAddress()
      InProcessArrowBridge.exportColumn(input, array, schema)
      result = InProcessArrowBridge.cdiToColumn(array, schema)
      assert(result.getInt(0) == 7)
      assert(array.memoryAddress() == arrayAddress)
      assert(schema.memoryAddress() == schemaAddress)
      assert(array.snapshot().release == 0L)
      assert(schema.snapshot().release == 0L)
    } finally {
      if (result != null) result.close()
      array.close()
      schema.close()
      input.close()
    }
    assert(allocator.getAllocatedMemory == before)
  }
}
