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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.catalyst.expressions.{Attribute, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.streaming.StatefulOperatorsHelper.StreamingAggregationStateManager
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class StatefulOperatorsHelperSuite extends StreamTest {
  import TestMaterial._

  test("StateManager v1 - get, put, iter") {
    val stateManager = newStateManager(KEYS_ATTRIBUTES, OUTPUT_ATTRIBUTES, 1)

    // in V1, input row is stored as value
    testGetPutIterOnStateManager(stateManager, OUTPUT_ATTRIBUTES, TEST_ROW, TEST_KEY_ROW, TEST_ROW)
  }

  // ============================ StateManagerImplV2 ============================
  test("StateManager v2 - get, put, iter") {
    val stateManager = newStateManager(KEYS_ATTRIBUTES, OUTPUT_ATTRIBUTES, 2)

    // in V2, row for values itself (excluding keys from input row) is stored as value
    // so that stored value doesn't have key part, but state manager V2 will provide same output
    // as V1 when getting row for key
    testGetPutIterOnStateManager(stateManager, VALUES_ATTRIBUTES, TEST_ROW, TEST_KEY_ROW,
      TEST_VALUE_ROW)
  }

  private def newStateManager(
      keysAttributes: Seq[Attribute],
      outputAttributes: Seq[Attribute],
      version: Int): StreamingAggregationStateManager = {
    StreamingAggregationStateManager.createStateManager(keysAttributes, outputAttributes, version)
  }

  private def testGetPutIterOnStateManager(
      stateManager: StreamingAggregationStateManager,
      expectedValueExpressions: Seq[Attribute],
      inputRow: UnsafeRow,
      expectedStateKey: UnsafeRow,
      expectedStateValue: UnsafeRow): Unit = {

    assert(stateManager.getValueExpressions === expectedValueExpressions)

    val memoryStateStore = new MemoryStateStore()
    stateManager.put(memoryStateStore, inputRow)

    assert(memoryStateStore.iterator().size === 1)

    val keyRow = stateManager.extractKey(inputRow)
    assert(keyRow === expectedStateKey)

    // iterate state store and verify whether expected format of key and value are stored
    val pair = memoryStateStore.iterator().next()
    assert(pair.key === keyRow)
    assert(pair.value === expectedStateValue)
    assert(stateManager.restoreOriginRow(pair) === inputRow)

    // verify the stored value once again via get
    assert(memoryStateStore.get(keyRow) === expectedStateValue)

    // state manager should return row which is same as input row regardless of format version
    assert(inputRow === stateManager.get(memoryStateStore, keyRow))
  }

}

object TestMaterial {
  val KEYS: Seq[String] = Seq("key1", "key2")
  val VALUES: Seq[String] = Seq("sum(key1)", "sum(key2)")

  val OUTPUT_SCHEMA: StructType = StructType(
    KEYS.map(createIntegerField) ++ VALUES.map(createIntegerField))

  val OUTPUT_ATTRIBUTES: Seq[Attribute] = OUTPUT_SCHEMA.toAttributes
  val KEYS_ATTRIBUTES: Seq[Attribute] = OUTPUT_ATTRIBUTES.filter { p =>
    KEYS.contains(p.name)
  }
  val VALUES_ATTRIBUTES: Seq[Attribute] = OUTPUT_ATTRIBUTES.filter { p =>
    VALUES.contains(p.name)
  }

  val TEST_ROW: UnsafeRow = {
    val unsafeRowProjection = UnsafeProjection.create(OUTPUT_SCHEMA)
    val row = unsafeRowProjection(new SpecificInternalRow(OUTPUT_SCHEMA))
    (KEYS ++ VALUES).zipWithIndex.foreach { case (_, index) => row.setInt(index, index) }
    row
  }

  val TEST_KEY_ROW: UnsafeRow = {
    val keyProjector = GenerateUnsafeProjection.generate(KEYS_ATTRIBUTES, OUTPUT_ATTRIBUTES)
    keyProjector(TEST_ROW)
  }

  val TEST_VALUE_ROW: UnsafeRow = {
    val valueProjector = GenerateUnsafeProjection.generate(VALUES_ATTRIBUTES, OUTPUT_ATTRIBUTES)
    valueProjector(TEST_ROW)
  }

  private def createIntegerField(name: String): StructField = {
    StructField(name, IntegerType, nullable = false)
  }
}
