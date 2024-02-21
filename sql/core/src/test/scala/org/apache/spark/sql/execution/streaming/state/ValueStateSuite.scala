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

import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.ValueState
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Class that adds tests for single value ValueState types used in arbitrary stateful
 * operators such as transformWithState
 */
class ValueStateSuite extends SharedSparkSession
  with BeforeAndAfter {

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  import StateStoreTestsHelper._

  val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)

  val schemaForValueRow: StructType = new StructType().add("value", BinaryType)

  private def newStoreProviderWithValueState(useColumnFamilies: Boolean):
    RocksDBStateStoreProvider = {
    newStoreProviderWithValueState(StateStoreId(newDir(), Random.nextInt(), 0),
      numColsPrefixKey = 0,
      useColumnFamilies = useColumnFamilies)
  }

  private def newStoreProviderWithValueState(
      storeId: StateStoreId,
      numColsPrefixKey: Int,
      sqlConf: SQLConf = SQLConf.get,
      conf: Configuration = new Configuration,
      useColumnFamilies: Boolean = false): RocksDBStateStoreProvider = {
    val provider = new RocksDBStateStoreProvider()
    provider.init(
      storeId, schemaForKeyRow, schemaForValueRow, numColsPrefixKey = numColsPrefixKey,
      useColumnFamilies,
      new StateStoreConf(sqlConf), conf)
    provider
  }

  private def tryWithProviderResource[T](
      provider: StateStoreProvider)(f: StateStoreProvider => T): T = {
    try {
      f(provider)
    } finally {
      provider.close()
    }
  }

  test("Implicit key operations") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val stateName = "testState"
      val testState: ValueState[Long] = handle.getValueState[Long]("testState")
      assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isEmpty)
      val ex = intercept[Exception] {
        testState.update(123)
      }

      assert(ex.isInstanceOf[SparkException])
      checkError(
        ex.asInstanceOf[SparkException],
        errorClass = "INTERNAL_ERROR_TWS",
        parameters = Map(
          "message" -> s"Implicit key not found in state store for stateName=$stateName"
        ),
        matchPVals = true
      )
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isDefined)
      testState.update(123)
      assert(testState.get() === 123)

      ImplicitGroupingKeyTracker.removeImplicitKey()
      assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isEmpty)

      val ex1 = intercept[Exception] {
        testState.update(123)
      }
      checkError(
        ex.asInstanceOf[SparkException],
        errorClass = "INTERNAL_ERROR_TWS",
        parameters = Map(
          "message" -> s"Implicit key not found in state store for stateName=$stateName"
        ),
        matchPVals = true
      )
    }
  }

  test("Value state operations for single instance") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val testState: ValueState[Long] = handle.getValueState[Long]("testState")
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.update(123)
      assert(testState.get() === 123)
      testState.clear()
      assert(!testState.exists())
      assert(testState.get() === null)

      testState.update(456)
      assert(testState.get() === 456)
      assert(testState.get() === 456)
      testState.update(123)
      assert(testState.get() === 123)

      testState.clear()
      assert(!testState.exists())
      assert(testState.get() === null)
    }
  }

  test("Value state operations for multiple instances") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val testState1: ValueState[Long] = handle.getValueState[Long]("testState1")
      val testState2: ValueState[Long] = handle.getValueState[Long]("testState2")
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState1.update(123)
      assert(testState1.get() === 123)
      testState1.clear()
      assert(!testState1.exists())
      assert(testState1.get() === null)

      testState2.update(456)
      assert(testState2.get() === 456)
      testState2.clear()
      assert(!testState2.exists())
      assert(testState2.get() === null)

      testState1.update(456)
      assert(testState1.get() === 456)
      assert(testState1.get() === 456)
      testState1.update(123)
      assert(testState1.get() === 123)

      testState2.update(123)
      assert(testState2.get() === 123)
      assert(testState2.get() === 123)
      testState2.update(456)
      assert(testState2.get() === 456)

      testState1.clear()
      assert(!testState1.exists())
      assert(testState1.get() === null)

      testState2.clear()
      assert(!testState2.exists())
      assert(testState2.get() === null)
    }
  }

  test("colFamily with HDFSBackedStateStoreProvider should fail") {
    val storeId = StateStoreId(newDir(), Random.nextInt(), 0)
    val provider = new HDFSBackedStateStoreProvider()
    val storeConf = new StateStoreConf(new SQLConf())
    val ex = intercept[StateStoreMultipleColumnFamiliesNotSupportedException] {
      provider.init(
        storeId, keySchema, valueSchema, 0, useColumnFamilies = true,
        storeConf, new Configuration)
    }
    checkError(
      ex,
      errorClass = "UNSUPPORTED_FEATURE.STATE_STORE_MULTIPLE_COLUMN_FAMILIES",
      parameters = Map(
        "stateStoreProvider" -> "HDFSStateStoreProvider"
      ),
      matchPVals = true
    )
  }
}
