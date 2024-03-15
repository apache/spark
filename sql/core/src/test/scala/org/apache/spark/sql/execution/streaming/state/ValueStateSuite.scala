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

import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{TimeoutMode, ValueState}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/** A case class for SQL encoder test purpose */
case class TestClass(var id: Long, var name: String)

/**
 * Class that adds tests for single value ValueState types used in arbitrary stateful
 * operators such as transformWithState
 */
class ValueStateSuite extends StateVariableSuiteBase {

  import StateStoreTestsHelper._

  test("Implicit key operations") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]], TimeoutMode.NoTimeouts())

      val stateName = "testState"
      val testState: ValueState[Long] = handle.getValueState[Long]("testState", Encoders.scalaLong)
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
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]], TimeoutMode.NoTimeouts())

      val testState: ValueState[Long] = handle.getValueState[Long]("testState", Encoders.scalaLong)
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
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]], TimeoutMode.NoTimeouts())

      val testState1: ValueState[Long] = handle.getValueState[Long](
        "testState1", Encoders.scalaLong)
      val testState2: ValueState[Long] = handle.getValueState[Long](
        "testState2", Encoders.scalaLong)
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

  test("Value state operations for unsupported type name should fail") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]],
        TimeoutMode.NoTimeouts())

      val cfName = "_testState"
      val ex = intercept[SparkUnsupportedOperationException] {
        handle.getValueState[Long](cfName, Encoders.scalaLong)
      }
      checkError(
        ex,
        errorClass = "STATE_STORE_CANNOT_CREATE_COLUMN_FAMILY_WITH_RESERVED_CHARS",
        parameters = Map(
          "colFamilyName" -> cfName
        ),
        matchPVals = true
      )
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
        "stateStoreProvider" -> "HDFSBackedStateStoreProvider"
      ),
      matchPVals = true
    )
  }

  test("test SQL encoder - Value state operations for Primitive(Double) instances") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]], TimeoutMode.NoTimeouts())

      val testState: ValueState[Double] = handle.getValueState[Double]("testState",
        Encoders.scalaDouble)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.update(1.0)
      assert(testState.get().equals(1.0))
      testState.clear()
      assert(!testState.exists())
      assert(testState.get() === null)

      testState.update(2.0)
      assert(testState.get().equals(2.0))
      testState.update(3.0)
      assert(testState.get().equals(3.0))

      testState.clear()
      assert(!testState.exists())
      assert(testState.get() === null)
    }
  }

  test("test SQL encoder - Value state operations for Primitive(Long) instances") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]], TimeoutMode.NoTimeouts())

      val testState: ValueState[Long] = handle.getValueState[Long]("testState",
        Encoders.scalaLong)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.update(1L)
      assert(testState.get().equals(1L))
      testState.clear()
      assert(!testState.exists())
      assert(testState.get() === null)

      testState.update(2L)
      assert(testState.get().equals(2L))
      testState.update(3L)
      assert(testState.get().equals(3L))

      testState.clear()
      assert(!testState.exists())
      assert(testState.get() === null)
    }
  }

  test("test SQL encoder - Value state operations for case class instances") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]], TimeoutMode.NoTimeouts())

      val testState: ValueState[TestClass] = handle.getValueState[TestClass]("testState",
        Encoders.product[TestClass])
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.update(TestClass(1, "testcase1"))
      assert(testState.get().equals(TestClass(1, "testcase1")))
      testState.clear()
      assert(!testState.exists())
      assert(testState.get() === null)

      testState.update(TestClass(2, "testcase2"))
      assert(testState.get() === TestClass(2, "testcase2"))
      testState.update(TestClass(3, "testcase3"))
      assert(testState.get() === TestClass(3, "testcase3"))

      testState.clear()
      assert(!testState.exists())
      assert(testState.get() === null)
    }
  }

  test("test SQL encoder - Value state operations for POJO instances") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]], TimeoutMode.NoTimeouts())

      val testState: ValueState[POJOTestClass] = handle.getValueState[POJOTestClass]("testState",
        Encoders.bean(classOf[POJOTestClass]))
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.update(new POJOTestClass("testcase1", 1))
      assert(testState.get().equals(new POJOTestClass("testcase1", 1)))
      testState.clear()
      assert(!testState.exists())
      assert(testState.get() === null)

      testState.update(new POJOTestClass("testcase2", 2))
      assert(testState.get().equals(new POJOTestClass("testcase2", 2)))
      testState.update(new POJOTestClass("testcase3", 3))
      assert(testState.get().equals(new POJOTestClass("testcase3", 3)))

      testState.clear()
      assert(!testState.exists())
      assert(testState.get() === null)
    }
  }
}

/**
 * Abstract Base Class that provides test utilities for different state variable
 * types (ValueState, ListState, MapState) used in arbitrary stateful operators.
 */
abstract class StateVariableSuiteBase extends SharedSparkSession
  with BeforeAndAfter {

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    ImplicitGroupingKeyTracker.removeImplicitKey()
    require(ImplicitGroupingKeyTracker.getImplicitKeyOption.isEmpty)
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  import StateStoreTestsHelper._

  protected var schemaForKeyRow: StructType = new StructType().add("key", BinaryType)
  protected var schemaForValueRow: StructType = new StructType().add("value", BinaryType)

  protected def newStoreProviderWithStateVariable(
      useColumnFamilies: Boolean): RocksDBStateStoreProvider = {
    newStoreProviderWithStateVariable(StateStoreId(newDir(), Random.nextInt(), 0),
      numColsPrefixKey = 0,
      useColumnFamilies = useColumnFamilies)
  }

  protected def newStoreProviderWithStateVariable(
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

  protected def tryWithProviderResource[T](
      provider: StateStoreProvider)(f: StateStoreProvider => T): T = {
    try {
      f(provider)
    } finally {
      provider.close()
    }
  }
}

