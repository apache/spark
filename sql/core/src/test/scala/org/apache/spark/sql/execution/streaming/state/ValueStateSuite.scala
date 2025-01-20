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

import java.time.Duration
import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl, ValueStateImplWithTTL}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{TimeMode, TTLConfig, ValueState}
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
  import testImplicits._

  test("Implicit key operations") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val stateName = "testState"
      val testState: ValueState[Long] = handle.getValueState[Long]("testState",
        TTLConfig.NONE)
      assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isEmpty)
      val ex = intercept[Exception] {
        testState.update(123)
      }

      assert(ex.isInstanceOf[SparkException])
      checkError(
        ex.asInstanceOf[SparkException],
        condition = "INTERNAL_ERROR_TWS",
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
        ex1.asInstanceOf[SparkException],
        condition = "INTERNAL_ERROR_TWS",
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
        stringEncoder, TimeMode.None())

      val testState: ValueState[Long] = handle.getValueState[Long]("testState",
        TTLConfig.NONE)
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
        stringEncoder, TimeMode.None())

      val testState1: ValueState[Long] = handle.getValueState[Long](
        "testState1", TTLConfig.NONE)
      val testState2: ValueState[Long] = handle.getValueState[Long](
        "testState2", TTLConfig.NONE)
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
        UUID.randomUUID(), stringEncoder, TimeMode.None())

      val cfName = "$testState"
      val ex = intercept[SparkUnsupportedOperationException] {
        handle.getValueState[Long](cfName, TTLConfig.NONE)
      }
      checkError(
        ex,
        condition = "STATE_STORE_CANNOT_CREATE_COLUMN_FAMILY_WITH_RESERVED_CHARS",
        parameters = Map(
          "colFamilyName" -> cfName
        ),
        matchPVals = false
      )
    }
  }

  test("colFamily with HDFSBackedStateStoreProvider should fail") {
    val storeId = StateStoreId(newDir(), Random.nextInt(), 0)
    val provider = new HDFSBackedStateStoreProvider()
    val storeConf = new StateStoreConf(new SQLConf())
    val ex = intercept[StateStoreMultipleColumnFamiliesNotSupportedException] {
      provider.init(
        storeId, keySchema, valueSchema, NoPrefixKeyStateEncoderSpec(keySchema),
        useColumnFamilies = true, storeConf, new Configuration)
    }
    checkError(
      ex,
      condition = "UNSUPPORTED_FEATURE.STATE_STORE_MULTIPLE_COLUMN_FAMILIES",
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
        stringEncoder, TimeMode.None())

      val testState: ValueState[Double] = handle.getValueState[Double]("testState",
        Encoders.scalaDouble, TTLConfig.NONE)
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
        stringEncoder, TimeMode.None())

      val testState: ValueState[Long] = handle.getValueState[Long]("testState",
        TTLConfig.NONE)
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
        stringEncoder, TimeMode.None())

      val testState: ValueState[TestClass] = handle.getValueState[TestClass]("testState",
        TTLConfig.NONE)
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
        stringEncoder, TimeMode.None())

      val testState: ValueState[POJOTestClass] = handle.getValueState[POJOTestClass]("testState",
         Encoders.bean(classOf[POJOTestClass]), TTLConfig.NONE)
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

  test(s"test Value state TTL") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val timestampMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.ProcessingTime(),
        batchTimestampMs = Some(timestampMs))

      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val testState: ValueStateImplWithTTL[String] = handle.getValueState[String]("testState",
        Encoders.STRING, ttlConfig).asInstanceOf[ValueStateImplWithTTL[String]]
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.update("v1")
      assert(testState.get() === "v1")
      assert(testState.getWithoutEnforcingTTL().get === "v1")

      val ttlExpirationMs = timestampMs + 60000
      var ttlValue = testState.getTTLValue()
      assert(ttlValue.isDefined)
      assert(ttlValue.get._2 === ttlExpirationMs)
      var ttlStateValueIterator = testState.getValueInTTLState()
      assert(ttlStateValueIterator.isDefined)

      // increment batchProcessingTime, or watermark and ensure expired value is not returned
      val nextBatchHandle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder,
        TimeMode.ProcessingTime(), batchTimestampMs = Some(ttlExpirationMs))

      val nextBatchTestState: ValueStateImplWithTTL[String] =
        nextBatchHandle.getValueState[String]("testState", Encoders.STRING, ttlConfig)
          .asInstanceOf[ValueStateImplWithTTL[String]]

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      // ensure get does not return the expired value
      assert(!nextBatchTestState.exists())
      assert(nextBatchTestState.get() === null)

      // ttl value should still exist in state
      ttlValue = nextBatchTestState.getTTLValue()
      assert(ttlValue.isDefined)
      assert(ttlValue.get._2 === ttlExpirationMs)
      ttlStateValueIterator = nextBatchTestState.getValueInTTLState()
      assert(ttlStateValueIterator.isDefined)
      assert(ttlStateValueIterator.get === ttlExpirationMs)

      // getWithoutTTL should still return the expired value
      assert(nextBatchTestState.getWithoutEnforcingTTL().get === "v1")

      nextBatchTestState.clear()
      assert(!nextBatchTestState.exists())
      assert(nextBatchTestState.get() === null)
    }
  }

  test("test null or zero TTL duration throws error") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val batchTimestampMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder,
        TimeMode.ProcessingTime(), batchTimestampMs = Some(batchTimestampMs))

      Seq(null, Duration.ofMinutes(-1)).foreach { ttlDuration =>
        val ttlConfig = TTLConfig(ttlDuration)
        val ex = intercept[SparkUnsupportedOperationException] {
          handle.getValueState[String]("testState", Encoders.STRING, ttlConfig)
        }

        checkError(
          ex,
          condition = "STATEFUL_PROCESSOR_TTL_DURATION_MUST_BE_POSITIVE",
          parameters = Map(
            "operationType" -> "update",
            "stateName" -> "testState"
          ),
          matchPVals = true
        )
      }
    }
  }

  test("Value state TTL with non-primitive type") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val timestampMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        encoderFor(Encoders.product[TestClass]).asInstanceOf[ExpressionEncoder[Any]],
        TimeMode.ProcessingTime(), batchTimestampMs = Some(timestampMs))

      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val testState: ValueStateImplWithTTL[POJOTestClass] =
        handle.getValueState[POJOTestClass]("testState",
        Encoders.bean(classOf[POJOTestClass]), ttlConfig)
          .asInstanceOf[ValueStateImplWithTTL[POJOTestClass]]
      ImplicitGroupingKeyTracker.setImplicitKey(TestClass(1L, "k1"))
      testState.update(new POJOTestClass("n1", 1))
      assert(testState.get() === new POJOTestClass("n1", 1))
      assert(testState.getWithoutEnforcingTTL().get === new POJOTestClass("n1", 1))

      val ttlExpirationMs = timestampMs + 60000
      val ttlValue = testState.getTTLValue()
      assert(ttlValue.isDefined)
      assert(ttlValue.get._2 === ttlExpirationMs)
      val ttlStateValueIterator = testState.getValueInTTLState()
      assert(ttlStateValueIterator.isDefined)
    }
  }
}

/**
 * Abstract Base Class that provides test utilities for different state variable
 * types (ValueState, ListState, MapState) used in arbitrary stateful operators.
 */
abstract class StateVariableSuiteBase extends SharedSparkSession
  with BeforeAndAfter with AlsoTestWithEncodingTypes {

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

  protected val stringEncoder = encoderFor(Encoders.STRING).asInstanceOf[ExpressionEncoder[Any]]

  // dummy schema for initializing rocksdb provider
  protected def schemaForKeyRow: StructType = new StructType().add("key", BinaryType)
  protected def schemaForValueRow: StructType = new StructType().add("value", BinaryType)

  protected def useMultipleValuesPerKey = false

  protected def newStoreProviderWithStateVariable(
      useColumnFamilies: Boolean): RocksDBStateStoreProvider = {
    newStoreProviderWithStateVariable(StateStoreId(newDir(), Random.nextInt(), 0),
      NoPrefixKeyStateEncoderSpec(schemaForKeyRow),
      useColumnFamilies = useColumnFamilies)
  }

  protected def newStoreProviderWithStateVariable(
      storeId: StateStoreId,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      sqlConf: SQLConf = SQLConf.get,
      conf: Configuration = new Configuration,
      useColumnFamilies: Boolean = false): RocksDBStateStoreProvider = {
    val provider = new RocksDBStateStoreProvider()
    provider.init(
      storeId, schemaForKeyRow, schemaForValueRow, keyStateEncoderSpec,
      useColumnFamilies,
      new StateStoreConf(sqlConf), conf, useMultipleValuesPerKey,
      Some(new TestStateSchemaProvider))
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

