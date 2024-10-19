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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, MapStateImplWithTTL, StatefulProcessorHandleImpl}
import org.apache.spark.sql.streaming.{ListState, MapState, TimeMode, TTLConfig, ValueState}
import org.apache.spark.sql.types.{BinaryType, StructType}

/**
 * Class that adds unit tests for MapState types used in arbitrary stateful
 * operators such as transformWithState
 */
class MapStateSuite extends StateVariableSuiteBase {
  // dummy schema for initializing rocksdb provider
  override def schemaForKeyRow: StructType = new StructType()
    .add("key", BinaryType)
    .add("userKey", BinaryType)

  test("Map state operations for single instance") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val testState: MapState[String, Double] =
        handle.getMapState[String, Double]("testState", Encoders.STRING, Encoders.scalaDouble)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      // put initial value
      testState.updateValue("k1", 1.0)
      assert(testState.getValue("k1") === 1.0)
      // update existing value, append new key-value pairs
      testState.updateValue("k1", 1.0)
      testState.updateValue("k2", 2.0)
      assert(testState.getValue("k1") === 1.0)
      assert(testState.getValue("k2") === 2.0)
      testState.updateValue("k1", 3.0)
      assert(testState.getValue("k1") === 3.0)

      assert(testState.keys().toSeq === Seq("k1", "k2"))
      assert(testState.values().toSeq === Seq(3.0, 2.0))

      // test remove
      testState.removeKey("k1")
      assert(testState.getValue("k1") === null)
      assert(!testState.containsKey("k1"))

      testState.clear()
      assert(!testState.exists())
      assert(testState.iterator().hasNext === false)
    }
  }

  test("Map state operations for multiple map instances") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val testState1: MapState[Long, Double] =
        handle.getMapState[Long, Double]("testState1", Encoders.scalaLong, Encoders.scalaDouble)
      val testState2: MapState[Long, Int] =
        handle.getMapState[Long, Int]("testState2", Encoders.scalaLong, Encoders.scalaInt)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      // put initial value
      testState1.updateValue(1L, 1.0)
      assert(testState1.getValue(1L) === 1.0)
      assert(!testState2.containsKey(1L))
      // update existing values, append new key-value pairs
      testState1.updateValue(1L, 2.0)
      testState2.updateValue(2L, 3)
      assert(testState1.getValue(1L) === 2.0)
      assert(testState2.getValue(2L) === 3)

      assert(testState1.keys().toSeq === Seq(1L))
      assert(testState2.keys().toSeq === Seq(2L))
      assert(testState1.values().toSeq === Seq(2.0))
      assert(testState2.values().toSeq === Seq(3))

      // test remove
      testState1.removeKey(1L)
      assert(testState1.getValue(1L) === null)
      assert(!testState1.containsKey(1L))

      testState2.clear()
      assert(!testState1.exists())
      assert(!testState2.exists())
      assert(testState1.iterator().hasNext === false)
      assert(testState2.iterator().hasNext === false)
    }
  }

  test("Map state operations with list, value, another map instances") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val mapTestState1: MapState[String, Int] =
        handle.getMapState[String, Int]("mapTestState1", Encoders.STRING, Encoders.scalaInt)
      val mapTestState2: MapState[String, Int] =
        handle.getMapState[String, Int]("mapTestState2", Encoders.STRING, Encoders.scalaInt)
      val valueTestState: ValueState[String] =
        handle.getValueState[String]("valueTestState", Encoders.STRING)
      val listTestState: ListState[String] =
        handle.getListState[String]("listTestState", Encoders.STRING)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      // put initial values
      valueTestState.update("v1")
      listTestState.put(Seq("v1").toArray)
      mapTestState1.updateValue("k1", 1)
      mapTestState2.updateValue("k2", 2)
      assert(valueTestState.get() === "v1")
      assert(listTestState.get().toSeq === Seq("v1"))
      assert(mapTestState1.getValue("k1") === 1)
      assert(mapTestState2.getValue("k2") === 2)
      // update existing values, append
      valueTestState.update("v2")
      listTestState.appendValue("v3")
      mapTestState1.updateValue("k1", 3)
      mapTestState2.updateValue("k2", 4)

      assert(valueTestState.get() === "v2")
      assert(listTestState.get().toSeq === Seq("v1", "v3"))
      assert(mapTestState1.getValue("k1") === 3)
      assert(mapTestState2.getValue("k2") === 4)

      // advanced append/get operations
      listTestState.appendList(Seq("v4").toArray)
      mapTestState1.updateValue("k3", 5)
      mapTestState2.updateValue("k4", 6)

      assert(valueTestState.get() === "v2")
      assert(listTestState.get().toSeq === Seq("v1", "v3", "v4"))
      assert(mapTestState1.keys().toSeq === Seq("k1", "k3"))
      assert(mapTestState2.values().toSeq === Seq(4, 6))

      // test remove
      valueTestState.clear()
      listTestState.clear()
      mapTestState1.clear()
      mapTestState2.removeKey("k4")

      assert(!valueTestState.exists())
      assert(!listTestState.exists())
      assert(!mapTestState1.exists())
      assert(mapTestState2.exists())
      assert(mapTestState2.iterator().toList === List(("k2", 4)))
    }
  }

  test("test Map state TTL") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val timestampMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.ProcessingTime(),
        batchTimestampMs = Some(timestampMs))

      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val testState: MapStateImplWithTTL[String, String] =
        handle.getMapState[String, String]("testState", Encoders.STRING,
          Encoders.STRING, ttlConfig).asInstanceOf[MapStateImplWithTTL[String, String]]
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.updateValue("k1", "v1")
      assert(testState.getValue("k1") === "v1")
      assert(testState.getWithoutEnforcingTTL("k1").get === "v1")

      val ttlExpirationMs = timestampMs + 60000
      var ttlValue = testState.getTTLValue("k1")
      assert(ttlValue.isDefined)
      assert(ttlValue.get._2 === ttlExpirationMs)
      var ttlStateValueIterator = testState.getKeyValuesInTTLState().map(_._2)
      assert(ttlStateValueIterator.hasNext)

      // increment batchProcessingTime, or watermark and ensure expired value is not returned
      val nextBatchHandle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder,
        TimeMode.ProcessingTime(), batchTimestampMs = Some(ttlExpirationMs))

      val nextBatchTestState: MapStateImplWithTTL[String, String] =
        nextBatchHandle.getMapState[String, String](
            "testState", Encoders.STRING, Encoders.STRING, ttlConfig)
            .asInstanceOf[MapStateImplWithTTL[String, String]]

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      // ensure get does not return the expired value
      assert(!nextBatchTestState.exists())
      assert(nextBatchTestState.getValue("k1") === null)

      // ttl value should still exist in state
      ttlValue = nextBatchTestState.getTTLValue("k1")
      assert(ttlValue.isDefined)
      assert(ttlValue.get._2 === ttlExpirationMs)
      ttlStateValueIterator = nextBatchTestState.getKeyValuesInTTLState().map(_._2)
      assert(ttlStateValueIterator.hasNext)
      assert(ttlStateValueIterator.next() === ttlExpirationMs)
      assert(ttlStateValueIterator.isEmpty)

      // getWithoutTTL should still return the expired value
      assert(nextBatchTestState.getWithoutEnforcingTTL("k1").get === "v1")

      nextBatchTestState.clear()
      assert(!nextBatchTestState.exists())
      assert(nextBatchTestState.getValue("k1") === null)
    }
  }

  test("test negative or zero TTL duration throws error") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val batchTimestampMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder,
        TimeMode.ProcessingTime(), batchTimestampMs = Some(batchTimestampMs))

      Seq(null, Duration.ZERO, Duration.ofMinutes(-1)).foreach { ttlDuration =>
        val ttlConfig = TTLConfig(ttlDuration)
        val ex = intercept[SparkUnsupportedOperationException] {
          handle.getMapState[String, String](
            "testState", Encoders.STRING, Encoders.STRING, ttlConfig)
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

  test("Map state with TTL with non-primitive types") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val timestampMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.ProcessingTime(),
        batchTimestampMs = Some(timestampMs))

      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val testState: MapStateImplWithTTL[POJOTestClass, TestClass] =
        handle.getMapState[POJOTestClass, TestClass]("testState",
          Encoders.bean(classOf[POJOTestClass]),
          Encoders.product[TestClass], ttlConfig)
          .asInstanceOf[MapStateImplWithTTL[POJOTestClass, TestClass]]

      ImplicitGroupingKeyTracker.setImplicitKey("testKey")
      testState.updateValue(new POJOTestClass("k1", 1), TestClass(1L, "v1"))
      assert(testState.getValue(new POJOTestClass("k1", 1)) === TestClass(1L, "v1"))
      assert(testState.getWithoutEnforcingTTL(
        new POJOTestClass("k1", 1)).get === TestClass(1L, "v1"))

      val ttlExpirationMs = timestampMs + 60000
      val ttlValue = testState.getTTLValue(new POJOTestClass("k1", 1))
      assert(ttlValue.isDefined)
      assert(ttlValue.get._2 === ttlExpirationMs)
      val ttlStateValueIterator = testState.getKeyValuesInTTLState().map(_._2)
      assert(ttlStateValueIterator.hasNext)
    }
  }
}
