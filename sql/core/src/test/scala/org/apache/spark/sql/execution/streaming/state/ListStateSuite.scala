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

import org.apache.spark.{SparkIllegalArgumentException, SparkUnsupportedOperationException}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, ListStateImplWithTTL, StatefulProcessorHandleImpl}
import org.apache.spark.sql.streaming.{ListState, TimeMode, TTLConfig, ValueState}

/**
 * Class that adds unit tests for ListState types used in arbitrary stateful
 * operators such as transformWithState
 */
class ListStateSuite extends StateVariableSuiteBase {
  import testImplicits._

  // overwrite useMultipleValuesPerKey in base suite to be true for list state
  override def useMultipleValuesPerKey: Boolean = true

  private def testMapStateWithNullUserKey()(runListOps: ListState[Long] => Unit): Unit = {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val listState: ListState[Long] = handle.getListState[Long]("listState",
        TTLConfig.NONE)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      val e = intercept[SparkIllegalArgumentException] {
        runListOps(listState)
      }

      checkError(
        exception = e,
        condition = "ILLEGAL_STATE_STORE_VALUE.NULL_VALUE",
        sqlState = Some("42601"),
        parameters = Map("stateName" -> "listState")
      )
    }
  }

  Seq("appendList", "put").foreach { listImplFunc =>
    test(s"Test list operation($listImplFunc) with null") {
      testMapStateWithNullUserKey() { listState =>
        listImplFunc match {
          case "appendList" => listState.appendList(null)
          case "put" => listState.put(null)
        }
      }
    }
  }

  test("List state operations for single instance") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val testState: ListState[Long] = handle.getListState[Long]("testState",
        TTLConfig.NONE)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      // simple put and get test
      testState.appendValue(123)
      assert(testState.get().toSeq === Seq(123))
      testState.clear()
      assert(!testState.exists())
      assert(testState.get().toSeq === Seq.empty[Long])

      // put list test
      testState.appendList(Array(123, 456))
      assert(testState.get().toSeq === Seq(123, 456))
      testState.appendValue(789)
      assert(testState.get().toSeq === Seq(123, 456, 789))

      testState.clear()
      assert(!testState.exists())
      assert(testState.get().toSeq === Seq.empty[Long])
    }
  }

  // verify that relative ordering of inserted elements is maintained on retrieval - long type
  test("list state - value ordering - long type") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val testState: ListState[Long] = handle.getListState[Long]("testState", Encoders.scalaLong,
        TTLConfig.NONE)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      var testSeq = Seq(931L, 8000L, 452300L, 4200L, -1L, 90L, 1L, 2L, 8L,
        -230L, -14569L, -92L, -7434253L, 35L, 6L, 9L, -323L, 5L)
      testState.put(testSeq.toArray)
      assert(testState.get().toSeq === testSeq)
      testState.appendValue(100L)
      testState.appendValue(9L)
      testState.appendValue(48972L)
      testSeq = testSeq ++ Seq(100L, 9L, 48972L)
      assert(testState.get().toSeq === testSeq)

      var appendSeq = Seq(-1L, 2942450L, 7L)
      testState.appendList(appendSeq.toArray)
      testSeq = testSeq ++ appendSeq
      assert(testState.get().toSeq === testSeq)

      testState.clear()
      testState.appendValue(3451L)
      testState.appendValue(24L)
      testState.appendValue(-14342429L)
      testSeq = Seq(3451L, 24L, -14342429L)
      assert(testState.get().toSeq === testSeq)

      appendSeq = Seq(931L, 8000L, 452300L, 4200L, -1L, 90L, 1L, 2L, 8L,
        -230L, -14569L, -92L, -7434253L, 35L, 6L, 9L, -323L, 5L)
      testState.appendList(appendSeq.toArray)
      testSeq = testSeq ++ appendSeq
      assert(testState.get().toSeq === testSeq)

      testState.clear()
      assert(!testState.exists())
      assert(testState.get().toSeq === Seq.empty[Long])
      store.commit()
    }
  }

  // verify that relative ordering of inserted elements is maintained on retrieval - string type
  test("list state - value ordering - string type") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val testState: ListState[String] = handle.getListState[String]("testState",
        Encoders.STRING, TTLConfig.NONE)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      var testSeq = Seq("test", "actual", "state", "value", "ordering", "string", "", "type")
      testState.put(testSeq.toArray)
      assert(testState.get().toSeq === testSeq)
      testState.appendValue("hello")
      testState.appendValue("world")
      testState.appendValue("verification")
      testSeq = testSeq ++ Seq("hello", "world", "verification")
      assert(testState.get().toSeq === testSeq)

      var appendSeq = Seq("test string", "stateful", "processor", "handle")
      testState.appendList(appendSeq.toArray)
      testSeq = testSeq ++ appendSeq
      assert(testState.get().toSeq === testSeq)

      testState.clear()
      testState.appendValue(" validate space in front")
      testState.appendValue("validate space in back ")
      testState.appendValue(" validate space in both ")
      testSeq = Seq(" validate space in front", "validate space in back ",
        " validate space in both ")
      assert(testState.get().toSeq === testSeq)

      appendSeq = Seq("test", "actual", "state", "value", "ordering", "string", "", "type",
        "hello", "world", "verification")
      testState.appendList(appendSeq.toArray)
      testSeq = testSeq ++ appendSeq
      assert(testState.get().toSeq === testSeq)

      testState.clear()
      assert(!testState.exists())
      assert(testState.get().toSeq === Seq.empty[String])
      store.commit()
    }
  }

  // verify that relative ordering of inserted elements is maintained on retrieval - case class type
  test("list state - value ordering - case class type") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val testState: ListState[TestClass] = handle.getListState[TestClass]("testState",
        Encoders.product[TestClass], TTLConfig.NONE)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      var testSeq = Seq(TestClass(931L, "test"), TestClass(8000L, "verification"),
        TestClass(452300L, "state"), TestClass(4200L, "actual"), TestClass(-1L, "value"),
        TestClass(90L, "ordering"), TestClass(1L, "string"))
      testState.put(testSeq.toArray)
      assert(testState.get().toSeq === testSeq)
      testState.appendValue(TestClass(2L, "type"))
      testState.appendValue(TestClass(-323L, "hello"))
      testState.appendValue(TestClass(48972L, " verify with space"))
      testSeq = testSeq ++ Seq(TestClass(2L, "type"), TestClass(-323L, "hello"),
        TestClass(48972L, " verify with space"))

      assert(testState.get().toSeq === testSeq)

      var appendSeq = Seq(TestClass(-1L, "space at end "),
        TestClass(2942450L, " space before and after "), TestClass(7L, "sample_string"))
      testState.appendList(appendSeq.toArray)
      testSeq = testSeq ++ appendSeq
      assert(testState.get().toSeq === testSeq)

      testState.clear()
      testState.appendValue(TestClass(3451L, "values"))
      testState.appendValue(TestClass(24L, "ordering"))
      testState.appendValue(TestClass(-14342429L, "state"))
      testSeq = Seq(TestClass(3451L, "values"), TestClass(24L, "ordering"),
        TestClass(-14342429L, "state"))
      assert(testState.get().toSeq === testSeq)

      appendSeq = Seq(TestClass(931L, "test"), TestClass(8000L, "verification"),
        TestClass(452300L, "state"), TestClass(4200L, "actual"), TestClass(-1L, "value"),
        TestClass(90L, "ordering"), TestClass(1L, "string"))
      testState.appendList(appendSeq.toArray)
      testSeq = testSeq ++ appendSeq
      assert(testState.get().toSeq === testSeq)

      testState.clear()
      assert(!testState.exists())
      assert(testState.get().toSeq === Seq.empty[TestClass])
      store.commit()
    }
  }

  test("List state operations for multiple instance") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val testState1: ListState[Long] = handle.getListState[Long]("testState1",
        TTLConfig.NONE)
      val testState2: ListState[Long] = handle.getListState[Long]("testState2",
        TTLConfig.NONE)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      // simple put and get test
      testState1.appendValue(123)
      testState2.appendValue(456)
      assert(testState1.get().toSeq === Seq(123))
      assert(testState2.get().toSeq === Seq(456))
      testState1.clear()
      assert(!testState1.exists())
      assert(testState2.exists())
      assert(testState1.get().toSeq === Seq.empty[Long])

      // put list test
      testState1.appendList(Array(123, 456))
      assert(testState1.get().toSeq === Seq(123, 456))
      testState2.appendList(Array(123))
      assert(testState2.get().toSeq === Seq(456, 123))

      testState1.appendValue(789)
      assert(testState1.get().toSeq === Seq(123, 456, 789))
      assert(testState2.get().toSeq === Seq(456, 123))

      testState2.clear()
      assert(!testState2.exists())
      assert(testState1.exists())
      assert(testState2.get().toSeq === Seq.empty[Long])
    }
  }

  test("List state operations with list, value, another list instances") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder, TimeMode.None())

      val listState1: ListState[Long] = handle.getListState[Long]("listState1",
        TTLConfig.NONE)
      val listState2: ListState[Long] = handle.getListState[Long]("listState2",
        TTLConfig.NONE)
      val valueState: ValueState[Long] = handle.getValueState[Long](
        "valueState", TTLConfig.NONE)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      // simple put and get test
      valueState.update(123)
      listState1.appendValue(123)
      listState2.appendValue(456)
      assert(listState1.get().toSeq === Seq(123))
      assert(listState2.get().toSeq === Seq(456))
      assert(valueState.get() === 123)

      listState1.clear()
      valueState.clear()
      assert(!listState1.exists())
      assert(listState2.exists())
      assert(!valueState.exists())
      assert(listState1.get().toSeq === Seq.empty[Long])
    }
  }

  test(s"test List state TTL") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val timestampMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder,
        TimeMode.ProcessingTime(), batchTimestampMs = Some(timestampMs))

      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val testState: ListStateImplWithTTL[String] = handle.getListState[String]("testState",
        Encoders.STRING, ttlConfig).asInstanceOf[ListStateImplWithTTL[String]]
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      testState.put(Array("v1", "v2", "v3"))
      assert(testState.get().toSeq === Seq("v1", "v2", "v3"))
      assert(testState.getWithoutEnforcingTTL().toSeq === Seq("v1", "v2", "v3"))

      val ttlExpirationMs = timestampMs + 60000
      var ttlValues = testState.getTTLValues()
      assert(ttlValues.nonEmpty)
      assert(ttlValues.forall(_._2 === ttlExpirationMs))
      var ttlStateValue = testState.getValueInTTLState()
      assert(ttlStateValue.isDefined)

      // increment batchProcessingTime, or watermark and ensure expired value is not returned
      val nextBatchHandle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder,
        TimeMode.ProcessingTime(), batchTimestampMs = Some(ttlExpirationMs))

      val nextBatchTestState: ListStateImplWithTTL[String] =
        nextBatchHandle.getListState[String]("testState", Encoders.STRING, ttlConfig)
          .asInstanceOf[ListStateImplWithTTL[String]]

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")

      // ensure get does not return the expired value
      assert(!nextBatchTestState.exists())
      assert(nextBatchTestState.get().isEmpty)

      // ttl value should still exist in state
      ttlValues = nextBatchTestState.getTTLValues()
      assert(ttlValues.nonEmpty)
      assert(ttlValues.forall(_._2 === ttlExpirationMs))
      ttlStateValue = nextBatchTestState.getValueInTTLState()
      assert(ttlStateValue.isDefined)
      assert(ttlStateValue.get === ttlExpirationMs)

      // getWithoutTTL should still return the expired value
      assert(nextBatchTestState.getWithoutEnforcingTTL().toSeq === Seq("v1", "v2", "v3"))

      nextBatchTestState.clear()
      assert(!nextBatchTestState.exists())
      assert(nextBatchTestState.get().isEmpty)
    }
  }

  test("test null or negative TTL duration throws error") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val batchTimestampMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        stringEncoder,
        TimeMode.ProcessingTime(), batchTimestampMs = Some(batchTimestampMs))

      Seq(null, Duration.ofMinutes(-1)).foreach { ttlDuration =>
        val ttlConfig = TTLConfig(ttlDuration)
        val ex = intercept[SparkUnsupportedOperationException] {
          handle.getListState[String]("testState", Encoders.STRING, ttlConfig)
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

  test("ListState TTL with non-primitive types") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val timestampMs = 10
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        encoderFor(Encoders.bean(classOf[POJOTestClass])).asInstanceOf[ExpressionEncoder[Any]],
        TimeMode.ProcessingTime(), batchTimestampMs = Some(timestampMs))

      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val testState: ListStateImplWithTTL[TestClass] =
        handle.getListState[TestClass]("testState",
        Encoders.product[TestClass], ttlConfig).asInstanceOf[ListStateImplWithTTL[TestClass]]
      ImplicitGroupingKeyTracker.setImplicitKey(new POJOTestClass("gk1", 1))
      testState.put(Array(TestClass(1L, "v1"), TestClass(2L, "v2"), TestClass(3L, "v3")))
      assert(testState.get().toSeq ===
        Seq(TestClass(1L, "v1"), TestClass(2L, "v2"), TestClass(3L, "v3")))
      assert(testState.getWithoutEnforcingTTL().toSeq ===
        Seq(TestClass(1L, "v1"), TestClass(2L, "v2"), TestClass(3L, "v3")))

      val ttlExpirationMs = timestampMs + 60000
      val ttlValues = testState.getTTLValues()
      assert(ttlValues.nonEmpty)
      assert(ttlValues.forall(_._2 === ttlExpirationMs))
      val ttlStateValue = testState.getValueInTTLState()
      assert(ttlStateValue.isDefined)
    }
  }
}
