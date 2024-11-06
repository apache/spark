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
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl, StatefulProcessorHandleState}
import org.apache.spark.sql.streaming.{TimeMode, TTLConfig}


/**
 * Class that adds tests to verify operations based on stateful processor handle
 * used primarily in queries based on the `transformWithState` operator.
 */
class StatefulProcessorHandleSuite extends StateVariableSuiteBase {

  import testImplicits._

  private def getTimeMode(timeMode: String): TimeMode = {
    timeMode match {
      case "None" => TimeMode.None()
      case "ProcessingTime" => TimeMode.ProcessingTime()
      case "EventTime" => TimeMode.EventTime()
      case _ => throw new IllegalArgumentException(s"Invalid timeMode=$timeMode")
    }
  }

  Seq("None", "ProcessingTime", "EventTime").foreach { timeMode =>
    test(s"value state creation with timeMode=$timeMode should succeed") {
      tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), stringEncoder, getTimeMode(timeMode))
        assert(handle.getHandleState === StatefulProcessorHandleState.CREATED)
        handle.getValueState[Long]("testState", TTLConfig.NONE)
      }
    }
  }

  private def verifyInvalidOperation(
      handle: StatefulProcessorHandleImpl,
      handleState: StatefulProcessorHandleState.Value,
      operationType: String)(fn: StatefulProcessorHandleImpl => Unit): Unit = {
    handle.setHandleState(handleState)
    assert(handle.getHandleState === handleState)
    val ex = intercept[SparkUnsupportedOperationException] {
      fn(handle)
    }
    checkError(
      ex,
      condition = "STATEFUL_PROCESSOR_CANNOT_PERFORM_OPERATION_WITH_INVALID_HANDLE_STATE",
      parameters = Map(
        "operationType" -> operationType,
        "handleState" -> handleState.toString
      ),
      matchPVals = true
    )
  }

  private def createValueStateInstance(handle: StatefulProcessorHandleImpl): Unit = {
    handle.getValueState[Long]("testState", TTLConfig.NONE)
  }

  private def registerTimer(handle: StatefulProcessorHandleImpl): Unit = {
    handle.registerTimer(1000L)
  }

  Seq("None", "ProcessingTime", "EventTime").foreach { timeMode =>
    test(s"value state creation with timeMode=$timeMode " +
      "and invalid state should fail") {
      tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), stringEncoder, getTimeMode(timeMode))

        Seq(StatefulProcessorHandleState.INITIALIZED,
          StatefulProcessorHandleState.DATA_PROCESSED,
          StatefulProcessorHandleState.TIMER_PROCESSED,
          StatefulProcessorHandleState.CLOSED).foreach { state =>
          verifyInvalidOperation(handle, state, "get_value_state") { handle =>
            createValueStateInstance(handle)
          }
        }
      }
    }
  }

  test("registering processing/event time timeouts with None timeMode should fail") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), stringEncoder, TimeMode.None())
      val ex = intercept[SparkUnsupportedOperationException] {
        handle.registerTimer(10000L)
      }

      checkError(
        ex,
        condition = "STATEFUL_PROCESSOR_CANNOT_PERFORM_OPERATION_WITH_INVALID_TIME_MODE",
        parameters = Map(
          "operationType" -> "register_timer",
          "timeMode" -> TimeMode.None().toString
        ),
        matchPVals = true
      )

      val ex2 = intercept[SparkUnsupportedOperationException] {
        handle.deleteTimer(10000L)
      }

      checkError(
        ex2,
        condition = "STATEFUL_PROCESSOR_CANNOT_PERFORM_OPERATION_WITH_INVALID_TIME_MODE",
        parameters = Map(
          "operationType" -> "delete_timer",
          "timeMode" -> TimeMode.None().toString
        ),
        matchPVals = true
      )
    }
  }

  Seq("ProcessingTime", "EventTime").foreach { timeMode =>
    test(s"registering timeouts with timeMode=$timeMode should succeed") {
      tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), stringEncoder, getTimeMode(timeMode))
        handle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
        assert(handle.getHandleState === StatefulProcessorHandleState.INITIALIZED)

        ImplicitGroupingKeyTracker.setImplicitKey("test_key")
        assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isDefined)

        handle.registerTimer(10000L)
        handle.deleteTimer(10000L)

        ImplicitGroupingKeyTracker.removeImplicitKey()
        assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isEmpty)
      }
    }
  }

  Seq("ProcessingTime", "EventTime").foreach { timeMode =>
    test(s"verify listing of registered timers with timeMode=$timeMode") {
      tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), stringEncoder, getTimeMode(timeMode))
        handle.setHandleState(StatefulProcessorHandleState.DATA_PROCESSED)
        assert(handle.getHandleState === StatefulProcessorHandleState.DATA_PROCESSED)

        ImplicitGroupingKeyTracker.setImplicitKey("test_key1")
        assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isDefined)

        // Generate some random timer timestamps in arbitrary sorted order
        val timerTimestamps1 = Seq(931L, 8000L, 452300L, 4200L, 90L,
          1L, 2L, 8L, 3L, 35L, 6L, 9L, 5L)
        timerTimestamps1.foreach { timestamp =>
          handle.registerTimer(timestamp)
        }

        val timers1 = handle.listTimers()
        assert(timers1.toSeq.sorted === timerTimestamps1.sorted)
        ImplicitGroupingKeyTracker.removeImplicitKey()

        ImplicitGroupingKeyTracker.setImplicitKey("test_key2")
        assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isDefined)

        // Generate some random timer timestamps in arbitrary sorted order
        val timerTimestamps2 = Seq(12000L, 14500L, 16000L)
        timerTimestamps2.foreach { timestamp =>
          handle.registerTimer(timestamp)
        }

        val timers2 = handle.listTimers()
        assert(timers2.toSeq.sorted === timerTimestamps2.sorted)
        ImplicitGroupingKeyTracker.removeImplicitKey()
        assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isEmpty)
      }
    }
  }

  Seq("ProcessingTime", "EventTime").foreach { timeMode =>
    test(s"registering timeouts with timeMode=$timeMode and invalid state should fail") {
      tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), stringEncoder, getTimeMode(timeMode))

        Seq(StatefulProcessorHandleState.CREATED,
          StatefulProcessorHandleState.TIMER_PROCESSED,
          StatefulProcessorHandleState.CLOSED).foreach { state =>
          verifyInvalidOperation(handle, state, "register_timer") { handle =>
            registerTimer(handle)
          }
        }
      }
    }
  }

  test("ttl States are populated for valueState and timeMode=ProcessingTime") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), stringEncoder, TimeMode.ProcessingTime(),
        batchTimestampMs = Some(10))

      val valueStateWithTTL = handle.getValueState[String]("testState",
        TTLConfig(Duration.ofHours(1)))

      // create another state without TTL, this should not be captured in the handle
      handle.getValueState[String]("testState", TTLConfig.NONE)

      assert(handle.ttlStates.size() === 1)
      assert(handle.ttlStates.get(0) === valueStateWithTTL)
    }
  }

  test("ttl States are populated for listState and timeMode=ProcessingTime") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), stringEncoder, TimeMode.ProcessingTime(),
        batchTimestampMs = Some(10))

      val listStateWithTTL = handle.getListState("testState",
        Encoders.STRING, TTLConfig(Duration.ofHours(1)))

      // create another state without TTL, this should not be captured in the handle
      handle.getListState("testState", Encoders.STRING, TTLConfig.NONE)

      assert(handle.ttlStates.size() === 1)
      assert(handle.ttlStates.get(0) === listStateWithTTL)
    }
  }

  test("ttl States are populated for mapState and timeMode=ProcessingTime") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), stringEncoder, TimeMode.ProcessingTime(),
        batchTimestampMs = Some(10))

      val mapStateWithTTL = handle.getMapState("testState",
        Encoders.STRING, Encoders.STRING, TTLConfig(Duration.ofHours(1)))

      // create another state without TTL, this should not be captured in the handle
      handle.getMapState("testState", Encoders.STRING, Encoders.STRING,
        TTLConfig.NONE)

      assert(handle.ttlStates.size() === 1)
      assert(handle.ttlStates.get(0) === mapStateWithTTL)
    }
  }

  test("ttl States are not populated for timeMode=None") {
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), stringEncoder, TimeMode.None())

      handle.getValueState("testValueState", Encoders.STRING, TTLConfig.NONE)
      handle.getListState("testListState", Encoders.STRING, TTLConfig.NONE)
      handle.getMapState("testMapState", Encoders.STRING, Encoders.STRING,
        TTLConfig.NONE)

      assert(handle.ttlStates.isEmpty)
    }
  }
}
