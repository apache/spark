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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl, StatefulProcessorHandleState}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.TimeoutMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Class that adds tests to verify operations based on stateful processor handle
 * used primarily in queries based on the `transformWithState` operator.
 */
class StatefulProcessorHandleSuite extends SharedSparkSession
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

  private def keyExprEncoder: ExpressionEncoder[Any] =
    Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]]

  private def newStoreProviderWithHandle(useColumnFamilies: Boolean):
    RocksDBStateStoreProvider = {
    newStoreProviderWithHandle(StateStoreId(newDir(), Random.nextInt(), 0),
      numColsPrefixKey = 0,
      useColumnFamilies = useColumnFamilies)
  }

  private def newStoreProviderWithHandle(
    storeId: StateStoreId,
    numColsPrefixKey: Int,
    sqlConf: Option[SQLConf] = None,
    conf: Configuration = new Configuration,
    useColumnFamilies: Boolean = false): RocksDBStateStoreProvider = {
    val provider = new RocksDBStateStoreProvider()
    provider.init(
      storeId, schemaForKeyRow, schemaForValueRow, numColsPrefixKey = numColsPrefixKey,
      useColumnFamilies,
      new StateStoreConf(sqlConf.getOrElse(SQLConf.get)), conf)
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

  private def getTimeoutMode(timeoutMode: String): TimeoutMode = {
    timeoutMode match {
      case "NoTimeouts" => TimeoutMode.NoTimeouts()
      case "ProcessingTime" => TimeoutMode.ProcessingTime()
      case "EventTime" => TimeoutMode.EventTime()
      case _ => throw new IllegalArgumentException(s"Invalid timeoutMode=$timeoutMode")
    }
  }

  Seq("NoTimeouts", "ProcessingTime", "EventTime").foreach { timeoutMode =>
    test(s"value state creation with timeoutMode=$timeoutMode should succeed") {
      tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), keyExprEncoder, getTimeoutMode(timeoutMode))
        assert(handle.getHandleState === StatefulProcessorHandleState.CREATED)
        handle.getValueState[Long]("testState")
      }
    }
  }

  private def verifyInvalidOperation(
      handle: StatefulProcessorHandleImpl,
      handleState: StatefulProcessorHandleState.Value,
      errorMsg: String)(fn: StatefulProcessorHandleImpl => Unit): Unit = {
    handle.setHandleState(handleState)
    assert(handle.getHandleState === handleState)
    val ex = intercept[Exception] {
      fn(handle)
    }
    assert(ex.getMessage.contains(errorMsg))
  }

  private def createValueStateInstance(handle: StatefulProcessorHandleImpl): Unit = {
    handle.getValueState[Long]("testState")
  }

  private def registerTimer(handle: StatefulProcessorHandleImpl): Unit = {
    handle.registerTimer(1000L)
  }

  Seq("NoTimeouts", "ProcessingTime", "EventTime").foreach { timeoutMode =>
    test(s"value state creation with timeoutMode=$timeoutMode " +
      "and invalid state should fail") {
      tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), keyExprEncoder, getTimeoutMode(timeoutMode))

        verifyInvalidOperation(handle, StatefulProcessorHandleState.INITIALIZED,
          "Cannot create state variable") { handle =>
          createValueStateInstance(handle)
        }

        verifyInvalidOperation(handle, StatefulProcessorHandleState.DATA_PROCESSED,
          "Cannot create state variable") { handle =>
          createValueStateInstance(handle)
        }

        verifyInvalidOperation(handle, StatefulProcessorHandleState.TIMER_PROCESSED,
          "Cannot create state variable") { handle =>
          createValueStateInstance(handle)
        }

        verifyInvalidOperation(handle, StatefulProcessorHandleState.CLOSED,
          "Cannot create state variable") { handle =>
          createValueStateInstance(handle)
        }
      }
    }
  }

  test("registering processing/event time timeouts with NoTimeout mode should fail") {
    tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), keyExprEncoder, TimeoutMode.NoTimeouts())
      val ex = intercept[Exception] {
        handle.registerTimer(10000L)
      }
      assert(ex.getMessage.contains("Cannot use timers"))

      val ex2 = intercept[Exception] {
        handle.deleteTimer(10000L)
      }
      assert(ex2.getMessage.contains("Cannot use timers"))
    }
  }

  Seq("ProcessingTime", "EventTime").foreach { timeoutMode =>
    test(s"registering timeouts with timeoutMode=$timeoutMode should succeed") {
      tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), keyExprEncoder, getTimeoutMode(timeoutMode))
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

  Seq("ProcessingTime", "EventTime").foreach { timeoutMode =>
    test(s"verify listing of registered timers with timeoutMode=$timeoutMode") {
      tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), keyExprEncoder, getTimeoutMode(timeoutMode))
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

  Seq("ProcessingTime", "EventTime").foreach { timeoutMode =>
    test(s"verify that expired timers are returned in sorted order with timeoutMode=$timeoutMode") {
      tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), keyExprEncoder, getTimeoutMode(timeoutMode))
        handle.setHandleState(StatefulProcessorHandleState.DATA_PROCESSED)
        assert(handle.getHandleState === StatefulProcessorHandleState.DATA_PROCESSED)

        ImplicitGroupingKeyTracker.setImplicitKey("test_key")
        assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isDefined)

        // Generate some random timer timestamps in arbitrary sorted order
        val timerTimestamps = Seq(931L, 8000L, 452300L, 4200L, 90L, 1L, 2L, 8L, 3L, 35L, 6L, 9L, 5L)
        timerTimestamps.foreach { timestamp =>
          handle.registerTimer(timestamp)
        }

        // Ensure that the expired timers are returned in sorted order
        var expiredTimers = handle.getExpiredTimers(1000000L).map(_._2).toSeq
        assert(expiredTimers === timerTimestamps.sorted)

        expiredTimers = handle.getExpiredTimers(5L).map(_._2).toSeq
        assert(expiredTimers === Seq(1L, 2L, 3L, 5L))

        expiredTimers = handle.getExpiredTimers(10L).map(_._2).toSeq
        assert(expiredTimers === Seq(1L, 2L, 3L, 5L, 6L, 8L, 9L))

        ImplicitGroupingKeyTracker.removeImplicitKey()
        assert(ImplicitGroupingKeyTracker.getImplicitKeyOption.isEmpty)
      }
    }
  }

  Seq("ProcessingTime", "EventTime").foreach { timeoutMode =>
    test(s"registering timeouts with timeoutMode=$timeoutMode and invalid state should fail") {
      tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
        val store = provider.getStore(0)
        val handle = new StatefulProcessorHandleImpl(store,
          UUID.randomUUID(), keyExprEncoder, getTimeoutMode(timeoutMode))

        verifyInvalidOperation(handle, StatefulProcessorHandleState.CREATED,
          "Cannot use timers") { handle =>
          registerTimer(handle)
        }

        verifyInvalidOperation(handle, StatefulProcessorHandleState.TIMER_PROCESSED,
          "Cannot use timers") { handle =>
          registerTimer(handle)
        }

        verifyInvalidOperation(handle, StatefulProcessorHandleState.CLOSED,
          "Cannot use timers") { handle =>
          registerTimer(handle)
        }
      }
    }
  }
}
