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

import org.apache.spark.sql.execution.streaming.{ImplicitKeyTracker, StatefulProcessorHandleImpl, StatefulProcessorHandleState}
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

  test("value state creation with no timeouts should succeed") {
    tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), TimeoutMode.NoTimeouts())
      assert(handle.getQueryInfo().getPartitionId === 0)
      assert(handle.getHandleState === StatefulProcessorHandleState.CREATED)
      handle.getValueState[Long]("testState")
    }
  }

  test("value state creation with processing time timeout should succeed") {
    tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), TimeoutMode.ProcessingTime())
      assert(handle.getQueryInfo().getPartitionId === 0)
      assert(handle.getHandleState === StatefulProcessorHandleState.CREATED)
      handle.getValueState[Long]("testState")
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
    handle.registerProcessingTimeTimer(1000L)
  }

  test("value state creation with processing time " +
    "timeout and invalid state should fail") {
    tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), TimeoutMode.ProcessingTime())

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

  test("registering processing time timeouts with NoTimeout mode should fail") {
    tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), TimeoutMode.NoTimeouts())
      assert(handle.getQueryInfo().getPartitionId === 0)
      val ex = intercept[Exception] {
        handle.registerProcessingTimeTimer(10000L)
      }
      assert(ex.getMessage.contains("Cannot register processing time timers"))

      val ex2 = intercept[Exception] {
        handle.deleteProcessingTimeTimer(10000L)
      }
      assert(ex2.getMessage.contains("Cannot delete processing time timers"))
    }
  }

  test("registering processing time timeouts with ProcessingTime mode should succeed") {
    tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), TimeoutMode.ProcessingTime())
      assert(handle.getQueryInfo().getPartitionId === 0)
      handle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
      assert(handle.getHandleState === StatefulProcessorHandleState.INITIALIZED)

      ImplicitKeyTracker.setImplicitKey("test_key")
      assert(ImplicitKeyTracker.getImplicitKeyOption.isDefined)

      handle.registerProcessingTimeTimer(10000L)
      handle.deleteProcessingTimeTimer(10000L)

      ImplicitKeyTracker.removeImplicitKey()
      assert(ImplicitKeyTracker.getImplicitKeyOption.isEmpty)
    }
  }

  test("registering processing time timeouts with invalid state should fail") {
    tryWithProviderResource(newStoreProviderWithHandle(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store,
        UUID.randomUUID(), TimeoutMode.ProcessingTime())

      verifyInvalidOperation(handle, StatefulProcessorHandleState.CREATED,
        "Cannot register processing time timer") { handle =>
        registerTimer(handle)
      }

      verifyInvalidOperation(handle, StatefulProcessorHandleState.TIMER_PROCESSED,
        "Cannot register processing time timer") { handle =>
        registerTimer(handle)
      }

      verifyInvalidOperation(handle, StatefulProcessorHandleState.CLOSED,
        "Cannot register processing time timer") { handle =>
        registerTimer(handle)
      }
    }
  }
}
