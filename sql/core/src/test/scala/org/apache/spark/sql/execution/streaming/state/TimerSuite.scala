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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, TimerStateImpl}
import org.apache.spark.sql.streaming.TimeoutMode

/**
 * Class that adds unit tests for Timer State used in arbitrary stateful
 * operators such as transformWithState
 */
class TimerSuite extends StateVariableSuiteBase {
  private def testWithTimeOutMode(testName: String)
      (testFunc: TimeoutMode => Unit): Unit = {
    Seq("Processing", "Event").foreach { timeoutMode =>
      test(s"$timeoutMode timer - " + testName) {
        timeoutMode match {
          case "Processing" => testFunc(TimeoutMode.ProcessingTime())
          case "Event" => testFunc(TimeoutMode.EventTime())
        }
      }
    }
  }

  testWithTimeOutMode("single instance with single key") { timeoutMode =>
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      val timerState = new TimerStateImpl(store, timeoutMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      timerState.registerTimer(1L * 1000)
      assert(timerState.listTimers().toSet === Set(1000L))
      assert(timerState.getExpiredTimers().toSet === Set(("test_key", 1000L)))

      timerState.registerTimer(20L * 1000)
      assert(timerState.listTimers().toSet === Set(20000L, 1000L))
      timerState.deleteTimer(20000L)
      assert(timerState.listTimers().toSet === Set(1000L))
    }
  }

  testWithTimeOutMode("multiple instances with single key") { timeoutMode =>
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      val timerState1 = new TimerStateImpl(store, timeoutMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      val timerState2 = new TimerStateImpl(store, timeoutMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      timerState1.registerTimer(1L * 1000)
      timerState2.registerTimer(15L * 1000)
      assert(timerState1.listTimers().toSet === Set(15000L, 1000L))
      assert(timerState1.getExpiredTimers().toSet ===
        Set(("test_key", 15000L), ("test_key", 1000L)))
      assert(timerState1.listTimers().toSet === Set(15000L, 1000L))

      timerState1.registerTimer(20L * 1000)
      assert(timerState1.listTimers().toSet === Set(20000L, 15000L, 1000L))
      timerState1.deleteTimer(20000L)
      assert(timerState1.listTimers().toSet === Set(15000L, 1000L))
    }
  }

  testWithTimeOutMode("multiple instances with multiple keys") { timeoutMode =>
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key1")
      val timerState1 = new TimerStateImpl(store, timeoutMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      timerState1.registerTimer(1L * 1000)
      timerState1.registerTimer(2L * 1000)
      assert(timerState1.listTimers().toSet === Set(1000L, 2000L))
      ImplicitGroupingKeyTracker.removeImplicitKey()

      ImplicitGroupingKeyTracker.setImplicitKey("test_key2")
      val timerState2 = new TimerStateImpl(store, timeoutMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      timerState2.registerTimer(15L * 1000)
      ImplicitGroupingKeyTracker.removeImplicitKey()

      ImplicitGroupingKeyTracker.setImplicitKey("test_key1")
      assert(timerState1.getExpiredTimers().toSet ===
        Set(("test_key2", 15000L), ("test_key1", 2000L), ("test_key1", 1000L)))
      assert(timerState1.listTimers().toSet === Set(1000L, 2000L))
      ImplicitGroupingKeyTracker.removeImplicitKey()

      ImplicitGroupingKeyTracker.setImplicitKey("test_key2")
      assert(timerState2.listTimers().toSet === Set(15000L))
      assert(timerState2.getExpiredTimers().toSet ===
        Set(("test_key2", 15000L), ("test_key1", 2000L), ("test_key1", 1000L)))
    }
  }
}
