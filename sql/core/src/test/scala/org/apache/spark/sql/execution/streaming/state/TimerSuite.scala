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
import org.apache.spark.sql.streaming.TimeMode

/**
 * Class that adds unit tests for Timer State used in arbitrary stateful
 * operators such as transformWithState
 */
class TimerSuite extends StateVariableSuiteBase {
  private def testWithTimeMode(testName: String)
      (testFunc: TimeMode => Unit): Unit = {
    Seq("Processing", "Event").foreach { timeoutMode =>
      test(s"$timeoutMode timer - " + testName) {
        timeoutMode match {
          case "Processing" => testFunc(TimeMode.ProcessingTime())
          case "Event" => testFunc(TimeMode.EventTime())
        }
      }
    }
  }

  testWithTimeMode("single instance with single key") { timeMode =>
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      val timerState = new TimerStateImpl(store, timeMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      timerState.registerTimer(1L * 1000)
      assert(timerState.listTimers().toSet === Set(1000L))
      assert(timerState.getExpiredTimers(Long.MaxValue).toSeq === Seq(("test_key", 1000L)))
      assert(timerState.getExpiredTimers(Long.MinValue).toSeq === Seq.empty[Long])

      timerState.registerTimer(20L * 1000)
      assert(timerState.listTimers().toSet === Set(20000L, 1000L))
      timerState.deleteTimer(20000L)
      assert(timerState.listTimers().toSet === Set(1000L))
    }
  }

  testWithTimeMode("multiple instances with single key") { timeMode =>
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      val timerState1 = new TimerStateImpl(store, timeMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      val timerState2 = new TimerStateImpl(store, timeMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      timerState1.registerTimer(1L * 1000)
      timerState2.registerTimer(15L * 1000)
      assert(timerState1.listTimers().toSet === Set(15000L, 1000L))
      assert(timerState1.getExpiredTimers(Long.MaxValue).toSeq ===
        Seq(("test_key", 1000L), ("test_key", 15000L)))
      // if timestamp equals to expiryTimestampsMs, will not considered expired
      assert(timerState1.getExpiredTimers(15000L).toSeq === Seq(("test_key", 1000L)))
      assert(timerState1.listTimers().toSet === Set(15000L, 1000L))

      timerState1.registerTimer(20L * 1000)
      assert(timerState1.listTimers().toSet === Set(20000L, 15000L, 1000L))
      timerState1.deleteTimer(20000L)
      assert(timerState1.listTimers().toSet === Set(15000L, 1000L))
    }
  }

  testWithTimeMode("multiple instances with multiple keys") { timeMode =>
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key1")
      val timerState1 = new TimerStateImpl(store, timeMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      timerState1.registerTimer(1L * 1000)
      timerState1.registerTimer(2L * 1000)
      assert(timerState1.listTimers().toSet === Set(1000L, 2000L))
      ImplicitGroupingKeyTracker.removeImplicitKey()

      ImplicitGroupingKeyTracker.setImplicitKey("test_key2")
      val timerState2 = new TimerStateImpl(store, timeMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      timerState2.registerTimer(15L * 1000)
      ImplicitGroupingKeyTracker.removeImplicitKey()

      ImplicitGroupingKeyTracker.setImplicitKey("test_key1")
      assert(timerState1.getExpiredTimers(Long.MaxValue).toSeq ===
        Seq(("test_key1", 1000L), ("test_key1", 2000L), ("test_key2", 15000L)))
      assert(timerState1.getExpiredTimers(10000L).toSeq ===
        Seq(("test_key1", 1000L), ("test_key1", 2000L)))
      assert(timerState1.listTimers().toSet === Set(1000L, 2000L))
      ImplicitGroupingKeyTracker.removeImplicitKey()

      ImplicitGroupingKeyTracker.setImplicitKey("test_key2")
      assert(timerState2.listTimers().toSet === Set(15000L))
      assert(timerState2.getExpiredTimers(1500L).toSeq === Seq(("test_key1", 1000L)))
    }
  }

  testWithTimeMode("Range scan on second index timer key - " +
    "verify timestamp is sorted for single instance") { timeMode =>
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      val timerState = new TimerStateImpl(store, timeMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      val timerTimerstamps = Seq(931L, 8000L, 452300L, 4200L, 90L, 1L, 2L, 8L, 3L, 35L, 6L, 9L, 5L)
      // register/put unordered timestamp into rocksDB
      timerTimerstamps.foreach(timerState.registerTimer)
      assert(timerState.getExpiredTimers(Long.MaxValue).toSeq.map(_._2) === timerTimerstamps.sorted)
      assert(timerState.getExpiredTimers(4200L).toSeq.map(_._2) ===
        timerTimerstamps.sorted.takeWhile(_ < 4200L))
      assert(timerState.getExpiredTimers(Long.MinValue).toSeq === Seq.empty)
      ImplicitGroupingKeyTracker.removeImplicitKey()
    }
  }

  testWithTimeMode("test range scan on second index timer key - " +
    "verify timestamp is sorted for multiple instances") { timeMode =>
    tryWithProviderResource(newStoreProviderWithStateVariable(true)) { provider =>
      val store = provider.getStore(0)

      ImplicitGroupingKeyTracker.setImplicitKey("test_key1")
      val timerState1 = new TimerStateImpl(store, timeMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      val timerTimestamps1 = Seq(64L, 32L, 1024L, 4096L, 0L, 1L)
      timerTimestamps1.foreach(timerState1.registerTimer)

      val timerState2 = new TimerStateImpl(store, timeMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      val timerTimestamps2 = Seq(931L, 8000L, 452300L, 4200L)
      timerTimestamps2.foreach(timerState2.registerTimer)
      ImplicitGroupingKeyTracker.removeImplicitKey()

      ImplicitGroupingKeyTracker.setImplicitKey("test_key3")
      val timerState3 = new TimerStateImpl(store, timeMode,
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])
      val timerTimerStamps3 = Seq(1L, 2L, 8L, 3L)
      timerTimerStamps3.foreach(timerState3.registerTimer)
      ImplicitGroupingKeyTracker.removeImplicitKey()

      assert(timerState1.getExpiredTimers(Long.MaxValue).toSeq.map(_._2) ===
        (timerTimestamps1 ++ timerTimestamps2 ++ timerTimerStamps3).sorted)
      assert(timerState1.getExpiredTimers(Long.MinValue).toSeq === Seq.empty)
      assert(timerState1.getExpiredTimers(8000L).toSeq.map(_._2) ===
        (timerTimestamps1 ++ timerTimestamps2 ++ timerTimerStamps3).sorted.takeWhile(_ < 8000L))
    }
  }
}
