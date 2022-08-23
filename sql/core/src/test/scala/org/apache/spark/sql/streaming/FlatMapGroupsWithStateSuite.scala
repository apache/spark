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

package org.apache.spark.sql.streaming

import java.io.File
import java.sql.{Date, Timestamp}

import org.apache.commons.io.FileUtils
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkException
import org.apache.spark.api.java.Optional
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Encoder, KeyValueGroupedDataset}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsWithState
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.RDDScanExec
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.{FlatMapGroupsWithStateExecHelper, MemoryStateStore, RocksDBStateStoreProvider, StateStore}
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.util.Utils

/** Class to check custom state types */
case class RunningCount(count: Long)

case class Result(key: Long, count: Int)

class FlatMapGroupsWithStateSuite extends StateStoreMetricsTest {

  import testImplicits._

  import FlatMapGroupsWithStateSuite._
  import GroupStateImpl._
  import GroupStateTimeout._

  /**
   * Sample `flatMapGroupsWithState` function implementation. It maintains the max event time as
   * state and set the timeout timestamp based on the current max event time seen. It returns the
   * max event time in the state, or -1 if the state was removed by timeout. Timeout is 5sec.
   */
  val sampleTestFunction =
      (key: String, values: Iterator[(String, Long)], state: GroupState[Long]) => {
    assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
    assertCanGetWatermark { state.getCurrentWatermarkMs() >= -1 }

    val timeoutDelaySec = 5
    if (state.hasTimedOut) {
      state.remove()
      Iterator((key, -1))
    } else {
      val valuesSeq = values.toSeq
      val maxEventTimeSec = math.max(valuesSeq.map(_._2).max, state.getOption.getOrElse(0L))
      val timeoutTimestampSec = maxEventTimeSec + timeoutDelaySec
      state.update(maxEventTimeSec)
      state.setTimeoutTimestamp(timeoutTimestampSec * 1000)
      Iterator((key, maxEventTimeSec.toInt))
    }
  }

  test("SPARK-35800: ensure TestGroupState creates instances the same as prod") {
    val testState = TestGroupState.create[Int](
      Optional.of(5), EventTimeTimeout, 1L, Optional.of(1L), hasTimedOut = false)

    val prodState = GroupStateImpl.createForStreaming[Int](
      Some(5), 1L, 1L, EventTimeTimeout, false, true)

    assert(testState.isInstanceOf[GroupStateImpl[Int]])

    assert(testState.isRemoved === prodState.isRemoved)
    assert(testState.isUpdated === prodState.isUpdated)
    assert(testState.exists === prodState.exists)
    assert(testState.get === prodState.get)
    assert(testState.getTimeoutTimestampMs === prodState.getTimeoutTimestampMs)
    assert(testState.hasTimedOut === prodState.hasTimedOut)
    assert(testState.getCurrentProcessingTimeMs === prodState.getCurrentProcessingTimeMs)
    assert(testState.getCurrentWatermarkMs === prodState.getCurrentWatermarkMs)

    testState.update(6)
    prodState.update(6)
    assert(testState.isUpdated === prodState.isUpdated)
    assert(testState.exists === prodState.exists)
    assert(testState.get === prodState.get)

    testState.remove()
    prodState.remove()
    assert(testState.exists === prodState.exists)
    assert(testState.isRemoved === prodState.isRemoved)
  }

  test("GroupState - get, exists, update, remove") {
    var state: TestGroupState[String] = null

    def testState(
        expectedData: Option[String],
        shouldBeUpdated: Boolean = false,
        shouldBeRemoved: Boolean = false): Unit = {
      if (expectedData.isDefined) {
        assert(state.exists)
        assert(state.get === expectedData.get)
      } else {
        assert(!state.exists)
        intercept[NoSuchElementException] {
          state.get
        }
      }
      assert(state.getOption === expectedData)
      assert(state.isUpdated === shouldBeUpdated)
      assert(state.isRemoved === shouldBeRemoved)
    }

    // === Tests for state in streaming queries ===
    // Updating empty state
    state = TestGroupState.create[String](
      Optional.empty[String], NoTimeout, 1, Optional.empty[Long], hasTimedOut = false)
    testState(None)
    state.update("")
    testState(Some(""), shouldBeUpdated = true)

    // Updating exiting state
    state = TestGroupState.create[String](
      Optional.of("2"), NoTimeout, 1, Optional.empty[Long], hasTimedOut = false)
    testState(Some("2"))
    state.update("3")
    testState(Some("3"), shouldBeUpdated = true)

    // Removing state
    state.remove()
    testState(None, shouldBeRemoved = true, shouldBeUpdated = false)
    state.remove()      // should be still callable
    state.update("4")
    testState(Some("4"), shouldBeRemoved = false, shouldBeUpdated = true)

    // Updating by null throw exception
    intercept[IllegalArgumentException] {
      state.update(null)
    }
  }

  test("GroupState - setTimeout - with NoTimeout") {
    for (initValue <- Seq(Optional.empty[Int], Optional.of((5)))) {
      val states = Seq(
        TestGroupState.create[Int](
          initValue, NoTimeout, 1000, Optional.empty[Long], hasTimedOut = false),
        GroupStateImpl.createForBatch(NoTimeout, watermarkPresent = false)
      )
      for (state <- states) {
        // for streaming queries
        testTimeoutDurationNotAllowed[UnsupportedOperationException](state)
        testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)

        // for batch queries
        testTimeoutDurationNotAllowed[UnsupportedOperationException](state)
        testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)
      }
    }
  }

  test("GroupState - setTimeout - with ProcessingTimeTimeout") {
    // for streaming queries
    var state = TestGroupState.create[Int](
      Optional.empty[Int], ProcessingTimeTimeout, 1000, Optional.empty[Long], hasTimedOut = false)
    assert(!state.getTimeoutTimestampMs.isPresent())
    state.setTimeoutDuration("-1 month 31 days 1 second")
    assert(state.getTimeoutTimestampMs.isPresent())
    assert(state.getTimeoutTimestampMs.get() === 2000)
    state.setTimeoutDuration(500)
    assert(state.getTimeoutTimestampMs.get() === 1500) // can be set without initializing state
    testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)

    state.update(5)
    assert(state.getTimeoutTimestampMs.isPresent())
    assert(state.getTimeoutTimestampMs.get() === 1500) // does not change
    state.setTimeoutDuration(1000)
    assert(state.getTimeoutTimestampMs.get() === 2000)
    state.setTimeoutDuration("2 second")
    assert(state.getTimeoutTimestampMs.get() === 3000)
    testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)

    state.remove()
    assert(state.getTimeoutTimestampMs.isPresent())
    assert(state.getTimeoutTimestampMs.get() === 3000) // does not change
    state.setTimeoutDuration(500) // can still be set
    assert(state.getTimeoutTimestampMs.get() === 1500)
    testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)

    // for batch queries
    state = GroupStateImpl.createForBatch(
      ProcessingTimeTimeout, watermarkPresent = false).asInstanceOf[GroupStateImpl[Int]]
    assert(!state.getTimeoutTimestampMs.isPresent())
    state.setTimeoutDuration(500)
    testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)

    state.update(5)
    state.setTimeoutDuration(1000)
    state.setTimeoutDuration("2 second")
    testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)

    state.remove()
    state.setTimeoutDuration(500)
    testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)
  }

  test("GroupState - setTimeout - with EventTimeTimeout") {
    var state = TestGroupState.create[Int](
        Optional.empty[Int], EventTimeTimeout, 1000, Optional.of(1000), hasTimedOut = false)
    assert(!state.getTimeoutTimestampMs.isPresent())
    testTimeoutDurationNotAllowed[UnsupportedOperationException](state)
    state.setTimeoutTimestamp(5000)
    assert(state.getTimeoutTimestampMs.get() === 5000) // can be set without initializing state

    state.update(5)
    assert(state.getTimeoutTimestampMs.get() === 5000) // does not change
    state.setTimeoutTimestamp(10000)
    assert(state.getTimeoutTimestampMs.get() === 10000)
    state.setTimeoutTimestamp(new Date(20000))
    assert(state.getTimeoutTimestampMs.get() === 20000)
    testTimeoutDurationNotAllowed[UnsupportedOperationException](state)

    state.remove()
    assert(state.getTimeoutTimestampMs.get() === 20000)
    state.setTimeoutTimestamp(5000)
    assert(state.getTimeoutTimestampMs.get() === 5000) // can be set after removing state
    testTimeoutDurationNotAllowed[UnsupportedOperationException](state)

    // for batch queries
    state = GroupStateImpl.createForBatch(
      EventTimeTimeout, watermarkPresent = false).asInstanceOf[GroupStateImpl[Int]]
    assert(!state.getTimeoutTimestampMs.isPresent())
    testTimeoutDurationNotAllowed[UnsupportedOperationException](state)
    state.setTimeoutTimestamp(5000)

    state.update(5)
    state.setTimeoutTimestamp(10000)
    state.setTimeoutTimestamp(new Date(20000))
    testTimeoutDurationNotAllowed[UnsupportedOperationException](state)

    state.remove()
    state.setTimeoutTimestamp(5000)
    testTimeoutDurationNotAllowed[UnsupportedOperationException](state)
  }

  test("GroupState - illegal params to setTimeout") {
    var state: TestGroupState[Int] = null

    // Test setTimeout() with illegal values
    def testIllegalTimeout(body: => Unit): Unit = {
      intercept[IllegalArgumentException] {
        body
      }
      assert(!state.getTimeoutTimestampMs.isPresent())
    }

    // Test setTimeout() with illegal values
    state = TestGroupState.create[Int](
      Optional.of(5), ProcessingTimeTimeout, 1000, Optional.empty[Long], hasTimedOut = false)

    testIllegalTimeout {
      state.setTimeoutDuration(-1000)
    }
    testIllegalTimeout {
      state.setTimeoutDuration(0)
    }
    testIllegalTimeout {
      state.setTimeoutDuration("-2 second")
    }
    testIllegalTimeout {
      state.setTimeoutDuration("-1 month")
    }

    testIllegalTimeout {
      state.setTimeoutDuration("1 month -31 day")
    }

    state = TestGroupState.create[Int](
        Optional.of(5), EventTimeTimeout, 1000, Optional.of(1000), hasTimedOut = false)
    testIllegalTimeout {
      state.setTimeoutTimestamp(-10000)
    }
    testIllegalTimeout {
      state.setTimeoutTimestamp(10000, "-3 second")
    }
    testIllegalTimeout {
      state.setTimeoutTimestamp(10000, "-1 month")
    }
    testIllegalTimeout {
      state.setTimeoutTimestamp(10000, "1 month -32 day")
    }
    testIllegalTimeout {
      state.setTimeoutTimestamp(new Date(-10000))
    }
    testIllegalTimeout {
      state.setTimeoutTimestamp(new Date(-10000), "-3 second")
    }
    testIllegalTimeout {
      state.setTimeoutTimestamp(new Date(-10000), "-1 month")
    }
    testIllegalTimeout {
      state.setTimeoutTimestamp(new Date(-10000), "1 month -32 day")
    }
  }

  test("SPARK-35800: illegal params to create") {
    // eventTimeWatermarkMs >= 0 if present
    var illegalArgument = intercept[IllegalArgumentException] {
      TestGroupState.create[Int](
        Optional.of(5), EventTimeTimeout, 100L, Optional.of(-1000), hasTimedOut = false)
    }
    assert(
      illegalArgument.getMessage.contains("eventTimeWatermarkMs must be 0 or positive if present"))
    illegalArgument = intercept[IllegalArgumentException] {
      GroupStateImpl.createForStreaming[Int](
        Some(5), 100L, -1000L, EventTimeTimeout, false, true)
    }
    assert(
      illegalArgument.getMessage.contains("eventTimeWatermarkMs must be 0 or positive if present"))

    // batchProcessingTimeMs must be positive
    illegalArgument = intercept[IllegalArgumentException] {
      TestGroupState.create[Int](
        Optional.of(5), EventTimeTimeout, -100L, Optional.of(1000), hasTimedOut = false)
    }
    assert(illegalArgument.getMessage.contains("batchProcessingTimeMs must be 0 or positive"))
    illegalArgument = intercept[IllegalArgumentException] {
      GroupStateImpl.createForStreaming[Int](
        Some(5), -100L, 1000L, EventTimeTimeout, false, true)
    }
    assert(illegalArgument.getMessage.contains("batchProcessingTimeMs must be 0 or positive"))

    // hasTimedOut cannot be true if there's no timeout configured
    var unsupportedOperation = intercept[UnsupportedOperationException] {
      TestGroupState.create[Int](
        Optional.of(5), NoTimeout, 100L, Optional.empty[Long], hasTimedOut = true)
    }
    assert(
      unsupportedOperation
        .getMessage.contains("hasTimedOut is true however there's no timeout configured"))
    unsupportedOperation = intercept[UnsupportedOperationException] {
      GroupStateImpl.createForStreaming[Int](
        Some(5), 100L, NO_TIMESTAMP, NoTimeout, true, false)
    }
    assert(
      unsupportedOperation
        .getMessage.contains("hasTimedOut is true however there's no timeout configured"))
  }

  test("GroupState - hasTimedOut") {
    for (timeoutConf <- Seq(NoTimeout, ProcessingTimeTimeout, EventTimeTimeout)) {
      // for streaming queries
      for (initState <- Seq(Optional.empty[Int], Optional.of(5))) {
        val state1 = TestGroupState.create[Int](
          initState, timeoutConf, 1000, Optional.empty[Long], hasTimedOut = false)
        assert(state1.hasTimedOut === false)

        // hasTimedOut can only be set as true when timeoutConf isn't NoTimeout
        if (timeoutConf != NoTimeout) {
          val state2 = TestGroupState.create[Int](
            initState, timeoutConf, 1000, Optional.empty[Long], hasTimedOut = true)
          assert(state2.hasTimedOut)
        }
      }

      // for batch queries
      assert(
        GroupStateImpl.createForBatch(timeoutConf, watermarkPresent = false).hasTimedOut === false)
    }
  }

  test("GroupState - getCurrentWatermarkMs") {
    def streamingState(
        timeoutConf: GroupStateTimeout,
        watermark: Optional[Long]): GroupState[Int] = {
      TestGroupState.create[Int](
        Optional.empty[Int], timeoutConf, 1000, watermark, hasTimedOut = false)
    }

    def batchState(timeoutConf: GroupStateTimeout, watermarkPresent: Boolean): GroupState[Any] = {
      GroupStateImpl.createForBatch(timeoutConf, watermarkPresent)
    }

    def assertWrongTimeoutError(test: => Unit): Unit = {
      val e = intercept[UnsupportedOperationException] { test }
      assert(e.getMessage.contains(
        "Cannot get event time watermark timestamp without setting watermark"))
    }

    for (timeoutConf <- Seq(NoTimeout, EventTimeTimeout, ProcessingTimeTimeout)) {
      // Tests for getCurrentWatermarkMs in streaming queries
      assertWrongTimeoutError {
        streamingState(timeoutConf, Optional.empty[Long]).getCurrentWatermarkMs()
      }
      assert(streamingState(timeoutConf, Optional.of(0)).getCurrentWatermarkMs() === 0)
      assert(streamingState(timeoutConf, Optional.of(1000)).getCurrentWatermarkMs() === 1000)
      assert(streamingState(timeoutConf, Optional.of(2000)).getCurrentWatermarkMs() === 2000)
      assert(batchState(EventTimeTimeout, watermarkPresent = true).getCurrentWatermarkMs() === -1)

      // Tests for getCurrentWatermarkMs in batch queries
      assertWrongTimeoutError {
        batchState(timeoutConf, watermarkPresent = false).getCurrentWatermarkMs()
      }
      assert(batchState(timeoutConf, watermarkPresent = true).getCurrentWatermarkMs() === -1)
    }
  }

  test("GroupState - getCurrentProcessingTimeMs") {
    def streamingState(
      timeoutConf: GroupStateTimeout,
      procTime: Long,
      watermarkPresent: Boolean): GroupState[Int] = {
      val eventTimeWatermarkMs = if (watermarkPresent) {
        Optional.of(1000L)
      } else {
        Optional.empty[Long]
      }
      TestGroupState.create[Int](
        Optional.of(1000), timeoutConf, procTime, eventTimeWatermarkMs, hasTimedOut = false)
    }

    def batchState(timeoutConf: GroupStateTimeout, watermarkPresent: Boolean): GroupState[Any] = {
      GroupStateImpl.createForBatch(timeoutConf, watermarkPresent)
    }

    for (timeoutConf <- Seq(NoTimeout, EventTimeTimeout, ProcessingTimeTimeout)) {
      for (watermarkPresent <- Seq(false, true)) {
        // Tests for getCurrentProcessingTimeMs in streaming queries
        // No negative processing time is allowed, and
        // illegal input validation has been added in the separate test
        assert(streamingState(timeoutConf, 0, watermarkPresent)
          .getCurrentProcessingTimeMs() === 0)
        assert(streamingState(timeoutConf, 1000, watermarkPresent)
          .getCurrentProcessingTimeMs() === 1000)
        assert(streamingState(timeoutConf, 2000, watermarkPresent)
          .getCurrentProcessingTimeMs() === 2000)

        // Tests for getCurrentProcessingTimeMs in batch queries
        val currentTime = System.currentTimeMillis()
        assert(batchState(timeoutConf, watermarkPresent).getCurrentProcessingTimeMs >= currentTime)
      }
    }
  }


  test("GroupState - primitive type") {
    var intState = TestGroupState.create[Int](
      Optional.empty[Int],
      NoTimeout,
      1000,
      Optional.empty[Long],
      hasTimedOut = false)
    intercept[NoSuchElementException] {
      intState.get
    }
    assert(intState.getOption === None)

    intState = TestGroupState.create[Int](
      Optional.of(10),
      NoTimeout,
      1000,
      Optional.empty[Long],
      hasTimedOut = false)

    assert(intState.get == 10)
    intState.update(0)
    assert(intState.get == 0)
    intState.remove()
    intercept[NoSuchElementException] {
      intState.get
    }
  }

  // Values used for testing InputProcessor
  val currentBatchTimestamp = 1000
  val currentBatchWatermark = 1000
  val beforeTimeoutThreshold = 999
  val afterTimeoutThreshold = 1001

  // Tests for InputProcessor.processNewData() when timeout = NoTimeout
  for (priorState <- Seq(None, Some(0))) {
    val priorStateStr = if (priorState.nonEmpty) "prior state set" else "no prior state"
    val testName = s"NoTimeout - $priorStateStr - "

    testStateUpdateWithData(
      testName + "no update",
      stateUpdates = state => {
        assert(state.getCurrentProcessingTimeMs() === currentBatchTimestamp)
        intercept[Exception] { state.getCurrentWatermarkMs() } // watermark not specified
        /* no updates */
      },
      timeoutConf = GroupStateTimeout.NoTimeout,
      priorState = priorState,
      expectedState = priorState)    // should not change

    testStateUpdateWithData(
      testName + "state updated",
      stateUpdates = state => { state.update(5) },
      timeoutConf = GroupStateTimeout.NoTimeout,
      priorState = priorState,
      expectedState = Some(5))     // should change

    testStateUpdateWithData(
      testName + "state removed",
      stateUpdates = state => { state.remove() },
      timeoutConf = GroupStateTimeout.NoTimeout,
      priorState = priorState,
      expectedState = None)        // should be removed
  }

  // Tests for InputProcessor.processTimedOutState() when timeout != NoTimeout
  for (priorState <- Seq(None, Some(0))) {
    for (priorTimeoutTimestamp <- Seq(NO_TIMESTAMP, 1000)) {
      var testName = ""
      if (priorState.nonEmpty) {
        testName += "prior state set, "
        if (priorTimeoutTimestamp == 1000) {
          testName += "prior timeout set"
        } else {
          testName += "no prior timeout"
        }
      } else {
        testName += "no prior state"
      }
      for (timeoutConf <- Seq(ProcessingTimeTimeout, EventTimeTimeout)) {

        testStateUpdateWithData(
          s"$timeoutConf - $testName - no update",
          stateUpdates = state => {
            assert(state.getCurrentProcessingTimeMs() === currentBatchTimestamp)
            intercept[Exception] { state.getCurrentWatermarkMs() } // watermark not specified
            /* no updates */
          },
          timeoutConf = timeoutConf,
          priorState = priorState,
          priorTimeoutTimestamp = priorTimeoutTimestamp,
          expectedState = priorState,                           // state should not change
          expectedTimeoutTimestamp = NO_TIMESTAMP) // timestamp should be reset

        testStateUpdateWithData(
          s"$timeoutConf - $testName - state updated",
          stateUpdates = state => { state.update(5) },
          timeoutConf = timeoutConf,
          priorState = priorState,
          priorTimeoutTimestamp = priorTimeoutTimestamp,
          expectedState = Some(5),                              // state should change
          expectedTimeoutTimestamp = NO_TIMESTAMP) // timestamp should be reset

        testStateUpdateWithData(
          s"$timeoutConf - $testName - state removed",
          stateUpdates = state => { state.remove() },
          timeoutConf = timeoutConf,
          priorState = priorState,
          priorTimeoutTimestamp = priorTimeoutTimestamp,
          expectedState = None)                                 // state should be removed
      }

      // Tests with ProcessingTimeTimeout
      if (priorState == None) {
        testStateUpdateWithData(
          s"ProcessingTimeTimeout - $testName - timeout updated without initializing state",
          stateUpdates = state => { state.setTimeoutDuration(5000) },
          timeoutConf = ProcessingTimeTimeout,
          priorState = None,
          priorTimeoutTimestamp = priorTimeoutTimestamp,
          expectedState = None,
          expectedTimeoutTimestamp = currentBatchTimestamp + 5000)
      }

      testStateUpdateWithData(
        s"ProcessingTimeTimeout - $testName - state and timeout duration updated",
        stateUpdates =
          (state: GroupState[Int]) => { state.update(5); state.setTimeoutDuration(5000) },
        timeoutConf = ProcessingTimeTimeout,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = Some(5),                                 // state should change
        expectedTimeoutTimestamp = currentBatchTimestamp + 5000) // timestamp should change

      testStateUpdateWithData(
        s"ProcessingTimeTimeout - $testName - timeout updated after state removed",
        stateUpdates = state => { state.remove(); state.setTimeoutDuration(5000) },
        timeoutConf = ProcessingTimeTimeout,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = None,
        expectedTimeoutTimestamp = currentBatchTimestamp + 5000)

      // Tests with EventTimeTimeout

      if (priorState == None) {
        testStateUpdateWithData(
          s"EventTimeTimeout - $testName - setting timeout without init state not allowed",
          stateUpdates = state => {
            state.setTimeoutTimestamp(10000)
          },
          timeoutConf = EventTimeTimeout,
          priorState = None,
          priorTimeoutTimestamp = priorTimeoutTimestamp,
          expectedState = None,
          expectedTimeoutTimestamp = 10000)
      }

      testStateUpdateWithData(
        s"EventTimeTimeout - $testName - state and timeout timestamp updated",
        stateUpdates =
          (state: GroupState[Int]) => { state.update(5); state.setTimeoutTimestamp(5000) },
        timeoutConf = EventTimeTimeout,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = Some(5),                                 // state should change
        expectedTimeoutTimestamp = 5000)                         // timestamp should change

      testStateUpdateWithData(
        s"EventTimeTimeout - $testName - timeout timestamp updated to before watermark",
        stateUpdates =
          (state: GroupState[Int]) => {
            state.update(5)
            intercept[IllegalArgumentException] {
              state.setTimeoutTimestamp(currentBatchWatermark - 1)  // try to set to < watermark
            }
          },
        timeoutConf = EventTimeTimeout,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = Some(5),                                 // state should change
        expectedTimeoutTimestamp = NO_TIMESTAMP)                 // timestamp should not update

      testStateUpdateWithData(
        s"EventTimeTimeout - $testName - setting timeout with state removal not allowed",
        stateUpdates = state => {
          state.remove(); state.setTimeoutTimestamp(10000)
        },
        timeoutConf = EventTimeTimeout,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = None,
        expectedTimeoutTimestamp = 10000)
    }
  }

  // Tests for InputProcessor.processTimedOutState()
  val preTimeoutState = Some(5)
  for (timeoutConf <- Seq(ProcessingTimeTimeout, EventTimeTimeout)) {
    testStateUpdateWithTimeout(
      s"$timeoutConf - should not timeout",
      stateUpdates = state => { assert(false, "function called without timeout") },
      timeoutConf = timeoutConf,
      priorTimeoutTimestamp = afterTimeoutThreshold,
      expectedState = preTimeoutState,                          // state should not change
      expectedTimeoutTimestamp = afterTimeoutThreshold)         // timestamp should not change

    testStateUpdateWithTimeout(
      s"$timeoutConf - should timeout - no update/remove",
      stateUpdates = state => {
        assert(state.getCurrentProcessingTimeMs() === currentBatchTimestamp)
        intercept[Exception] { state.getCurrentWatermarkMs() } // watermark not specified
        /* no updates */
      },
      timeoutConf = timeoutConf,
      priorTimeoutTimestamp = beforeTimeoutThreshold,
      expectedState = preTimeoutState,                          // state should not change
      expectedTimeoutTimestamp = NO_TIMESTAMP)     // timestamp should be reset

    testStateUpdateWithTimeout(
      s"$timeoutConf - should timeout - update state",
      stateUpdates = state => { state.update(5) },
      timeoutConf = timeoutConf,
      priorTimeoutTimestamp = beforeTimeoutThreshold,
      expectedState = Some(5),                                  // state should change
      expectedTimeoutTimestamp = NO_TIMESTAMP)     // timestamp should be reset

    testStateUpdateWithTimeout(
      s"$timeoutConf - should timeout - remove state",
      stateUpdates = state => { state.remove() },
      timeoutConf = timeoutConf,
      priorTimeoutTimestamp = beforeTimeoutThreshold,
      expectedState = None,                                     // state should be removed
      expectedTimeoutTimestamp = NO_TIMESTAMP)
  }

  testStateUpdateWithTimeout(
    "ProcessingTimeTimeout - should timeout - timeout duration updated",
    stateUpdates = state => { state.setTimeoutDuration(2000) },
    timeoutConf = ProcessingTimeTimeout,
    priorTimeoutTimestamp = beforeTimeoutThreshold,
    expectedState = preTimeoutState,                          // state should not change
    expectedTimeoutTimestamp = currentBatchTimestamp + 2000)       // timestamp should change

  testStateUpdateWithTimeout(
    "ProcessingTimeTimeout - should timeout - timeout duration and state updated",
    stateUpdates = state => { state.update(5); state.setTimeoutDuration(2000) },
    timeoutConf = ProcessingTimeTimeout,
    priorTimeoutTimestamp = beforeTimeoutThreshold,
    expectedState = Some(5),                                  // state should change
    expectedTimeoutTimestamp = currentBatchTimestamp + 2000)  // timestamp should change

  testStateUpdateWithTimeout(
    "EventTimeTimeout - should timeout - timeout timestamp updated",
    stateUpdates = state => { state.setTimeoutTimestamp(5000) },
    timeoutConf = EventTimeTimeout,
    priorTimeoutTimestamp = beforeTimeoutThreshold,
    expectedState = preTimeoutState,                          // state should not change
    expectedTimeoutTimestamp = 5000)                          // timestamp should change

  testStateUpdateWithTimeout(
    "EventTimeTimeout - should timeout - timeout and state updated",
    stateUpdates = state => { state.update(5); state.setTimeoutTimestamp(5000) },
    timeoutConf = EventTimeTimeout,
    priorTimeoutTimestamp = beforeTimeoutThreshold,
    expectedState = Some(5),                                  // state should change
    expectedTimeoutTimestamp = 5000)                          // timestamp should change

  testWithAllStateVersions("flatMapGroupsWithState - streaming") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }

      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        Iterator.empty
      } else {
        state.update(RunningCount(count))
        Iterator((key, count.toString))
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(Update, GroupStateTimeout.NoTimeout)(stateFunc)

    testStream(result, Update)(
      AddData(inputData, "a"),
      CheckNewAnswer(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a", "b"),
      CheckNewAnswer(("a", "2"), ("b", "1")),
      assertNumStateRows(total = 2, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
      CheckNewAnswer(("b", "2")),
      assertNumStateRows(
        total = Seq(1), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(1))),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
      CheckNewAnswer(("a", "1"), ("c", "1")),
      assertNumStateRows(total = 3, updated = 2)
    )
  }

  test("flatMapGroupsWithState - streaming + func returns iterator that updates state lazily") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    // Additionally, it updates state lazily as the returned iterator get consumed
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      values.flatMap { _ =>
        val count = state.getOption.map(_.count).getOrElse(0L) + 1
        if (count == 3) {
          state.remove()
          None
        } else {
          state.update(RunningCount(count))
          Some((key, count.toString))
        }
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(Update, GroupStateTimeout.NoTimeout)(stateFunc)
    testStream(result, Update)(
      AddData(inputData, "a", "a", "b"),
      CheckNewAnswer(("a", "1"), ("a", "2"), ("b", "1")),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
      CheckNewAnswer(("b", "2")),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
      CheckNewAnswer(("a", "1"), ("c", "1"))
    )
  }

  testWithAllStateVersions("flatMapGroupsWithState - streaming + aggregation") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {

      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        Iterator(key -> "-1")
      } else {
        state.update(RunningCount(count))
        Iterator(key -> count.toString)
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(Append, GroupStateTimeout.NoTimeout)(stateFunc)
        .groupByKey(_._1)
        .count()

    testStream(result, Complete)(
      AddData(inputData, "a"),
      CheckNewAnswer(("a", 1)),
      AddData(inputData, "a", "b"),
      // mapGroups generates ("a", "2"), ("b", "1"); so increases counts of a and b by 1
      CheckNewAnswer(("a", 2), ("b", 1)),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"),
      // mapGroups should remove state for "a" and generate ("a", "-1"), ("b", "2") ;
      // so increment a and b by 1
      CheckNewAnswer(("a", 3), ("b", 2)),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"),
      // mapGroups should recreate state for "a" and generate ("a", "1"), ("c", "1") ;
      // so increment a and c by 1
      CheckNewAnswer(("a", 4), ("b", 2), ("c", 1))
    )
  }

  test("flatMapGroupsWithState - batch") {
    // Function that returns running count only if its even, otherwise does not return
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() > 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }

      if (state.exists) throw new IllegalArgumentException("state.exists should be false")
      Iterator((key, values.size))
    }
    val df = Seq("a", "a", "b").toDS
      .groupByKey(x => x)
      .flatMapGroupsWithState(Update, GroupStateTimeout.NoTimeout)(stateFunc).toDF
    checkAnswer(df, Seq(("a", 2), ("b", 1)).toDF)
  }

  testWithAllStateVersions("flatMapGroupsWithState - streaming with processing time timeout") {
    // Function to maintain the count as state and set the proc. time timeout delay of 10 seconds.
    // It returns the count if changed, or -1 if the state was removed by timeout.
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }

      if (state.hasTimedOut) {
        state.remove()
        Iterator((key, "-1"))
      } else {
        val count = state.getOption.map(_.count).getOrElse(0L) + values.size
        state.update(RunningCount(count))
        state.setTimeoutDuration("10 seconds")
        Iterator((key, count.toString))
      }
    }

    val clock = new StreamManualClock
    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(Update, ProcessingTimeTimeout)(stateFunc)

    testStream(result, Update)(
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
      AddData(inputData, "a"),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),

      AddData(inputData, "b"),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(("b", "1")),
      assertNumStateRows(total = 2, updated = 1),

      AddData(inputData, "b"),
      AdvanceManualClock(10 * 1000),
      CheckNewAnswer(("a", "-1"), ("b", "2")),
      assertNumStateRows(
        total = Seq(1), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(1))),

      StopStream,
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),

      AddData(inputData, "c"),
      AdvanceManualClock(11 * 1000),
      CheckNewAnswer(("b", "-1"), ("c", "1")),
      assertNumStateRows(
        total = Seq(1), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(1))),

      AdvanceManualClock(12 * 1000),
      AssertOnQuery { _ => clock.getTimeMillis() == 35000 },
      Execute { q =>
        failAfter(streamingTimeout) {
          while (q.lastProgress.timestamp != "1970-01-01T00:00:35.000Z") {
            Thread.sleep(1)
          }
        }
      },
      CheckNewAnswer(("c", "-1")),
      assertNumStateRows(
        total = Seq(0), updated = Seq(0), droppedByWatermark = Seq(0), removed = Some(Seq(1)))
    )
  }

  testWithAllStateVersions("flatMapGroupsWithState - streaming w/ event time timeout + watermark") {
    val inputData = MemoryStream[(String, Int)]
    val result =
      inputData.toDS
        .select($"_1".as("key"), timestamp_seconds($"_2").as("eventTime"))
        .withWatermark("eventTime", "10 seconds")
        .as[(String, Long)]
        .groupByKey(_._1)
        .flatMapGroupsWithState(Update, EventTimeTimeout)(sampleTestFunction)

    testStream(result, Update)(
      StartStream(),

      AddData(inputData, ("a", 11), ("a", 13), ("a", 15)),
      // Max event time = 15. Timeout timestamp for "a" = 15 + 5 = 20. Watermark = 15 - 10 = 5.
      CheckNewAnswer(("a", 15)),  // Output = max event time of a

      AddData(inputData, ("a", 4)),       // Add data older than watermark for "a"
      CheckNewAnswer(),                   // No output as data should get filtered by watermark

      AddData(inputData, ("a", 10)),      // Add data newer than watermark for "a"
      CheckNewAnswer(("a", 15)),          // Max event time is still the same
      // Timeout timestamp for "a" is still 20 as max event time for "a" is still 15.
      // Watermark is still 5 as max event time for all data is still 15.

      AddData(inputData, ("b", 31)),      // Add data newer than watermark for "b", not "a"
      // Watermark = 31 - 10 = 21, so "a" should be timed out as timeout timestamp for "a" is 20.
      CheckNewAnswer(("a", -1), ("b", 31))           // State for "a" should timeout and emit -1
    )
  }

  test("flatMapGroupsWithState - uses state format version 2 by default") {
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      state.update(RunningCount(count))
      Iterator((key, count.toString))
    }

    val inputData = MemoryStream[String]
    val result = inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(Update, GroupStateTimeout.NoTimeout)(stateFunc)

    testStream(result, Update)(
      AddData(inputData, "a"),
      CheckNewAnswer(("a", "1")),
      Execute { query =>
        // Verify state format = 2
        val f = query.lastExecution.executedPlan.collect { case f: FlatMapGroupsWithStateExec => f }
        assert(f.size == 1)
        assert(f.head.stateFormatVersion == 2)
      }
    )
  }

  test("flatMapGroupsWithState - recovery from checkpoint uses state format version 1") {
    val inputData = MemoryStream[(String, Int)]
    val result =
      inputData.toDS
        .select($"_1".as("key"), timestamp_seconds($"_2").as("eventTime"))
        .withWatermark("eventTime", "10 seconds")
        .as[(String, Long)]
        .groupByKey(_._1)
        .flatMapGroupsWithState(Update, EventTimeTimeout)(sampleTestFunction)

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-2.3.1-flatMapGroupsWithState-state-format-1/").toURI

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    inputData.addData(("a", 11), ("a", 13), ("a", 15))
    inputData.addData(("a", 4))

    testStream(result, Update)(
      StartStream(
        checkpointLocation = checkpointDir.getAbsolutePath,
        additionalConfs = Map(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> "2")),
      /*
      Note: The checkpoint was generated using the following input in Spark version 2.3.1

      AddData(inputData, ("a", 11), ("a", 13), ("a", 15)),
      // Max event time = 15. Timeout timestamp for "a" = 15 + 5 = 20. Watermark = 15 - 10 = 5.
      CheckNewAnswer(("a", 15)),  // Output = max event time of a

      AddData(inputData, ("a", 4)),       // Add data older than watermark for "a"
      CheckNewAnswer(),                   // No output as data should get filtered by watermark
      */

      AddData(inputData, ("a", 10)),      // Add data newer than watermark for "a"
      CheckNewAnswer(("a", 15)),          // Max event time is still the same
      // Timeout timestamp for "a" is still 20 as max event time for "a" is still 15.
      // Watermark is still 5 as max event time for all data is still 15.

      Execute { query =>
        // Verify state format = 1
        val f = query.lastExecution.executedPlan.collect { case f: FlatMapGroupsWithStateExec => f }
        assert(f.size == 1)
        assert(f.head.stateFormatVersion == 1)
      },

      AddData(inputData, ("b", 31)),      // Add data newer than watermark for "b", not "a"
      // Watermark = 31 - 10 = 21, so "a" should be timed out as timeout timestamp for "a" is 20.
      CheckNewAnswer(("a", -1), ("b", 31))           // State for "a" should timeout and emit -1
    )
  }

  test("mapGroupsWithState - streaming") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }

      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        (key, "-1")
      } else {
        state.update(RunningCount(count))
        (key, count.toString)
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc) // Types = State: MyState, Out: (Str, Str)

    testStream(result, Update)(
      AddData(inputData, "a"),
      CheckNewAnswer(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a", "b"),
      CheckNewAnswer(("a", "2"), ("b", "1")),
      assertNumStateRows(total = 2, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and return count as -1
      CheckNewAnswer(("a", "-1"), ("b", "2")),
      assertNumStateRows(total = 1, updated = 1),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1
      CheckNewAnswer(("a", "1"), ("c", "1")),
      assertNumStateRows(total = 3, updated = 2)
    )
  }

  test("mapGroupsWithState - batch") {
    // Test the following
    // - no initial state
    // - timeouts operations work, does not throw any error [SPARK-20792]
    // - works with primitive state type
    // - can get processing time
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[Int]) => {
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() > 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }

      if (state.exists) throw new IllegalArgumentException("state.exists should be false")
      state.setTimeoutTimestamp(0, "1 hour")
      state.update(10)
      (key, values.size)
    }

    checkAnswer(
      spark.createDataset(Seq("a", "a", "b"))
        .groupByKey(x => x)
        .mapGroupsWithState(EventTimeTimeout)(stateFunc)
        .toDF,
      spark.createDataset(Seq(("a", 2), ("b", 1))).toDF)
  }

  test("SPARK-35896: metrics in StateOperatorProgress are output correctly") {
    val inputData = MemoryStream[(String, Int)]
    val result =
      inputData.toDS
        .select($"_1".as("key"), timestamp_seconds($"_2").as("eventTime"))
        .withWatermark("eventTime", "10 seconds")
        .as[(String, Long)]
        .groupByKey(_._1)
        .flatMapGroupsWithState(Update, EventTimeTimeout)(sampleTestFunction)

    testStream(result, Update)(
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "3")),

      AddData(inputData, ("a", 11), ("a", 13), ("a", 15)),
      // Max event time = 15. Timeout timestamp for "a" = 15 + 5 = 20. Watermark = 15 - 10 = 5.
      CheckNewAnswer(("a", 15)),  // Output = max event time of a
      assertNumStateRows(
        total = Seq(1), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(0))),

      AddData(inputData, ("a", 4)),       // Add data older than watermark for "a"
      CheckNewAnswer(),                   // No output as data should get filtered by watermark
      assertStateOperatorProgressMetric(operatorName = "flatMapGroupsWithState",
        numShufflePartitions = 3, numStateStoreInstances = 3),

      AddData(inputData, ("a", 10)),      // Add data newer than watermark for "a"
      CheckNewAnswer(("a", 15)),          // Max event time is still the same
      // Timeout timestamp for "a" is still 20 as max event time for "a" is still 15.
      // Watermark is still 5 as max event time for all data is still 15.

      AddData(inputData, ("b", 31)),      // Add data newer than watermark for "b", not "a"
      // Watermark = 31 - 10 = 21, so "a" should be timed out as timeout timestamp for "a" is 20.
      CheckNewAnswer(("a", -1), ("b", 31)), // State for "a" should timeout and emit -1
      assertNumStateRows(
        total = Seq(1), updated = Seq(2), droppedByWatermark = Seq(0), removed = Some(Seq(1)))
    )
  }

  testWithAllStateVersions("SPARK-29438: ensure UNION doesn't lead (flat)MapGroupsWithState" +
    " to use shifted partition IDs") {
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      state.update(RunningCount(count))
      (key, count.toString)
    }

    def constructUnionDf(desiredPartitionsForInput1: Int)
      : (MemoryStream[String], MemoryStream[String], DataFrame) = {
      val input1 = MemoryStream[String](desiredPartitionsForInput1)
      val input2 = MemoryStream[String]
      val df1 = input1.toDF()
        .select($"value", $"value")
      val df2 = input2.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc) // Types = State: MyState, Out: (Str, Str)
        .toDF()

      // Unioned DF would have columns as (String, String)
      (input1, input2, df1.union(df2))
    }

    withTempDir { checkpointDir =>
      val (input1, input2, unionDf) = constructUnionDf(2)
      testStream(unionDf, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        MultiAddData(input1, "input1-a")(input2, "input2-a"),
        CheckNewAnswer(("input1-a", "input1-a"), ("input2-a", "1")),
        StopStream
      )

      // We're restoring the query with different number of partitions in left side of UNION,
      // which may lead right side of union to have mismatched partition IDs (e.g. if it relies on
      // TaskContext.partitionId()). This test will verify (flat)MapGroupsWithState doesn't have
      // such issue.

      val (newInput1, newInput2, newUnionDf) = constructUnionDf(3)

      newInput1.addData("input1-a")
      newInput2.addData("input2-a")

      testStream(newUnionDf, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        MultiAddData(newInput1, "input1-a")(newInput2, "input2-a", "input2-b"),
        CheckNewAnswer(("input1-a", "input1-a"), ("input2-a", "2"), ("input2-b", "1"))
      )
    }
  }

  testQuietly("StateStore.abort on task failure handling") {
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      if (FlatMapGroupsWithStateSuite.failInTask) throw new Exception("expected failure")
      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      state.update(RunningCount(count))
      (key, count)
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc) // Types = State: MyState, Out: (Str, Str)

    def setFailInTask(value: Boolean): AssertOnQuery = AssertOnQuery { q =>
      FlatMapGroupsWithStateSuite.failInTask = value
      true
    }

    testStream(result, Update)(
      setFailInTask(false),
      AddData(inputData, "a"),
      CheckNewAnswer(("a", 1L)),
      AddData(inputData, "a"),
      CheckNewAnswer(("a", 2L)),
      setFailInTask(true),
      AddData(inputData, "a"),
      ExpectFailure[SparkException](),   // task should fail but should not increment count
      setFailInTask(false),
      StartStream(),
      CheckNewAnswer(("a", 3L))     // task should not fail, and should show correct count
    )
  }

  test("output partitioning is unknown") {
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => key
    val inputData = MemoryStream[String]
    val result = inputData.toDS.groupByKey(x => x).mapGroupsWithState(stateFunc)
    testStream(result, Update)(
      AddData(inputData, "a"),
      CheckNewAnswer("a"),
      AssertOnQuery(_.lastExecution.executedPlan.outputPartitioning === UnknownPartitioning(0))
    )
  }

  test("disallow complete mode") {
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[Int]) => {
      Iterator[String]()
    }

    var e = intercept[IllegalArgumentException] {
      MemoryStream[String].toDS().groupByKey(x => x).flatMapGroupsWithState(
        OutputMode.Complete, GroupStateTimeout.NoTimeout)(stateFunc)
    }
    assert(e.getMessage === "The output mode of function should be append or update")

    val javaStateFunc = new FlatMapGroupsWithStateFunction[String, String, Int, String] {
      import java.util.{Iterator => JIterator}
      override def call(
        key: String,
        values: JIterator[String],
        state: GroupState[Int]): JIterator[String] = { null }
    }
    e = intercept[IllegalArgumentException] {
      MemoryStream[String].toDS().groupByKey(x => x).flatMapGroupsWithState(
        javaStateFunc, OutputMode.Complete,
        implicitly[Encoder[Int]], implicitly[Encoder[String]], GroupStateTimeout.NoTimeout)
    }
    assert(e.getMessage === "The output mode of function should be append or update")
  }

  import testImplicits._

  /**
   * FlatMapGroupsWithState function that returns the key, value as passed to it
   * along with the updated state. The state is incremented for every value.
   */
  val flatMapGroupsWithStateFunc =
    (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      val valList = values.toSeq
      if (valList.isEmpty) {
        // When the function is called on just the initial state make sure the other fields
        // are set correctly
        assert(state.exists)
      }
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }
      assert(!state.hasTimedOut)
      if (key.contains("EventTime")) {
        state.setTimeoutTimestamp(0, "1 hour")
      }
      if (key.contains("ProcessingTime")) {
        state.setTimeoutDuration("1  hour")
      }
      val count = state.getOption.map(_.count).getOrElse(0L) + valList.size
      // We need to check if not explicitly calling update will still save the init state or not
      if (!key.contains("NoUpdate")) {
        // this is not reached when valList is empty and the state count is 2
        state.update(new RunningCount(count))
      }
      Iterator((key, valList, count.toString))
    }

  Seq("1", "2", "6").foreach { shufflePartitions =>
    testWithAllStateVersions(s"flatMapGroupsWithState - initial " +
        s"state - all cases - shuffle partitions ${shufflePartitions}") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> shufflePartitions) {
        // We will test them on different shuffle partition configuration to make sure the
        // grouping by key will still work. On higher number of shuffle partitions its possible
        // that all keys end up on different partitions.
        val initialState: Dataset[(String, RunningCount)] = Seq(
          ("keyInStateAndData-1", new RunningCount(1)),
          ("keyInStateAndData-2", new RunningCount(2)),
          ("keyNoUpdate", new RunningCount(2)), // state.update will not be called
          ("keyOnlyInState-1", new RunningCount(1))
        ).toDS()

        val it = initialState.groupByKey(x => x._1).mapValues(_._2)
        val inputData = MemoryStream[String]
        val result =
          inputData.toDS()
            .groupByKey(x => x)
            .flatMapGroupsWithState(
              Update, GroupStateTimeout.NoTimeout, it)(flatMapGroupsWithStateFunc)

        testStream(result, Update)(
          AddData(inputData, "keyOnlyInData", "keyInStateAndData-2"),
          CheckNewAnswer(
            ("keyOnlyInState-1", Seq[String](), "1"),
            ("keyNoUpdate", Seq[String](), "2"), // update will not be called
            ("keyInStateAndData-2", Seq[String]("keyInStateAndData-2"), "3"), // inc by 1
            ("keyInStateAndData-1", Seq[String](), "1"),
            ("keyOnlyInData", Seq[String]("keyOnlyInData"), "1") // inc by 1
          ),
          assertNumStateRows(total = 5, updated = 4),
          // Stop and Start stream to make sure initial state doesn't get applied again.
          StopStream,
          StartStream(),
          AddData(inputData, "keyInStateAndData-1"),
          CheckNewAnswer(
            // state incremented by 1
            ("keyInStateAndData-1", Seq[String]("keyInStateAndData-1"), "2")
          ),
          assertNumStateRows(total = 5, updated = 1),
          StopStream
        )
      }
    }
  }

  testWithAllStateVersions("flatMapGroupsWithState - initial state - case class key") {
    val stateFunc = (key: User, values: Iterator[User], state: GroupState[Long]) => {
      val valList = values.toSeq
      if (valList.isEmpty) {
        // When the function is called on just the initial state make sure the other fields
        // are set correctly
        assert(state.exists)
      }
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }
      assert(!state.hasTimedOut)
      val count = state.getOption.getOrElse(0L) + valList.size
      // We need to check if not explicitly calling update will still save the state or not
      if (!key.name.contains("NoUpdate")) {
        // this is not reached when valList is empty and the state count is 2
        state.update(count)
      }
      Iterator((key, valList.map(_.name), count.toString))
    }

    val ds = Seq(
      (User("keyInStateAndData", "1"), (1L)),
      (User("keyOnlyInState", "1"), (1L)),
      (User("keyNoUpdate", "2"), (2L)) // state.update will not be called on this in the function
    ).toDS().groupByKey(_._1).mapValues(_._2)

    val inputData = MemoryStream[User]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(Update, NoTimeout(), ds)(stateFunc)

    testStream(result, Update)(
      AddData(inputData, User("keyInStateAndData", "1"), User("keyOnlyInData", "1")),
      CheckNewAnswer(
        (("keyInStateAndData", "1"), Seq[String]("keyInStateAndData"), "2"),
        (("keyOnlyInState", "1"), Seq[String](), "1"),
        (("keyNoUpdate", "2"), Seq[String](), "2"),
        (("keyOnlyInData", "1"), Seq[String]("keyOnlyInData"), "1")
      ),
      assertNumStateRows(total = 4, updated = 3), // (keyOnlyInState, 2) does not call update()
      // Stop and Start stream to make sure initial state doesn't get applied again.
      StopStream,
      StartStream(),
      AddData(inputData, User("keyOnlyInData", "1")),
      CheckNewAnswer(
        (("keyOnlyInData", "1"), Seq[String]("keyOnlyInData"), "2")
      ),
      assertNumStateRows(total = 4, updated = 1),
      StopStream
    )
  }

  testQuietly("flatMapGroupsWithState - initial state - duplicate keys") {
    val initialState = Seq(
      ("a", new RunningCount(2)),
      ("a", new RunningCount(1))
    ).toDS().groupByKey(_._1).mapValues(_._2)

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(Update, NoTimeout(), initialState)(flatMapGroupsWithStateFunc)
    testStream(result, Update)(
      AddData(inputData, "a"),
      ExpectFailure[SparkException] { e =>
        assert(e.getCause.getMessage.contains("The initial state provided contained " +
          "multiple rows(state) with the same key"))
      }
    )
  }

  Seq(NoTimeout(), EventTimeTimeout(), ProcessingTimeTimeout()).foreach { timeout =>
    test(s"flatMapGroupsWithState - initial state - batch mode - timeout ${timeout}") {
      // We will test them on different shuffle partition configuration to make sure the
      // grouping by key will still work. On higher number of shuffle partitions its possible
      // that all keys end up on different partitions.
      val initialState = Seq(
        (s"keyInStateAndData-1-$timeout", new RunningCount(1)),
        ("keyInStateAndData-2", new RunningCount(2)),
        ("keyNoUpdate", new RunningCount(2)), // state.update will not be called
        ("keyOnlyInState-1", new RunningCount(1))
      ).toDS().groupByKey(x => x._1).mapValues(_._2)

      val inputData = Seq(
        ("keyOnlyInData"), ("keyInStateAndData-2")
      )
      val result = inputData.toDS().groupByKey(x => x)
        .flatMapGroupsWithState(
          Update, timeout, initialState)(flatMapGroupsWithStateFunc)

      val expected = Seq(
        ("keyOnlyInState-1", Seq[String](), "1"),
        ("keyNoUpdate", Seq[String](), "2"), // update will not be called
        ("keyInStateAndData-2", Seq[String]("keyInStateAndData-2"), "3"), // inc by 1
        (s"keyInStateAndData-1-$timeout", Seq[String](), "1"),
        ("keyOnlyInData", Seq[String]("keyOnlyInData"), "1") // inc by 1
      ).toDF()
      checkAnswer(result.toDF(), expected)
    }
  }

  testQuietly("flatMapGroupsWithState - initial state - batch mode - duplicate state") {
    val initialState = Seq(
      ("a", new RunningCount(1)),
      ("a", new RunningCount(2))
    ).toDS().groupByKey(x => x._1).mapValues(_._2)

    val e = intercept[SparkException] {
      Seq("a", "b").toDS().groupByKey(x => x)
        .flatMapGroupsWithState(Update, NoTimeout(), initialState)(flatMapGroupsWithStateFunc)
        .show()
    }
    assert(e.getMessage.contains(
      "The initial state provided contained multiple rows(state) with the same key." +
        " Make sure to de-duplicate the initial state before passing it."))
  }

  testQuietly("flatMapGroupsWithState - initial state - streaming initial state") {
    val initialStateData = MemoryStream[(String, RunningCount)]
    initialStateData.addData(("a", new RunningCount(1)))

    val inputData = MemoryStream[String]

    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(
          Update, NoTimeout(), initialStateData.toDS().groupByKey(_._1).mapValues(_._2)
        )(flatMapGroupsWithStateFunc)

    val e = intercept[AnalysisException] {
      result.writeStream
        .format("console")
        .start()
    }

    val expectedError = "Non-streaming DataFrame/Dataset is not supported" +
      " as the initial state in [flatMap|map]GroupsWithState" +
      " operation on a streaming DataFrame/Dataset"
    assert(e.message.contains(expectedError))
  }

  test("flatMapGroupsWithState - initial state - initial state has flatMapGroupsWithState") {
    val initialStateDS = Seq(("keyInStateAndData", new RunningCount(1))).toDS()
    val initialState: KeyValueGroupedDataset[String, RunningCount] =
      initialStateDS.groupByKey(_._1).mapValues(_._2)
        .mapGroupsWithState(
          GroupStateTimeout.NoTimeout())(
            (key: String, values: Iterator[RunningCount], state: GroupState[Boolean]) => {
              (key, values.next())
            }
          ).groupByKey(_._1).mapValues(_._2)

    val inputData = MemoryStream[String]

    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(
          Update, NoTimeout(), initialState
        )(flatMapGroupsWithStateFunc)

    testStream(result, Update)(
      AddData(inputData, "keyInStateAndData"),
      CheckNewAnswer(
        ("keyInStateAndData", Seq[String]("keyInStateAndData"), "2")
      ),
      StopStream
    )
  }

  test("SPARK-38320 - flatMapGroupsWithState state with data should not timeout") {
    withTempDir { dir =>
      withSQLConf(
        (SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false"),
        (SQLConf.CHECKPOINT_LOCATION.key -> dir.getCanonicalPath),
        (SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName)) {

        val inputData = MemoryStream[Timestamp]
        val stateFunc = (key: Int, values: Iterator[Timestamp], state: GroupState[Int]) => {
          // Should never timeout. All batches should have data and even if a timeout is set,
          // it should get cleared when the key receives data per contract.
          require(!state.hasTimedOut, "The state should not have timed out!")
          // Set state and timeout once, only on the first call. The timeout should get cleared
          // in the subsequent batch which has data for the key.
          if (!state.exists) {
            state.update(0)
            state.setTimeoutTimestamp(500)  // Timeout at 500 milliseconds.
          }
          0
        }

        val query = inputData.toDS()
          .withWatermark("value", "0 seconds")
          .groupByKey(_ => 0)  // Always the same key: 0.
          .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(stateFunc)
          .writeStream
          .format("console")
          .outputMode("update")
          .start()

        try {
          // 2 batches. Records are routed to the same key 0. The first batch sets timeout on
          // the key, the second batch with data should clear the timeout.
          (1 to 2).foreach {i =>
            inputData.addData(new Timestamp(i * 1000))
            query.processAllAvailable()
          }
        } finally {
          query.stop()
        }
      }
    }
  }

  testWithAllStateVersions("mapGroupsWithState - initial state - null key") {
    val mapGroupsWithStateFunc =
        (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      val valList = values.toList
      val count = state.getOption.map(_.count).getOrElse(0L) + valList.size
      state.update(new RunningCount(count))
      (key, state.get.count.toString)
    }
    val initialState = Seq(
      ("key", new RunningCount(5)),
      (null, new RunningCount(2))
    ).toDS().groupByKey(_._1).mapValues(_._2)

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(NoTimeout(), initialState)(mapGroupsWithStateFunc)
    testStream(result, Update)(
      AddData(inputData, "key", null),
      CheckNewAnswer(
        ("key", "6"), // state is incremented by 1
        (null, "3") // incremented by 1
      ),
      assertNumStateRows(total = 2, updated = 2),
      StopStream
    )
  }

  testWithAllStateVersions("flatMapGroupsWithState - initial state - processing time timeout") {
    // function will return -1 on timeout and returns count of the state otherwise
    val stateFunc =
        (key: String, values: Iterator[(String, Long)], state: GroupState[RunningCount]) => {
      if (state.hasTimedOut) {
        state.remove()
        Iterator((key, "-1"))
      } else {
        val count = state.getOption.map(_.count).getOrElse(0L) + values.size
        state.update(RunningCount(count))
        state.setTimeoutDuration("10 seconds")
        Iterator((key, count.toString))
      }
    }

    val clock = new StreamManualClock
    val inputData = MemoryStream[(String, Long)]
    val initialState = Seq(
      ("c", new RunningCount(2))
    ).toDS().groupByKey(_._1).mapValues(_._2)
    val result =
      inputData.toDF().toDF("key", "time")
        .selectExpr("key", "timestamp_seconds(time) as timestamp")
        .withWatermark("timestamp", "10 second")
        .as[(String, Long)]
        .groupByKey(x => x._1)
        .flatMapGroupsWithState(Update, ProcessingTimeTimeout(), initialState)(stateFunc)

    testStream(result, Update)(
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
      AddData(inputData, ("a", 1L)),
      AdvanceManualClock(1 * 1000), // a and c are processed here for the first time.
      CheckNewAnswer(("a", "1"), ("c", "2")),
      AdvanceManualClock(10 * 1000),
      AddData(inputData, ("b", 1L)), // this will trigger c and a to get timed out
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(("a", "-1"), ("b", "1"), ("c", "-1"))
    )
  }

  def testWithTimeout(timeoutConf: GroupStateTimeout): Unit = {
    test("SPARK-20714: watermark does not fail query when timeout = " + timeoutConf) {
      // Function to maintain running count up to 2, and then remove the count
      // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
      val stateFunc =
          (key: String, values: Iterator[(String, Long)], state: GroupState[RunningCount]) => {
        if (state.hasTimedOut) {
          state.remove()
          Iterator((key, "-1"))
        } else {
          val count = state.getOption.map(_.count).getOrElse(0L) + values.size
          state.update(RunningCount(count))
          state.setTimeoutDuration("10 seconds")
          Iterator((key, count.toString))
        }
      }

      val clock = new StreamManualClock
      val inputData = MemoryStream[(String, Long)]
      val result =
        inputData.toDF().toDF("key", "time")
          .selectExpr("key", "timestamp_seconds(time) as timestamp")
          .withWatermark("timestamp", "10 second")
          .as[(String, Long)]
          .groupByKey(x => x._1)
          .flatMapGroupsWithState(Update, ProcessingTimeTimeout)(stateFunc)

      testStream(result, Update)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, ("a", 1L)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("a", "1"))
      )
    }
  }
  testWithTimeout(NoTimeout)
  testWithTimeout(ProcessingTimeTimeout)

  def testStateUpdateWithData(
      testName: String,
      stateUpdates: GroupState[Int] => Unit,
      timeoutConf: GroupStateTimeout,
      priorState: Option[Int],
      priorTimeoutTimestamp: Long = NO_TIMESTAMP,
      expectedState: Option[Int] = None,
      expectedTimeoutTimestamp: Long = NO_TIMESTAMP,
      expectedException: Class[_ <: Exception] = null): Unit = {

    if (priorState.isEmpty && priorTimeoutTimestamp != NO_TIMESTAMP) {
      return // there can be no prior timestamp, when there is no prior state
    }
    test(s"InputProcessor - process new data - $testName") {
      val mapGroupsFunc = (key: Int, values: Iterator[Int], state: GroupState[Int]) => {
        assert(state.hasTimedOut === false, "hasTimedOut not false")
        assert(values.nonEmpty, "Some value is expected")
        stateUpdates(state)
        Iterator.empty
      }
      testStateUpdate(
        testTimeoutUpdates = false, mapGroupsFunc, timeoutConf,
        priorState, priorTimeoutTimestamp,
        expectedState, expectedTimeoutTimestamp, expectedException)
    }
  }

  def testStateUpdateWithTimeout(
      testName: String,
      stateUpdates: GroupState[Int] => Unit,
      timeoutConf: GroupStateTimeout,
      priorTimeoutTimestamp: Long,
      expectedState: Option[Int],
      expectedTimeoutTimestamp: Long = NO_TIMESTAMP): Unit = {

    test(s"InputProcessor - process timed out state - $testName") {
      val mapGroupsFunc = (key: Int, values: Iterator[Int], state: GroupState[Int]) => {
        assert(state.hasTimedOut, "hasTimedOut not true")
        assert(values.isEmpty, "values not empty")
        stateUpdates(state)
        Iterator.empty
      }

      testStateUpdate(
        testTimeoutUpdates = true, mapGroupsFunc, timeoutConf = timeoutConf,
        preTimeoutState, priorTimeoutTimestamp, expectedState, expectedTimeoutTimestamp, null)
    }
  }

  def testStateUpdate(
      testTimeoutUpdates: Boolean,
      mapGroupsFunc: (Int, Iterator[Int], GroupState[Int]) => Iterator[Int],
      timeoutConf: GroupStateTimeout,
      priorState: Option[Int],
      priorTimeoutTimestamp: Long,
      expectedState: Option[Int],
      expectedTimeoutTimestamp: Long,
      expectedException: Class[_ <: Exception]): Unit = {

    val store = newStateStore()
    val mapGroupsSparkPlan = newFlatMapGroupsWithStateExec(
      mapGroupsFunc, timeoutConf, currentBatchTimestamp)
    val inputProcessor = new mapGroupsSparkPlan.InputProcessor(store)
    val stateManager = mapGroupsSparkPlan.stateManager
    val key = intToRow(0)
    // Prepare store with prior state configs
    if (priorState.nonEmpty || priorTimeoutTimestamp != NO_TIMESTAMP) {
      stateManager.putState(store, key, priorState.orNull, priorTimeoutTimestamp)
    }

    // Call updating function to update state store
    def callFunction() = {
      val returnedIter = if (testTimeoutUpdates) {
        inputProcessor.processTimedOutState()
      } else {
        inputProcessor.processNewData(Iterator(key))
      }
      returnedIter.size // consume the iterator to force state updates
    }
    if (expectedException != null) {
      // Call function and verify the exception type
      val e = intercept[Exception] { callFunction() }
      assert(e.getClass === expectedException, "Exception thrown but of the wrong type")
    } else {
      // Call function to update and verify updated state in store
      callFunction()
      val updatedState = stateManager.getState(store, key)
      assert(Option(updatedState.stateObj).map(_.toString.toInt) === expectedState,
        "final state not as expected")
      assert(updatedState.timeoutTimestamp === expectedTimeoutTimestamp,
        "final timeout timestamp not as expected")
    }
  }

  def newFlatMapGroupsWithStateExec(
      func: (Int, Iterator[Int], GroupState[Int]) => Iterator[Int],
      timeoutType: GroupStateTimeout = GroupStateTimeout.NoTimeout,
      batchTimestampMs: Long = NO_TIMESTAMP): FlatMapGroupsWithStateExec = {
    val stateFormatVersion = spark.conf.get(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION)
    val emptyRdd = spark.sparkContext.emptyRDD[InternalRow]
    MemoryStream[Int]
      .toDS
      .groupByKey(x => x)
      .flatMapGroupsWithState[Int, Int](Append, timeoutConf = timeoutType)(func)
      .logicalPlan.collectFirst {
        case FlatMapGroupsWithState(f, k, v, g, d, o, s, m, _, t,
            hasInitialState, sga, sda, se, i, c) =>
          FlatMapGroupsWithStateExec(
            f, k, v, se, g, sga, d, sda, o, None, s, stateFormatVersion, m, t,
            Some(currentBatchTimestamp), Some(currentBatchWatermark),
            RDDScanExec(g, emptyRdd, "rdd"),
            hasInitialState,
            RDDScanExec(g, emptyRdd, "rdd"))
      }.get
  }

  def testTimeoutDurationNotAllowed[T <: Exception: Manifest](state: TestGroupState[_]): Unit = {
    val prevTimestamp = state.getTimeoutTimestampMs
    intercept[T] { state.setTimeoutDuration(1000) }
    assert(state.getTimeoutTimestampMs === prevTimestamp)
    intercept[T] { state.setTimeoutDuration("2 second") }
    assert(state.getTimeoutTimestampMs === prevTimestamp)
  }

  def testTimeoutTimestampNotAllowed[T <: Exception: Manifest](state: TestGroupState[_]): Unit = {
    val prevTimestamp = state.getTimeoutTimestampMs
    intercept[T] { state.setTimeoutTimestamp(2000) }
    assert(state.getTimeoutTimestampMs === prevTimestamp)
    intercept[T] { state.setTimeoutTimestamp(2000, "1 second") }
    assert(state.getTimeoutTimestampMs === prevTimestamp)
    intercept[T] { state.setTimeoutTimestamp(new Date(2000)) }
    assert(state.getTimeoutTimestampMs === prevTimestamp)
    intercept[T] { state.setTimeoutTimestamp(new Date(2000), "1 second") }
    assert(state.getTimeoutTimestampMs === prevTimestamp)
  }

  def newStateStore(): StateStore = new MemoryStateStore()

  val intProj = UnsafeProjection.create(Array[DataType](IntegerType))
  def intToRow(i: Int): UnsafeRow = {
    intProj.apply(new GenericInternalRow(Array[Any](i))).copy()
  }

  def rowToInt(row: UnsafeRow): Int = row.getInt(0)

  def testWithAllStateVersions(name: String)(func: => Unit): Unit = {
    for (version <- FlatMapGroupsWithStateExecHelper.supportedVersions) {
      test(s"$name - state format version $version") {
        withSQLConf(
            SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> version.toString,
            SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false") {
          func
        }
      }
    }
  }
}

case class User(name: String, id: String)

object FlatMapGroupsWithStateSuite {

  var failInTask = true

  def assertCanGetProcessingTime(predicate: => Boolean): Unit = {
    if (!predicate) throw new TestFailedException("Could not get processing time", 20)
  }

  def assertCanGetWatermark(predicate: => Boolean): Unit = {
    if (!predicate) throw new TestFailedException("Could not get processing time", 20)
  }

  def assertCannotGetWatermark(func: => Unit): Unit = {
    try {
      func
    } catch {
      case u: UnsupportedOperationException =>
        return
      case _: Throwable =>
        throw new TestFailedException("Unexpected exception when trying to get watermark", 20)
    }
    throw new TestFailedException("Could get watermark when not expected", 20)
  }
}
