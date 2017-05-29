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

import java.sql.Date
import java.util.concurrent.ConcurrentHashMap

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkException
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsWithState
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.RDDScanExec
import org.apache.spark.sql.execution.streaming.{FlatMapGroupsWithStateExec, GroupStateImpl, MemoryStream}
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreId, StoreUpdate}
import org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite.MemoryStateStore
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{DataType, IntegerType}

/** Class to check custom state types */
case class RunningCount(count: Long)

case class Result(key: Long, count: Int)

class FlatMapGroupsWithStateSuite extends StateStoreMetricsTest with BeforeAndAfterAll {

  import testImplicits._
  import GroupStateImpl._
  import GroupStateTimeout._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("GroupState - get, exists, update, remove") {
    var state: GroupStateImpl[String] = null

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
      assert(state.hasUpdated === shouldBeUpdated)
      assert(state.hasRemoved === shouldBeRemoved)
    }

    // === Tests for state in streaming queries ===
    // Updating empty state
    state = GroupStateImpl.createForStreaming(None, 1, 1, NoTimeout, hasTimedOut = false)
    testState(None)
    state.update("")
    testState(Some(""), shouldBeUpdated = true)

    // Updating exiting state
    state = GroupStateImpl.createForStreaming(Some("2"), 1, 1, NoTimeout, hasTimedOut = false)
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

  test("GroupState - setTimeout**** with NoTimeout") {
    for (initValue <- Seq(None, Some(5))) {
      val states = Seq(
        GroupStateImpl.createForStreaming(initValue, 1000, 1000, NoTimeout, hasTimedOut = false),
        GroupStateImpl.createForBatch(NoTimeout)
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

  test("GroupState - setTimeout**** with ProcessingTimeTimeout") {
    // for streaming queries
    var state: GroupStateImpl[Int] = GroupStateImpl.createForStreaming(
      None, 1000, 1000, ProcessingTimeTimeout, hasTimedOut = false)
    assert(state.getTimeoutTimestamp === NO_TIMESTAMP)
    state.setTimeoutDuration(500)
    assert(state.getTimeoutTimestamp === 1500) // can be set without initializing state
    testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)

    state.update(5)
    assert(state.getTimeoutTimestamp === 1500) // does not change
    state.setTimeoutDuration(1000)
    assert(state.getTimeoutTimestamp === 2000)
    state.setTimeoutDuration("2 second")
    assert(state.getTimeoutTimestamp === 3000)
    testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)

    state.remove()
    assert(state.getTimeoutTimestamp === 3000) // does not change
    state.setTimeoutDuration(500) // can still be set
    assert(state.getTimeoutTimestamp === 1500)
    testTimeoutTimestampNotAllowed[UnsupportedOperationException](state)

    // for batch queries
    state = GroupStateImpl.createForBatch(ProcessingTimeTimeout).asInstanceOf[GroupStateImpl[Int]]
    assert(state.getTimeoutTimestamp === NO_TIMESTAMP)
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

  test("GroupState - setTimeout**** with EventTimeTimeout") {
    var state: GroupStateImpl[Int] = GroupStateImpl.createForStreaming(
      None, 1000, 1000, EventTimeTimeout, false)

    assert(state.getTimeoutTimestamp === NO_TIMESTAMP)
    testTimeoutDurationNotAllowed[UnsupportedOperationException](state)
    state.setTimeoutTimestamp(5000)
    assert(state.getTimeoutTimestamp === 5000) // can be set without initializing state

    state.update(5)
    assert(state.getTimeoutTimestamp === 5000) // does not change
    state.setTimeoutTimestamp(10000)
    assert(state.getTimeoutTimestamp === 10000)
    state.setTimeoutTimestamp(new Date(20000))
    assert(state.getTimeoutTimestamp === 20000)
    testTimeoutDurationNotAllowed[UnsupportedOperationException](state)

    state.remove()
    assert(state.getTimeoutTimestamp === 20000)
    state.setTimeoutTimestamp(5000)
    assert(state.getTimeoutTimestamp === 5000) // can be set after removing state
    testTimeoutDurationNotAllowed[UnsupportedOperationException](state)

    // for batch queries
    state = GroupStateImpl.createForBatch(EventTimeTimeout).asInstanceOf[GroupStateImpl[Int]]
    assert(state.getTimeoutTimestamp === NO_TIMESTAMP)
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

  test("GroupState - illegal params to setTimeout****") {
    var state: GroupStateImpl[Int] = null

    // Test setTimeout****() with illegal values
    def testIllegalTimeout(body: => Unit): Unit = {
      intercept[IllegalArgumentException] {
        body
      }
      assert(state.getTimeoutTimestamp === NO_TIMESTAMP)
    }

    state = GroupStateImpl.createForStreaming(
      Some(5), 1000, 1000, ProcessingTimeTimeout, hasTimedOut = false)
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
      state.setTimeoutDuration("1 month -1 day")
    }

    state = GroupStateImpl.createForStreaming(
      Some(5), 1000, 1000, EventTimeTimeout, hasTimedOut = false)
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
      state.setTimeoutTimestamp(10000, "1 month -1 day")
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
      state.setTimeoutTimestamp(new Date(-10000), "1 month -1 day")
    }
  }

  test("GroupState - hasTimedOut") {
    for (timeoutConf <- Seq(NoTimeout, ProcessingTimeTimeout, EventTimeTimeout)) {
      // for streaming queries
      for (initState <- Seq(None, Some(5))) {
        val state1 = GroupStateImpl.createForStreaming(
          initState, 1000, 1000, timeoutConf, hasTimedOut = false)
        assert(state1.hasTimedOut === false)

        val state2 = GroupStateImpl.createForStreaming(
          initState, 1000, 1000, timeoutConf, hasTimedOut = true)
        assert(state2.hasTimedOut === true)
      }

      // for batch queries
      assert(GroupStateImpl.createForBatch(timeoutConf).hasTimedOut === false)
    }
  }

  test("GroupState - primitive type") {
    var intState = GroupStateImpl.createForStreaming[Int](
      None, 1000, 1000, NoTimeout, hasTimedOut = false)
    intercept[NoSuchElementException] {
      intState.get
    }
    assert(intState.getOption === None)

    intState = GroupStateImpl.createForStreaming[Int](
      Some(10), 1000, 1000, NoTimeout, hasTimedOut = false)
    assert(intState.get == 10)
    intState.update(0)
    assert(intState.get == 0)
    intState.remove()
    intercept[NoSuchElementException] {
      intState.get
    }
  }

  // Values used for testing StateStoreUpdater
  val currentBatchTimestamp = 1000
  val currentBatchWatermark = 1000
  val beforeTimeoutThreshold = 999
  val afterTimeoutThreshold = 1001

  // Tests for StateStoreUpdater.updateStateForKeysWithData() when timeout = NoTimeout
  for (priorState <- Seq(None, Some(0))) {
    val priorStateStr = if (priorState.nonEmpty) "prior state set" else "no prior state"
    val testName = s"NoTimeout - $priorStateStr - "

    testStateUpdateWithData(
      testName + "no update",
      stateUpdates = state => { /* do nothing */ },
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

  // Tests for StateStoreUpdater.updateStateForKeysWithData() when timeout != NoTimeout
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
          stateUpdates = state => { /* do nothing */ },
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
    }
  }

  // Currently disallowed cases for StateStoreUpdater.updateStateForKeysWithData(),
  // Try to remove these cases in the future
  for (priorTimeoutTimestamp <- Seq(NO_TIMESTAMP, 1000)) {
    val testName =
      if (priorTimeoutTimestamp != NO_TIMESTAMP) "prior timeout set" else "no prior timeout"
    testStateUpdateWithData(
      s"ProcessingTimeTimeout - $testName - setting timeout without init state not allowed",
      stateUpdates = state => { state.setTimeoutDuration(5000) },
      timeoutConf = ProcessingTimeTimeout,
      priorState = None,
      priorTimeoutTimestamp = priorTimeoutTimestamp,
      expectedException = classOf[IllegalStateException])

    testStateUpdateWithData(
      s"ProcessingTimeTimeout - $testName - setting timeout with state removal not allowed",
      stateUpdates = state => { state.remove(); state.setTimeoutDuration(5000) },
      timeoutConf = ProcessingTimeTimeout,
      priorState = Some(5),
      priorTimeoutTimestamp = priorTimeoutTimestamp,
      expectedException = classOf[IllegalStateException])

    testStateUpdateWithData(
      s"EventTimeTimeout - $testName - setting timeout without init state not allowed",
      stateUpdates = state => { state.setTimeoutTimestamp(10000) },
      timeoutConf = EventTimeTimeout,
      priorState = None,
      priorTimeoutTimestamp = priorTimeoutTimestamp,
      expectedException = classOf[IllegalStateException])

    testStateUpdateWithData(
      s"EventTimeTimeout - $testName - setting timeout with state removal not allowed",
      stateUpdates = state => { state.remove(); state.setTimeoutTimestamp(10000) },
      timeoutConf = EventTimeTimeout,
      priorState = Some(5),
      priorTimeoutTimestamp = priorTimeoutTimestamp,
      expectedException = classOf[IllegalStateException])
  }

  // Tests for StateStoreUpdater.updateStateForTimedOutKeys()
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
      stateUpdates = state => { /* do nothing */ },
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

  test("StateStoreUpdater - rows are cloned before writing to StateStore") {
    // function for running count
    val func = (key: Int, values: Iterator[Int], state: GroupState[Int]) => {
      state.update(state.getOption.getOrElse(0) + values.size)
      Iterator.empty
    }
    val store = newStateStore()
    val plan = newFlatMapGroupsWithStateExec(func)
    val updater = new plan.StateStoreUpdater(store)
    val data = Seq(1, 1, 2)
    val returnIter = updater.updateStateForKeysWithData(data.iterator.map(intToRow))
    returnIter.size // consume the iterator to force store updates
    val storeData = store.iterator.map { case (k, v) => (rowToInt(k), rowToInt(v)) }.toSet
    assert(storeData === Set((1, 2), (2, 1)))
  }

  test("flatMapGroupsWithState - streaming") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {

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
      CheckLastBatch(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a", "b"),
      CheckLastBatch(("a", "2"), ("b", "1")),
      assertNumStateRows(total = 2, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
      CheckLastBatch(("b", "2")),
      assertNumStateRows(total = 1, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
      CheckLastBatch(("a", "1"), ("c", "1")),
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
      CheckLastBatch(("a", "1"), ("a", "2"), ("b", "1")),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
      CheckLastBatch(("b", "2")),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
      CheckLastBatch(("a", "1"), ("c", "1"))
    )
  }

  test("flatMapGroupsWithState - streaming + aggregation") {
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
      CheckLastBatch(("a", 1)),
      AddData(inputData, "a", "b"),
      // mapGroups generates ("a", "2"), ("b", "1"); so increases counts of a and b by 1
      CheckLastBatch(("a", 2), ("b", 1)),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"),
      // mapGroups should remove state for "a" and generate ("a", "-1"), ("b", "2") ;
      // so increment a and b by 1
      CheckLastBatch(("a", 3), ("b", 2)),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"),
      // mapGroups should recreate state for "a" and generate ("a", "1"), ("c", "1") ;
      // so increment a and c by 1
      CheckLastBatch(("a", 4), ("b", 2), ("c", 1))
    )
  }

  test("flatMapGroupsWithState - batch") {
    // Function that returns running count only if its even, otherwise does not return
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      if (state.exists) throw new IllegalArgumentException("state.exists should be false")
      Iterator((key, values.size))
    }
    val df = Seq("a", "a", "b").toDS
      .groupByKey(x => x)
      .flatMapGroupsWithState(Update, GroupStateTimeout.NoTimeout)(stateFunc).toDF
    checkAnswer(df, Seq(("a", 2), ("b", 1)).toDF)
  }

  test("flatMapGroupsWithState - streaming with processing time timeout") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
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
      StartStream(ProcessingTime("1 second"), triggerClock = clock),
      AddData(inputData, "a"),
      AdvanceManualClock(1 * 1000),
      CheckLastBatch(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),

      AddData(inputData, "b"),
      AdvanceManualClock(1 * 1000),
      CheckLastBatch(("b", "1")),
      assertNumStateRows(total = 2, updated = 1),

      AddData(inputData, "b"),
      AdvanceManualClock(10 * 1000),
      CheckLastBatch(("a", "-1"), ("b", "2")),
      assertNumStateRows(total = 1, updated = 2),

      StopStream,
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),

      AddData(inputData, "c"),
      AdvanceManualClock(11 * 1000),
      CheckLastBatch(("b", "-1"), ("c", "1")),
      assertNumStateRows(total = 1, updated = 2),

      AddData(inputData, "c"),
      AdvanceManualClock(20 * 1000),
      CheckLastBatch(("c", "2")),
      assertNumStateRows(total = 1, updated = 1)
    )
  }

  test("flatMapGroupsWithState - streaming with event time timeout + watermark") {
    // Function to maintain the max event time
    // Returns the max event time in the state, or -1 if the state was removed by timeout
    val stateFunc = (
        key: String,
        values: Iterator[(String, Long)],
        state: GroupState[Long]) => {
      val timeoutDelay = 5
      if (key != "a") {
        Iterator.empty
      } else {
        if (state.hasTimedOut) {
          state.remove()
          Iterator((key, -1))
        } else {
          val valuesSeq = values.toSeq
          val maxEventTime = math.max(valuesSeq.map(_._2).max, state.getOption.getOrElse(0L))
          val timeoutTimestampMs = maxEventTime + timeoutDelay
          state.update(maxEventTime)
          state.setTimeoutTimestamp(timeoutTimestampMs * 1000)
          Iterator((key, maxEventTime.toInt))
        }
      }
    }
    val inputData = MemoryStream[(String, Int)]
    val result =
      inputData.toDS
        .select($"_1".as("key"), $"_2".cast("timestamp").as("eventTime"))
        .withWatermark("eventTime", "10 seconds")
        .as[(String, Long)]
        .groupByKey(_._1)
        .flatMapGroupsWithState(Update, EventTimeTimeout)(stateFunc)

    testStream(result, Update)(
      StartStream(ProcessingTime("1 second")),
      AddData(inputData, ("a", 11), ("a", 13), ("a", 15)), // Set timeout timestamp of ...
      CheckLastBatch(("a", 15)),                           // "a" to 15 + 5 = 20s, watermark to 5s
      AddData(inputData, ("a", 4)),       // Add data older than watermark for "a"
      CheckLastBatch(),                   // No output as data should get filtered by watermark
      AddData(inputData, ("dummy", 35)),  // Set watermark = 35 - 10 = 25s
      CheckLastBatch(),                   // No output as no data for "a"
      AddData(inputData, ("a", 24)),      // Add data older than watermark, should be ignored
      CheckLastBatch(("a", -1))           // State for "a" should timeout and emit -1
    )
  }

  test("mapGroupsWithState - streaming") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {

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
      CheckLastBatch(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a", "b"),
      CheckLastBatch(("a", "2"), ("b", "1")),
      assertNumStateRows(total = 2, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and return count as -1
      CheckLastBatch(("a", "-1"), ("b", "2")),
      assertNumStateRows(total = 1, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1
      CheckLastBatch(("a", "1"), ("c", "1")),
      assertNumStateRows(total = 3, updated = 2)
    )
  }

  test("mapGroupsWithState - batch") {
    // Test the following
    // - no initial state
    // - timeouts operations work, does not throw any error [SPARK-20792]
    // - works with primitive state type
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[Int]) => {
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
      CheckLastBatch(("a", 1L)),
      AddData(inputData, "a"),
      CheckLastBatch(("a", 2L)),
      setFailInTask(true),
      AddData(inputData, "a"),
      ExpectFailure[SparkException](),   // task should fail but should not increment count
      setFailInTask(false),
      StartStream(),
      CheckLastBatch(("a", 3L))     // task should not fail, and should show correct count
    )
  }

  test("output partitioning is unknown") {
    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => key
    val inputData = MemoryStream[String]
    val result = inputData.toDS.groupByKey(x => x).mapGroupsWithState(stateFunc)
    testStream(result, Update)(
      AddData(inputData, "a"),
      CheckLastBatch("a"),
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
          .selectExpr("key", "cast(time as timestamp) as timestamp")
          .withWatermark("timestamp", "10 second")
          .as[(String, Long)]
          .groupByKey(x => x._1)
          .flatMapGroupsWithState(Update, ProcessingTimeTimeout)(stateFunc)

      testStream(result, Update)(
        StartStream(ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, ("a", 1L)),
        AdvanceManualClock(1 * 1000),
        CheckLastBatch(("a", "1"))
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
    test(s"StateStoreUpdater - updates with data - $testName") {
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

    test(s"StateStoreUpdater - updates for timeout - $testName") {
      val mapGroupsFunc = (key: Int, values: Iterator[Int], state: GroupState[Int]) => {
        assert(state.hasTimedOut === true, "hasTimedOut not true")
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
    val updater = new mapGroupsSparkPlan.StateStoreUpdater(store)
    val key = intToRow(0)
    // Prepare store with prior state configs
    if (priorState.nonEmpty) {
      val row = updater.getStateRow(priorState.get)
      updater.setTimeoutTimestamp(row, priorTimeoutTimestamp)
      store.put(key.copy(), row.copy())
    }

    // Call updating function to update state store
    def callFunction() = {
      val returnedIter = if (testTimeoutUpdates) {
        updater.updateStateForTimedOutKeys()
      } else {
        updater.updateStateForKeysWithData(Iterator(key))
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
      val updatedStateRow = store.get(key)
      assert(
        updater.getStateObj(updatedStateRow).map(_.toString.toInt) === expectedState,
        "final state not as expected")
      if (updatedStateRow.nonEmpty) {
        assert(
          updater.getTimeoutTimestamp(updatedStateRow.get) === expectedTimeoutTimestamp,
          "final timeout timestamp not as expected")
      }
    }
  }

  def newFlatMapGroupsWithStateExec(
      func: (Int, Iterator[Int], GroupState[Int]) => Iterator[Int],
      timeoutType: GroupStateTimeout = GroupStateTimeout.NoTimeout,
      batchTimestampMs: Long = NO_TIMESTAMP): FlatMapGroupsWithStateExec = {
    MemoryStream[Int]
      .toDS
      .groupByKey(x => x)
      .flatMapGroupsWithState[Int, Int](Append, timeoutConf = timeoutType)(func)
      .logicalPlan.collectFirst {
        case FlatMapGroupsWithState(f, k, v, g, d, o, s, m, _, t, _) =>
          FlatMapGroupsWithStateExec(
            f, k, v, g, d, o, None, s, m, t,
            Some(currentBatchTimestamp), Some(currentBatchWatermark), RDDScanExec(g, null, "rdd"))
      }.get
  }

  def testTimeoutDurationNotAllowed[T <: Exception: Manifest](state: GroupStateImpl[_]): Unit = {
    val prevTimestamp = state.getTimeoutTimestamp
    intercept[T] { state.setTimeoutDuration(1000) }
    assert(state.getTimeoutTimestamp === prevTimestamp)
    intercept[T] { state.setTimeoutDuration("2 second") }
    assert(state.getTimeoutTimestamp === prevTimestamp)
  }

  def testTimeoutTimestampNotAllowed[T <: Exception: Manifest](state: GroupStateImpl[_]): Unit = {
    val prevTimestamp = state.getTimeoutTimestamp
    intercept[T] { state.setTimeoutTimestamp(2000) }
    assert(state.getTimeoutTimestamp === prevTimestamp)
    intercept[T] { state.setTimeoutTimestamp(2000, "1 second") }
    assert(state.getTimeoutTimestamp === prevTimestamp)
    intercept[T] { state.setTimeoutTimestamp(new Date(2000)) }
    assert(state.getTimeoutTimestamp === prevTimestamp)
    intercept[T] { state.setTimeoutTimestamp(new Date(2000), "1 second") }
    assert(state.getTimeoutTimestamp === prevTimestamp)
  }

  def newStateStore(): StateStore = new MemoryStateStore()

  val intProj = UnsafeProjection.create(Array[DataType](IntegerType))
  def intToRow(i: Int): UnsafeRow = {
    intProj.apply(new GenericInternalRow(Array[Any](i))).copy()
  }

  def rowToInt(row: UnsafeRow): Int = row.getInt(0)
}

object FlatMapGroupsWithStateSuite {

  var failInTask = true

  class MemoryStateStore extends StateStore() {
    import scala.collection.JavaConverters._
    private val map = new ConcurrentHashMap[UnsafeRow, UnsafeRow]

    override def iterator(): Iterator[(UnsafeRow, UnsafeRow)] = {
      map.entrySet.iterator.asScala.map { case e => (e.getKey, e.getValue) }
    }

    override def filter(c: (UnsafeRow, UnsafeRow) => Boolean): Iterator[(UnsafeRow, UnsafeRow)] = {
      iterator.filter { case (k, v) => c(k, v) }
    }

    override def get(key: UnsafeRow): Option[UnsafeRow] = Option(map.get(key))
    override def put(key: UnsafeRow, newValue: UnsafeRow): Unit = map.put(key, newValue)
    override def remove(key: UnsafeRow): Unit = { map.remove(key) }
    override def remove(condition: (UnsafeRow) => Boolean): Unit = {
      iterator.map(_._1).filter(condition).foreach(map.remove)
    }
    override def commit(): Long = version + 1
    override def abort(): Unit = { }
    override def id: StateStoreId = null
    override def version: Long = 0
    override def updates(): Iterator[StoreUpdate] = { throw new UnsupportedOperationException }
    override def numKeys(): Long = map.size
    override def hasCommitted: Boolean = true
  }
}
