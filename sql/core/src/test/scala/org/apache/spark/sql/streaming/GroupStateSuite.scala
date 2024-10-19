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

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.api.java.Optional
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.execution.streaming.GroupStateImpl.NO_TIMESTAMP
import org.apache.spark.sql.streaming.GroupStateTimeout.{EventTimeTimeout, NoTimeout, ProcessingTimeTimeout}

class GroupStateSuite extends SparkFunSuite {

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
    assert(testState.getCurrentProcessingTimeMs() === prodState.getCurrentProcessingTimeMs())
    assert(testState.getCurrentWatermarkMs() === prodState.getCurrentWatermarkMs())

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
        testTimeoutDurationNotAllowed[SparkUnsupportedOperationException](state)
        testTimeoutTimestampNotAllowed[SparkUnsupportedOperationException](state)

        // for batch queries
        testTimeoutDurationNotAllowed[SparkUnsupportedOperationException](state)
        testTimeoutTimestampNotAllowed[SparkUnsupportedOperationException](state)
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
    testTimeoutTimestampNotAllowed[SparkUnsupportedOperationException](state)

    state.update(5)
    assert(state.getTimeoutTimestampMs.isPresent())
    assert(state.getTimeoutTimestampMs.get() === 1500) // does not change
    state.setTimeoutDuration(1000)
    assert(state.getTimeoutTimestampMs.get() === 2000)
    state.setTimeoutDuration("2 second")
    assert(state.getTimeoutTimestampMs.get() === 3000)
    testTimeoutTimestampNotAllowed[SparkUnsupportedOperationException](state)

    state.remove()
    assert(state.getTimeoutTimestampMs.isPresent())
    assert(state.getTimeoutTimestampMs.get() === 3000) // does not change
    state.setTimeoutDuration(500) // can still be set
    assert(state.getTimeoutTimestampMs.get() === 1500)
    testTimeoutTimestampNotAllowed[SparkUnsupportedOperationException](state)

    // for batch queries
    state = GroupStateImpl.createForBatch(
      ProcessingTimeTimeout, watermarkPresent = false).asInstanceOf[GroupStateImpl[Int]]
    assert(!state.getTimeoutTimestampMs.isPresent())
    state.setTimeoutDuration(500)
    testTimeoutTimestampNotAllowed[SparkUnsupportedOperationException](state)

    state.update(5)
    state.setTimeoutDuration(1000)
    state.setTimeoutDuration("2 second")
    testTimeoutTimestampNotAllowed[SparkUnsupportedOperationException](state)

    state.remove()
    state.setTimeoutDuration(500)
    testTimeoutTimestampNotAllowed[SparkUnsupportedOperationException](state)
  }

  test("GroupState - setTimeout - with EventTimeTimeout") {
    var state = TestGroupState.create[Int](
      Optional.empty[Int], EventTimeTimeout, 1000, Optional.of(1000), hasTimedOut = false)
    assert(!state.getTimeoutTimestampMs.isPresent())
    testTimeoutDurationNotAllowed[SparkUnsupportedOperationException](state)
    state.setTimeoutTimestamp(5000)
    assert(state.getTimeoutTimestampMs.get() === 5000) // can be set without initializing state

    state.update(5)
    assert(state.getTimeoutTimestampMs.get() === 5000) // does not change
    state.setTimeoutTimestamp(10000)
    assert(state.getTimeoutTimestampMs.get() === 10000)
    state.setTimeoutTimestamp(new Date(20000))
    assert(state.getTimeoutTimestampMs.get() === 20000)
    testTimeoutDurationNotAllowed[SparkUnsupportedOperationException](state)

    state.remove()
    assert(state.getTimeoutTimestampMs.get() === 20000)
    state.setTimeoutTimestamp(5000)
    assert(state.getTimeoutTimestampMs.get() === 5000) // can be set after removing state
    testTimeoutDurationNotAllowed[SparkUnsupportedOperationException](state)

    // for batch queries
    state = GroupStateImpl.createForBatch(
      EventTimeTimeout, watermarkPresent = false).asInstanceOf[GroupStateImpl[Int]]
    assert(!state.getTimeoutTimestampMs.isPresent())
    testTimeoutDurationNotAllowed[SparkUnsupportedOperationException](state)
    state.setTimeoutTimestamp(5000)

    state.update(5)
    state.setTimeoutTimestamp(10000)
    state.setTimeoutTimestamp(new Date(20000))
    testTimeoutDurationNotAllowed[SparkUnsupportedOperationException](state)

    state.remove()
    state.setTimeoutTimestamp(5000)
    testTimeoutDurationNotAllowed[SparkUnsupportedOperationException](state)
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
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        TestGroupState.create[Int](
          Optional.of(5), NoTimeout, 100L, Optional.empty[Long], hasTimedOut = true)
      },
      condition = "_LEGACY_ERROR_TEMP_3168",
      parameters = Map.empty)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        GroupStateImpl.createForStreaming[Int](Some(5), 100L, NO_TIMESTAMP, NoTimeout, true, false)
      },
      condition = "_LEGACY_ERROR_TEMP_3168",
      parameters = Map.empty)
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
      checkError(
        exception = intercept[SparkUnsupportedOperationException] { test },
        condition = "_LEGACY_ERROR_TEMP_2204",
        parameters = Map.empty)
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
        assert(
          batchState(timeoutConf, watermarkPresent).getCurrentProcessingTimeMs() >= currentTime)
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
}
