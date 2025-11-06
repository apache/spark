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
package org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.testing

import java.sql.Timestamp
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{
  ExpiredTimerInfo,
  OutputMode,
  StatefulProcessor,
  StreamTest,
  TimeMode,
  TimerValues,
  TTLConfig
}
import org.apache.spark.sql.streaming.ListState
import org.apache.spark.sql.streaming.MapState
import org.apache.spark.sql.streaming.ValueState

class TestClock(var currentInstant: Instant, zone: ZoneId = ZoneId.systemDefault()) extends Clock {
  override def getZone: ZoneId = zone
  override def withZone(zone: ZoneId): Clock = new TestClock(currentInstant, zone)
  override def instant(): Instant = currentInstant

  def advanceBy(duration: Duration): Unit = {
    currentInstant = currentInstant.plus(duration)
  }
}

/** Test StatefulProcessor implementation that maintains a running count. */
class RunningCountProcessor[T](ttl: TTLConfig = TTLConfig.NONE)
    extends StatefulProcessor[String, T, (String, Long)] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState[Long]("count", Encoders.scalaLong, ttl)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[T],
      timerValues: TimerValues
  ): Iterator[(String, Long)] = {
    val incoming = inputRows.size
    val current = countState.get()
    val updated = current + incoming
    countState.update(updated)
    Iterator.single((key, updated))
  }
}

// Input: (key, score) as (String, Double)
// Output: (key, score) as (String, Double) for the top K snapshot each batch
class TopKProcessor(k: Int, ttl: TTLConfig = TTLConfig.NONE)
    extends StatefulProcessor[String, (String, Double), (String, Double)] {

  @transient private var topKState: ListState[Double] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    topKState = getHandle.getListState[Double]("topK", Encoders.scalaDouble, ttl)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Double)],
      timerValues: TimerValues
  ): Iterator[(String, Double)] = {
    // Load existing list into a buffer
    val current = ArrayBuffer[Double]()
    topKState.get().foreach(current += _)

    // Add new values and recompute top-K
    inputRows.foreach {
      case (_, score) =>
        current += score
    }
    val updatedTopK = current.sorted(Ordering[Double].reverse).take(k)

    // Persist back
    topKState.clear()
    topKState.put(updatedTopK.toArray)

    // Emit snapshot of top-K for this key
    updatedTopK.iterator.map(v => (key, v))
  }
}

// Input: (key, word) as (String, String)
// Output: (key, word, count) as (String, String, Long) for each word in the batch
class WordFrequencyProcessor(ttl: TTLConfig = TTLConfig.NONE)
    extends StatefulProcessor[String, (String, String), (String, String, Long)] {

  @transient private var freqState: MapState[String, Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    freqState = getHandle
      .getMapState[String, Long]("frequencies", Encoders.STRING, Encoders.scalaLong, ttl)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues
  ): Iterator[(String, String, Long)] = {
    val results = ArrayBuffer[(String, String, Long)]()

    inputRows.foreach {
      case (_, word) =>
        val currentCount = if (freqState.containsKey(word)) {
          freqState.getValue(word)
        } else {
          0L
        }
        val updatedCount = currentCount + 1
        freqState.updateValue(word, updatedCount)
        results += ((key, word, updatedCount))
    }

    results.iterator
  }
}

/**
 * Session timeout processor that demonstrates timer usage.
 *
 * Input: (userId, activityType) as (String, String)
 * Output: (userId, event, count) as (String, String, Long)
 *
 * Behavior:
 * - Tracks activity count per user session
 * - Registers a timeout timer on first activity (5 seconds from current time)
 * - Updates timer on each new activity (resets 5-second countdown)
 * - When timer expires, emits ("userId", "SESSION_TIMEOUT", activityCount) and clears state
 * - Regular activities emit ("userId", "ACTIVITY", currentCount)
 */
class SessionTimeoutProcessor(val timeoutDurationMs: Long = 5000L)
    extends StatefulProcessor[String, (String, String), (String, String, Long)] {

  @transient private var activityCountState: ValueState[Long] = _
  @transient private var timerState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    activityCountState =
      getHandle.getValueState[Long]("activityCount", Encoders.scalaLong, TTLConfig.NONE)
    timerState = getHandle.getValueState[Long]("timer", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues
  ): Iterator[(String, String, Long)] = {
    val currentCount = if (activityCountState.exists()) activityCountState.get() else 0L
    val newCount = currentCount + inputRows.size
    activityCountState.update(newCount)

    // Delete old timer if exists and register new one
    if (timerState.exists()) {
      getHandle.deleteTimer(timerState.get())
    }

    val newTimerMs = timerValues.getCurrentProcessingTimeInMs() + timeoutDurationMs
    getHandle.registerTimer(newTimerMs)
    timerState.update(newTimerMs)

    Iterator.single((key, "ACTIVITY", newCount))
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[(String, String, Long)] = {
    val count = if (activityCountState.exists()) activityCountState.get() else 0L

    // Clear session state
    activityCountState.clear()
    timerState.clear()

    Iterator.single((key, "SESSION_TIMEOUT", count))
  }
}

/**
 * Multi-timer processor that registers multiple timers with different delays.
 *
 * Input: (userId, command) as (String, String)
 * Output: (userId, timerType, timestamp) as (String, String, Long)
 *
 * On first input, registers three timers: SHORT (1s), MEDIUM (3s), LONG (5s)
 * When each timer expires, emits the timer type and timestamp
 */
class MultiTimerProcessor
    extends StatefulProcessor[String, (String, String), (String, String, Long)] {

  @transient private var timerTypeMapState: MapState[Long, String] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    timerTypeMapState = getHandle.getMapState[Long, String](
      "timerTypeMap",
      Encoders.scalaLong,
      Encoders.STRING,
      TTLConfig.NONE
    )
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues
  ): Iterator[(String, String, Long)] = {
    inputRows.size // consume iterator

    val baseTime = timerValues.getCurrentProcessingTimeInMs()
    val shortTimer = baseTime + 1000L
    val mediumTimer = baseTime + 3000L
    val longTimer = baseTime + 5000L

    getHandle.registerTimer(shortTimer) // SHORT - 1 second
    getHandle.registerTimer(mediumTimer) // MEDIUM - 3 seconds
    getHandle.registerTimer(longTimer) // LONG - 5 seconds

    timerTypeMapState.updateValue(shortTimer, "SHORT")
    timerTypeMapState.updateValue(mediumTimer, "MEDIUM")
    timerTypeMapState.updateValue(longTimer, "LONG")

    Iterator.empty
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[(String, String, Long)] = {
    val expiryTime = expiredTimerInfo.getExpiryTimeInMs()
    val timerType = timerTypeMapState.getValue(expiryTime)

    // Clean up the timer type from the map
    timerTypeMapState.removeKey(expiryTime)

    Iterator.single((key, timerType, expiryTime))
  }
}

/**
 * Event time window processor that demonstrates event time timer usage.
 *
 * Input: (eventId, eventTime) as (String, Timestamp)
 * Output: (userId, status, count) as (String, String, Long)
 *
 * Behavior:
 * - Tracks event count per user window
 * - On first event, registers a timer for windowDurationMs from the first event time
 * - Accumulates events in the window
 * - When timer expires (based on watermark), emits window summary and starts new window
 */
class EventTimeWindowProcessor(val windowDurationMs: Long = 10000L)
    extends StatefulProcessor[String, (String, Timestamp), (String, String, Long)] {

  @transient private var eventCountState: ValueState[Long] = _
  @transient private var windowEndTimeState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    eventCountState =
      getHandle.getValueState[Long]("eventCount", Encoders.scalaLong, TTLConfig.NONE)
    windowEndTimeState =
      getHandle.getValueState[Long]("windowEndTime", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Timestamp)],
      timerValues: TimerValues
  ): Iterator[(String, String, Long)] = {
    val events = inputRows.toList
    val currentCount = if (eventCountState.exists()) eventCountState.get() else 0L
    val newCount = currentCount + events.size
    eventCountState.update(newCount)

    // If this is the first event in a window, register timer
    if (!windowEndTimeState.exists()) {
      val firstEventTime = events.head._2.getTime
      val windowEnd = firstEventTime + windowDurationMs
      getHandle.registerTimer(windowEnd)
      windowEndTimeState.update(windowEnd)
      Iterator.single((key, "WINDOW_START", newCount))
    } else {
      Iterator.single((key, "WINDOW_CONTINUE", newCount))
    }
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[(String, String, Long)] = {
    val count = if (eventCountState.exists()) eventCountState.get() else 0L

    // Clear window state
    eventCountState.clear()
    windowEndTimeState.clear()

    Iterator.single((key, "WINDOW_END", count))
  }
}

/** Test suite for TwsTester utility class. */
class TwsTesterSuite extends SparkFunSuite {

  test("TwsTester should correctly test RunningCountProcessor") {
    val input: List[(String, String)] = List(
      ("key1", "a"),
      ("key2", "b"),
      ("key1", "c"),
      ("key2", "b"),
      ("key1", "c"),
      ("key1", "c"),
      ("key3", "q")
    )
    val tester = new TwsTester(new RunningCountProcessor[String]())
    val ans1: List[(String, Long)] = tester.test(input)
    assert(ans1.sorted == List(("key1", 4L), ("key2", 2L), ("key3", 1L)).sorted)

    assert(tester.peekValueState[Long]("count", "key1").get == 4L)
    assert(tester.peekValueState[Long]("count", "key2").get == 2L)
    assert(tester.peekValueState[Long]("count", "key3").get == 1L)
    assert(tester.peekValueState[Long]("count", "key4").isEmpty)

    val ans2 = tester.testOneRow("key1", "q")
    assert(ans2 == List(("key1", 5L)))
    assert(tester.peekValueState[Long]("count", "key1").get == 5L)
    assert(tester.peekValueState[Long]("count", "key2").get == 2L)

    val ans3 = tester.test(List(("key1", "a"), ("key2", "a")))
    assert(ans3.sorted == List(("key1", 6L), ("key2", 3L)))
  }

  test("TwsTester should allow direct access to ValueState") {
    val processor = new RunningCountProcessor[String]()
    val tester = new TwsTester[String, String, (String, Long)](processor)
    tester.setValueState[Long]("count", "foo", 5)
    tester.test(List(("foo", "a")))
    assert(tester.peekValueState[Long]("count", "foo").get == 6L)
  }

  test("TwsTester should correctly test TopKProcessor") {
    val input: List[(String, (String, Double))] = List(
      ("key2", ("c", 30.0)),
      ("key2", ("d", 40.0)),
      ("key1", ("b", 2.0)),
      ("key1", ("c", 3.0)),
      ("key2", ("a", 10.0)),
      ("key2", ("b", 20.0)),
      ("key3", ("a", 100.0)),
      ("key1", ("a", 1.0))
    )
    val tester = new TwsTester(new TopKProcessor(2))
    val ans1 = tester.test(input)
    assert(
      ans1.sorted == List(
        ("key1", 2.0),
        ("key1", 3.0),
        ("key2", 30.0),
        ("key2", 40.0),
        ("key3", 100.0)
      )
    )
    assert(tester.peekListState[Double]("topK", "key1") == List(3.0, 2.0))
    assert(tester.peekListState[Double]("topK", "key2") == List(40.0, 30.0))
    assert(tester.peekListState[Double]("topK", "key3") == List(100.0))
    assert(tester.peekListState[Double]("topK", "key4").isEmpty)

    val ans2 = tester.test(List(("key1", ("a", 10.0))))
    assert(ans2.sorted == List(("key1", 3.0), ("key1", 10.0)))
    assert(tester.peekListState[Double]("topK", "key1") == List(10.0, 3.0))
  }

  test("TwsTester should allow direct access to ListState") {
    val tester = new TwsTester(new TopKProcessor(2))
    tester.setListState("topK", "a", List(6.0, 5.0))
    tester.setListState("topK", "b", List(8.0, 7.0))
    tester.testOneRow("a", ("", 10.0))
    tester.testOneRow("b", ("", 7.5))
    tester.testOneRow("c", ("", 1.0))

    assert(tester.peekListState[Double]("topK", "a") == List(10.0, 6.0))
    assert(tester.peekListState[Double]("topK", "b") == List(8.0, 7.5))
    assert(tester.peekListState[Double]("topK", "c") == List(1.0))
    assert(tester.peekListState[Double]("topK", "d") == List())
  }

  test("TwsTester should correctly test WordFrequencyProcessor") {
    val input: List[(String, (String, String))] = List(
      ("user1", ("", "hello")),
      ("user1", ("", "world")),
      ("user1", ("", "hello")),
      ("user2", ("", "hello")),
      ("user2", ("", "spark")),
      ("user1", ("", "world"))
    )
    val tester = new TwsTester(new WordFrequencyProcessor())
    val ans1 = tester.test(input)

    assert(
      ans1.sorted == List(
        ("user1", "hello", 1L),
        ("user1", "hello", 2L),
        ("user1", "world", 1L),
        ("user1", "world", 2L),
        ("user2", "hello", 1L),
        ("user2", "spark", 1L)
      ).sorted
    )

    // Check state using peekMapState
    assert(
      tester.peekMapState[String, Long]("frequencies", "user1") == Map("hello" -> 2L, "world" -> 2L)
    )
    assert(
      tester.peekMapState[String, Long]("frequencies", "user2") == Map("hello" -> 1L, "spark" -> 1L)
    )
    assert(tester.peekMapState[String, Long]("frequencies", "user3") == Map())
    assert(tester.peekMapState[String, Long]("frequencies", "user3").isEmpty)

    // Process more data for user1
    val ans2 = tester.test(List(("user1", ("", "hello")), ("user1", ("", "test"))))
    assert(ans2.sorted == List(("user1", "hello", 3L), ("user1", "test", 1L)).sorted)
    assert(
      tester.peekMapState[String, Long]("frequencies", "user1") == Map(
        "hello" -> 3L,
        "world" -> 2L,
        "test" -> 1L
      )
    )
  }

  test("TwsTester should allow direct access to MapState") {
    val tester = new TwsTester(new WordFrequencyProcessor())

    // Set initial state directly
    tester.setMapState("frequencies", "user1", Map("hello" -> 5L, "world" -> 3L))
    tester.setMapState("frequencies", "user2", Map("spark" -> 10L))

    // Process new words
    tester.testOneRow("user1", ("", "hello"))
    tester.testOneRow("user1", ("", "goodbye"))
    tester.testOneRow("user2", ("", "spark"))
    tester.testOneRow("user3", ("", "new"))

    // Verify updated state
    assert(
      tester.peekMapState[String, Long]("frequencies", "user1") == Map(
        "hello" -> 6L,
        "world" -> 3L,
        "goodbye" -> 1L
      )
    )
    assert(tester.peekMapState[String, Long]("frequencies", "user2") == Map("spark" -> 11L))
    assert(tester.peekMapState[String, Long]("frequencies", "user3") == Map("new" -> 1L))
    assert(tester.peekMapState[String, Long]("frequencies", "user4") == Map())
  }

  test("TwsTester should expire old value state according to TTL") {
    val processor = new RunningCountProcessor[String](TTLConfig(Duration.ofSeconds(100)))
    val testClock = new TestClock(Instant.EPOCH)
    val tester = new TwsTester(processor, testClock)

    tester.testOneRow("key1", "b")
    assert(tester.peekValueState[Long]("count", "key1").get == 1L)
    testClock.advanceBy(Duration.ofSeconds(101))
    assert(tester.peekValueState[Long]("count", "key1").isEmpty)
  }

  test("TwsTester should expire old list state according to TTL") {
    val testClock = new TestClock(Instant.EPOCH)
    val ttlConfig = TTLConfig(Duration.ofSeconds(100))
    val processor = new TopKProcessor(2, ttlConfig)
    val tester = new TwsTester(processor, testClock)

    tester.testOneRow("key1", ("a", 1.0))
    tester.testOneRow("key2", ("a", 1.5))
    assert(tester.peekListState[Double]("topK", "key1") == List(1.0))
    assert(tester.peekListState[Double]("topK", "key2") == List(1.5))

    testClock.advanceBy(Duration.ofSeconds(50))
    tester.testOneRow("key2", ("a", 2.0))
    assert(tester.peekListState[Double]("topK", "key1") == List(1.0))
    assert(tester.peekListState[Double]("topK", "key2") == List(2.0, 1.5))

    testClock.advanceBy(Duration.ofSeconds(51))
    assert(tester.peekListState[Double]("topK", "key1") == List())
    assert(tester.peekListState[Double]("topK", "key2") == List(2.0, 1.5))

    testClock.advanceBy(Duration.ofSeconds(50))
    assert(tester.peekListState[Double]("topK", "key1") == List())
    assert(tester.peekListState[Double]("topK", "key2") == List())
  }

  test("TwsTester should expire old map state according to TTL") {
    val testClock = new TestClock(Instant.EPOCH)
    val ttlConfig = TTLConfig(Duration.ofSeconds(100))
    val processor = new WordFrequencyProcessor(ttlConfig)
    val tester = new TwsTester(processor, testClock)

    tester.testOneRow("key1", ("a", "spark"))
    tester.testOneRow("key2", ("a", "beta"))
    assert(tester.peekMapState[String, Long]("frequencies", "key1") == Map("spark" -> 1L))
    assert(tester.peekMapState[String, Long]("frequencies", "key2") == Map("beta" -> 1L))

    testClock.advanceBy(Duration.ofSeconds(50))
    tester.testOneRow("key2", ("a", "spark"))
    assert(tester.peekMapState[String, Long]("frequencies", "key1") == Map("spark" -> 1L))
    assert(
      tester.peekMapState[String, Long]("frequencies", "key2") == Map("beta" -> 1L, "spark" -> 1L)
    )

    testClock.advanceBy(Duration.ofSeconds(51))
    assert(tester.peekMapState[String, Long]("frequencies", "key1") == Map())
    assert(
      tester.peekMapState[String, Long]("frequencies", "key2") == Map("beta" -> 1L, "spark" -> 1L)
    )

    testClock.advanceBy(Duration.ofSeconds(50))
    assert(tester.peekMapState[String, Long]("frequencies", "key1") == Map())
    assert(tester.peekMapState[String, Long]("frequencies", "key2") == Map())
  }

  test("TwsTester should test one row with value state") {
    val processor = new RunningCountProcessor[String]()
    val tester = new TwsTester(processor)

    val (rows, newState) = tester.testOneRowWithValueState("key1", "a", "count", 10L)
    assert(rows == List(("key1", 11L)))
    assert(newState == 11L)
  }

  test("TwsTester should handle session timeout with timer") {
    val testClock = new TestClock(Instant.ofEpochMilli(10000L))
    val processor = new SessionTimeoutProcessor(timeoutDurationMs = 5000L)
    val tester = new TwsTester(processor, testClock, timeMode = TimeMode.ProcessingTime)

    // First activity - should register timer at 15000ms
    val result1 = tester.test(List(("user1", ("user1", "login"))))
    assert(result1 == List(("user1", "ACTIVITY", 1L)))
    assert(tester.peekValueState[Long]("activityCount", "user1").get == 1L)
    assert(tester.peekValueState[Long]("timer", "user1").get == 15000L)

    // Second activity before timeout - should update timer to 20000ms
    testClock.advanceBy(Duration.ofMillis(3000L)) // now at 13000ms
    val result2 = tester.test(List(("user1", ("user1", "click"))))
    assert(result2 == List(("user1", "ACTIVITY", 2L)))
    assert(tester.peekValueState[Long]("activityCount", "user1").get == 2L)
    assert(tester.peekValueState[Long]("timer", "user1").get == 18000L)

    // Advance time past timeout - timer should fire
    testClock.advanceBy(Duration.ofMillis(6000L)) // now at 19000ms, timer at 18000ms should fire
    val result3 = tester.test(List()) // empty input, but timer should fire
    assert(result3 == List(("user1", "SESSION_TIMEOUT", 2L)))
    assert(tester.peekValueState[Long]("activityCount", "user1").isEmpty)
    assert(tester.peekValueState[Long]("timer", "user1").isEmpty)
  }

  test("TwsTester should process timers before input rows") {
    val testClock = new TestClock(Instant.ofEpochMilli(10000L))
    val processor = new SessionTimeoutProcessor(timeoutDurationMs = 5000L)
    val tester = new TwsTester(processor, testClock, timeMode = TimeMode.ProcessingTime)

    // Register initial activity and timer
    tester.test(List(("user1", ("user1", "start"))))
    assert(tester.peekValueState[Long]("activityCount", "user1").get == 1L)

    // Advance past timer expiry
    testClock.advanceBy(Duration.ofMillis(6000L)) // now at 16000ms, timer at 15000ms expired

    // Process new input - timer should fire BEFORE input is processed
    val result = tester.test(List(("user1", ("user1", "new_activity"))))

    // First output should be timeout (from expired timer), second should be new activity
    assert(result.length == 2)
    assert(result(0) == ("user1", "SESSION_TIMEOUT", 1L)) // Timer fired first
    assert(result(1) == ("user1", "ACTIVITY", 1L)) // Then new activity (count reset)

    // After processing, should have new state
    assert(tester.peekValueState[Long]("activityCount", "user1").get == 1L)
  }

  test("TwsTester should handle multiple timers in same batch") {
    val testClock = new TestClock(Instant.ofEpochMilli(10000L))
    val processor = new MultiTimerProcessor()
    val tester = new TwsTester(processor, testClock, timeMode = TimeMode.ProcessingTime)

    // Register all three timers (SHORT=11000ms, MEDIUM=13000ms, LONG=15000ms)
    val result1 = tester.test(List(("user1", ("user1", "start"))))
    assert(result1.isEmpty)

    // Advance to fire SHORT timer only
    testClock.advanceBy(Duration.ofMillis(1500L)) // now at 11500ms
    val result2 = tester.test(List())
    assert(result2.length == 1)
    assert(result2(0) == ("user1", "SHORT", 11000L))

    // Advance to fire MEDIUM and LONG timers together
    testClock.advanceBy(Duration.ofMillis(4000L)) // now at 15500ms
    val result3 = tester.test(List())

    // Timers should fire in order of expiry time
    assert(result3.length == 2)
    assert(result3(0) == ("user1", "MEDIUM", 13000L))
    assert(result3(1) == ("user1", "LONG", 15000L))
  }

  test("TwsTester should not process timers twice") {
    val testClock = new TestClock(Instant.ofEpochMilli(10000L))
    val processor = new SessionTimeoutProcessor(timeoutDurationMs = 5000L)
    val tester = new TwsTester(processor, testClock, timeMode = TimeMode.ProcessingTime)

    // Register timer at 15000ms
    tester.test(List(("user1", ("user1", "start"))))
    assert(tester.peekValueState[Long]("activityCount", "user1").get == 1L)

    // Advance past timer and process - timer fires
    testClock.advanceBy(Duration.ofMillis(6000L)) // now at 16000ms
    val result1 = tester.test(List())
    assert(result1.length == 1)
    assert(result1(0) == ("user1", "SESSION_TIMEOUT", 1L))
    assert(tester.peekValueState[Long]("activityCount", "user1").isEmpty)

    // Process again at same time - timer should NOT fire again
    val result2 = tester.test(List())
    assert(result2.isEmpty)

    // Process again with even later time - timer should still not fire
    testClock.advanceBy(Duration.ofMillis(10000L)) // now at 26000ms
    val result3 = tester.test(List())
    assert(result3.isEmpty)
  }

  test("TwsTester should handle timers for multiple keys independently") {
    val testClock = new TestClock(Instant.ofEpochMilli(10000L))
    val processor = new SessionTimeoutProcessor(timeoutDurationMs = 5000L)
    val tester = new TwsTester(processor, testClock, timeMode = TimeMode.ProcessingTime)

    // Register activities for two users at different times
    tester.test(List(("user1", ("user1", "start")))) // timer at 15000ms
    testClock.advanceBy(Duration.ofMillis(2000L)) // now at 12000ms
    tester.test(List(("user2", ("user2", "start")))) // timer at 17000ms

    // Advance to fire only user1's timer
    testClock.advanceBy(Duration.ofMillis(4000L)) // now at 16000ms
    val result1 = tester.test(List())
    assert(result1.length == 1)
    assert(result1(0) == ("user1", "SESSION_TIMEOUT", 1L))
    assert(tester.peekValueState[Long]("activityCount", "user1").isEmpty)
    assert(tester.peekValueState[Long]("activityCount", "user2").get == 1L) // user2 still active

    // Advance to fire user2's timer
    testClock.advanceBy(Duration.ofMillis(2000L)) // now at 18000ms
    val result2 = tester.test(List())
    assert(result2.length == 1)
    assert(result2(0) == ("user2", "SESSION_TIMEOUT", 1L))
    assert(tester.peekValueState[Long]("activityCount", "user2").isEmpty)
  }

  test("TwsTester should handle timer deletion correctly") {
    val testClock = new TestClock(Instant.ofEpochMilli(10000L))
    val processor = new SessionTimeoutProcessor(timeoutDurationMs = 5000L)
    val tester = new TwsTester(processor, testClock, timeMode = TimeMode.ProcessingTime)

    // Register initial timer at 15000ms
    tester.test(List(("user1", ("user1", "start"))))
    assert(tester.peekValueState[Long]("timer", "user1").get == 15000L)

    // New activity deletes old timer and registers new one at 18000ms
    testClock.advanceBy(Duration.ofMillis(3000L)) // now at 13000ms
    tester.test(List(("user1", ("user1", "activity"))))
    assert(tester.peekValueState[Long]("timer", "user1").get == 18000L)

    // Advance past original timer time (15000ms) but before new timer (18000ms)
    testClock.advanceBy(Duration.ofMillis(3000L)) // now at 16000ms
    val result = tester.test(List())
    // Old timer at 15000ms should NOT fire since it was deleted
    assert(result.isEmpty)
    assert(tester.peekValueState[Long]("activityCount", "user1").get == 2L) // still active

    // Advance past new timer time
    testClock.advanceBy(Duration.ofMillis(3000L)) // now at 19000ms
    val result2 = tester.test(List())
    // New timer at 18000ms should fire
    assert(result2.length == 1)
    assert(result2(0) == ("user1", "SESSION_TIMEOUT", 2L))
  }

  test("TwsTester should handle EventTime mode with watermark") {
    val processor = new EventTimeWindowProcessor(windowDurationMs = 10000L)
    val tester = new TwsTester(processor, timeMode = TimeMode.EventTime)

    // Configure watermark: extract timestamp from input, 2 second delay
    // In real Spark, the watermark column must be of Timestamp type
    tester.withWatermark(
      (input: (String, Timestamp)) => input._2,
      "2 seconds"
    )

    // Batch 1: Process events with timestamps 10000, 12000, 15000
    // Max event time = 15000, watermark after batch = 15000 - 2000 = 13000
    val result1 = tester.test(
      List(
        ("user1", ("event1", new Timestamp(10000L))),
        ("user1", ("event2", new Timestamp(12000L))),
        ("user1", ("event3", new Timestamp(15000L)))
      )
    )
    // First batch: registers timer at 20000 (10000 + 10000), no timers fire yet
    assert(result1 == List(("user1", "WINDOW_START", 3L)))
    assert(tester.peekValueState[Long]("eventCount", "user1").get == 3L)
    assert(tester.peekValueState[Long]("windowEndTime", "user1").get == 20000L)

    // Batch 2: Process more events with timestamps 18000, 20000
    // Watermark before batch = 13000, so timer at 20000 doesn't fire yet
    // Max event time = 20000, watermark after batch = 20000 - 2000 = 18000
    // Timer at 20000 still doesn't fire (watermark < timer)
    val result2 = tester.test(
      List(
        ("user1", ("event4", new Timestamp(18000L))),
        ("user1", ("event5", new Timestamp(20000L)))
      )
    )
    assert(result2 == List(("user1", "WINDOW_CONTINUE", 5L)))
    assert(tester.peekValueState[Long]("eventCount", "user1").get == 5L)

    // Batch 3: Process event with timestamp 23000
    // Watermark before batch = 18000, so timer at 20000 doesn't fire yet
    // Max event time = 23000, watermark after batch = 23000 - 2000 = 21000
    val result3 = tester.test(List(("user1", ("event6", new Timestamp(23000L)))))
    assert(result3 == List(("user1", "WINDOW_CONTINUE", 6L)))
    assert(tester.peekValueState[Long]("eventCount", "user1").get == 6L)

    // Batch 4: Process another event with timestamp 25000
    // Watermark before batch = 21000, so timer at 20000 FIRES now!
    // Then new event is processed, starting a new window
    // Watermark after batch = 25000 - 2000 = 23000
    val result4 = tester.test(List(("user1", ("event7", new Timestamp(25000L)))))

    // Timer fires first (with count 6), then new event starts new window
    assert(result4.length == 2)
    assert(result4(0) == ("user1", "WINDOW_END", 6L)) // Timer fired with final count
    assert(result4(1) == ("user1", "WINDOW_START", 1L)) // New window started
    assert(tester.peekValueState[Long]("eventCount", "user1").get == 1L)
    assert(tester.peekValueState[Long]("windowEndTime", "user1").get == 35000L)
  }
}

/**
 * Integration test suite that compares TwsTester results with real streaming execution.
 * Thread auditing is disabled because this suite runs actual streaming queries with RocksDB
 * and shuffle operations, which spawn daemon threads (e.g., Netty boss/worker threads,
 * file client threads, ForkJoinPool workers, and cleaner threads) that shut down
 * asynchronously after SparkContext.stop().
 */
class TwsTesterFuzzTestSuite extends StreamTest {
  import testImplicits._

  // Disable thread auditing for this suite since it runs integration tests with
  // real streaming queries that create asynchronously-stopped threads
  override protected val enableAutoThreadAudit = false

  /**
   * Processes given input using TwsTester and real transformWithState+testStream.
   *
   * Asserts that results are identical.
   */
  def compareTws[
      K: org.apache.spark.sql.Encoder,
      I: org.apache.spark.sql.Encoder,
      O: org.apache.spark.sql.Encoder](
      processor: StatefulProcessor[K, I, O],
      input: List[(K, I)]): Unit = {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "5"
    ) {
      val outputMode = OutputMode.Append()
      implicit val tupleEncoder = org.apache.spark.sql.Encoders.tuple(
        implicitly[org.apache.spark.sql.Encoder[K]],
        implicitly[org.apache.spark.sql.Encoder[I]]
      )
      val inputStream = MemoryStream[(K, I)]
      val result = inputStream
        .toDS()
        .groupByKey(_._1)
        .mapValues(_._2)
        .transformWithState(processor, TimeMode.None(), outputMode)

      val output: List[O] = new TwsTester(processor).test(input)
      testStream(result, outputMode)(
        AddData(inputStream, input: _*),
        CheckNewAnswer(output.head, output.tail: _*),
        StopStream
      )
    }
  }

  test("fuzz test with RunningCountProcessor") {
    val random = new scala.util.Random(0)
    val input = List.fill(1000) {
      (s"key${random.nextInt(10)}", random.alphanumeric.take(5).mkString)
    }
    val processor = new RunningCountProcessor[String]()
    compareTws(processor, input)
  }

  test("fuzz test with TopKProcessor") {
    val random = new scala.util.Random(0)
    val input = List.fill(1000) {
      (
        s"key${random.nextInt(10)}",
        (random.alphanumeric.take(5).mkString, random.nextDouble() * 100)
      )
    }
    val processor = new TopKProcessor(5)
    compareTws(processor, input)
  }

  test("fuzz test with WordFrequencyProcessor") {
    val random = new scala.util.Random(0)
    val words = Array("spark", "scala", "flink", "kafka", "hadoop", "hive", "presto", "trino")
    val input = List.fill(1000) {
      (s"key${random.nextInt(10)}", ("", words(random.nextInt(words.length))))
    }
    val processor = new WordFrequencyProcessor()
    compareTws(processor, input)
  }
}
