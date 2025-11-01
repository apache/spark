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

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.ListState
import org.apache.spark.sql.streaming.MapState
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StatefulProcessor
import org.apache.spark.sql.streaming.TimeMode
import org.apache.spark.sql.streaming.TimerValues
import org.apache.spark.sql.streaming.TTLConfig
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
class RunningCountProcessor(ttl: TTLConfig = TTLConfig.NONE)
    extends StatefulProcessor[String, (String, String), (String, Long)] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState[Long]("count", Encoders.scalaLong, ttl)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
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

/** Test suite for TwsTester utility class. */
class TwsTesterSuite extends SparkFunSuite {

  test("TwsTester should correctly test RunningCountProcessor") {
    val input: List[(String, (String, String))] = List(
      ("key1", ("a", "b")),
      ("key2", ("b", "c")),
      ("key1", ("c", "d")),
      ("key2", ("b", "c")),
      ("key1", ("c", "d")),
      ("key1", ("c", "d")),
      ("key3", ("q", "f"))
    )
    val tester = new TwsTester(new RunningCountProcessor())
    val ans1: List[(String, Long)] = tester.test(input)
    assert(ans1.sorted == List(("key1", 4L), ("key2", 2L), ("key3", 1L)).sorted)

    assert(tester.peekValueState[Long]("count", "key1").get == 4L)
    assert(tester.peekValueState[Long]("count", "key2").get == 2L)
    assert(tester.peekValueState[Long]("count", "key3").get == 1L)
    assert(tester.peekValueState[Long]("count", "key4").isEmpty)

    val ans2 = tester.testOneRow("key1", ("q", "p"))
    assert(ans2 == List(("key1", 5L)))
    assert(tester.peekValueState[Long]("count", "key1").get == 5L)
    assert(tester.peekValueState[Long]("count", "key2").get == 2L)

    val ans3 = tester.test(List(("key1", ("a", "b")), ("key2", ("a", "b"))))
    assert(ans3.sorted == List(("key1", 6L), ("key2", 3L)))
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
    val processor = new RunningCountProcessor(TTLConfig(Duration.ofSeconds(100)))
    val testClock = new TestClock(Instant.EPOCH)
    val tester = new TwsTester(processor, testClock)

    tester.testOneRow("key1", ("b", "c"))
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
}
