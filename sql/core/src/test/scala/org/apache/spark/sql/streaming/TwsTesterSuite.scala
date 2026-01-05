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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf

/** Test suite for TwsTester utility class. */
class TwsTesterSuite extends SparkFunSuite {

  test("TwsTester should correctly test RunningCountProcessor") {
    val tester = new TwsTester(new RunningCountProcessor[String]())
    assert(tester.test("key1", List("a")) == List(("key1", 1L)))
    assert(tester.test("key2", List("a", "a")) == List(("key2", 2L)))
    assert(tester.test("key3", List("a")) == List(("key3", 1L)))
    assert(tester.test("key1", List("a", "a", "a")) == List(("key1", 4L)))

    assert(tester.peekValueState[Long]("count", "key1").get == 4L)
    assert(tester.peekValueState[Long]("count", "key2").get == 2L)
    assert(tester.peekValueState[Long]("count", "key3").get == 1L)
    assert(tester.peekValueState[Long]("count", "key4").isEmpty)
  }

  test("TwsTester should allow direct access to ValueState") {
    val processor = new RunningCountProcessor[String]()
    val tester = new TwsTester[String, String, (String, Long)](processor)
    tester.updateValueState[Long]("count", "foo", 5)
    tester.test("foo", List("a"))
    assert(tester.peekValueState[Long]("count", "foo").get == 6L)
  }

  test("TwsTester should correctly test TopKProcessor") {
    val tester = new TwsTester(new TopKProcessor(2))
    val ans1 = tester.test("key1", List(("b", 2.0), ("c", 3.0), ("a", 1.0)))
    assert(ans1 == List(("key1", 3.0), ("key1", 2.0)))
    val ans2 = tester.test("key2", List(("a", 10.0), ("b", 20.0), ("c", 30.0), ("d", 40.0)))
    assert(ans2 == List(("key2", 40.0), ("key2", 30.0)))
    val ans3 = tester.test("key3", List(("a", 100.0)))
    assert(ans3 == List(("key3", 100.0)))

    assert(tester.peekListState[Double]("topK", "key1") == List(3.0, 2.0))
    assert(tester.peekListState[Double]("topK", "key2") == List(40.0, 30.0))
    assert(tester.peekListState[Double]("topK", "key3") == List(100.0))
    assert(tester.peekListState[Double]("topK", "key4").isEmpty)

    val ans4 = tester.test("key1", List(("a", 10.0)))
    assert(ans4 == List(("key1", 10.0), ("key1", 3.0)))
    assert(tester.peekListState[Double]("topK", "key1") == List(10.0, 3.0))
  }

  test("TwsTester should allow direct access to ListState") {
    val tester = new TwsTester(new TopKProcessor(2))
    tester.updateListState("topK", "a", List(6.0, 5.0))
    tester.updateListState("topK", "b", List(8.0, 7.0))
    tester.test("a", List(("", 10.0)))
    tester.test("b", List(("", 7.5)))
    tester.test("c", List(("", 1.0)))

    assert(tester.peekListState[Double]("topK", "a") == List(10.0, 6.0))
    assert(tester.peekListState[Double]("topK", "b") == List(8.0, 7.5))
    assert(tester.peekListState[Double]("topK", "c") == List(1.0))
    assert(tester.peekListState[Double]("topK", "d") == List())
  }

  test("TwsTester should correctly test WordFrequencyProcessor") {
    val tester = new TwsTester(new WordFrequencyProcessor())
    val ans1 =
      tester.test("user1", List(("", "hello"), ("", "world"), ("", "hello"), ("", "world")))
    assert(
      ans1.sorted == List(
        ("user1", "hello", 1L),
        ("user1", "hello", 2L),
        ("user1", "world", 1L),
        ("user1", "world", 2L)
      ).sorted
    )

    val ans2 = tester.test("user2", List(("", "hello"), ("", "spark")))
    assert(ans2.sorted == List(("user2", "hello", 1L), ("user2", "spark", 1L)).sorted)

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
    val ans3 = tester.test("user1", List(("", "hello"), ("", "test")))
    assert(ans3.sorted == List(("user1", "hello", 3L), ("user1", "test", 1L)).sorted)
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
    tester.updateMapState("frequencies", "user1", Map("hello" -> 5L, "world" -> 3L))
    tester.updateMapState("frequencies", "user2", Map("spark" -> 10L))

    // Process new words
    tester.test("user1", List(("", "hello")))
    tester.test("user1", List(("", "goodbye")))
    tester.test("user2", List(("", "spark")))
    tester.test("user3", List(("", "new")))

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

  test("TwsTester can be used to test step function") {
    val processor = new RunningCountProcessor[String]()
    val tester = new TwsTester(processor)

    // Example of helper function using TwsTester to inspect how processing a single row changes
    // state.
    def testStepFunction(key: String, inputRow: String, stateIn: Long): Long = {
      tester.updateValueState[Long]("count", key, stateIn)
      tester.test(key, List(inputRow))
      tester.peekValueState("count", key).get
    }

    assert(testStepFunction("key1", "a", 10L) == 11L)
  }

  test("TwsTester should call handleInitialState") {
    val processor = new RunningCountProcessor[String]()
    val tester = new TwsTester(processor, initialState = List(("a", 10L), ("b", 20L)))
    assert(tester.peekValueState[Long]("count", "a").get == 10L)
    assert(tester.peekValueState[Long]("count", "b").get == 20L)

    val ans1 = tester.test("a", List("a"))
    val ans2 = tester.test("c", List("c"))
    assert(ans1 == List(("a", 11L)))
    assert(ans2 == List(("c", 1L)))
  }

  test("TwsTester should fail when initialState is passed but not supported") {
    val processor = new TopKProcessor(5)
    val exception = intercept[IllegalArgumentException] {
      new TwsTester(processor, initialState = List(("a", List(1.0, 2.0))))
    }
    assert(exception.getMessage.contains("stateful processor doesn't support initial state"))
  }

  test("TwsTester should test RunningCountProcessor row-by-row") {
    val tester = new TwsTester(new RunningCountProcessor[String]())

    // Example of helper function to test how TransformWithState processes rows one-by-one, which
    // can be used to simulate real-time mode.
    def testRowByRow(input: List[(String, String)]): List[(String, Long)] = {
      input.flatMap { case (key, value) => tester.test(key, List(value)) }
    }

    val input: List[(String, String)] = List(
      ("key1", "a"),
      ("key2", "b"),
      ("key1", "c"),
      ("key2", "b"),
      ("key1", "c"),
      ("key1", "c"),
      ("key3", "q")
    )
    val ans: List[(String, Long)] = testRowByRow(input)
    assert(
      ans == List(
        ("key1", 1L),
        ("key2", 1L),
        ("key1", 2L),
        ("key2", 2L),
        ("key1", 3L),
        ("key1", 4L),
        ("key3", 1L)
      )
    )
  }

  test("TwsTester should exercise all state methods") {
    val tester = new TwsTester(new AllMethodsTestProcessor())
    val results = tester.test(
      "k",
      List(
        "value-exists", // false
        "value-set", // set to 42
        "value-exists", // true
        "value-clear", // clear
        "value-exists", // false again
        "list-exists", // false
        "list-append", // append a, b
        "list-exists", // true
        "list-append-array", // append c, d
        "list-get", // a,b,c,d
        "map-exists", // false
        "map-add", // add x=1, y=2, z=3
        "map-exists", // true
        "map-keys", // x,y,z
        "map-values", // 1,2,3
        "map-iterator", // x=1,y=2,z=3
        "map-remove", // remove y
        "map-keys", // x,z
        "map-clear", // clear map
        "map-exists" // false
      )
    )

    assert(
      results == List(
        ("k", "value-exists:false"),
        ("k", "value-set:done"),
        ("k", "value-exists:true"),
        ("k", "value-clear:done"),
        ("k", "value-exists:false"),
        ("k", "list-exists:false"),
        ("k", "list-append:done"),
        ("k", "list-exists:true"),
        ("k", "list-append-array:done"),
        ("k", "list-get:a,b,c,d"),
        ("k", "map-exists:false"),
        ("k", "map-add:done"),
        ("k", "map-exists:true"),
        ("k", "map-keys:x,y,z"),
        ("k", "map-values:1,2,3"),
        ("k", "map-iterator:x=1,y=2,z=3"),
        ("k", "map-remove:done"),
        ("k", "map-keys:x,z"),
        ("k", "map-clear:done"),
        ("k", "map-exists:false")
      )
    )
  }

  test("TwsTester: value-get before value-set") {
    val tester = new TwsTester(new AllMethodsTestProcessor())
    val results = tester.test("k", List("value-get", "value-set", "value-get"))
    assert(results == List(("k", ""), ("k", "value-set:done"), ("k", "42")))
  }

  test("TwsTester: list operations") {
    val tester = new TwsTester(new AllMethodsTestProcessor())
    val results =
      tester.test(
        "k",
        List(
          "list-get",
          "list-append",
          "list-get",
          "list-append",
          "list-get",
          "list-put",
          "list-get",
          "list-clear",
          "list-get"
        )
      )
    assert(
      results == List(
        ("k", "list-get:"),
        ("k", "list-append:done"),
        ("k", "list-get:a,b"),
        ("k", "list-append:done"),
        ("k", "list-get:a,b,a,b"),
        ("k", "list-put:done"),
        ("k", "list-get:put"),
        ("k", "list-clear:done"),
        ("k", "list-get:")
      )
    )
  }

  test("TwsTester should delete value state") {
    val valueTester = new TwsTester(new RunningCountProcessor[String]())
    valueTester.updateValueState[Long]("count", "key1", 10L)
    valueTester.updateValueState[Long]("count", "key2", 20L)
    assert(valueTester.peekValueState[Long]("count", "key1").get == 10L)
    valueTester.deleteState("count", "key1")
    assert(valueTester.peekValueState[Long]("count", "key1").isEmpty)
    assert(valueTester.peekValueState[Long]("count", "key2").get == 20L)
  }

  test("TwsTester should delete list state") {
    val listTester = new TwsTester(new TopKProcessor(3))
    listTester.updateListState("topK", "key1", List(1.0, 2.0, 3.0))
    listTester.updateListState("topK", "key2", List(4.0, 5.0))
    assert(listTester.peekListState[Double]("topK", "key1") == List(1.0, 2.0, 3.0))
    listTester.deleteState("topK", "key1")
    assert(listTester.peekListState[Double]("topK", "key1").isEmpty)
    assert(listTester.peekListState[Double]("topK", "key2") == List(4.0, 5.0))
  }

  test("TwsTester should delete map state") {
    val mapTester = new TwsTester(new WordFrequencyProcessor())
    mapTester.updateMapState("frequencies", "user1", Map("hello" -> 5L, "world" -> 3L))
    mapTester.updateMapState("frequencies", "user2", Map("spark" -> 10L))
    assert(
      mapTester
        .peekMapState[String, Long]("frequencies", "user1") == Map("hello" -> 5L, "world" -> 3L)
    )
    mapTester.deleteState("frequencies", "user1")
    assert(mapTester.peekMapState[String, Long]("frequencies", "user1").isEmpty)
    assert(mapTester.peekMapState[String, Long]("frequencies", "user2") == Map("spark" -> 10L))
  }

  test("TwsTester should expire ValueState after TTL") {
    import java.time.Duration
    val ttl = TTLConfig(Duration.ofMillis(5000))
    val tester = new TwsTester(
      new RunningCountProcessor[String](ttl),
      timeMode = TimeMode.ProcessingTime()
    )

    // Process input for key1 - state should be set
    tester.test("key1", List("a"))
    assert(tester.peekValueState[Long]("count", "key1").get == 1L)

    // Advance time by 3 seconds - state should still exist
    tester.advanceProcessingTime(3000)
    assert(tester.peekValueState[Long]("count", "key1").get == 1L)

    // Advance time by 3 more seconds (total 6s) - state should be expired
    tester.advanceProcessingTime(3000)
    assert(tester.peekValueState[Long]("count", "key1").isEmpty)
  }

  test("TwsTester should expire ListState after TTL") {
    import java.time.Duration
    val ttl = TTLConfig(Duration.ofMillis(5000))
    val tester = new TwsTester(
      new TopKProcessor(3, ttl),
      timeMode = TimeMode.ProcessingTime()
    )

    // Process input for key1 - state should be set
    tester.test("key1", List(("a", 1.0), ("b", 2.0), ("c", 3.0)))
    assert(tester.peekListState[Double]("topK", "key1") == List(3.0, 2.0, 1.0))

    // Advance time by 3 seconds - state should still exist
    tester.advanceProcessingTime(3000)
    assert(tester.peekListState[Double]("topK", "key1") == List(3.0, 2.0, 1.0))

    // Advance time by 3 more seconds (total 6s) - state should be expired
    tester.advanceProcessingTime(3000)
    assert(tester.peekListState[Double]("topK", "key1").isEmpty)
  }

  test("TwsTester should expire MapState after TTL") {
    import java.time.Duration
    val ttl = TTLConfig(Duration.ofMillis(5000))
    val tester = new TwsTester(
      new WordFrequencyProcessor(ttl),
      timeMode = TimeMode.ProcessingTime()
    )

    // Process input for user1 - state should be set
    tester.test("user1", List(("", "hello"), ("", "world")))
    assert(
      tester.peekMapState[String, Long]("frequencies", "user1") == Map("hello" -> 1L, "world" -> 1L)
    )

    // Advance time by 3 seconds - state should still exist
    tester.advanceProcessingTime(3000)
    assert(
      tester.peekMapState[String, Long]("frequencies", "user1") == Map("hello" -> 1L, "world" -> 1L)
    )

    // Advance time by 3 more seconds (total 6s) - state should be expired
    tester.advanceProcessingTime(3000)
    assert(tester.peekMapState[String, Long]("frequencies", "user1").isEmpty)
  }

  test("TwsTester should support ProcessingTime timers") {
    val tester = new TwsTester(
      new SessionTimeoutProcessor(),
      timeMode = TimeMode.ProcessingTime()
    )

    // Process input for key1 - should register a timer at t=10000
    val result1 = tester.test("key1", List("hello"))
    assert(result1 == List(("key1", "received:hello")))

    // Advance time by 5 seconds - timer should NOT fire yet
    val expired1 = tester.advanceProcessingTime(5000)
    assert(expired1.isEmpty)

    // Process input for key2 at t=5000 - should register timer at t=15000
    val result2 = tester.test("key2", List("world"))
    assert(result2 == List(("key2", "received:world")))

    // Advance time by 6 seconds (total t=11000) - key1's timer should fire
    val expired2 = tester.advanceProcessingTime(6000)
    assert(expired2 == List(("key1", "session-expired@11000")))

    // Advance time by 5 seconds (total t=16000) - key2's timer should fire
    val expired3 = tester.advanceProcessingTime(5000)
    assert(expired3 == List(("key2", "session-expired@16000")))

    // Verify state is cleared after session expiry
    assert(tester.peekValueState[Long]("lastSeen", "key1").isEmpty)
    assert(tester.peekValueState[Long]("lastSeen", "key2").isEmpty)
  }

  test("TwsTester should support EventTime timers fired by data-driven watermark") {
    import java.sql.Timestamp

    // Event time extractor: input is (eventTimeMs, value), extract eventTimeMs
    val eventTimeExtractor: ((Long, String)) => Timestamp = {
      case (eventTimeMs, _) => new Timestamp(eventTimeMs)
    }

    val tester = new TwsTester(
      new EventTimeSessionProcessor(),
      timeMode = TimeMode.EventTime(),
      eventTimeExtractor = eventTimeExtractor,
      watermarkDelayMs = 2000 // 2 second watermark delay
    )

    // Process event at t=10000 for key1 - registers timer at t=15000
    // Watermark becomes: 10000 - 2000 = 8000 (timer at 15000 won't fire)
    val result1 = tester.test("key1", List((10000L, "hello")))
    assert(result1 == List(("key1", "received:hello@10000")))

    // Process event at t=12000 for key2 - registers timer at t=17000
    // Watermark becomes: 12000 - 2000 = 10000 (still not enough for key1's timer at 15000)
    val result2 = tester.test("key2", List((12000L, "world")))
    assert(result2 == List(("key2", "received:world@12000")))

    // Process event at t=20000 for key3 - watermark becomes 18000
    // This should fire key1's timer at 15000 and key2's timer at 17000
    val result3 = tester.test("key3", List((20000L, "new")))
    assert(result3.contains(("key3", "received:new@20000")))
    // Both key1 and key2 timers should have fired
    assert(result3.exists(r => r._1 == "key1" && r._2.startsWith("session-expired@watermark=")))
    assert(result3.exists(r => r._1 == "key2" && r._2.startsWith("session-expired@watermark=")))

    // Verify state is cleared for key1 and key2 after timer expiry
    assert(tester.peekValueState[Long]("lastEventTime", "key1").isEmpty)
    assert(tester.peekValueState[Long]("lastEventTime", "key2").isEmpty)
    // key3 should still have state (timer at 25000 hasn't fired yet)
    assert(tester.peekValueState[Long]("lastEventTime", "key3").isDefined)
  }

  test("TwsTester should support EventTime timers fired by manual watermark advance") {
    import java.sql.Timestamp

    val eventTimeExtractor: ((Long, String)) => Timestamp = {
      case (eventTimeMs, _) => new Timestamp(eventTimeMs)
    }

    val tester = new TwsTester(
      new EventTimeSessionProcessor(),
      timeMode = TimeMode.EventTime(),
      eventTimeExtractor = eventTimeExtractor,
      watermarkDelayMs = 2000
    )

    // Process event at t=10000 for key1 - registers timer at t=15000
    // Watermark: 10000 - 2000 = 8000
    val result1 = tester.test("key1", List((10000L, "hello")))
    assert(result1 == List(("key1", "received:hello@10000")))

    // Process another event at t=12000 for key1 - should cancel timer at t=15000
    // and register new timer at t=17000
    // Watermark: 12000 - 2000 = 10000
    val result2 = tester.test("key1", List((12000L, "hello2")))
    assert(result2 == List(("key1", "received:hello2@12000")))

    // Advance watermark to 16000 (past where old timer at 15000 would have fired)
    // No timer should fire because the old timer was cancelled
    val expired1 = tester.advanceWatermark(6000) // 10000 + 6000 = 16000
    assert(expired1.isEmpty, "Old timer should have been cancelled, but it fired")

    // Verify state is still present (session not expired yet)
    assert(tester.peekValueState[Long]("lastEventTime", "key1").isDefined)

    // Now advance watermark past the new timer at 17000
    val expired2 = tester.advanceWatermark(2000) // 16000 + 2000 = 18000
    assert(expired2.size == 1)
    assert(expired2.head._1 == "key1")
    assert(expired2.head._2.startsWith("session-expired@watermark="))

    // Verify state is cleared
    assert(tester.peekValueState[Long]("lastEventTime", "key1").isEmpty)
  }

  test("TwsTester should filter late events in EventTime mode") {
    import java.sql.Timestamp

    // Create a processor that counts events - we'll use the count to verify filtering
    // Input format: (eventTimeMs: Long, value: String)
    val eventTimeExtractor: ((Long, String)) => Timestamp = {
      case (eventTimeMs, _) => new Timestamp(eventTimeMs)
    }

    val tester = new TwsTester(
      new EventTimeCountProcessor(),
      timeMode = TimeMode.EventTime(),
      eventTimeExtractor = eventTimeExtractor,
      watermarkDelayMs = 1000 // 1 second watermark delay
    )

    // Initially watermark is 0, so all events should be processed
    // Process events at t=5000, t=6000, t=7000 - watermark becomes 7000 - 1000 = 6000
    val result1 = tester.test("key1", List((5000L, "a"), (6000L, "b"), (7000L, "c")))
    assert(result1 == List(("key1", 3L))) // 3 events counted
    assert(tester.peekValueState[Long]("count", "key1").get == 3L)

    // Now watermark is 6000. Send events with mixed event times:
    // - (4000L, "late1") -> event time 4000 < watermark 6000, should be FILTERED
    // - (5000L, "late2") -> event time 5000 < watermark 6000, should be FILTERED
    // - (6000L, "ontime1") -> event time 6000 >= watermark 6000, should be PROCESSED
    // - (8000L, "ontime2") -> event time 8000 >= watermark 6000, should be PROCESSED
    val result2 = tester.test(
      "key1",
      List(
        (4000L, "late1"),
        (5000L, "late2"),
        (6000L, "ontime1"),
        (8000L, "ontime2")
      )
    )
    // Only 2 events should be processed (the on-time ones)
    assert(result2 == List(("key1", 5L))) // 3 + 2 = 5
    assert(tester.peekValueState[Long]("count", "key1").get == 5L)

    // Watermark should have advanced to max(8000) - 1000 = 7000
    // Now send another late event at t=6500 (< 7000), should be filtered
    val result3 = tester.test("key1", List((6500L, "late3")))
    assert(result3 == List(("key1", 5L))) // Still 5, no new events processed
    assert(tester.peekValueState[Long]("count", "key1").get == 5L)

    // Send an on-time event at t=10000
    val result4 = tester.test("key1", List((10000L, "ontime3")))
    assert(result4 == List(("key1", 6L))) // 5 + 1 = 6
    assert(tester.peekValueState[Long]("count", "key1").get == 6L)
  }

  test("TwsTester should filter all late events when all are older than watermark") {
    import java.sql.Timestamp

    val eventTimeExtractor: ((Long, String)) => Timestamp = {
      case (eventTimeMs, _) => new Timestamp(eventTimeMs)
    }

    val tester = new TwsTester(
      new EventTimeCountProcessor(),
      timeMode = TimeMode.EventTime(),
      eventTimeExtractor = eventTimeExtractor,
      watermarkDelayMs = 0 // No delay for simplicity
    )

    // Process events to advance watermark to 10000
    tester.test("key1", List((10000L, "a")))
    assert(tester.peekValueState[Long]("count", "key1").get == 1L)

    // Now send only late events - all should be filtered, count unchanged
    val result = tester.test(
      "key1",
      List(
        (1000L, "late1"),
        (5000L, "late2"),
        (9999L, "late3")
      )
    )
    // No events processed - handleInputRows is called with empty iterator
    // This should still return output but count shouldn't change
    assert(result == List(("key1", 1L))) // Count still 1
    assert(tester.peekValueState[Long]("count", "key1").get == 1L)
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
   * Asserts that {@code tester} is equivalent to streaming query transforming {@code inputStream}
   * to {@code result}, when both are fed with data from {@code batches}.
   */
  private def checkTwsTesterEndToEnd[
      K: org.apache.spark.sql.Encoder,
      I: org.apache.spark.sql.Encoder,
      O: org.apache.spark.sql.Encoder](
      tester: TwsTester[K, I, O],
      batches: List[List[(K, I)]],
      inputStream: MemoryStream[(K, I)],
      result: Dataset[O]): Unit = {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "5"
    ) {
      val expectedResults: List[List[O]] = batches
        .map(batch => batch.groupBy(_._1).flatMap(p => tester.test(p._1, p._2.map(_._2))).toList)
        .toList
      assert(batches.size == expectedResults.size)

      val actions: Seq[StreamAction] = (batches zip expectedResults).flatMap {
          case (batch, expected) =>
            Seq(
              AddData(inputStream, batch: _*),
              CheckNewAnswer(expected.head, expected.tail: _*)
            )
        } :+ StopStream
      testStream(result, OutputMode.Append())(actions: _*)
    }
  }

  /**
   * Asserts that {@code tester} processes given {@code batches} in the same way as Spark streaming
   * query with {@code transformWithState} would.
   */
  private def checkTwsTester[
      K: org.apache.spark.sql.Encoder,
      I: org.apache.spark.sql.Encoder,
      O: org.apache.spark.sql.Encoder](
      processor: StatefulProcessor[K, I, O],
      batches: List[List[(K, I)]]): Unit = {
    implicit val tupleEncoder = org.apache.spark.sql.Encoders.tuple(
      implicitly[org.apache.spark.sql.Encoder[K]],
      implicitly[org.apache.spark.sql.Encoder[I]]
    )
    val inputStream = MemoryStream[(K, I)]
    val result = inputStream
      .toDS()
      .groupByKey(_._1)
      .mapValues(_._2)
      .transformWithState(processor, TimeMode.None(), OutputMode.Append())
    checkTwsTesterEndToEnd(new TwsTester(processor), batches, inputStream, result)
  }

  private def split[T](xs: List[T], numParts: Int): List[List[T]] = {
    require(numParts > 0 && xs.size % numParts == 0)
    val partSize = xs.size / numParts
    (0 until numParts).map { i =>
      xs.slice(i * partSize, (i + 1) * partSize)
    }.toList
  }

  test("fuzz test with RunningCountProcessor") {
    val random = new scala.util.Random(0)
    val input = List.fill(1000) {
      (s"key${random.nextInt(10)}", random.alphanumeric.take(5).mkString)
    }
    val processor = new RunningCountProcessor[String]()
    checkTwsTester(processor, split(input, 2))
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
    checkTwsTester(processor, split(input, 2))
  }

  test("fuzz test with WordFrequencyProcessor") {
    val random = new scala.util.Random(0)
    val words = Array("spark", "scala", "flink", "kafka", "hadoop", "hive", "presto", "trino")
    val input = List.fill(1000) {
      (s"key${random.nextInt(10)}", ("", words(random.nextInt(words.length))))
    }
    val processor = new WordFrequencyProcessor()
    checkTwsTester(processor, split(input, 2))
  }

  test("fuzz test for AllMethodsTestProcessor") {
    val random = new scala.util.Random(0)
    val commands = Array(
      "value-exists",
      "value-set",
      "value-clear",
      "list-exists",
      "list-append",
      "list-append-array",
      "list-get",
      "map-exists",
      "map-add",
      "map-keys",
      "map-values",
      "map-iterator",
      "map-remove",
      "map-clear"
    )
    val input = List.fill(500) {
      (s"key${random.nextInt(5)}", commands(random.nextInt(commands.length)))
    }
    val processor = new AllMethodsTestProcessor()
    checkTwsTester(processor, split(input, 2))
  }
}
