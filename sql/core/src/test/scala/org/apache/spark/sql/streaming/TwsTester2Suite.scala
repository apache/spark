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

import org.apache.spark.sql.streaming.processors._
import org.apache.spark.sql.test.SharedSparkSession

/** Test suite for TwsTester2 utility class - Spark-based streaming tester. */
class TwsTester2Suite extends SharedSparkSession {
  import testImplicits._
  
  // ===== ValueState Tests (IMPLEMENTED) =====
  
  test("TwsTester2 should correctly test RunningCountProcessor") {
    val input: List[(String, String)] = List(
      ("key1", "a"),
      ("key2", "b"),
      ("key1", "c"),
      ("key2", "b"),
      ("key1", "c"),
      ("key1", "c"),
      ("key3", "q")
    )
    val tester = new TwsTester2(new RunningCountProcessor[String]())
    val ans1: List[(String, Long)] = tester.test(input)
    assert(ans1.sorted == List(("key1", 4L), ("key2", 2L), ("key3", 1L)).sorted)

    assert(tester.peekValueState[Long]("count", "key1").get == 4L)
    assert(tester.peekValueState[Long]("count", "key2").get == 2L)
    assert(tester.peekValueState[Long]("count", "key3").get == 1L)
    assert(tester.peekValueState[Long]("count", "key4").isEmpty)

    val ans2 = tester.test(List(("key1", "q")))
    assert(ans2 == List(("key1", 5L)))
    assert(tester.peekValueState[Long]("count", "key1").get == 5L)
    assert(tester.peekValueState[Long]("count", "key2").get == 2L)

    val ans3 = tester.test(List(("key1", "a"), ("key2", "a")))
    assert(ans3.sorted == List(("key1", 6L), ("key2", 3L)))
    
    tester.stop()
  }

  test("TwsTester2 should allow direct access to ValueState") {
    val processor = new RunningCountProcessor[String]()
    val tester = new TwsTester2[String, String, (String, Long)](processor)
    tester.setValueState[Long]("count", "foo", 5)
    tester.test(List(("foo", "a")))
    assert(tester.peekValueState[Long]("count", "foo").get == 6L)
    
    tester.stop()
  }

  // ===== ListState Tests (IMPLEMENTED) =====
  
  test("TwsTester2 should correctly test TopKProcessor") {
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
    val tester = new TwsTester2(new TopKProcessor(2))
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
    
    tester.stop()
  }

  test("TwsTester2 should allow direct access to ListState") {
    val tester = new TwsTester2(new TopKProcessor(2))
    tester.setListState("topK", "a", List(6.0, 5.0))
    tester.setListState("topK", "b", List(8.0, 7.0))
    tester.test(List(("a", ("", 10.0))))
    tester.test(List(("b", ("", 7.5))))
    tester.test(List(("c", ("", 1.0))))

    assert(tester.peekListState[Double]("topK", "a") == List(10.0, 6.0))
    assert(tester.peekListState[Double]("topK", "b") == List(8.0, 7.5))
    assert(tester.peekListState[Double]("topK", "c") == List(1.0))
    assert(tester.peekListState[Double]("topK", "d") == List())
    
    tester.stop()
  }

  // ===== MapState Tests (IMPLEMENTED) =====
  
  // TODO: "TwsTester2 should correctly test WordFrequencyProcessor"
  // TODO: "TwsTester2 should allow direct access to MapState"

  // ===== TTL Tests (NOT YET IMPLEMENTED) =====
  
  // TODO: TwsTester2 should expire old value state according to TTL
  // TODO: TwsTester2 should expire old list state according to TTL
  // TODO: TwsTester2 should expire old map state according to TTL

  // ===== Additional ValueState Test Methods (NOT YET IMPLEMENTED) =====
  
  // TODO: TwsTester2 should test one row with value state

  // ===== Timer Tests (NOT YET IMPLEMENTED) =====
  
  // TODO: TwsTester2 should handle session timeout with timer
  // TODO: TwsTester2 should process input before timers
  // TODO: TwsTester2 should handle multiple timers in same batch
  // TODO: TwsTester2 should not process timers twice
  // TODO: TwsTester2 should handle timers for multiple keys independently
  // TODO: TwsTester2 should handle timer deletion correctly

  // ===== Event Time and Watermark Tests (NOT YET IMPLEMENTED) =====
  
  // TODO: TwsTester2 should handle EventTime mode with watermark
  // TODO: TwsTester2 should filter late events based on watermark

  // ===== Initial State Tests (NOT YET IMPLEMENTED) =====
  
  // TODO: TwsTester2 should call handleInitialState

  // ===== Row-by-Row Tests (NOT YET IMPLEMENTED) =====
  
  // TODO: TwsTester2 should test RunningCountProcessor row-by-row
}
