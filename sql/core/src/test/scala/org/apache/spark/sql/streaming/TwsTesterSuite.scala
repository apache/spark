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
import org.apache.spark.sql.streaming.processors._

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

  test("TwsTester should test one row with value state") {
    val processor = new RunningCountProcessor[String]()
    val tester = new TwsTester(processor)

    val (rows, newState) = tester.testOneRowWithValueState("key1", "a", "count", 10L)
    assert(rows == List(("key1", 11L)))
    assert(newState == 11L)
  }

  test("TwsTester should call handleInitialState") {
    val processor = new RunningCountProcessor[String]()
    val tester = new TwsTester(processor, initialState = List(("a", 10L), ("b", 20L)))
    assert(tester.peekValueState[Long]("count", "a").get == 10L)
    assert(tester.peekValueState[Long]("count", "b").get == 20L)

    val ans = tester.test(List(("a", "a"), ("c", "c")))
    assert(ans == List(("a", 11L), ("c", 1L)))
  }

  test("TwsTester should test RunningCountProcessor row-by-row") {
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
    val ans: List[(String, Long)] = tester.testRowByRow(input)
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
    val results = tester.test(List(
      ("k", "value-exists"),      // false
      ("k", "value-set"),          // set to 42
      ("k", "value-exists"),       // true
      ("k", "value-clear"),        // clear
      ("k", "value-exists"),       // false again
      ("k", "list-exists"),        // false
      ("k", "list-append"),        // append a, b
      ("k", "list-exists"),        // true
      ("k", "list-append-array"),  // append c, d
      ("k", "list-get"),           // a,b,c,d
      ("k", "map-exists"),         // false
      ("k", "map-add"),            // add x=1, y=2, z=3
      ("k", "map-exists"),         // true
      ("k", "map-keys"),           // x,y,z
      ("k", "map-values"),         // 1,2,3
      ("k", "map-iterator"),       // x=1,y=2,z=3
      ("k", "map-remove"),         // remove y
      ("k", "map-keys"),           // x,z
      ("k", "map-clear"),          // clear map
      ("k", "map-exists"),         // false
      ("k", "delete-state")        // delete value state
    ))

    assert(results == List(
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
      ("k", "map-exists:false"),
      ("k", "delete-state:done")
    ))
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
  def checkTwsTester[
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
      val expectedResults: List[List[O]] = batches.map(batch => tester.test(batch)).toList
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
   * Asserts that {@code tester} processes given {@code input} in the same way as Spark streaming
   * query with {@code transformWithState} would.
   *
   * This is simplified version of {@code checkTwsTester} for the case where there is only one batch
   * and no timers (time mode is TimeMode.None).
   */
  def checkTwsTesterOneBatch[
      K: org.apache.spark.sql.Encoder,
      I: org.apache.spark.sql.Encoder,
      O: org.apache.spark.sql.Encoder](
      processor: StatefulProcessor[K, I, O],
      input: List[(K, I)]): Unit = {
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
    checkTwsTester(new TwsTester(processor), List(input), inputStream, result)
  }

  test("fuzz test with RunningCountProcessor") {
    val random = new scala.util.Random(0)
    val input = List.fill(1000) {
      (s"key${random.nextInt(10)}", random.alphanumeric.take(5).mkString)
    }
    val processor = new RunningCountProcessor[String]()
    checkTwsTesterOneBatch(processor, input)
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
    checkTwsTesterOneBatch(processor, input)
  }

  test("fuzz test with WordFrequencyProcessor") {
    val random = new scala.util.Random(0)
    val words = Array("spark", "scala", "flink", "kafka", "hadoop", "hive", "presto", "trino")
    val input = List.fill(1000) {
      (s"key${random.nextInt(10)}", ("", words(random.nextInt(words.length))))
    }
    val processor = new WordFrequencyProcessor()
    checkTwsTesterOneBatch(processor, input)
  }
}
