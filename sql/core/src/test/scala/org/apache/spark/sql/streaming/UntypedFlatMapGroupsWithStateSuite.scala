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

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.{NoTimeout, ProcessingTimeTimeout}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.{Append, Complete, Update}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.streaming.GroupStateTimeout.EventTimeTimeout
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

class UntypedFlatMapGroupsWithStateSuite extends StateStoreMetricsTest {

  import testImplicits._

  import FlatMapGroupsWithStateSuite._

  /**
   * Sample `flatMapGroupsWithState` function implementation. It maintains the max event time as
   * state and set the timeout timestamp based on the current max event time seen. It returns the
   * max event time in the state, or -1 if the state was removed by timeout. Timeout is 5sec.
   */
  val sampleTestFunction =
    (key: Row, values: Iterator[Row], state: GroupState[Row]) => {
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCanGetWatermark { state.getCurrentWatermarkMs() >= -1 }

      // key: String, values: (String, Timestamp), state: Long, output: (String, Int)
      val keyAsString = key.getString(0)

      val timeoutDelaySec = 5
      if (state.hasTimedOut) {
        state.remove()
        Iterator(Row(keyAsString, -1))
      } else {
        val valuesSeq = values.toSeq
        val maxEventTimeSec = math.max(valuesSeq.map(_.getTimestamp(1).getTime / 1000).max,
          state.getOption.map(_.getLong(0)).getOrElse(0L))
        val timeoutTimestampSec = maxEventTimeSec + timeoutDelaySec
        state.update(Row(maxEventTimeSec))
        state.setTimeoutTimestamp(timeoutTimestampSec * 1000)
        Iterator(Row(keyAsString, maxEventTimeSec.toInt))
      }
    }

  test("flatMapGroupsWithState - streaming") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    val stateFunc = (key: Row, values: Iterator[Row], state: GroupState[Row]) => {
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }

      val count = state.getOption.map(_.getLong(0)).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        Iterator.empty
      } else {
        state.update(Row(count))
        Iterator(Row(key.getString(0), count.toString))
      }
    }

    val inputData = MemoryStream[String]
    val outputStructType = StructType(
      Seq(
        StructField("key", StringType),
        StructField("countAsString", StringType)))
    val stateStructType = StructType(Seq(StructField("count", LongType)))
    val result =
      inputData.toDS()
        .groupBy("value")
        .flatMapGroupsWithState(
          outputStructType, stateStructType, Update, GroupStateTimeout.NoTimeout)(stateFunc)

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
    val stateFunc = (key: Row, values: Iterator[Row], state: GroupState[Row]) => {
      values.flatMap { _ =>
        val count = state.getOption.map(_.getLong(0)).getOrElse(0L) + 1
        if (count == 3) {
          state.remove()
          None
        } else {
          state.update(Row(count))
          Some(Row(key.getString(0), count.toString))
        }
      }
    }

    val inputData = MemoryStream[String]
    val outputStructType = StructType(
      Seq(
        StructField("key", StringType),
        StructField("countAsString", StringType)))
    val stateStructType = StructType(Seq(StructField("count", LongType)))
    val result =
      inputData.toDS()
        .groupBy("value")
        .flatMapGroupsWithState(outputStructType, stateStructType, Update,
          GroupStateTimeout.NoTimeout)(stateFunc)
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

  test("flatMapGroupsWithState - streaming + aggregation") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: Row, values: Iterator[Row], state: GroupState[Row]) => {

      val keyAsString = key.getString(0)

      val count = state.getOption.map(_.getLong(0)).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        Iterator(Row(keyAsString, "-1"))
      } else {
        state.update(Row(count))
        Iterator(Row(keyAsString, count.toString))
      }
    }

    val inputData = MemoryStream[String]
    val outputStructType = StructType(
      Seq(
        StructField("key", StringType),
        StructField("countAsString", StringType)))
    val stateStructType = StructType(Seq(StructField("count", LongType)))
    val result =
      inputData.toDS()
        .groupBy("value")
        .flatMapGroupsWithState(outputStructType, stateStructType, Append,
          GroupStateTimeout.NoTimeout)(stateFunc)
        .groupBy("key")
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

  test("flatMapGroupsWithState - streaming with processing time timeout") {
    // Function to maintain the count as state and set the proc. time timeout delay of 10 seconds.
    // It returns the count if changed, or -1 if the state was removed by timeout.
    val stateFunc = (key: Row, values: Iterator[Row], state: GroupState[Row]) => {
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }

      val keyAsString = key.getString(0)
      if (state.hasTimedOut) {
        state.remove()
        Iterator(Row(keyAsString, "-1"))
      } else {
        val count = state.getOption.map(_.getLong(0)).getOrElse(0L) + values.size
        state.update(Row(count))
        state.setTimeoutDuration("10 seconds")
        Iterator(Row(keyAsString, count.toString))
      }
    }

    val clock = new StreamManualClock
    val inputData = MemoryStream[String]
    val outputStructType = StructType(
      Seq(
        StructField("key", StringType),
        StructField("countAsString", StringType)))
    val stateStructType = StructType(Seq(StructField("count", LongType)))
    val result =
      inputData.toDS()
        .groupBy("value")
        .flatMapGroupsWithState(outputStructType, stateStructType, Update,
          ProcessingTimeTimeout)(stateFunc)

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

  test("flatMapGroupsWithState - streaming w/ event time timeout + watermark") {
    val inputData = MemoryStream[(String, Int)]
    val outputStructType = StructType(
      Seq(
        StructField("key", StringType),
        StructField("maxEventTimeSec", IntegerType)))
    val stateStructType = StructType(Seq(StructField("maxEventTimeSec", LongType)))
    val result =
      inputData.toDS
        .select($"_1".as("key"), timestamp_seconds($"_2").as("eventTime"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy("key")
        .flatMapGroupsWithState(outputStructType, stateStructType, Update,
          EventTimeTimeout)(sampleTestFunction)

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

  test("mapGroupsWithState - streaming") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: Row, values: Iterator[Row], state: GroupState[Row]) => {
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }

      val keyAsString = key.getString(0)
      val count = state.getOption.map(_.getLong(0)).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        Row(keyAsString, "-1")
      } else {
        state.update(Row(count))
        Row(keyAsString, count.toString)
      }
    }

    val inputData = MemoryStream[String]
    val outputStructType = StructType(
      Seq(
        StructField("key", StringType),
        StructField("countAsString", StringType)))
    val stateStructType = StructType(Seq(StructField("count", LongType)))
    val result =
      inputData.toDS()
        .groupBy("value")
        .mapGroupsWithState(outputStructType, stateStructType,
          GroupStateTimeout.NoTimeout)(stateFunc) // Types = State: MyState, Out: (Str, Str)

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

  def testWithTimeout(timeoutConf: GroupStateTimeout): Unit = {
    test("SPARK-20714: watermark does not fail query when timeout = " + timeoutConf) {
      // Function to maintain running count up to 2, and then remove the count
      // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
      // String, (String, Long), RunningCount(Long)
      val stateFunc =
      (key: Row, values: Iterator[Row], state: GroupState[Row]) => {
        val keyAsString = key.getString(0)
        if (state.hasTimedOut) {
          state.remove()
          Iterator(Row(keyAsString, "-1"))
        } else {
          val count = state.getOption.map(_.getLong(0)).getOrElse(0L) + values.size
          state.update(Row(count))
          state.setTimeoutDuration("10 seconds")
          Iterator(Row(keyAsString, count.toString))
        }
      }

      val clock = new StreamManualClock
      val inputData = MemoryStream[(String, Long)]
      val outputStructType = StructType(
        Seq(
          StructField("key", StringType),
          StructField("countAsString", StringType)))
      val stateStructType = StructType(Seq(StructField("count", LongType)))
      val result =
        inputData.toDF().toDF("key", "time")
          .selectExpr("key", "timestamp_seconds(time) as timestamp")
          .withWatermark("timestamp", "10 second")
          .groupBy("key")
          .flatMapGroupsWithState(outputStructType, stateStructType, Update,
            ProcessingTimeTimeout)(stateFunc)

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
}

object UntypedFlatMapGroupsWithStateSuite {

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
