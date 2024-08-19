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
import java.time.Duration
import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object TransformWithStateSuiteUtils {
  val NUM_SHUFFLE_PARTITIONS = 5
}

class RunningCountStatefulProcessor extends StatefulProcessor[String, String, (String, String)]
  with Logging {
  @transient protected var _countState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + 1
    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }
}

class RunningCountStatefulProcessorWithTTL
  extends StatefulProcessor[String, String, (String, String)]
  with Logging {
  @transient protected var _countState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState",
      Encoders.scalaLong, TTLConfig(Duration.ofMillis(1000)))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + 1
    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }
}

// Class to test that changing between Value and List State fails
// between query runs
class RunningCountListStatefulProcessor
  extends StatefulProcessor[String, String, (String, String)]
    with Logging {
  @transient protected var _countState: ListState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getListState[Long](
      "countState", Encoders.scalaLong)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    Iterator.empty
  }
}

class RunningCountStatefulProcessorInt
  extends StatefulProcessor[String, String, (String, String)] {
  @transient protected var _countState: ValueState[Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Int]("countState", Encoders.scalaInt)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    val count = _countState.getOption().getOrElse(0) + 1
    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }
}

// Class to verify stateful processor usage with adding processing time timers
class RunningCountStatefulProcessorWithProcTimeTimer extends RunningCountStatefulProcessor {
  private def handleProcessingTimeBasedTimers(
      key: String,
      expiryTimestampMs: Long): Iterator[(String, String)] = {
    _countState.clear()
    Iterator((key, "-1"))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {

    if (expiredTimerInfo.isValid()) {
      handleProcessingTimeBasedTimers(key, expiredTimerInfo.getExpiryTimeInMs())
    } else {
      val currCount = _countState.getOption().getOrElse(0L)
      if (currCount == 0 && (key == "a" || key == "c")) {
        getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs()
          + 5000)
      }

      val count = currCount + 1
      if (count == 3) {
        _countState.clear()
        Iterator.empty
      } else {
        _countState.update(count)
        Iterator((key, count.toString))
      }
    }
  }
}

// Class to verify stateful processor usage with updating processing time timers
class RunningCountStatefulProcessorWithProcTimeTimerUpdates
  extends RunningCountStatefulProcessor {
  @transient private var _timerState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode) : Unit = {
    super.init(outputMode, timeMode)
    _timerState = getHandle.getValueState[Long]("timerState", Encoders.scalaLong)
  }

  private def handleProcessingTimeBasedTimers(
      key: String,
      expiryTimestampMs: Long): Iterator[(String, String)] = {
    _timerState.clear()
    Iterator((key, "-1"))
  }

  protected def processUnexpiredRows(
      key: String,
      currCount: Long,
      count: Long,
      timerValues: TimerValues): Unit = {
    _countState.update(count)
    if (key == "a") {
      var nextTimerTs: Long = 0L
      if (currCount == 0) {
        nextTimerTs = timerValues.getCurrentProcessingTimeInMs() + 5000
        getHandle.registerTimer(nextTimerTs)
        _timerState.update(nextTimerTs)
      } else if (currCount == 1) {
        getHandle.deleteTimer(_timerState.get())
        nextTimerTs = timerValues.getCurrentProcessingTimeInMs() + 7500
        getHandle.registerTimer(nextTimerTs)
        _timerState.update(nextTimerTs)
      }
    }
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    if (expiredTimerInfo.isValid()) {
      handleProcessingTimeBasedTimers(key, expiredTimerInfo.getExpiryTimeInMs())
    } else {
      val currCount = _countState.getOption().getOrElse(0L)
      val count = currCount + inputRows.size
      processUnexpiredRows(key, currCount, count, timerValues)
      Iterator((key, count.toString))
    }
  }
}

class RunningCountStatefulProcessorWithMultipleTimers
  extends RunningCountStatefulProcessor {
  private def handleProcessingTimeBasedTimers(
      key: String,
      expiryTimestampMs: Long): Iterator[(String, String)] = {
    val currCount = _countState.getOption().getOrElse(0L)
    if (getHandle.listTimers().size == 1) {
      _countState.clear()
    }
    Iterator((key, currCount.toString))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    if (expiredTimerInfo.isValid()) {
      handleProcessingTimeBasedTimers(key, expiredTimerInfo.getExpiryTimeInMs())
    } else {
      val currCount = _countState.getOption().getOrElse(0L)
      val count = currCount + inputRows.size
      _countState.update(count)
      if (getHandle.listTimers().isEmpty) {
        getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 5000)
        getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 10000)
        getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 15000)
        assert(getHandle.listTimers().size == 3)
      }
      Iterator.empty
    }
  }
}

class MaxEventTimeStatefulProcessor
  extends StatefulProcessor[String, (String, Long), (String, Int)]
  with Logging {
  @transient var _maxEventTimeState: ValueState[Long] = _
  @transient var _timerState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _maxEventTimeState = getHandle.getValueState[Long]("maxEventTimeState",
      Encoders.scalaLong)
    _timerState = getHandle.getValueState[Long]("timerState", Encoders.scalaLong)
  }

  protected def processUnexpiredRows(maxEventTimeSec: Long): Unit = {
    val timeoutDelaySec = 5
    val timeoutTimestampMs = (maxEventTimeSec + timeoutDelaySec) * 1000
    _maxEventTimeState.update(maxEventTimeSec)

    val registeredTimerMs: Long = _timerState.getOption().getOrElse(0L)
    if (registeredTimerMs < timeoutTimestampMs) {
      getHandle.deleteTimer(registeredTimerMs)
      getHandle.registerTimer(timeoutTimestampMs)
      _timerState.update(timeoutTimestampMs)
    }
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Long)],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, Int)] = {
    if (expiredTimerInfo.isValid()) {
      _maxEventTimeState.clear()
      Iterator((key, -1))
    } else {
      val valuesSeq = inputRows.toSeq
      val maxEventTimeSec = math.max(valuesSeq.map(_._2).max,
        _maxEventTimeState.getOption().getOrElse(0L))
      processUnexpiredRows(maxEventTimeSec)
      Iterator((key, maxEventTimeSec.toInt))
    }
  }
}

class RunningCountMostRecentStatefulProcessor
  extends StatefulProcessor[String, (String, String), (String, String, String)]
  with Logging {
  @transient private var _countState: ValueState[Long] = _
  @transient private var _mostRecent: ValueState[String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong)
    _mostRecent = getHandle.getValueState[String]("mostRecent", Encoders.STRING)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + 1
    val mostRecent = _mostRecent.getOption().getOrElse("")

    var output = List[(String, String, String)]()
    inputRows.foreach { row =>
      _mostRecent.update(row._2)
      _countState.update(count)
      output = (key, count.toString, mostRecent) :: output
    }
    output.iterator
  }
}

class MostRecentStatefulProcessorWithDeletion
  extends StatefulProcessor[String, (String, String), (String, String)]
  with Logging {
  @transient private var _mostRecent: ValueState[String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    getHandle.deleteIfExists("countState")
    _mostRecent = getHandle.getValueState[String]("mostRecent", Encoders.STRING)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    val mostRecent = _mostRecent.getOption().getOrElse("")

    var output = List[(String, String)]()
    inputRows.foreach { row =>
      _mostRecent.update(row._2)
      output = (key, mostRecent) :: output
    }
    output.iterator
  }
}

// Class to verify incorrect usage of stateful processor
class RunningCountStatefulProcessorWithError extends RunningCountStatefulProcessor {
  @transient private var _tempState: ValueState[Long] = _

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    // Trying to create value state here should fail
    _tempState = getHandle.getValueState[Long]("tempState", Encoders.scalaLong)
    Iterator.empty
  }
}

// class for verify state schema is correctly written for all state var types
class StatefulProcessorWithCompositeTypes extends RunningCountStatefulProcessor {
  @transient private var _listState: ListState[TestClass] = _
  @transient private var _mapState: MapState[POJOTestClass, String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong)
    _listState = getHandle.getListState[TestClass](
      "listState", Encoders.product[TestClass])
    _mapState = getHandle.getMapState[POJOTestClass, String](
      "mapState", Encoders.bean(classOf[POJOTestClass]), Encoders.STRING)
  }
}

/**
 * Class that adds tests for transformWithState stateful streaming operator
 */
class TransformWithStateSuite extends StateStoreMetricsTest
  with AlsoTestWithChangelogCheckpointingEnabled {

  import testImplicits._

  test("transformWithState - streaming with rocksdb and invalid processor should fail") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
      TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessorWithError(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[StatefulProcessorCannotPerformOperationWithInvalidHandleState] { t =>
          assert(t.getMessage.contains("invalid handle state"))
        }
      )
    }
  }

  test("transformWithState - streaming with rocksdb should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
      TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        CheckNewAnswer(("a", "1")),
        Execute { q =>
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numValueStateVars") > 0)
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numRegisteredTimers") == 0)
        },
        AddData(inputData, "a", "b"),
        CheckNewAnswer(("a", "2"), ("b", "1")),
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
  }

  test("transformWithState - streaming with rocksdb and processing time timer " +
   "should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessorWithProcTimeTimer(),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("a", "1")),
        Execute { q =>
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numValueStateVars") > 0)
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numRegisteredTimers") === 1)
        },
        AddData(inputData, "b"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("b", "1")),

        AddData(inputData, "b"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("a", "-1"), ("b", "2")),

        StopStream,
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "b"),
        AddData(inputData, "c"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("c", "1")), // should remove 'b' as count reaches 3

        AddData(inputData, "d"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("c", "-1"), ("d", "1")),
        StopStream
      )
    }
  }

  test("transformWithState - streaming with rocksdb and processing time timer " +
   "and updating timers should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(
          new RunningCountStatefulProcessorWithProcTimeTimerUpdates(),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("a", "1")), // at batch 0, ts = 1, timer = "a" -> [6] (= 1 + 5)

        AddData(inputData, "a"),
        AdvanceManualClock(2 * 1000),
        CheckNewAnswer(("a", "2")), // at batch 1, ts = 3, timer = "a" -> [9.5] (2 + 7.5)
        StopStream,

        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "d"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("a", "-1"), ("d", "1")), // at batch 2, ts = 13, timer for "a" is expired.
        // If the timer of "a" was not replaced (pure addition), it would have triggered the timer
        // two times here and produced ("a", "-1") two times.
        StopStream
      )
    }
  }

  test("transformWithState - streaming with rocksdb and processing time timer " +
   "and multiple timers should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(
          new RunningCountStatefulProcessorWithMultipleTimers(),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000), // at batch 0, add 3 timers for given key = "a"

        AddData(inputData, "a"),
        AdvanceManualClock(6 * 1000),
        CheckNewAnswer(("a", "2")), // at ts = 7, first timer expires and produces ("a", "2")

        AddData(inputData, "a"),
        AdvanceManualClock(5 * 1000),
        CheckNewAnswer(("a", "3")), // at ts = 12, second timer expires and produces ("a", "3")
        StopStream,

        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(5 * 1000),
        CheckNewAnswer(("a", "4")), // at ts = 17, third timer expires and produces ("a", "4")
        StopStream
      )
    }
  }

  test("transformWithState - streaming with rocksdb and event time based timer") {
    val inputData = MemoryStream[(String, Int)]
    val result =
      inputData.toDS()
        .select($"_1".as("key"), timestamp_seconds($"_2").as("eventTime"))
        .withWatermark("eventTime", "10 seconds")
        .as[(String, Long)]
        .groupByKey(_._1)
        .transformWithState(
          new MaxEventTimeStatefulProcessor(),
          TimeMode.EventTime(),
          OutputMode.Update())

    testStream(result, OutputMode.Update())(
      StartStream(),

      AddData(inputData, ("a", 11), ("a", 13), ("a", 15)),
      // Max event time = 15. Timeout timestamp for "a" = 15 + 5 = 20. Watermark = 15 - 10 = 5.
      CheckNewAnswer(("a", 15)), // Output = max event time of a

      AddData(inputData, ("a", 4)), // Add data older than watermark for "a"
      CheckNewAnswer(), // No output as data should get filtered by watermark

      AddData(inputData, ("a", 10)), // Add data newer than watermark for "a"
      CheckNewAnswer(("a", 15)), // Max event time is still the same
      // Timeout timestamp for "a" is still 20 as max event time for "a" is still 15.
      // Watermark is still 5 as max event time for all data is still 15.

      AddData(inputData, ("b", 31)), // Add data newer than watermark for "b", not "a"
      // Watermark = 31 - 10 = 21, so "a" should be timed out as timeout timestamp for "a" is 20.
      CheckNewAnswer(("a", -1), ("b", 31)), // State for "a" should timeout and emit -1
      Execute { q =>
        // Filter for idle progress events and then verify the custom metrics for stateful operator
        val progData = q.recentProgress.filter(prog => prog.stateOperators.size > 0)
        assert(progData.filter(prog =>
          prog.stateOperators(0).customMetrics.get("numValueStateVars") > 0).size > 0)
        assert(progData.filter(prog =>
          prog.stateOperators(0).customMetrics.get("numRegisteredTimers") > 0).size > 0)
        assert(progData.filter(prog =>
          prog.stateOperators(0).customMetrics.get("numDeletedTimers") > 0).size > 0)
      }
    )
  }

  test("Use statefulProcessor without transformWithState - handle should be absent") {
    val processor = new RunningCountStatefulProcessor()
    val ex = intercept[Exception] {
      processor.getHandle
    }
    checkError(
      ex.asInstanceOf[SparkRuntimeException],
      errorClass = "STATE_STORE_HANDLE_NOT_INITIALIZED",
      parameters = Map.empty
    )
  }

  test("transformWithState - batch should succeed") {
    val inputData = Seq("a", "b")
    val result = inputData.toDS()
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(),
        TimeMode.None(),
        OutputMode.Append())

    val df = result.toDF()
    checkAnswer(df, Seq(("a", "1"), ("b", "1")).toDF())
  }

  // TODO SPARK-48796 after restart state id will not be the same
  ignore("transformWithState - test deleteIfExists operator") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { chkptDir =>
        val dirPath = chkptDir.getCanonicalPath
        val inputData = MemoryStream[(String, String)]
        val stream1 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new RunningCountMostRecentStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        val stream2 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new MostRecentStatefulProcessorWithDeletion(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(stream1, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "1", "")),
          StopStream
        )
        testStream(stream2, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, ("a", "str2"), ("b", "str3")),
          CheckNewAnswer(("a", "str1"),
            ("b", "")), // should not factor in previous count state
          Execute { q =>
            assert(q.lastProgress.stateOperators(0).customMetrics.get("numValueStateVars") > 0)
            assert(q.lastProgress.stateOperators(0).customMetrics.get("numDeletedStateVars") > 0)
          },
          StopStream
        )
      }
    }
  }

  test("transformWithState - two input streams") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData1 = MemoryStream[String]
      val inputData2 = MemoryStream[String]

      val result = inputData1.toDS()
        .union(inputData2.toDS())
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData1, "a"),
        CheckNewAnswer(("a", "1")),
        AddData(inputData2, "a", "b"),
        CheckNewAnswer(("a", "2"), ("b", "1")),
        AddData(inputData1, "a", "b"), // should remove state for "a" and not return anything for a
        CheckNewAnswer(("b", "2")),
        AddData(inputData1, "d", "e"),
        AddData(inputData2, "a", "c"), // should recreate state for "a" and return count as 1
        CheckNewAnswer(("a", "1"), ("c", "1"), ("d", "1"), ("e", "1")),
        StopStream
      )
    }
  }

  test("transformWithState - three input streams") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData1 = MemoryStream[String]
      val inputData2 = MemoryStream[String]
      val inputData3 = MemoryStream[String]

      // union 3 input streams
      val result = inputData1.toDS()
        .union(inputData2.toDS())
        .union(inputData3.toDS())
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData1, "a"),
        CheckNewAnswer(("a", "1")),
        AddData(inputData2, "a", "b"),
        CheckNewAnswer(("a", "2"), ("b", "1")),
        AddData(inputData3, "a", "b"), // should remove state for "a" and not return anything for a
        CheckNewAnswer(("b", "2")),
        AddData(inputData1, "d", "e"),
        AddData(inputData2, "a", "c"), // should recreate state for "a" and return count as 1
        CheckNewAnswer(("a", "1"), ("c", "1"), ("d", "1"), ("e", "1")),
        AddData(inputData3, "a", "c", "d", "e"),
        CheckNewAnswer(("a", "2"), ("c", "2"), ("d", "2"), ("e", "2")),
        StopStream
      )
    }
  }

  test("transformWithState - two input streams, different key type") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData1 = MemoryStream[String]
      val inputData2 = MemoryStream[Long]

      val result = inputData1.toDS()
        // union inputData2 by casting it to a String
        .union(inputData2.toDS().map(_.toString))
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData1, "1"),
        CheckNewAnswer(("1", "1")),
        AddData(inputData2, 1L, 2L),
        CheckNewAnswer(("1", "2"), ("2", "1")),
        AddData(inputData1, "1", "2"), // should remove state for "1" and not return anything.
        CheckNewAnswer(("2", "2")),
        AddData(inputData1, "4", "5"),
        AddData(inputData2, 1L, 3L), // should recreate state for "1" and return count as 1
        CheckNewAnswer(("1", "1"), ("3", "1"), ("4", "1"), ("5", "1")),
        StopStream
      )
    }
  }

  /** Create a text file with a single data item */
  private def createFile(data: String, srcDir: File): File =
    stringToFile(new File(srcDir, s"${UUID.randomUUID()}.txt"), data)

  private def createFileStream(srcDir: File): Dataset[(String, String)] = {
    spark
      .readStream
      .option("maxFilesPerTrigger", "1")
      .text(srcDir.getCanonicalPath)
      .select("value").as[String]
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(),
        TimeMode.None(),
        OutputMode.Update())
  }

  test("transformWithState - availableNow trigger mode, rate limit is respected") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { srcDir =>

        Seq("a", "b", "c").foreach(createFile(_, srcDir))

        // Set up a query to read text files one at a time
        val df = createFileStream(srcDir)

        testStream(df)(
          StartStream(trigger = Trigger.AvailableNow()),
          ProcessAllAvailable(),
          CheckNewAnswer(("a", "1"), ("b", "1"), ("c", "1")),
          StopStream,
          Execute { _ =>
            createFile("a", srcDir)
          },
          StartStream(trigger = Trigger.AvailableNow()),
          ProcessAllAvailable(),
          CheckNewAnswer(("a", "2"))
        )

        var index = 0
        val foreachBatchDf = df.writeStream
          .foreachBatch((_: Dataset[(String, String)], _: Long) => {
            index += 1
          })
          .trigger(Trigger.AvailableNow())
          .start()

        try {
          foreachBatchDf.awaitTermination()
          assert(index == 4)
        } finally {
          foreachBatchDf.stop()
        }
      }
    }
  }

  test("transformWithState - availableNow trigger mode, multiple restarts") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { srcDir =>
        Seq("a", "b", "c").foreach(createFile(_, srcDir))
        val df = createFileStream(srcDir)

        var index = 0

        def startTriggerAvailableNowQueryAndCheck(expectedIdx: Int): Unit = {
          val q = df.writeStream
            .foreachBatch((_: Dataset[(String, String)], _: Long) => {
              index += 1
            })
            .trigger(Trigger.AvailableNow)
            .start()
          try {
            assert(q.awaitTermination(streamingTimeout.toMillis))
            assert(index == expectedIdx)
          } finally {
            q.stop()
          }
        }
        // start query for the first time
        startTriggerAvailableNowQueryAndCheck(3)

        // add two files and restart
        createFile("a", srcDir)
        createFile("b", srcDir)
        startTriggerAvailableNowQueryAndCheck(8)

        // try restart again
        createFile("d", srcDir)
        startTriggerAvailableNowQueryAndCheck(14)
      }
    }
  }

  test("transformWithState - verify StateSchemaV3 writes correct SQL schema of key/value") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val metadataPathPostfix = "state/0/_stateSchema/default"
        val stateSchemaPath = new Path(checkpointDir.toString,
          s"$metadataPathPostfix")
        val hadoopConf = spark.sessionState.newHadoopConf()
        val fm = CheckpointFileManager.create(stateSchemaPath, hadoopConf)

        val keySchema = new StructType().add("value", StringType)
        val schema0 = StateStoreColFamilySchema(
          "countState",
          keySchema,
          new StructType().add("value", LongType, false),
          Some(NoPrefixKeyStateEncoderSpec(keySchema)),
          None
        )
        val schema1 = StateStoreColFamilySchema(
          "listState",
          keySchema,
          new StructType()
              .add("id", LongType, false)
              .add("name", StringType),
          Some(NoPrefixKeyStateEncoderSpec(keySchema)),
          None
        )

        val userKeySchema = new StructType()
          .add("id", IntegerType, false)
          .add("name", StringType)
        val compositeKeySchema = new StructType()
          .add("key", new StructType().add("value", StringType))
          .add("userKey", userKeySchema)
        val schema2 = StateStoreColFamilySchema(
          "mapState",
          compositeKeySchema,
          new StructType().add("value", StringType),
          Some(PrefixKeyScanStateEncoderSpec(compositeKeySchema, 1)),
          Option(userKeySchema)
        )

        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new StatefulProcessorWithCompositeTypes(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a", "b"),
          CheckNewAnswer(("a", "1"), ("b", "1")),
          Execute { q =>
            q.lastProgress.runId
            val schemaFilePath = fm.list(stateSchemaPath).toSeq.head.getPath
            val providerId = StateStoreProviderId(StateStoreId(
              checkpointDir.getCanonicalPath, 0, 0), q.lastProgress.runId)
            val checker = new StateSchemaCompatibilityChecker(providerId,
              hadoopConf, Some(schemaFilePath))
            val colFamilySeq = checker.readSchemaFile()

            assert(TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS ==
              q.lastProgress.stateOperators.head.customMetrics.get("numValueStateVars").toInt)
            assert(TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS ==
              q.lastProgress.stateOperators.head.customMetrics.get("numListStateVars").toInt)
            assert(TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS ==
              q.lastProgress.stateOperators.head.customMetrics.get("numMapStateVars").toInt)

            assert(colFamilySeq.length == 3)
            assert(colFamilySeq.map(_.toString).toSet == Set(
              schema0, schema1, schema2
            ).map(_.toString))
          },
          StopStream
        )
      }
    }
  }

  test("transformWithState - verify that OperatorStateMetadataV2" +
    " file is being written correctly") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream,
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "2")),
          StopStream
        )

        val df = spark.read.format("state-metadata").load(checkpointDir.toString)

        // check first 6 columns of the row, and then read the last column of the row separately
        checkAnswer(
            df.select(
              "operatorId", "operatorName", "stateStoreName", "numPartitions", "minBatchId",
              "maxBatchId"),
            Seq(Row(0, "transformWithStateExec", "default", 5, 0, 1))
        )
        val operatorPropsJson = df.select("operatorProperties").collect().head.getString(0)
        val operatorProperties = TransformWithStateOperatorProperties.fromJson(operatorPropsJson)
        assert(operatorProperties.timeMode == "NoTime")
        assert(operatorProperties.outputMode == "Update")
        assert(operatorProperties.stateVariables.length == 1)
        assert(operatorProperties.stateVariables.head.stateName == "countState")
      }
    }
  }

  test("test that invalid schema evolution fails query for column family") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorInt(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          ExpectFailure[StateStoreValueSchemaNotCompatible] {
            (t: Throwable) => {
              assert(t.getMessage.contains("Please check number and type of fields."))
            }
          }
        )
      }
    }
  }

  test("test that different outputMode after query restart fails") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Append())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          ExpectFailure[StateStoreInvalidConfigAfterRestart] { e =>
            checkError(
              e.asInstanceOf[SparkUnsupportedOperationException],
              errorClass = "STATE_STORE_INVALID_CONFIG_AFTER_RESTART",
              parameters = Map(
                "configName" -> "outputMode",
                "oldConfig" -> "Update",
                "newConfig" -> "Append")
            )
          }
        )
      }
    }
  }

  test("test that changing between different state variable types fails") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountListStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          ExpectFailure[StateStoreInvalidVariableTypeChange] { t =>
            checkError(
              t.asInstanceOf[SparkUnsupportedOperationException],
              errorClass = "STATE_STORE_INVALID_VARIABLE_TYPE_CHANGE",
              parameters = Map(
                "stateVarName" -> "countState",
                "newType" -> "ListState",
                "oldType" -> "ValueState")
            )
          }
        )
      }
    }
  }

  test("test that different timeMode after query restart fails") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val clock = new StreamManualClock
        val inputData = MemoryStream[String]
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
        testStream(result2, OutputMode.Update())(
          StartStream(
            checkpointLocation = checkpointDir.getCanonicalPath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData, "a"),
          AdvanceManualClock(1 * 1000),
          ExpectFailure[StateStoreInvalidConfigAfterRestart] { e =>
            checkError(
              e.asInstanceOf[SparkUnsupportedOperationException],
              errorClass = "STATE_STORE_INVALID_CONFIG_AFTER_RESTART",
              parameters = Map(
                "configName" -> "timeMode",
                "oldConfig" -> "NoTime",
                "newConfig" -> "ProcessingTime")
            )
          }
        )
      }
    }
  }

  test("test that introducing TTL after restart fails query") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val clock = new StreamManualClock
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(
            trigger = Trigger.ProcessingTime("1 second"),
            checkpointLocation = checkpointDir.getCanonicalPath,
            triggerClock = clock),
          AddData(inputData, "a"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "1")),
          AdvanceManualClock(1 * 1000),
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorWithTTL(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
        testStream(result2, OutputMode.Update())(
          StartStream(
            trigger = Trigger.ProcessingTime("1 second"),
            checkpointLocation = checkpointDir.getCanonicalPath,
            triggerClock = clock),
          AddData(inputData, "a"),
          AdvanceManualClock(1 * 1000),
          ExpectFailure[StateStoreValueSchemaNotCompatible] { t =>
            checkError(
              t.asInstanceOf[SparkUnsupportedOperationException],
              errorClass = "STATE_STORE_VALUE_SCHEMA_NOT_COMPATIBLE",
              parameters = Map(
                "storedValueSchema" -> "StructType(StructField(value,LongType,false))",
                "newValueSchema" ->
                  ("StructType(StructField(value,StructType(StructField(value,LongType,false))," +
                    "true),StructField(ttlExpirationMs,LongType,true))")
              )
            )
          }
        )
      }
    }
  }

  test("SPARK-49070: transformWithState - valid initial state plan") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { srcDir =>
        Seq("a", "b", "c").foreach(createFile(_, srcDir))
        val df = createFileStream(srcDir)

        var index = 0

        val q = df.writeStream
          .foreachBatch((_: Dataset[(String, String)], _: Long) => {
            index += 1
          })
          .trigger(Trigger.AvailableNow)
          .start()

        try {
          assert(q.awaitTermination(streamingTimeout.toMillis))

          val sparkPlan =
            q.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
          val transformWithStateExec = sparkPlan.collect {
            case p: TransformWithStateExec => p
          }.head

          assert(!transformWithStateExec.hasInitialState)

          // EnsureRequirements should not apply on the initial state plan
          val exchange = transformWithStateExec.initialState.collect {
            case s: ShuffleExchangeExec => s
          }

          assert(exchange.isEmpty)
        } finally {
          q.stop()
        }
      }
    }
  }
}

class TransformWithStateValidationSuite extends StateStoreMetricsTest {
  import testImplicits._

  test("transformWithState - streaming with hdfsStateStoreProvider should fail") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS()
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(),
        TimeMode.None(),
        OutputMode.Update())

    testStream(result, OutputMode.Update())(
      AddData(inputData, "a"),
      ExpectFailure[StateStoreMultipleColumnFamiliesNotSupportedException] { t =>
        assert(t.getMessage.contains("not supported"))
      }
    )
  }

  test("transformWithStateWithInitialState - streaming with hdfsStateStoreProvider should fail") {
    val inputData = MemoryStream[InitInputRow]
    val initDf = Seq(("init_1", 40.0), ("init_2", 100.0)).toDS()
      .groupByKey(x => x._1)
      .mapValues(x => x)
    val result = inputData.toDS()
      .groupByKey(x => x.key)
      .transformWithState(new AccumulateStatefulProcessorWithInitState(),
        TimeMode.None(), OutputMode.Append(), initDf
      )
    testStream(result, OutputMode.Update())(
      AddData(inputData, InitInputRow("a", "add", -1.0)),
      ExpectFailure[StateStoreMultipleColumnFamiliesNotSupportedException] {
        (t: Throwable) => {
          assert(t.getMessage.contains("not supported"))
        }
      }
    )
  }
}
