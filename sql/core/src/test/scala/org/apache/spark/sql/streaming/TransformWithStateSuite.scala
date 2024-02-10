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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.{AlsoTestWithChangelogCheckpointingEnabled, RocksDBStateStoreProvider, StateStoreMultipleColumnFamiliesNotSupportedException}
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

object TransformWithStateSuiteUtils {
  val NUM_SHUFFLE_PARTITIONS = 5
}

class RunningCountStatefulProcessor extends StatefulProcessor[String, String, (String, String)]
  with Logging {
  @transient var _countState: ValueState[Long] = _
  @transient var _processorHandle: StatefulProcessorHandle = _

  override def init(
      handle: StatefulProcessorHandle,
      outputMode: OutputMode) : Unit = {
    _processorHandle = handle
    assert(handle.getQueryInfo().getBatchId >= 0)
    _countState = _processorHandle.getValueState[Long]("countState")
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + 1
    if (count == 3) {
      _countState.remove()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }

  override def close(): Unit = {}
}

// Class to verify stateful processor usage with adding processing time timers
class RunningCountStatefulProcessorWithProcTimeTimer extends RunningCountStatefulProcessor {
  private def handleProcessingTimeBasedTimers(
      key: String,
      expiryTimestampMs: Long): Iterator[(String, String)] = {
    _countState.remove()
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
        _processorHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs()
          + 5000)
      }

      val count = currCount + 1
      if (count == 3) {
        _countState.remove()
        Iterator.empty
      } else {
        _countState.update(count)
        Iterator((key, count.toString))
      }
    }
  }
}

// Class to verify stateful processor usage with adding/deleting processing time timers
class RunningCountStatefulProcessorWithAddRemoveProcTimeTimer
  extends RunningCountStatefulProcessor {
  @transient private var _timerState: ValueState[Long] = _

  override def init(
      handle: StatefulProcessorHandle,
      outputMode: OutputMode) : Unit = {
    super.init(handle, outputMode)
    _timerState = _processorHandle.getValueState[Long]("timerState")
  }

  private def handleProcessingTimeBasedTimers(
      key: String,
      expiryTimestampMs: Long): Iterator[(String, String)] = {
    _timerState.remove()
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
      val count = currCount + inputRows.size
      _countState.update(count)
      if (key == "a") {
        var nextTimerTs: Long = 0L
        if (currCount == 0) {
          nextTimerTs = timerValues.getCurrentProcessingTimeInMs() + 5000
          _processorHandle.registerTimer(nextTimerTs)
          _timerState.update(nextTimerTs)
        } else if (currCount == 1) {
          _processorHandle.deleteTimer(_timerState.get())
          nextTimerTs = timerValues.getCurrentProcessingTimeInMs() + 7500
          _processorHandle.registerTimer(nextTimerTs)
          _timerState.update(nextTimerTs)
        }
      }
      Iterator((key, count.toString))
    }
  }
}

class MaxEventTimeStatefulProcessor
  extends StatefulProcessor[String, (String, Long), (String, Int)]
  with Logging {
  @transient var _maxEventTimeState: ValueState[Long] = _
  @transient var _processorHandle: StatefulProcessorHandle = _
  @transient var _timerState: ValueState[Long] = _

  override def init(
      handle: StatefulProcessorHandle,
      outputMode: OutputMode): Unit = {
    _processorHandle = handle
    _maxEventTimeState = _processorHandle.getValueState[Long]("maxEventTimeState")
    _timerState = _processorHandle.getValueState[Long]("timerState")
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Long)],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, Int)] = {
    val timeoutDelaySec = 5
    if (expiredTimerInfo.isValid()) {
      _maxEventTimeState.remove()
      Iterator((key, -1))
    } else {
      val valuesSeq = inputRows.toSeq
      val maxEventTimeSec = math.max(valuesSeq.map(_._2).max,
        _maxEventTimeState.getOption().getOrElse(0L))
      val timeoutTimestampMs = (maxEventTimeSec + timeoutDelaySec) * 1000
      _maxEventTimeState.update(maxEventTimeSec)

      val registeredTimerMs: Long = _timerState.getOption().getOrElse(0L)
      if (registeredTimerMs < timeoutTimestampMs) {
        _processorHandle.deleteTimer(registeredTimerMs)
        _processorHandle.registerTimer(timeoutTimestampMs)
        _timerState.update(timeoutTimestampMs)
      }
      Iterator((key, maxEventTimeSec.toInt))
    }
  }
}

class RunningCountMostRecentStatefulProcessor
  extends StatefulProcessor[String, (String, String), (String, String, String)]
  with Logging {
  @transient private var _countState: ValueState[Long] = _
  @transient private var _mostRecent: ValueState[String] = _
  @transient var _processorHandle: StatefulProcessorHandle = _

  override def init(
      handle: StatefulProcessorHandle,
      outputMode: OutputMode) : Unit = {
    _processorHandle = handle
    assert(handle.getQueryInfo().getBatchId >= 0)
    _countState = _processorHandle.getValueState[Long]("countState")
    _mostRecent = _processorHandle.getValueState[String]("mostRecent")
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

  override def close(): Unit = {}
}

class MostRecentStatefulProcessorWithDeletion
  extends StatefulProcessor[String, (String, String), (String, String)]
  with Logging {
  @transient private var _mostRecent: ValueState[String] = _
  @transient var _processorHandle: StatefulProcessorHandle = _

  override def init(
       handle: StatefulProcessorHandle,
       outputMode: OutputMode) : Unit = {
    _processorHandle = handle
    assert(handle.getQueryInfo().getBatchId >= 0)
    _processorHandle.deleteIfExists("countState")
    _mostRecent = _processorHandle.getValueState[String]("mostRecent")
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

  override def close(): Unit = {}
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
    _tempState = _processorHandle.getValueState[Long]("tempState")
    Iterator.empty
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
          TimeoutMode.NoTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[SparkException] { t =>
          assert(t.getCause.getMessage.contains("Cannot create state variable"))
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
          TimeoutMode.NoTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        CheckNewAnswer(("a", "1")),
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
          TimeoutMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("a", "1")),

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
        CheckNewAnswer(("c", "1")),
        AddData(inputData, "d"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("c", "-1"), ("d", "1")),
        StopStream
      )
    }
  }

  test("transformWithState - streaming with rocksdb and processing time timer " +
   "and add/remove timers should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(
          new RunningCountStatefulProcessorWithAddRemoveProcTimeTimer(),
          TimeoutMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("a", "1")),

        AddData(inputData, "a"),
        AdvanceManualClock(2 * 1000),
        CheckNewAnswer(("a", "2")),
        StopStream,

        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "d"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("a", "-1"), ("d", "1")),
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
          TimeoutMode.EventTime(),
          OutputMode.Update())

    testStream(result, OutputMode.Update())(
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

  test("transformWithState - batch should succeed") {
    val inputData = Seq("a", "b")
    val result = inputData.toDS()
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(),
        TimeoutMode.NoTimeouts(),
        OutputMode.Append())

    val df = result.toDF()
    checkAnswer(df, Seq(("a", "1"), ("b", "1")).toDF())
  }

  test("transformWithState - test deleteIfExists operator") {
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
            TimeoutMode.NoTimeouts(),
            OutputMode.Update())

        val stream2 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new MostRecentStatefulProcessorWithDeletion(),
            TimeoutMode.NoTimeouts(),
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
          TimeoutMode.NoTimeouts(),
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
          TimeoutMode.NoTimeouts(),
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
          TimeoutMode.NoTimeouts(),
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
}

class TransformWithStateValidationSuite extends StateStoreMetricsTest {
  import testImplicits._

  test("transformWithState - streaming with hdfsStateStoreProvider should fail") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS()
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(),
        TimeoutMode.NoTimeouts(),
        OutputMode.Update())

    testStream(result, OutputMode.Update())(
      AddData(inputData, "a"),
      ExpectFailure[StateStoreMultipleColumnFamiliesNotSupportedException] { t =>
        assert(t.getMessage.contains("not supported"))
      }
    )
  }
}
