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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.{Dataset, Encoders, KeyValueGroupedDataset}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{AlsoTestWithChangelogCheckpointingEnabled, RocksDBStateStoreProvider}
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

case class InitInputRow(key: String, action: String, value: Double)
case class InputRowForInitialState(
    key: String, value: Double, entries: List[Double], mapping: Map[Double, Int])

abstract class StatefulProcessorWithInitialStateTestClass[V]
    extends StatefulProcessorWithInitialState[
        String, InitInputRow, (String, String, Double), V] {
  @transient var _valState: ValueState[Double] = _
  @transient var _listState: ListState[Double] = _
  @transient var _mapState: MapState[Double, Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _valState = getHandle.getValueState[Double]("testValueInit", Encoders.scalaDouble)
    _listState = getHandle.getListState[Double]("testListInit", Encoders.scalaDouble)
    _mapState = getHandle.getMapState[Double, Int](
      "testMapInit", Encoders.scalaDouble, Encoders.scalaInt)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InitInputRow],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String, Double)] = {
    var output = List[(String, String, Double)]()
    for (row <- inputRows) {
      if (row.action == "getOption") {
        output = (key, row.action, _valState.getOption().getOrElse(-1.0)) :: output
      } else if (row.action == "update") {
        _valState.update(row.value)
      } else if (row.action == "remove") {
        _valState.clear()
      } else if (row.action == "getList") {
        _listState.get().foreach { element =>
          output = (key, row.action, element) :: output
        }
      } else if (row.action == "appendList") {
        _listState.appendValue(row.value)
      } else if (row.action == "clearList") {
        _listState.clear()
      } else if (row.action == "getCount") {
        val count =
          if (!_mapState.containsKey(row.value)) 0
          else _mapState.getValue(row.value)
        output = (key, row.action, count.toDouble) :: output
      } else if (row.action == "incCount") {
        val count =
          if (!_mapState.containsKey(row.value)) 0
          else _mapState.getValue(row.value)
        _mapState.updateValue(row.value, count + 1)
      } else if (row.action == "clearCount") {
        _mapState.removeKey(row.value)
      }
    }
    output.iterator
  }
}

class AccumulateStatefulProcessorWithInitState
    extends StatefulProcessorWithInitialStateTestClass[(String, Double)] {
  override def handleInitialState(
      key: String,
      initialState: (String, Double),
      timerValues: TimerValues): Unit = {
    _valState.update(initialState._2)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InitInputRow],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String, Double)] = {
    var output = List[(String, String, Double)]()
    for (row <- inputRows) {
      if (row.action == "getOption") {
        output = (key, row.action, _valState.getOption().getOrElse(0.0)) :: output
      } else if (row.action == "add") {
        // Update state variable as accumulative sum
        val accumulateSum = _valState.getOption().getOrElse(0.0) + row.value
        _valState.update(accumulateSum)
      } else if (row.action == "remove") {
        _valState.clear()
      }
    }
    output.iterator
  }
}

class InitialStateInMemoryTestClass
  extends StatefulProcessorWithInitialStateTestClass[InputRowForInitialState] {
  override def handleInitialState(
      key: String,
      initialState: InputRowForInitialState,
      timerValues: TimerValues): Unit = {
    _valState.update(initialState.value)
    _listState.appendList(initialState.entries.toArray)
    val inMemoryMap = initialState.mapping
    inMemoryMap.foreach { kvPair =>
      _mapState.updateValue(kvPair._1, kvPair._2)
    }
  }
}

/**
 * Class to test stateful processor with initial state and processing timers.
 * Timers can be registered during initial state handling and output rows can be
 * emitted after timer expires even if the grouping key in initial state is not
 * seen in new input rows.
 */
class StatefulProcessorWithInitialStateProcTimerClass
  extends StatefulProcessorWithInitialState[String, String, (String, String), String] {
  @transient private var _timerState: ValueState[Long] = _
  @transient protected var _countState: ValueState[Long] = _

  private def handleProcessingTimeBasedTimers(
      key: String,
      expiryTimestampMs: Long): Iterator[(String, String)] = {
    _timerState.clear()
    Iterator((key, "-1"))
  }

  private def processUnexpiredRows(
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

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode) : Unit = {
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong)
    _timerState = getHandle.getValueState[Long]("timerState", Encoders.scalaLong)
  }

  override def handleInitialState(
      key: String,
      initialState: String,
      timerValues: TimerValues): Unit = {
    // keep a _countState to count the occurrence of grouping key
    // Will register a timer if key "a" is met for the first time
    processUnexpiredRows(key, 0L, 1L, timerValues)
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

/**
 * Class to test stateful processor with initial state and event timers.
 * Timers can be registered during initial state handling and output rows can be
 * emitted after timer expires even if the grouping key in initial state is not
 * seen in new input rows.
 */
class StatefulProcessorWithInitialStateEventTimerClass
  extends StatefulProcessorWithInitialState[String, (String, Long), (String, Int), (String, Long)] {
  @transient var _maxEventTimeState: ValueState[Long] = _
  @transient var _timerState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _maxEventTimeState = getHandle.getValueState[Long]("maxEventTimeState",
      Encoders.scalaLong)
    _timerState = getHandle.getValueState[Long]("timerState", Encoders.scalaLong)
  }

  private def processUnexpiredRows(maxEventTimeSec: Long): Unit = {
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

  override def handleInitialState(
      key: String,
      initialState: (String, Long),
      timerValues: TimerValues): Unit = {
    // keep a _maxEventTimeState to track the max eventTime seen so far
    // register a timer if bigger eventTime is seen
    val maxEventTimeSec = math.max(initialState._2,
      _maxEventTimeState.getOption().getOrElse(0L))
    processUnexpiredRows(maxEventTimeSec)
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

/**
 * Class that adds tests for transformWithState stateful
 * streaming operator with user-defined initial state
 */
class TransformWithStateInitialStateSuite extends StateStoreMetricsTest
  with AlsoTestWithChangelogCheckpointingEnabled {

  import testImplicits._

  private def createInitialDfForTest: KeyValueGroupedDataset[String, (String, Double)] = {
    Seq(("init_1", 40.0), ("init_2", 100.0)).toDS()
      .groupByKey(x => x._1)
      .mapValues(x => x)
  }


  test("transformWithStateWithInitialState - correctness test, " +
    "run with multiple state variables - in-memory type") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InitInputRow]
      val kvDataSet = inputData.toDS()
        .groupByKey(x => x.key)
      val initStateDf =
        Seq(InputRowForInitialState("init_1", 40.0, List(40.0), Map(40.0 -> 1)),
          InputRowForInitialState("init_2", 100.0, List(100.0), Map(100.0 -> 1)))
          .toDS().groupByKey(x => x.key).mapValues(x => x)
      val query = kvDataSet.transformWithState(new InitialStateInMemoryTestClass(),
        TimeMode.None(), OutputMode.Append(), initStateDf)

      testStream(query, OutputMode.Update())(
        // non-exist key test
        AddData(inputData, InitInputRow("k1", "update", 37.0)),
        AddData(inputData, InitInputRow("k2", "update", 40.0)),
        AddData(inputData, InitInputRow("non-exist", "getOption", -1.0)),
        CheckNewAnswer(("non-exist", "getOption", -1.0)),
        AddData(inputData, InitInputRow("k1", "appendList", 37.0)),
        AddData(inputData, InitInputRow("k2", "appendList", 40.0)),
        AddData(inputData, InitInputRow("non-exist", "getList", -1.0)),
        CheckNewAnswer(),

        AddData(inputData, InitInputRow("k1", "incCount", 37.0)),
        AddData(inputData, InitInputRow("k2", "incCount", 40.0)),
        AddData(inputData, InitInputRow("non-exist", "getCount", -1.0)),
        CheckNewAnswer(("non-exist", "getCount", 0.0)),
        AddData(inputData, InitInputRow("k2", "incCount", 40.0)),
        AddData(inputData, InitInputRow("k2", "getCount", 40.0)),
        CheckNewAnswer(("k2", "getCount", 2.0)),

        // test every row in initial State is processed
        AddData(inputData, InitInputRow("init_1", "getOption", -1.0)),
        CheckNewAnswer(("init_1", "getOption", 40.0)),
        AddData(inputData, InitInputRow("init_2", "getOption", -1.0)),
        CheckNewAnswer(("init_2", "getOption", 100.0)),

        AddData(inputData, InitInputRow("init_1", "getList", -1.0)),
        CheckNewAnswer(("init_1", "getList", 40.0)),
        AddData(inputData, InitInputRow("init_2", "getList", -1.0)),
        CheckNewAnswer(("init_2", "getList", 100.0)),

        AddData(inputData, InitInputRow("init_1", "getCount", 40.0)),
        CheckNewAnswer(("init_1", "getCount", 1.0)),
        AddData(inputData, InitInputRow("init_2", "getCount", 100.0)),
        CheckNewAnswer(("init_2", "getCount", 1.0)),

        // Update row with key in initial row will work
        AddData(inputData, InitInputRow("init_1", "update", 50.0)),
        AddData(inputData, InitInputRow("init_1", "getOption", -1.0)),
        CheckNewAnswer(("init_1", "getOption", 50.0)),
        AddData(inputData, InitInputRow("init_1", "remove", -1.0)),
        AddData(inputData, InitInputRow("init_1", "getOption", -1.0)),
        CheckNewAnswer(("init_1", "getOption", -1.0)),

        AddData(inputData, InitInputRow("init_1", "appendList", 50.0)),
        AddData(inputData, InitInputRow("init_1", "getList", -1.0)),
        CheckNewAnswer(("init_1", "getList", 50.0), ("init_1", "getList", 40.0)),

        AddData(inputData, InitInputRow("init_1", "incCount", 40.0)),
        AddData(inputData, InitInputRow("init_1", "getCount", 40.0)),
        CheckNewAnswer(("init_1", "getCount", 2.0)),

        // test remove
        AddData(inputData, InitInputRow("k1", "remove", -1.0)),
        AddData(inputData, InitInputRow("k1", "getOption", -1.0)),
        CheckNewAnswer(("k1", "getOption", -1.0)),

        AddData(inputData, InitInputRow("init_1", "clearCount", -1.0)),
        AddData(inputData, InitInputRow("init_1", "getCount", -1.0)),
        CheckNewAnswer(("init_1", "getCount", 0.0)),

        AddData(inputData, InitInputRow("init_1", "clearList", -1.0)),
        AddData(inputData, InitInputRow("init_1", "getList", -1.0)),
        CheckNewAnswer()
      )
    }
  }

  test("transformWithStateWithInitialState -" +
    " correctness test, processInitialState should only run once") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val initStateDf = createInitialDfForTest
      val inputData = MemoryStream[InitInputRow]
      val query = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new AccumulateStatefulProcessorWithInitState(),
          TimeMode.None(), OutputMode.Append(), initStateDf
        )
      testStream(query, OutputMode.Update())(
        AddData(inputData, InitInputRow("init_1", "add", 50.0)),
        AddData(inputData, InitInputRow("init_2", "add", 60.0)),
        AddData(inputData, InitInputRow("init_1", "add", 50.0)),
        // If processInitialState was processed multiple times,
        // following checks will fail
        AddData(inputData,
          InitInputRow("init_1", "getOption", -1.0), InitInputRow("init_2", "getOption", -1.0)),
        CheckNewAnswer(("init_2", "getOption", 160.0), ("init_1", "getOption", 140.0))
      )
    }
  }

  test("transformWithStateWithInitialState - batch should succeed") {
    val inputData = Seq(InitInputRow("k1", "add", 37.0), InitInputRow("k1", "getOption", -1.0))
    val result = inputData.toDS()
      .groupByKey(x => x.key)
      .transformWithState(new AccumulateStatefulProcessorWithInitState(),
        TimeMode.None(),
        OutputMode.Append(),
        createInitialDfForTest)

    val df = result.toDF()
    checkAnswer(df, Seq(("k1", "getOption", 37.0)).toDF())
  }

  test("transformWithStateWithInitialState - " +
    "cannot re-initialize state during initial state handling") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val initDf = Seq(("init_1", 40.0), ("init_2", 100.0), ("init_1", 50.0)).toDS()
        .groupByKey(x => x._1).mapValues(x => x)
      val inputData = MemoryStream[InitInputRow]
      val query = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new AccumulateStatefulProcessorWithInitState(),
          TimeMode.None(),
          OutputMode.Append(),
          initDf)

      testStream(query, OutputMode.Update())(
        AddData(inputData, InitInputRow("k1", "add", 50.0)),
        Execute { q =>
          val e = intercept[Exception] {
            q.processAllAvailable()
          }
          checkError(
            exception = e.getCause.asInstanceOf[SparkUnsupportedOperationException],
            errorClass = "STATEFUL_PROCESSOR_CANNOT_REINITIALIZE_STATE_ON_KEY",
            sqlState = Some("42802"),
            parameters = Map("groupingKey" -> "init_1")
          )
        }
      )
    }
  }

  test("transformWithStateWithInitialState - streaming with processing time timer, " +
    "can emit expired initial state rows when grouping key is not received for new input rows") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      val initDf = Seq("a", "b").toDS().groupByKey(x => x)
      val inputData = MemoryStream[String]
      val result = inputData.toDS().groupByKey(x => x)
        .transformWithState(
          new StatefulProcessorWithInitialStateProcTimerClass(),
          TimeMode.ProcessingTime(),
          OutputMode.Update(),
          initDf)

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "c"),
        AdvanceManualClock(1 * 1000),
        // registered timer for "a" and "b" is 6000, first batch is processed at ts = 1000
        CheckNewAnswer(("c", "1")),

        AddData(inputData, "c"),
        AdvanceManualClock(6 * 1000), // ts = 7000, "a" expires
        // "a" is never received for new input rows, but emit here because it expires
        CheckNewAnswer(("a", "-1"), ("c", "2")),

        AddData(inputData, "a", "a"),
        AdvanceManualClock(1 * 1000), // ts = 8000, register timer for "a" as ts = 15500
        CheckNewAnswer(("a", "3")),

        AddData(inputData, "b"),
        AdvanceManualClock(8 * 1000), // ts = 16000, timer for "a" expired
        CheckNewAnswer(("a", "-1"), ("b", "2")), // initial state for "b" is also processed
        StopStream
      )
    }
  }

  test("transformWithStateWithInitialState - streaming with event time timer, " +
    "can emit expired initial state rows when grouping key is not received for new input rows") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      def eventTimeDf(ds: Dataset[(String, Int)]): KeyValueGroupedDataset[String, (String, Long)] =
        ds.select($"_1".as("key"), timestamp_seconds($"_2").as("eventTime"))
          .withWatermark("eventTime", "10 seconds")
          .as[(String, Long)].groupByKey(_._1)

      val initDf = eventTimeDf(Seq(("a", 11), ("b", 5)).toDS())

      val inputData = MemoryStream[(String, Int)]
      val result = eventTimeDf(inputData.toDS())
        .transformWithState(
          new StatefulProcessorWithInitialStateEventTimerClass(),
          TimeMode.EventTime(),
          OutputMode.Update(),
          initDf)

      testStream(result, OutputMode.Update())(
        StartStream(),
        AddData(inputData, ("a", 13), ("a", 15)),
        CheckNewAnswer(("a", 15)), // Output = max event time of a

        AddData(inputData, ("c", 7)), // Add data older than watermark for "a" and "b"
        CheckNewAnswer(("c", 7)),

        AddData(inputData, ("d", 31)), // Add data newer than watermark
        // "b" is never received for new input rows, but emit here because it expired
        CheckNewAnswer(("a", -1), ("b", -1), ("c", -1), ("d", 31)),
        StopStream
      )
    }
  }
}
