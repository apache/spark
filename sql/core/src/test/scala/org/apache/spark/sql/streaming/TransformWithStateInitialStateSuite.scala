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

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, KeyValueGroupedDataset}
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{AlsoTestWithRocksDBFeatures, RocksDBStateStoreProvider}
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

case class InitInputRow(key: String, action: String, value: Double)
case class InputRowForInitialState(
    key: String, value: Double, entries: List[Double], mapping: Map[Double, Int])

case class UnionInitialStateRow(
    groupingKey: String,
    value: Option[Long],
    listValue: Option[Long],
    userMapKey: Option[String],
    userMapValue: Option[Long]
)

case class UnionUnflattenInitialStateRow(
    groupingKey: String,
    value: Option[Long],
    listValue: Option[Seq[Long]],
    mapValue: Option[Map[String, Long]]
)

abstract class StatefulProcessorWithInitialStateTestClass[V]
    extends StatefulProcessorWithInitialState[
        String, InitInputRow, (String, String, Double), V] {
  import implicits._

  @transient var _valState: ValueState[Double] = _
  @transient var _listState: ListState[Double] = _
  @transient var _mapState: MapState[Double, Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _valState = getHandle.getValueState[Double]("testValueInit", TTLConfig.NONE)
    _listState = getHandle.getListState[Double]("testListInit", TTLConfig.NONE)
    _mapState = getHandle.getMapState[Double, Int]("testMapInit", TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InitInputRow],
      timerValues: TimerValues): Iterator[(String, String, Double)] = {
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

/**
 * Class that updates all state variables with input rows. Act as a counter -
 * keep the count in value state; keep all the occurrences in list state; and
 * keep a map of key -> occurrence count in the map state.
 */
abstract class InitialStateWithStateDataSourceBase[V]
  extends StatefulProcessorWithInitialState[
    String, String, (String, String), V] {
  @transient var _valState: ValueState[Long] = _
  @transient var _listState: ListState[Long] = _
  @transient var _mapState: MapState[String, Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _valState = getHandle.getValueState[Long]("testVal", Encoders.scalaLong, TTLConfig.NONE)
    _listState = getHandle.getListState[Long]("testList", Encoders.scalaLong, TTLConfig.NONE)
    _mapState = getHandle.getMapState[String, Long](
      "testMap", Encoders.STRING, Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val curCountValue = if (_valState.exists()) {
      _valState.get()
    } else {
      0L
    }
    var cnt = curCountValue
    inputRows.foreach { row =>
      cnt += 1
      _listState.appendValue(cnt)
      val mapCurVal = if (_mapState.containsKey(row)) {
        _mapState.getValue(row)
      } else {
        0
      }
      _mapState.updateValue(row, mapCurVal + 1L)
    }
    _valState.update(cnt)
    Iterator.single((key, cnt.toString))
  }

  override def close(): Unit = super.close()
}

class InitialStatefulProcessorWithStateDataSource
  extends InitialStateWithStateDataSourceBase[UnionInitialStateRow] {
  override def handleInitialState(
      key: String, initialState: UnionInitialStateRow, timerValues: TimerValues): Unit = {
    if (initialState.value.isDefined) {
      _valState.update(initialState.value.get)
    } else if (initialState.listValue.isDefined) {
      _listState.appendValue(initialState.listValue.get)
    } else if (initialState.userMapKey.isDefined) {
      _mapState.updateValue(
        initialState.userMapKey.get, initialState.userMapValue.get)
    }
  }
}

class InitialStatefulProcessorWithUnflattenStateDataSource
  extends InitialStateWithStateDataSourceBase[UnionUnflattenInitialStateRow] {
  override def handleInitialState(
      key: String, initialState: UnionUnflattenInitialStateRow, timerValues: TimerValues): Unit = {
    if (initialState.value.isDefined) {
      _valState.update(initialState.value.get)
    } else if (initialState.listValue.isDefined) {
      _listState.appendList(
        initialState.listValue.get.toArray)
    } else if (initialState.mapValue.isDefined) {
      initialState.mapValue.get.keys.foreach { key =>
        _mapState.updateValue(key, initialState.mapValue.get.get(key).get)
      }
    }
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
      timerValues: TimerValues): Iterator[(String, String, Double)] = {
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
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong,
      TTLConfig.NONE)
    _timerState = getHandle.getValueState[Long]("timerState", Encoders.scalaLong,
      TTLConfig.NONE)
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
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currCount = _countState.getOption().getOrElse(0L)
    val count = currCount + inputRows.size
    processUnexpiredRows(key, currCount, count, timerValues)
    Iterator((key, count.toString))
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    _timerState.clear()
    Iterator((key, "-1"))
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
      Encoders.scalaLong, TTLConfig.NONE)
    _timerState = getHandle.getValueState[Long]("timerState", Encoders.scalaLong,
      TTLConfig.NONE)
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
      timerValues: TimerValues): Iterator[(String, Int)] = {
    val valuesSeq = inputRows.toSeq
    val maxEventTimeSec = math.max(valuesSeq.map(_._2).max,
      _maxEventTimeState.getOption().getOrElse(0L))
    processUnexpiredRows(maxEventTimeSec)
    Iterator((key, maxEventTimeSec.toInt))
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, Int)] = {
    _maxEventTimeState.clear()
    Iterator((key, -1))
  }
}

/**
 * Class that adds tests for transformWithState stateful
 * streaming operator with user-defined initial state
 */
class TransformWithStateInitialStateSuite extends StateStoreMetricsTest
  with AlsoTestWithRocksDBFeatures {

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

      val clock = new StreamManualClock

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
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        // non-exist key test
        AddData(inputData, InitInputRow("k1", "update", 37.0)),
        AddData(inputData, InitInputRow("k2", "update", 40.0)),
        AddData(inputData, InitInputRow("non-exist", "getOption", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("non-exist", "getOption", -1.0)),
        Execute { q =>
          assert(q.lastProgress
            .stateOperators(0).customMetrics.get("initialStateProcessingTimeMs") > 0)
        },
        AddData(inputData, InitInputRow("k1", "appendList", 37.0)),
        AddData(inputData, InitInputRow("k2", "appendList", 40.0)),
        AddData(inputData, InitInputRow("non-exist", "getList", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),

        AddData(inputData, InitInputRow("k1", "incCount", 37.0)),
        AddData(inputData, InitInputRow("k2", "incCount", 40.0)),
        AddData(inputData, InitInputRow("non-exist", "getCount", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("non-exist", "getCount", 0.0)),

        AddData(inputData, InitInputRow("k2", "incCount", 40.0)),
        AddData(inputData, InitInputRow("k2", "getCount", 40.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("k2", "getCount", 2.0)),

        // test every row in initial State is processed
        AddData(inputData, InitInputRow("init_1", "getOption", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_1", "getOption", 40.0)),

        AddData(inputData, InitInputRow("init_2", "getOption", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_2", "getOption", 100.0)),

        AddData(inputData, InitInputRow("init_1", "getList", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_1", "getList", 40.0)),

        AddData(inputData, InitInputRow("init_2", "getList", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_2", "getList", 100.0)),

        AddData(inputData, InitInputRow("init_1", "getCount", 40.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_1", "getCount", 1.0)),

        AddData(inputData, InitInputRow("init_2", "getCount", 100.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_2", "getCount", 1.0)),

        // Update row with key in initial row will work
        AddData(inputData, InitInputRow("init_1", "update", 50.0)),
        AddData(inputData, InitInputRow("init_1", "getOption", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_1", "getOption", 50.0)),

        AddData(inputData, InitInputRow("init_1", "remove", -1.0)),
        AddData(inputData, InitInputRow("init_1", "getOption", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_1", "getOption", -1.0)),

        AddData(inputData, InitInputRow("init_1", "appendList", 50.0)),
        AddData(inputData, InitInputRow("init_1", "getList", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_1", "getList", 50.0), ("init_1", "getList", 40.0)),

        AddData(inputData, InitInputRow("init_1", "incCount", 40.0)),
        AddData(inputData, InitInputRow("init_1", "getCount", 40.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_1", "getCount", 2.0)),

        // test remove
        AddData(inputData, InitInputRow("k1", "remove", -1.0)),
        AddData(inputData, InitInputRow("k1", "getOption", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("k1", "getOption", -1.0)),

        AddData(inputData, InitInputRow("init_1", "clearCount", -1.0)),
        AddData(inputData, InitInputRow("init_1", "getCount", -1.0)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("init_1", "getCount", 0.0)),

        AddData(inputData, InitInputRow("init_1", "clearList", -1.0)),
        AddData(inputData, InitInputRow("init_1", "getList", -1.0)),
        AdvanceManualClock(1 * 1000),
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
        Execute { q =>
          assert(q.lastProgress
            .stateOperators(0).customMetrics.get("initialStateProcessingTimeMs") > 0)
        },

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

  Seq(true, false).foreach { flattenOption =>
    Seq(("5", "2"), ("5", "8"), ("5", "5")).foreach { partitions =>
      test("state data source reader dataframe as initial state " +
        s"(flatten option=$flattenOption, shuffle partition for 1st stream=${partitions._1}, " +
        s"shuffle partition for 1st stream=${partitions._2})") {
        withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName) {
          withTempPaths(2) { checkpointDirs =>
            SQLConf.get.setConfString(SQLConf.SHUFFLE_PARTITIONS.key, partitions._1)
            val inputData = MemoryStream[String]
            val result = inputData.toDS()
              .groupByKey(x => x)
              .transformWithState(new InitialStatefulProcessorWithStateDataSource(),
                TimeMode.None(),
                OutputMode.Update())

            testStream(result, OutputMode.Update())(
              StartStream(checkpointLocation = checkpointDirs(0).getCanonicalPath),
              AddData(inputData, "a", "b"),
              CheckNewAnswer(("a", "1"), ("b", "1")),
              AddData(inputData, "a", "b", "a"),
              CheckNewAnswer(("a", "3"), ("b", "2"))
            )

            // We are trying to mimic a use case where users load all state data rows
            // from a previous tws query as initial state and start a new tws query.
            // In this use case, users will need to create a single dataframe with
            // all the state rows from different state variables with different schema.
            // We can only read from one state variable from one state data source reader
            // query, and they are of different schema. We will get one dataframe from each
            // state variable, and we union them together into a single dataframe.
            val valueDf = spark.read
              .format("statestore")
              .option(StateSourceOptions.PATH, checkpointDirs(0).getAbsolutePath)
              .option(StateSourceOptions.STATE_VAR_NAME, "testVal")
              .load()
              .drop("partition_id")

            val listDf = spark.read
              .format("statestore")
              .option(StateSourceOptions.PATH, checkpointDirs(0).getAbsolutePath)
              .option(StateSourceOptions.STATE_VAR_NAME, "testList")
              .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, flattenOption)
              .load()
              .drop("partition_id")

            val mapDf = spark.read
              .format("statestore")
              .option(StateSourceOptions.PATH, checkpointDirs(0).getAbsolutePath)
              .option(StateSourceOptions.STATE_VAR_NAME, "testMap")
              .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, flattenOption)
              .load()
              .drop("partition_id")

            // create a df where each row contains all value, list, map state rows
            // fill the missing column with null.
            SQLConf.get.setConfString(SQLConf.SHUFFLE_PARTITIONS.key, partitions._2)
            val inputData2 = MemoryStream[String]
            val query = startQueryWithDataSourceDataframeAsInitState(
              flattenOption, valueDf, listDf, mapDf, inputData2)

            testStream(query, OutputMode.Update())(
              StartStream(checkpointLocation = checkpointDirs(1).getCanonicalPath),
              // check initial state is updated for state vars
              AddData(inputData2, "c"),
              CheckNewAnswer(("c", "1")),
              Execute { _ =>
                val valueDf2 = spark.read
                  .format("statestore")
                  .option(StateSourceOptions.PATH, checkpointDirs(1).getAbsolutePath)
                  .option(StateSourceOptions.STATE_VAR_NAME, "testVal")
                  .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, flattenOption)
                  .load()
                  .drop("partition_id")
                  .filter(col("key.value") =!= "c")

                val listDf2 = spark.read
                  .format("statestore")
                  .option(StateSourceOptions.PATH, checkpointDirs(1).getAbsolutePath)
                  .option(StateSourceOptions.STATE_VAR_NAME, "testList")
                  .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, flattenOption)
                  .load()
                  .drop("partition_id")
                  .filter(col("key.value") =!= "c")

                val mapDf2 = spark.read
                  .format("statestore")
                  .option(StateSourceOptions.PATH, checkpointDirs(1).getAbsolutePath)
                  .option(StateSourceOptions.STATE_VAR_NAME, "testMap")
                  .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, flattenOption)
                  .load()
                  .drop("partition_id")
                  .filter(col("key.value") =!= "c")

                checkAnswer(valueDf, valueDf2)
                checkAnswer(listDf, listDf2)
                checkAnswer(mapDf, mapDf2)
              }
            )
          }
        }
      }
    }
  }

  private def startQueryWithDataSourceDataframeAsInitState(
      flattenOption: Boolean,
      valDf: DataFrame,
      listDf: DataFrame,
      mapDf: DataFrame,
      inputData: MemoryStream[String]): DataFrame = {
    if (flattenOption) {
      // when we read the state rows with flattened option set to true, values of a composite
      // state variable will be flattened into multiple rows where each row is a
      // key -> single value pair
      val valueDf = valDf.selectExpr("key.value AS groupingKey", "value.value AS value")
      val flattenListDf = listDf
        .selectExpr("key.value AS groupingKey", "list_element.value AS listValue")
      val flattenMapDf = mapDf
        .selectExpr(
          "key.value AS groupingKey",
          "user_map_key.value AS userMapKey",
          "user_map_value.value AS userMapValue")
      val df_joined =
        valueDf.unionByName(flattenListDf, true)
          .unionByName(flattenMapDf, true)
      val kvDataSet = inputData.toDS().groupByKey(x => x)
      val initDf = df_joined.as[UnionInitialStateRow].groupByKey(x => x.groupingKey)
      (kvDataSet.transformWithState(
        new InitialStatefulProcessorWithStateDataSource(),
        TimeMode.None(), OutputMode.Append(), initDf).toDF())
    } else {
      // when we read the state rows with flattened option set to false, values of a composite
      // state variable will be composed into a single row of list/map type
      val valueDf = valDf.selectExpr("key.value AS groupingKey", "value.value AS value")
      val unflattenListDf = listDf
        .selectExpr("key.value AS groupingKey",
          "list_value.value as listValue")
      val unflattenMapDf = mapDf
        .selectExpr(
          "key.value AS groupingKey",
          "map_from_entries(transform(map_entries(map_value), x -> " +
            "struct(x.key.value, x.value.value))) as mapValue")
      val df_joined =
        valueDf.unionByName(unflattenListDf, true)
          .unionByName(unflattenMapDf, true)
      val kvDataSet = inputData.toDS().groupByKey(x => x)
      val initDf = df_joined.as[UnionUnflattenInitialStateRow].groupByKey(x => x.groupingKey)
      kvDataSet.transformWithState(
        new InitialStatefulProcessorWithUnflattenStateDataSource(),
        TimeMode.None(), OutputMode.Append(), initDf).toDF()
    }
  }
}
