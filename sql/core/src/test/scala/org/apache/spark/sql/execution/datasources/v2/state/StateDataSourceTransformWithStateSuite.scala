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
package org.apache.spark.sql.execution.datasources.v2.state

import java.io.File
import java.time.Duration

import org.apache.hadoop.conf.Configuration

import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{AlsoTestWithEncodingTypes, AlsoTestWithRocksDBFeatures, RocksDBFileManager, RocksDBStateStoreProvider, TestClass}
import org.apache.spark.sql.functions.{col, explode, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{InputMapRow, ListState, MapInputEvent, MapOutputEvent, MapStateTTLProcessor, MaxEventTimeStatefulProcessor, OutputMode, RunningCountStatefulProcessor, RunningCountStatefulProcessorWithProcTimeTimerUpdates, StatefulProcessor, StateStoreMetricsTest, TestMapStateProcessor, TimeMode, TimerValues, TransformWithStateSuiteUtils, Trigger, TTLConfig, ValueState}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

/** Stateful processor of single value state var with non-primitive type */
class StatefulProcessorWithSingleValueVar extends RunningCountStatefulProcessor {
  @transient private var _valueState: ValueState[TestClass] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _valueState = getHandle.getValueState[TestClass](
      "valueState", Encoders.product[TestClass], TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = _valueState.getOption().getOrElse(TestClass(0L, "dummyKey")).id + 1
    _valueState.update(TestClass(count, "dummyKey"))
    Iterator((key, count.toString))
  }
}

class StatefulProcessorWithTTL
  extends StatefulProcessor[String, String, (String, String)] {
  @transient protected var _countState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState",
      Encoders.scalaLong, TTLConfig(Duration.ofMillis(30000)))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
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

/** Stateful processor tracking groups belonging to sessions with/without TTL */
class SessionGroupsStatefulProcessor extends
  StatefulProcessor[String, (String, String), String] {
  @transient private var _groupsList: ListState[String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _groupsList = getHandle.getListState("groupsList", Encoders.STRING, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues): Iterator[String] = {
    inputRows.foreach { inputRow =>
      _groupsList.appendValue(inputRow._2)
    }
    Iterator.empty
  }
}

class SessionGroupsStatefulProcessorWithTTL extends
  StatefulProcessor[String, (String, String), String] {
  @transient private var _groupsListWithTTL: ListState[String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _groupsListWithTTL = getHandle.getListState("groupsListWithTTL", Encoders.STRING,
      TTLConfig(Duration.ofMillis(30000)))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues): Iterator[String] = {
    inputRows.foreach { inputRow =>
      _groupsListWithTTL.appendValue(inputRow._2)
    }
    Iterator.empty
  }
}

/**
 * Test suite to verify integration of state data source reader with the transformWithState operator
 */
@SlowSQLTest
class StateDataSourceTransformWithStateSuite extends StateStoreMetricsTest
  with AlsoTestWithEncodingTypes with AlsoTestWithRocksDBFeatures {

  import testImplicits._

  test("state data source integration - value state with single variable") {
    withTempDir { tempDir =>
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key ->
          TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new StatefulProcessorWithSingleValueVar(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          AddData(inputData, "b"),
          CheckNewAnswer(("b", "1")),
          StopStream
        )

        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "valueState")
          .load()

        val resultDf = stateReaderDf.selectExpr(
          "key.value AS groupingKey",
          "value.id AS valueId", "value.name AS valueName",
          "partition_id")

        checkAnswer(resultDf,
          Seq(Row("a", 1L, "dummyKey", 0), Row("b", 1L, "dummyKey", 1)))

        // non existent state variable should fail
        val ex = intercept[Exception] {
          spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
            .option(StateSourceOptions.STATE_VAR_NAME, "non-exist")
            .load()
        }
        assert(ex.isInstanceOf[StateDataSourceInvalidOptionValue])
        assert(ex.getMessage.contains("State variable non-exist is not defined"))

        // Verify that trying to read timers in TimeMode as None fails
        val ex1 = intercept[Exception] {
          spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
            .option(StateSourceOptions.READ_REGISTERED_TIMERS, true)
            .load()
        }
        assert(ex1.isInstanceOf[StateDataSourceInvalidOptionValue])
        assert(ex1.getMessage.contains("Registered timers are not available"))
      }
    }
  }

  testWithChangelogCheckpointingEnabled("state data source cdf integration - " +
    "value state with single variable") {
    withTempDir { tempDir =>
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key ->
          TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new StatefulProcessorWithSingleValueVar(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          AddData(inputData, "b"),
          CheckNewAnswer(("b", "1")),
          StopStream
        )

        val changeFeedDf = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
            .option(StateSourceOptions.STATE_VAR_NAME, "valueState")
            .option(StateSourceOptions.READ_CHANGE_FEED, true)
            .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
            .load()

        val opDf = changeFeedDf.selectExpr(
          "change_type",
          "key.value AS groupingKey",
          "value.id AS valueId", "value.name AS valueName",
          "partition_id")

        checkAnswer(opDf,
          Seq(Row("update", "a", 1L, "dummyKey", 0), Row("update", "b", 1L, "dummyKey", 1)))
      }
    }
  }

  test("state data source integration - value state with single variable and TTL") {
    withTempDir { tempDir =>
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key ->
          TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new StatefulProcessorWithTTL(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, "a"),
          AddData(inputData, "b"),
          Execute { _ =>
            // wait for the batch to run since we are using processing time
            Thread.sleep(5000)
          },
          StopStream
        )

        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "countState")
          .load()

        val resultDf = stateReaderDf.selectExpr(
          "key.value", "value.value", "value.ttlExpirationMs", "partition_id")

        var count = 0L
        resultDf.collect().foreach { row =>
          count = count + 1
          assert(row.getLong(2) > 0)
        }

        // verify that 2 state rows are present
        assert(count === 2)

        val answerDf = stateReaderDf.selectExpr(
          "key.value AS groupingKey",
          "value.value.value AS valueId", "partition_id")
        checkAnswer(answerDf,
          Seq(Row("a", 1L, 0), Row("b", 1L, 1)))

        // non existent state variable should fail
        val ex = intercept[Exception] {
          spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
            .option(StateSourceOptions.STATE_VAR_NAME, "non-exist")
            .load()
        }
        assert(ex.isInstanceOf[StateDataSourceInvalidOptionValue])
        assert(ex.getMessage.contains("State variable non-exist is not defined"))
      }
    }
  }

  testWithChangelogCheckpointingEnabled("state data source cdf integration - " +
    "value state with single variable and TTL") {
    withTempDir { tempDir =>
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key ->
          TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new StatefulProcessorWithTTL(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        val clock = new StreamManualClock
        testStream(result)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = tempDir.getCanonicalPath),
          AddData(inputData, "a"),
          AddData(inputData, "b"),
          AdvanceManualClock(5 * 1000),
          CheckNewAnswer(("a", "1"), ("b", "1")),
          AddData(inputData, "c"),
          AdvanceManualClock(30 * 1000),
          CheckNewAnswer(("c", "1")),
          AddData(inputData, "d"),
          AdvanceManualClock(30 * 1000),
          CheckNewAnswer(("d", "1")),
          StopStream
        )

        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "countState")
          .option(StateSourceOptions.READ_CHANGE_FEED, true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
          .load()

        val resultDf = stateReaderDf.selectExpr(
          "key.value", "value.value", "value.ttlExpirationMs", "partition_id")

        var count = 0L
        resultDf.collect().foreach { row =>
          if (!row.anyNull) {
            count = count + 1
            assert(row.getLong(2) > 0)
          }
        }

        // verify that 4 state rows are present
        assert(count === 4)

        val answerDf = stateReaderDf.selectExpr(
          "change_type",
          "key.value AS groupingKey",
          "value.value.value AS valueId", "partition_id")
        checkAnswer(answerDf,
          Seq(Row("update", "a", 1L, 0),
            Row("update", "b", 1L, 1),
            Row("update", "c", 1L, 2),
            Row("delete", "a", null, 0),
            Row("delete", "b", null, 1),
            Row("update", "d", 1L, 4),
            Row("delete", "c", null, 2)))
      }
    }
  }

  test("state data source integration - list state") {
    withTempDir { tempDir =>
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName) {

        val inputData = MemoryStream[(String, String)]
        val result = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new SessionGroupsStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, ("session1", "group2")),
          AddData(inputData, ("session1", "group1")),
          AddData(inputData, ("session2", "group1")),
          CheckNewAnswer(),
          AddData(inputData, ("session3", "group7")),
          AddData(inputData, ("session1", "group4")),
          CheckNewAnswer(),
          StopStream
        )

        // Verify that the state can be read in flattened/non-flattened modes
        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "groupsList")
          .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, false)
          .load()

        val listStateDf = stateReaderDf
          .selectExpr(
      "key.value AS groupingKey",
            "list_value.value AS valueList",
            "partition_id")
          .select($"groupingKey",
            explode($"valueList"))

        checkAnswer(listStateDf,
          Seq(Row("session1", "group1"), Row("session1", "group2"), Row("session1", "group4"),
            Row("session2", "group1"), Row("session3", "group7")))

        val flattenedReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "groupsList")
          .load()

        val resultDf = flattenedReaderDf.selectExpr(
          "key.value AS groupingKey",
          "list_element.value AS valueList")
        checkAnswer(resultDf,
          Seq(Row("session1", "group1"), Row("session1", "group2"), Row("session1", "group4"),
            Row("session2", "group1"), Row("session3", "group7")))
      }
    }
  }

  testWithChangelogCheckpointingEnabled("state data source cdf integration - list state") {
    withTempDir { tempDir =>
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName) {

        val inputData = MemoryStream[(String, String)]
        val result = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new SessionGroupsStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, ("session1", "group2")),
          AddData(inputData, ("session1", "group1")),
          AddData(inputData, ("session2", "group1")),
          CheckNewAnswer(),
          AddData(inputData, ("session3", "group7")),
          AddData(inputData, ("session1", "group4")),
          CheckNewAnswer(),
          StopStream
        )

        val flattenedReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "groupsList")
          .option(StateSourceOptions.READ_CHANGE_FEED, true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
          .load()

        val resultDf = flattenedReaderDf.selectExpr(
          "change_type",
          "key.value AS groupingKey",
          "list_element.value AS valueList",
          "partition_id")
        checkAnswer(resultDf,
          Seq(Row("append", "session1", "group1", 0),
            Row("append", "session1", "group2", 0),
            Row("append", "session1", "group4", 0),
            Row("append", "session2", "group1", 0),
            Row("append", "session3", "group7", 3)))
      }
    }
  }

  test("state data source integration - list state and TTL") {
    withTempDir { tempDir =>
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key ->
          TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
        val inputData = MemoryStream[(String, String)]
        val result = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new SessionGroupsStatefulProcessorWithTTL(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, ("session1", "group2")),
          AddData(inputData, ("session1", "group1")),
          AddData(inputData, ("session2", "group1")),
          AddData(inputData, ("session3", "group7")),
          AddData(inputData, ("session1", "group4")),
          Execute { _ =>
            // wait for the batch to run since we are using processing time
            Thread.sleep(5000)
          },
          StopStream
        )

        // Verify that the state can be read in flattened/non-flattened modes
        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "groupsListWithTTL")
          .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, false)
          .load()

        val listStateDf = stateReaderDf
          .selectExpr(
      "key.value AS groupingKey",
            "list_value AS valueList",
            "partition_id")
          .select($"groupingKey",
            explode($"valueList").as("valueList"))

        val resultDf = listStateDf.selectExpr("valueList.ttlExpirationMs")
        var count = 0L
        resultDf.collect().foreach { row =>
          count = count + 1
          assert(row.getLong(0) > 0)
        }

        // verify that 5 state rows are present
        assert(count === 5)

        val valuesDf = listStateDf.selectExpr("groupingKey",
          "valueList.value.value AS groupId")

        checkAnswer(valuesDf,
          Seq(Row("session1", "group1"), Row("session1", "group2"), Row("session1", "group4"),
          Row("session2", "group1"), Row("session3", "group7")))

        val flattenedStateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "groupsListWithTTL")
          .load()

        val flattenedResultDf = flattenedStateReaderDf
          .selectExpr("list_element.ttlExpirationMs AS ttlExpirationMs")
        var flattenedCount = 0L
        flattenedResultDf.collect().foreach { row =>
          flattenedCount = flattenedCount + 1
          assert(row.getLong(0) > 0)
        }

        // verify that 5 state rows are present
        assert(flattenedCount === 5)

        val outputDf = flattenedStateReaderDf
          .selectExpr("key.value AS groupingKey",
            "list_element.value.value AS groupId")

        checkAnswer(outputDf,
          Seq(Row("session1", "group1"), Row("session1", "group2"), Row("session1", "group4"),
          Row("session2", "group1"), Row("session3", "group7")))
      }
    }
  }

  testWithChangelogCheckpointingEnabled("state data source cdf integration - list state and TTL") {
    withTempDir { tempDir =>
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key ->
          TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
        val inputData = MemoryStream[(String, String)]
        val result = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new SessionGroupsStatefulProcessorWithTTL(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        val clock = new StreamManualClock
        testStream(result)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = tempDir.getCanonicalPath),
          AddData(inputData, ("session1", "group2")),
          AddData(inputData, ("session1", "group1")),
          AddData(inputData, ("session2", "group1")),
          AdvanceManualClock(5 * 1000),
          CheckNewAnswer(),
          AddData(inputData, ("session3", "group7")),
          AddData(inputData, ("session1", "group4")),
          AdvanceManualClock(30 * 1000),
          CheckNewAnswer(),
          StopStream
        )

        val flattenedStateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "groupsListWithTTL")
          .option(StateSourceOptions.READ_CHANGE_FEED, true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
          .load()

        val flattenedResultDf = flattenedStateReaderDf
          .selectExpr("list_element.ttlExpirationMs AS ttlExpirationMs")
        var flattenedCount = 0L
        flattenedResultDf.collect().foreach { row =>
          if (!row.anyNull) {
            flattenedCount = flattenedCount + 1
            assert(row.getLong(0) > 0)
          }
        }

        // verify that 6 state rows are present
        assert(flattenedCount === 6)

        val outputDf = flattenedStateReaderDf
          .selectExpr(
            "change_type",
            "key.value AS groupingKey",
            "list_element.value.value AS groupId",
            "partition_id")

        checkAnswer(outputDf,
          Seq(Row("append", "session1", "group1", 0),
            Row("append", "session1", "group2", 0),
            Row("append", "session1", "group4", 0),
            Row("append", "session2", "group1", 0),
            Row("append", "session3", "group7", 3),
            Row("delete", "session1", null, 0),
            Row("delete", "session2", null, 0),
            Row("update", "session1", "group4", 0)))
      }
    }
  }

  test("state data source integration - map state with single variable") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { tempDir =>
        val inputData = MemoryStream[InputMapRow]
        val result = inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new TestMapStateProcessor(),
            TimeMode.None(),
            OutputMode.Append())
        testStream(result, OutputMode.Append())(
          StartStream(checkpointLocation = tempDir.getCanonicalPath),
          AddData(inputData, InputMapRow("k1", "updateValue", ("v1", "10"))),
          AddData(inputData, InputMapRow("k1", "exists", ("", ""))),
          AddData(inputData, InputMapRow("k2", "exists", ("", ""))),
          CheckNewAnswer(("k1", "exists", "true"), ("k2", "exists", "false")),

          AddData(inputData, InputMapRow("k1", "updateValue", ("v2", "5"))),
          AddData(inputData, InputMapRow("k2", "updateValue", ("v2", "3"))),
          ProcessAllAvailable(),
          StopStream
        )

        // Verify that the state can be read in flattened/non-flattened modes
        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "sessionState")
          .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, false)
          .load()

        val resultDf = stateReaderDf.selectExpr(
          "key.value AS groupingKey", "map_value AS mapValue")

        checkAnswer(resultDf,
          Seq(
            Row("k1",
              Map(Row("v1") -> Row("10"), Row("v2") -> Row("5"))),
            Row("k2",
              Map(Row("v2") -> Row("3"))))
        )

        val flattenedStateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "sessionState")
          .load()

        val outputDf = flattenedStateReaderDf
          .selectExpr("key.value AS groupingKey",
            "user_map_key.value AS mapKey",
            "user_map_value.value AS mapValue")

        checkAnswer(outputDf,
          Seq(
            Row("k1", "v1", "10"),
            Row("k1", "v2", "5"),
            Row("k2", "v2", "3"))
        )
      }
    }
  }

  testWithChangelogCheckpointingEnabled("state data source cdf integration - " +
   "map state with single variable") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { tempDir =>
        val inputData = MemoryStream[InputMapRow]
        val result = inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new TestMapStateProcessor(),
            TimeMode.None(),
            OutputMode.Append())
        testStream(result, OutputMode.Append())(
          StartStream(checkpointLocation = tempDir.getCanonicalPath),
          AddData(inputData, InputMapRow("k1", "updateValue", ("v1", "10"))),
          AddData(inputData, InputMapRow("k1", "exists", ("", ""))),
          AddData(inputData, InputMapRow("k2", "exists", ("", ""))),
          CheckNewAnswer(("k1", "exists", "true"), ("k2", "exists", "false")),

          AddData(inputData, InputMapRow("k1", "updateValue", ("v2", "5"))),
          AddData(inputData, InputMapRow("k2", "updateValue", ("v2", "3"))),
          ProcessAllAvailable(),
          StopStream
        )

        val flattenedStateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "sessionState")
          .option(StateSourceOptions.READ_CHANGE_FEED, true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
          .load()

        val outputDf = flattenedStateReaderDf
          .selectExpr(
            "change_type",
            "key.value AS groupingKey",
            "user_map_key.value AS mapKey",
            "user_map_value.value AS mapValue",
            "partition_id")

        checkAnswer(outputDf,
          Seq(
            Row("update", "k1", "v1", "10", 4L),
            Row("update", "k1", "v2", "5", 4L),
            Row("update", "k2", "v2", "3", 2L))
        )
      }
    }
  }

  test("state data source integration - map state TTL with single variable") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { tempDir =>
        val inputStream = MemoryStream[MapInputEvent]
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        val result = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            new MapStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Append())

        val clock = new StreamManualClock
        testStream(result)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = tempDir.getCanonicalPath),
          AddData(inputStream,
            MapInputEvent("k1", "key1", "put", 1),
            MapInputEvent("k1", "key2", "put", 2)
          ),
          AdvanceManualClock(1 * 1000), // batch timestamp: 1000
          CheckNewAnswer(),
          AddData(inputStream,
            MapInputEvent("k1", "key1", "get", -1),
            MapInputEvent("k1", "key2", "get", -1)
          ),
          AdvanceManualClock(30 * 1000), // batch timestamp: 31000
          CheckNewAnswer(
            MapOutputEvent("k1", "key1", 1, isTTLValue = false, -1),
            MapOutputEvent("k1", "key2", 2, isTTLValue = false, -1)
          ),
          // get values from ttl state
          AddData(inputStream,
            MapInputEvent("k1", "", "get_values_in_ttl_state", -1)
          ),
          AdvanceManualClock(1 * 1000), // batch timestamp: 32000
          CheckNewAnswer(
            MapOutputEvent("k1", "key1", -1, isTTLValue = true, 61000),
            MapOutputEvent("k1", "key2", -1, isTTLValue = true, 61000)
          ),
          StopStream
        )

        // Verify that the state can be read in flattened/non-flattened modes
        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "mapState")
          .option(StateSourceOptions.FLATTEN_COLLECTION_TYPES, false)
          .load()

        val resultDf = stateReaderDf.selectExpr(
          "key.value AS groupingKey", "map_value AS mapValue")

        checkAnswer(resultDf,
          Seq(
            Row("k1",
              Map(Row("key2") -> Row(Row(2), 61000L),
                Row("key1") -> Row(Row(1), 61000L))))
        )

        val flattenedStateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "mapState")
          .load()

        val outputDf = flattenedStateReaderDf
          .selectExpr("key.value AS groupingKey",
            "user_map_key.value AS mapKey",
            "user_map_value.value.value AS mapValue",
            "user_map_value.ttlExpirationMs AS ttlTimestamp")

        checkAnswer(outputDf,
          Seq(
            Row("k1", "key1", 1, 61000L),
            Row("k1", "key2", 2, 61000L))
        )
      }
    }
  }

  testWithChangelogCheckpointingEnabled("state data source cdf integration - " +
   "map state TTL with single variable") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { tempDir =>
        val inputStream = MemoryStream[MapInputEvent]
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        val result = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            new MapStateTTLProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Append())

        val clock = new StreamManualClock
        testStream(result)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = tempDir.getCanonicalPath),
          AddData(inputStream,
            MapInputEvent("k1", "key1", "put", 1),
            MapInputEvent("k1", "key2", "put", 2)
          ),
          AdvanceManualClock(1 * 1000), // batch timestamp: 1000
          CheckNewAnswer(),
          AddData(inputStream,
            MapInputEvent("k1", "key1", "get", -1),
            MapInputEvent("k1", "key2", "get", -1)
          ),
          AdvanceManualClock(30 * 1000), // batch timestamp: 31000
          CheckNewAnswer(
            MapOutputEvent("k1", "key1", 1, isTTLValue = false, -1),
            MapOutputEvent("k1", "key2", 2, isTTLValue = false, -1)
          ),
          // get values from ttl state
          AddData(inputStream,
            MapInputEvent("k1", "", "get_values_in_ttl_state", -1)
          ),
          AdvanceManualClock(1 * 1000), // batch timestamp: 32000
          CheckNewAnswer(
            MapOutputEvent("k1", "key1", -1, isTTLValue = true, 61000),
            MapOutputEvent("k1", "key2", -1, isTTLValue = true, 61000)
          ),
          AddData(inputStream,
            MapInputEvent("k2", "key3", "put", 3)
          ),
          AdvanceManualClock(30 * 1000), // batch timestamp: 62000
          CheckNewAnswer(),
          StopStream
        )

        val flattenedStateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "mapState")
          .option(StateSourceOptions.READ_CHANGE_FEED, true)
          .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
          .load()

        val outputDf = flattenedStateReaderDf
          .selectExpr(
            "change_type",
            "key.value AS groupingKey",
            "user_map_key.value AS mapKey",
            "user_map_value.value.value AS mapValue",
            "user_map_value.ttlExpirationMs AS ttlTimestamp",
            "partition_id")

        checkAnswer(outputDf,
          Seq(
            Row("update", "k1", "key1", 1, 61000L, 4L),
            Row("update", "k1", "key2", 2, 61000L, 4L),
            Row("delete", "k1", "key1", null, null, 4L),
            Row("delete", "k1", "key2", null, null, 4L),
            Row("update", "k2", "key3", 3, 122000L, 2L))
        )
      }
    }
  }

  test("state data source - processing-time timers integration") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { tempDir =>
        val clock = new StreamManualClock

        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(
            new RunningCountStatefulProcessorWithProcTimeTimerUpdates(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = tempDir.getCanonicalPath),
          AddData(inputData, "a"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "1")), // at batch 0, ts = 1, timer = "a" -> [6] (= 1 + 5)
          AddData(inputData, "a"),
          AdvanceManualClock(2 * 1000),
          CheckNewAnswer(("a", "2")), // at batch 1, ts = 3, timer = "a" -> [10.5] (3 + 7.5)
          StopStream)

        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.READ_REGISTERED_TIMERS, true)
          .load()

        val resultDf = stateReaderDf.selectExpr(
       "key.value AS groupingKey",
          "expiration_timestamp_ms AS expiryTimestamp",
          "partition_id")

        checkAnswer(resultDf,
          Seq(Row("a", 10500L, 0)))
      }
    }
  }

  test("state data source - event-time timers integration") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { tempDir =>
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
          StartStream(checkpointLocation = tempDir.getCanonicalPath),

          AddData(inputData, ("a", 11), ("a", 13), ("a", 15)),
          // Max event time = 15. Timeout timestamp for "a" = 15 + 5 = 20. Watermark = 15 - 10 = 5.
          CheckNewAnswer(("a", 15)), // Output = max event time of a

          AddData(inputData, ("a", 4)), // Add data older than watermark for "a"
          CheckNewAnswer(), // No output as data should get filtered by watermark
          StopStream)

        val stateReaderDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
          .option(StateSourceOptions.READ_REGISTERED_TIMERS, true)
          .load()

        val resultDf = stateReaderDf.selectExpr(
          "key.value AS groupingKey",
          "expiration_timestamp_ms AS expiryTimestamp",
          "partition_id")

        checkAnswer(resultDf,
          Seq(Row("a", 20000L, 0)))
      }
    }
  }

  /**
   * Note that we cannot use the golden files approach for transformWithState. The new schema
   * format keeps track of the schema file path as an absolute path which cannot be used with
   * the getResource model used in other similar tests. Hence, we force the snapshot creation
   * for given versions and ensure that we are loading from given start snapshot version for loading
   * the state data.
   */
  testWithChangelogCheckpointingEnabled("snapshotStartBatchId with transformWithState") {
    class AggregationStatefulProcessor extends StatefulProcessor[Int, (Int, Long), (Int, Long)] {
      @transient protected var _countState: ValueState[Long] = _

      override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
        _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong,
          TTLConfig.NONE)
      }

      override def handleInputRows(
          key: Int,
          inputRows: Iterator[(Int, Long)],
          timerValues: TimerValues): Iterator[(Int, Long)] = {
        val count = _countState.getOption().getOrElse(0L)
        var totalSum = 0L
        inputRows.foreach { entry =>
          totalSum += entry._2
        }
        _countState.update(count + totalSum)
        Iterator((key, count + totalSum))
      }
    }

    withTempDir { tmpDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key ->
          TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString,
        SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1") {
        val inputData = MemoryStream[(Int, Long)]
        val query = inputData
          .toDS()
          .groupByKey(_._1)
          .transformWithState(new AggregationStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Append())
        testStream(query)(
          StartStream(checkpointLocation = tmpDir.getCanonicalPath),
          AddData(inputData, (1, 1L), (2, 2L), (3, 3L), (4, 4L)),
          ProcessAllAvailable(),
          AddData(inputData, (5, 1L), (6, 2L), (7, 3L), (8, 4L)),
          ProcessAllAvailable(),
          AddData(inputData, (9, 1L), (10, 2L), (11, 3L), (12, 4L)),
          ProcessAllAvailable(),
          AddData(inputData, (13, 1L), (14, 2L), (15, 3L), (16, 4L)),
          ProcessAllAvailable(),
          AddData(inputData, (17, 1L), (18, 2L), (19, 3L), (20, 4L)),
          ProcessAllAvailable(),
          // Ensure that we get a chance to upload created snapshots
          Execute { _ => Thread.sleep(5000) },
          StopStream
        )
      }

      // Create a file manager for the state store with opId=0 and partition=4
      val dfsRootDir = new File(tmpDir.getAbsolutePath + "/state/0/4")
      val fileManager = new RocksDBFileManager(
        dfsRootDir.getAbsolutePath, Utils.createTempDir(), new Configuration,
        CompressionCodec.LZ4)

      // Read the changelog for one of the partitions at version 3 and
      // ensure that we have two entries
      // For this test - keys 9 and 12 are written at version 3 for partition 4
      val changelogReader = fileManager.getChangelogReader(3)
      val entries = changelogReader.toSeq
      assert(entries.size == 2)
      val retainEntry = entries.head

      // Retain one of the entries and delete the changelog file
      val changelogFilePath = dfsRootDir.getAbsolutePath + "/3.changelog"
      Utils.deleteRecursively(new File(changelogFilePath))

      // Write the retained entry back to the changelog
      val changelogWriter = fileManager.getChangeLogWriter(3)
      changelogWriter.put(retainEntry._2, retainEntry._3)
      changelogWriter.commit()

      // Ensure that we have only one entry in the changelog for version 3
      // For this test - key 9 is retained and key 12 is deleted
      val changelogReader1 = fileManager.getChangelogReader(3)
      val entries1 = changelogReader1.toSeq
      assert(entries1.size == 1)

      // Ensure that the state matches for the partition that is not modified and does not match for
      // the other partition
      Seq(1, 4).foreach { partition =>
        val stateSnapshotDf = spark
          .read
          .format("statestore")
          .option("snapshotPartitionId", partition)
          .option("snapshotStartBatchId", 1)
          .option("stateVarName", "countState")
          .load(tmpDir.getCanonicalPath)

        val stateDf = spark
          .read
          .format("statestore")
          .option("stateVarName", "countState")
          .load(tmpDir.getCanonicalPath)
          .filter(col("partition_id") === partition)

        if (partition == 1) {
          checkAnswer(stateSnapshotDf, stateDf)
        } else {
          // Ensure that key 12 is not present in the final state loaded from given snapshot
          val resultDfForSnapshot = stateSnapshotDf.selectExpr(
            "key.value AS groupingKey",
            "value.value AS count",
            "partition_id")
          checkAnswer(resultDfForSnapshot,
            Seq(Row(16, 4L, 4),
              Row(17, 1L, 4),
              Row(19, 3L, 4),
              Row(2, 2L, 4),
              Row(6, 2L, 4),
              Row(9, 1L, 4)))

          // Ensure that key 12 is present in the final state loaded from the latest snapshot
          val resultDf = stateDf.selectExpr(
            "key.value AS groupingKey",
            "value.value AS count",
            "partition_id")

          checkAnswer(resultDf,
            Seq(Row(16, 4L, 4),
              Row(17, 1L, 4),
              Row(19, 3L, 4),
              Row(2, 2L, 4),
              Row(6, 2L, 4),
              Row(9, 1L, 4),
              Row(12, 4L, 4)))
        }
      }
    }
  }
}
