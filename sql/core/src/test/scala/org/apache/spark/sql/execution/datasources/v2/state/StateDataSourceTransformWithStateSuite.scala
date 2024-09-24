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

import java.time.Duration

import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{AlsoTestWithChangelogCheckpointingEnabled, RocksDBStateStoreProvider, TestClass}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{ExpiredTimerInfo, InputMapRow, ListState, MapInputEvent, MapOutputEvent, MapStateTTLProcessor, OutputMode, RunningCountStatefulProcessor, StatefulProcessor, StateStoreMetricsTest, TestMapStateProcessor, TimeMode, TimerValues, TransformWithStateSuiteUtils, Trigger, TTLConfig, ValueState}
import org.apache.spark.sql.streaming.util.StreamManualClock

/** Stateful processor of single value state var with non-primitive type */
class StatefulProcessorWithSingleValueVar extends RunningCountStatefulProcessor {
  @transient private var _valueState: ValueState[TestClass] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _valueState = getHandle.getValueState[TestClass](
      "valueState", Encoders.product[TestClass])
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
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

/** Stateful processor tracking groups belonging to sessions with/without TTL */
class SessionGroupsStatefulProcessor extends
  StatefulProcessor[String, (String, String), String] {
  @transient private var _groupsList: ListState[String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _groupsList = getHandle.getListState("groupsList", Encoders.STRING)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[String] = {
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
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[String] = {
    inputRows.foreach { inputRow =>
      _groupsListWithTTL.appendValue(inputRow._2)
    }
    Iterator.empty
  }
}

/**
 * Test suite to verify integration of state data source reader with the transformWithState operator
 */
class StateDataSourceTransformWithStateSuite extends StateStoreMetricsTest
  with AlsoTestWithChangelogCheckpointingEnabled {

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

        // TODO: this should be removed when readChangeFeed is supported for value state
        val ex1 = intercept[Exception] {
          spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
            .option(StateSourceOptions.STATE_VAR_NAME, "valueState")
            .option(StateSourceOptions.READ_CHANGE_FEED, "true")
            .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
            .load()
        }
        assert(ex1.isInstanceOf[StateDataSourceConflictOptions])
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

        // TODO: this should be removed when readChangeFeed is supported for TTL based state
        // variables
        val ex1 = intercept[Exception] {
          spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, tempDir.getAbsolutePath)
            .option(StateSourceOptions.STATE_VAR_NAME, "countState")
            .option(StateSourceOptions.READ_CHANGE_FEED, "true")
            .option(StateSourceOptions.CHANGE_START_BATCH_ID, 0)
            .load()
        }
        assert(ex1.isInstanceOf[StateDataSourceConflictOptions])
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
}
