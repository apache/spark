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

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{AlsoTestWithChangelogCheckpointingEnabled, RocksDBStateStoreProvider}
import org.apache.spark.sql.internal.SQLConf

case class InputMapRow(key: String, action: String, value: (String, String))

class TestMapStateProcessor
  extends StatefulProcessor[String, InputMapRow, (String, String, String)] {

  @transient var _mapState: MapState[String, String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _mapState = getHandle.getMapState("sessionState", Encoders.STRING, Encoders.STRING)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InputMapRow],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String, String)] = {

    var output = List[(String, String, String)]()

    for (row <- inputRows) {
      if (row.action == "exists") {
        output = (key, "exists", _mapState.exists().toString) :: output
      } else if (row.action == "getValue") {
        output = (key, row.value._1, _mapState.getValue(row.value._1)) :: output
      } else if (row.action == "containsKey") {
        output = (key, row.value._1,
          if (_mapState.containsKey(row.value._1)) "true" else "false") :: output
      } else if (row.action == "updateValue") {
        _mapState.updateValue(row.value._1, row.value._2)
      } else if (row.action == "iterator") {
        _mapState.iterator().foreach { pair =>
          output = (key, pair._1, pair._2) :: output
        }
      } else if (row.action == "keys") {
        _mapState.keys().foreach { key =>
          output = (row.key, key, row.value._2) :: output
        }
      } else if (row.action == "values") {
        _mapState.values().foreach { value =>
          output = (row.key, row.value._1, value) :: output
        }
      } else if (row.action == "removeKey") {
        _mapState.removeKey(row.value._1)
      } else if (row.action == "clear") {
        _mapState.clear()
      }
    }
    output.iterator
  }

  override def close(): Unit = {}
}

/**
 * Class that adds integration tests for MapState types used in arbitrary stateful
 * operators such as transformWithState.
 */
class TransformWithMapStateSuite extends StreamTest
  with AlsoTestWithChangelogCheckpointingEnabled {
  import testImplicits._

  private def testMapStateWithNullUserKey(inputMapRow: InputMapRow): Unit = {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputMapRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestMapStateProcessor(),
          TimeMode.None(),
          OutputMode.Update())


      testStream(result, OutputMode.Update())(
        AddData(inputData, inputMapRow),
        ExpectFailure[SparkIllegalArgumentException] { e => {
          checkError(
            exception = e.asInstanceOf[SparkIllegalArgumentException],
            condition = "ILLEGAL_STATE_STORE_VALUE.NULL_VALUE",
            sqlState = Some("42601"),
            parameters = Map("stateName" -> "sessionState")
          )
        }}
      )
    }
  }

  test("Test retrieving value with non-existing user key") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputMapRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestMapStateProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputMapRow("k1", "getValue", ("v1", ""))),
        CheckAnswer(("k1", "v1", null))
      )
    }
  }

  Seq("getValue", "containsKey", "updateValue", "removeKey").foreach { mapImplFunc =>
    test(s"Test $mapImplFunc with null user key") {
      testMapStateWithNullUserKey(InputMapRow("k1", mapImplFunc, (null, "")))
    }
  }

  test("Test put value with null value") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputMapRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestMapStateProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputMapRow("k1", "updateValue", ("k1", null))),
        ExpectFailure[SparkIllegalArgumentException] { e => {
          checkError(
            exception = e.asInstanceOf[SparkIllegalArgumentException],
            condition = "ILLEGAL_STATE_STORE_VALUE.NULL_VALUE",
            sqlState = Some("42601"),
            parameters = Map("stateName" -> "sessionState"))
        }}
      )
    }
  }

  test("Test map state correctness") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputData = MemoryStream[InputMapRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestMapStateProcessor(),
          TimeMode.None(),
          OutputMode.Append())
      testStream(result, OutputMode.Append())(
        // Test exists()
        AddData(inputData, InputMapRow("k1", "updateValue", ("v1", "10"))),
        AddData(inputData, InputMapRow("k1", "exists", ("", ""))),
        AddData(inputData, InputMapRow("k2", "exists", ("", ""))),
        CheckNewAnswer(("k1", "exists", "true"), ("k2", "exists", "false")),

        // Test get and put with composite key
        AddData(inputData, InputMapRow("k1", "updateValue", ("v2", "5"))),

        AddData(inputData, InputMapRow("k2", "updateValue", ("v2", "3"))),
        AddData(inputData, InputMapRow("k2", "updateValue", ("v2", "12"))),
        AddData(inputData, InputMapRow("k2", "updateValue", ("v4", "1"))),

        // Different grouping key, same user key
        AddData(inputData, InputMapRow("k1", "getValue", ("v2", ""))),
        CheckNewAnswer(("k1", "v2", "5")),
        // Same grouping key, same user key, update value should reflect
        AddData(inputData, InputMapRow("k2", "getValue", ("v2", ""))),
        CheckNewAnswer(("k2", "v2", "12")),

        // Test get full map for a given grouping key - prefixScan
        AddData(inputData, InputMapRow("k2", "iterator", ("", ""))),
        CheckNewAnswer(("k2", "v2", "12"), ("k2", "v4", "1")),

        AddData(inputData, InputMapRow("k2", "keys", ("", ""))),
        CheckNewAnswer(("k2", "v2", ""), ("k2", "v4", "")),

        AddData(inputData, InputMapRow("k2", "values", ("", ""))),
        CheckNewAnswer(("k2", "", "12"), ("k2", "", "1")),

        // Test remove functionalities
        AddData(inputData, InputMapRow("k1", "removeKey", ("v2", ""))),
        AddData(inputData, InputMapRow("k1", "containsKey", ("v2", ""))),
        CheckNewAnswer(("k1", "v2", "false")),

        AddData(inputData, InputMapRow("k2", "clear", ("", ""))),
        AddData(inputData, InputMapRow("k2", "iterator", ("", ""))),
        CheckNewAnswer(),
        AddData(inputData, InputMapRow("k2", "exists", ("", ""))),
        AddData(inputData, InputMapRow("k1", "clear", ("", ""))),
        AddData(inputData, InputMapRow("k3", "updateValue", ("v7", "11"))),
        CheckNewAnswer(("k2", "exists", "false")),
        Execute { q =>
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numMapStateVars") > 0)
          assert(q.lastProgress.stateOperators(0).numRowsUpdated === 1)
          assert(q.lastProgress.stateOperators(0).numRowsRemoved === 1)
        }
      )
    }
  }

  test("transformWithMapState - batch should succeed") {
    val inputData = Seq(
      InputMapRow("k1", "updateValue", ("v1", "10")),
      InputMapRow("k1", "getValue", ("v1", "")))
    val result = inputData.toDS()
      .groupByKey(x => x.key)
      .transformWithState(new TestMapStateProcessor(),
        TimeMode.None(),
        OutputMode.Append())

    val df = result.toDF()
    checkAnswer(df, Seq(("k1", "v1", "10")).toDF())
  }
}
