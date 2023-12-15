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
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf

case class InputRow(key: String, action: String, value: String)

class TestListStateProcessor
  extends StatefulProcessor[String, InputRow, (String, String)] {

  @transient var _processorHandle: StatefulProcessorHandle = _
  @transient var _listState: ListState[String] = _

  override def init(handle: StatefulProcessorHandle, outputMode: OutputMode): Unit = {
    _processorHandle = handle
    _listState = handle.getListState("sessionState")
  }

  override def handleInputRows(key: String,
      inputRows: Iterator[InputRow],
      timerValues: TimerValues): Iterator[(String, String)] = {

    var output = List[(String, String)]()

    for (row <- inputRows) {
      if (row.action == "emit") {
        output = (key, row.value) :: output
      } else if (row.action == "emitAllInState") {
        _listState.get().foreach(v => {
          output = (key, v) :: output
        })
        _listState.remove()
      } else if (row.action == "append") {
        _listState.appendValue(row.value)
      } else if (row.action == "appendAll") {
        _listState.appendList(row.value.split(",").toSeq)
      } else if (row.action == "put") {
        _listState.put(row.value.split(",").toSeq)
      } else if (row.action == "remove") {
        _listState.remove()
      } else if (row.action == "tryAppendingNull") {
        _listState.appendValue(null)
      } else if (row.action == "tryAppendingNullValueInList") {
        _listState.appendList(List(null))
      } else if (row.action == "tryAppendingNullList") {
        _listState.appendList(null)
      } else if (row.action == "tryPutNullList") {
        _listState.put(null)
      } else if (row.action == "tryPuttingNullInList") {
        _listState.put(List(null))
      }
    }
    output.iterator
  }


  override def close(): Unit = {
  }
}

class TransformWithListStateSuite extends StreamTest {
  import testImplicits._

  test("test appending null value in list state throw exception") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeoutMode.noTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update()) (
        AddData(inputData, InputRow("k1", "tryAppendingNull", "")),
        ExpectFailure[SparkException](e => {
          assert(e.getMessage.contains("value added to ListState should be non-null"))
        })
      )
    }
  }

  test("test putting null value in list state throw exception") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeoutMode.noTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputRow("k1", "tryPuttingNullInList", "")),
        ExpectFailure[SparkException](e => {
          assert(e.getMessage.contains("value added to ListState should be non-null"))
        })
      )
    }
  }

  test("test putting null list in list state throw exception") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeoutMode.noTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputRow("k1", "tryPutNullList", "")),
        ExpectFailure[SparkException](e => {
          assert(e.getMessage.contains("newState list should be non-null"))
        })
      )
    }
  }

  test("test appending null list in list state throw exception") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeoutMode.noTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputRow("k1", "tryAppendingNullList", "")),
        ExpectFailure[SparkException](e => {
          assert(e.getMessage.contains("newState list should be non-null"))
        })
      )
    }
  }

  test("test list state correctness") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestListStateProcessor(),
          TimeoutMode.noTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update()) (
        // no interaction test
        AddData(inputData, InputRow("k1", "emit", "v1")),
        CheckNewAnswer(("k1", "v1")),
        // check simple append
        AddData(inputData, InputRow("k1", "append", "v2")),
        AddData(inputData, InputRow("k1", "emitAllInState", "")),
        CheckNewAnswer(("k1", "v2")),
        // multiple appends are correctly stored and emitted
        AddData(inputData, InputRow("k2", "append", "v1")),
        AddData(inputData, InputRow("k1", "append", "v4")),
        AddData(inputData, InputRow("k2", "append", "v2")),
        AddData(inputData, InputRow("k1", "emit", "v5")),
        AddData(inputData, InputRow("k2", "emit", "v3")),
        CheckNewAnswer(("k1", "v5"), ("k2", "v3")),
        AddData(inputData, InputRow("k1", "emitAllInState", "")),
        AddData(inputData, InputRow("k2", "emitAllInState", "")),
        CheckNewAnswer(("k2", "v1"), ("k2", "v2"), ("k1", "v4")),
        // check appendAll with append
        AddData(inputData, InputRow("k3", "appendAll", "v1,v2,v3")),
        AddData(inputData, InputRow("k3", "emit", "v4")),
        AddData(inputData, InputRow("k3", "append", "v5")),
        CheckNewAnswer(("k3", "v4")),
        AddData(inputData, InputRow("k3", "emitAllInState", "")),
        CheckNewAnswer(("k3", "v1"), ("k3", "v2"), ("k3", "v3"), ("k3", "v5")),
        // check removal cleans up all data in state
        AddData(inputData, InputRow("k4", "append", "v2")),
        AddData(inputData, InputRow("k4", "appendList", "v3,v4")),
        AddData(inputData, InputRow("k4", "remove", "")),
        CheckNewAnswer(),
        // check put cleans up previous state and adds new state
        AddData(inputData, InputRow("k5", "appendAll", "v1,v2,v3")),
        AddData(inputData, InputRow("k5", "append", "v4")),
        AddData(inputData, InputRow("k5", "put", "v5,v6")),
        AddData(inputData, InputRow("k5", "emitAllInState", "")),
        CheckNewAnswer(("k5", "v5"), ("k5", "v6"))
      )
    }
  }
}
