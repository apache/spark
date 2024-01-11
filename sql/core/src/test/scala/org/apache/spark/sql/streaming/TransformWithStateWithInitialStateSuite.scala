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
import org.apache.spark.sql.{AnalysisException, KeyValueGroupedDataset, SaveMode}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf

case class InputRow(key: String, action: String, value: Double)

class StatefulProcessorWithInitialStateTestClass
  extends StatefulProcessorWithInitialState[String,
    InputRow, (String, String, Double), (String, Double)] {
  @transient var _valState: ValueState[Double] = _
  @transient var _processorHandle: StatefulProcessorHandle = _

  override def handleInitialState(key: String,
     initialState: (String, Double)): Unit = {
    _valState.update(initialState._2)
  }

  override def init(processorHandle: StatefulProcessorHandle,
     operatorOutputMode: OutputMode): Unit = {
    _processorHandle = processorHandle
    _valState = _processorHandle.getValueState[Double]("testInit")
  }

  override def close(): Unit = {}

  override def handleInputRows(key: String,
     inputRows: Iterator[InputRow],
     timerValues: TimerValues): Iterator[(String, String, Double)] = {
    var output = List[(String, String, Double)]()
    for (row <- inputRows) {
      if (row.action == "getOption") {
        output = (key, row.action, _valState.getOption().getOrElse(-1.0)) :: output
      } else if (row.action == "update") {
        _valState.update(row.value)
      } else if (row.action == "remove") {
        _valState.remove()
      }
    }
    output.iterator
  }
}

class AccumulateStatefulProcessorWithInitState extends StatefulProcessorWithInitialStateTestClass {
  override def handleInputRows(key: String,
     inputRows: Iterator[InputRow],
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
        _valState.remove()
      }
    }
    output.iterator
  }
}

class StatefulProcessorWithInitialStateSuite extends StreamTest {
  import testImplicits._

  private def createInitialDfForTest: KeyValueGroupedDataset[String, (String, Double)] = {
    Seq(("init_1", 40.0), ("init_2", 100.0)).toDS()
      .groupByKey(x => x._1)
      .mapValues(x => x)
  }

  test ("transformWithStateWithInitialState - streaming with rocksdb should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val initStateDf = createInitialDfForTest

      val inputData = MemoryStream[InputRow]
      val query = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new
            StatefulProcessorWithInitialStateTestClass(),
          TimeoutMode.NoTimeouts(), OutputMode.Append(), initStateDf
        )

      testStream(query, OutputMode.Update())(
        // Operations in the base class will work
        AddData(inputData, InputRow("k1", "update", 37.0)),
        AddData(inputData, InputRow("k2", "update", 40.0)),
        AddData(inputData, InputRow("non-exist", "getOption", -1.0)),
        CheckNewAnswer(("non-exist", "getOption", -1.0)),
        AddData(inputData, InputRow("k1", "remove", -1.0)),
        AddData(inputData, InputRow("k1", "getOption", -1.0)),
        CheckNewAnswer(("k1", "getOption", -1.0)),

        // Every row in initial State is processed
        AddData(inputData, InputRow("init_1", "getOption", -1.0)),
        CheckNewAnswer(("init_1", "getOption", 40.0)),
        AddData(inputData, InputRow("init_2", "getOption", -1.0)),
        CheckNewAnswer(("init_2", "getOption", 100.0)),
        // Update row with key in initial row will work
        AddData(inputData, InputRow("init_1", "update", 50.0)),
        AddData(inputData, InputRow("init_1", "getOption", -1.0)),
        CheckNewAnswer(("init_1", "getOption", 50.0)),
        AddData(inputData, InputRow("init_1", "remove", -1.0)),
        AddData(inputData, InputRow("init_1", "getOption", -1.0)),
        CheckNewAnswer(("init_1", "getOption", -1.0))
      )
    }
  }

  test("transformWithStateWithInitialState - processInitialState should only run once") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val initStateDf = createInitialDfForTest

      val inputData = MemoryStream[InputRow]
      val query = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new
            AccumulateStatefulProcessorWithInitState(),
          TimeoutMode.NoTimeouts(), OutputMode.Append(), initStateDf
        )

      testStream(query, OutputMode.Update())(
        AddData(inputData, InputRow("init_1", "add", 50.0)),
        AddData(inputData, InputRow("init_2", "add", 60.0)),
        AddData(inputData, InputRow("init_1", "add", 50.0)),

        // If processInitialState was processed multiple times,
        // following checks will fail
        AddData(inputData, InputRow("init_1", "getOption", -1.0)),
        CheckNewAnswer(("init_1", "getOption", 140.0)),
        AddData(inputData, InputRow("init_2", "getOption", -1.0)),
        CheckNewAnswer(("init_2", "getOption", 160.0))
      )
    }
  }

  test("transformWithStateWithInitialState - batch should fail") {
    val ex = intercept[Exception] {
      val initStateDf = createInitialDfForTest
      val df = Seq(InputRow("a", "b", -1.0)).toDS()
        .groupByKey(x => x.key)
        .transformWithState(new
            StatefulProcessorWithInitialStateTestClass(),
          TimeoutMode.NoTimeouts(), OutputMode.Append(), initStateDf
        )
        .write
        .format("noop")
        .mode(SaveMode.Append)
        .save()
    }
    assert(ex.isInstanceOf[AnalysisException])
    assert(ex.getMessage.contains("not supported"))
  }

  test("transformWithStateWithInitialState - streaming with hdfsStateStoreProvider should fail") {
    val inputData = MemoryStream[InputRow]
    val result = inputData.toDS()
      .groupByKey(x => x.key)
      .transformWithState(new
          StatefulProcessorWithInitialStateTestClass(),
        TimeoutMode.NoTimeouts(), OutputMode.Append(), createInitialDfForTest
      )

    testStream(result, OutputMode.Update())(
      AddData(inputData, InputRow("a", "update", -1.0)),
      ExpectFailure[SparkException] {
        (t: Throwable) => {
          assert(t.getCause.getMessage.contains("not supported"))
        }
      }
    )
  }
}
