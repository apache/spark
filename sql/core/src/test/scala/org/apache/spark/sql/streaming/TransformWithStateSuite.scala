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
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf

class RunningCountStatefulProcessor extends StatefulProcessor[String, String, (String, String)] {
  @transient private var _countState: ValueState[Long] = _
  @transient private var _processorHandle: StatefulProcessorHandle = _

  override def init(handle: StatefulProcessorHandle,
    outputMode: OutputMode) : Unit = {
    _processorHandle = handle
    _countState = _processorHandle.getValueState[Long]("countState")
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + inputRows.size
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

class TransformWithStateSuite extends StateStoreMetricsTest {

  import testImplicits._

  test("transformWithState - batch") {
    val ex = intercept[Exception] {
      val df = Seq("a", "a", "b").toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor, OutputMode.Append())
        .write
        .format("noop")
        .mode(SaveMode.Append)
        .save()
    }
    assert(ex.isInstanceOf[AnalysisException])
    assert(ex.getMessage.contains("not supported"))
  }

  test("transformWithState - streaming") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS()
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(), OutputMode.Update())

    testStream(result, OutputMode.Update())(
      AddData(inputData, "a"),
      ExpectFailure[SparkException] {
        (t: Throwable) => { assert(t.getCause.getMessage.contains("not supported")) }
      }
    )
  }

  test("transformWithState - streaming with rocksdb") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(), OutputMode.Update())

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
}
