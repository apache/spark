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

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf

case class InputRow(key: String, action: String, value: String)

case class ProcessorInitialState(
    sensorId: String,
    maxTemperature: Double,
    numViolations: Long
)

class TemperatureAlertStatefulProcessorWithInitialState(
   val threshold: Double,
   val allowedViolations: Long)
  extends StatefulProcessorWithInitialState[String,
    InputRow, (String, Double, Long), (String, String)] {
  @transient private var _maxTemperatureState: ValueState[Double] = _
  @transient private var _numViolationsState: ValueState[Long] = _
  @transient private var _processorHandle: StatefulProcessorHandle = _

  override def handleInitialState(key: String,
     initialState: (String, String)): Unit = {
    println(s"I am here in handleiniti: ${initialState._1}, ${initialState._2}")
    _maxTemperatureState.update(1.0)
  }

  override def init(processorHandle: StatefulProcessorHandle,
                    operatorOutputMode: OutputMode): Unit = {
    _processorHandle = processorHandle
    _maxTemperatureState = _processorHandle.getValueState[Double]("testInit")
  }

  override def close(): Unit = {}

  override def handleInputRows(key: String,
     inputRows: Iterator[InputRow],
     timerValues: TimerValues): Iterator[(String, Double, Long)] = {
    Iterator((key, 1.0, 1L))
  }
}

class StatefulProcessorWithInitialStateSuite extends StreamTest {
  import testImplicits._

  test("some test") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val initInputData: Seq[(String, String)] = Seq(
        ("sensor_1", "102"),
        ("sensor_2", "101"))

      val initStateDf = initInputData.toDS()
        .groupByKey(x => x._1)
        .mapValues(x => x)

      val inputData = MemoryStream[InputRow]
      val query = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new
            TemperatureAlertStatefulProcessorWithInitialState(
              100.0, 5),
          TimeoutMode.NoTimeouts(), OutputMode.Append(), initStateDf
        )

      testStream(query, OutputMode.Update())(
        AddData(inputData, InputRow("k1", "ac", "v1")),
        CheckAnswer(("k1", 1.0, 1L))
      )
    }
  }
}
