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

import java.time.Duration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

case class InputEvent(
    key: String,
    action: String,
    value: Int,
    ttl: Duration)

case class OutputEvent(
    key: String,
    exists: Boolean,
    value: Int)

class ValueStateTTLProcessor
  extends StatefulProcessor[String, InputEvent, OutputEvent]
  with Logging {

  @transient private var _valueState: ValueState[Int] = _

  override def init(outputMode: OutputMode, timeoutMode: TimeoutMode): Unit = {
    _valueState = getHandle.getValueState("valueState", Encoders.scalaInt)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InputEvent],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()

    for (row <- inputRows) {
      if (row.action == "get") {
        val currState = _valueState.getOption()

        if (currState.isDefined) {
          results = OutputEvent(key, exists = true, currState.get) :: results
        } else {
          results = OutputEvent(key, exists = false, -1) :: results
        }
      } else if (row.action == "put") {
        _valueState.update(row.value, row.ttl)
      }
    }

    results.iterator
  }
}

class TransformWithStateTTLSuite
  extends StreamTest {
  import testImplicits._

  test("validate state is evicted at ttl expiry") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          new ValueStateTTLProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream, InputEvent("k1", "put", 1, Duration.ofMinutes(1))),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // get this state
        AddData(inputStream, InputEvent("k1", "get", 1, null)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", exists = true, 1)),
        // advance clock so that state expires
        AdvanceManualClock(60 * 1000),
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        // validate state does not exist anymore
        CheckNewAnswer(OutputEvent("k1", exists = false, -1))
        // ensure this state does not exist any longer in State
      )
    }
  }
}
