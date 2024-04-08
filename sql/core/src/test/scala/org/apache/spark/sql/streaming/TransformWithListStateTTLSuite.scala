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
import org.apache.spark.sql.execution.streaming.{ListStateImplWithTTL, MemoryStream}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

class ListStateTTLProcessor(ttlConfig: TTLConfig)
  extends StatefulProcessor[String, InputEvent, OutputEvent]
    with Logging {

  @transient private var _listState: ListStateImplWithTTL[Int] = _

  override def init(
     outputMode: OutputMode,
     timeoutMode: TimeoutMode,
     ttlMode: TTLMode): Unit = {
    _listState = getHandle
      .getListState("listState", Encoders.scalaInt, ttlConfig)
      .asInstanceOf[ListStateImplWithTTL[Int]]
  }

  override def handleInputRows(
    key: String,
    inputRows: Iterator[InputEvent],
    timerValues: TimerValues,
    expiredTimerInfo: ExpiredTimerInfo): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()

    inputRows.foreach { row =>
      val resultIter = processRow(row, _listState)
      resultIter.foreach { r =>
        results = r :: results
      }
    }

    results.iterator
  }

  def processRow(
    row: InputEvent,
    listState: ListStateImplWithTTL[Int]): Iterator[OutputEvent] = {

    var results = List[OutputEvent]()
    val key = row.key
    if (row.action == "get") {
      val currState = listState.get()
      currState.foreach { v =>
        results = OutputEvent(key, v, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_without_enforcing_ttl") {
      val currState = listState.getWithoutEnforcingTTL()
      currState.foreach { v =>
        results = OutputEvent(key, v, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_ttl_value_from_state") {
      val ttlExpiration = listState.getTTLValues()
      ttlExpiration.filter(_.isDefined).foreach { ttlExpiration =>
        results = OutputEvent(key, -1, isTTLValue = true, ttlExpiration.get) :: results
      }
    } else if (row.action == "put") {
      listState.put(Array(row.value))
    } else if (row.action == "append") {
      listState.appendValue(row.value)
    } else if (row.action == "get_values_in_ttl_state") {
      val ttlValues = listState.getValuesInTTLState()
      ttlValues.foreach { v =>
        results = OutputEvent(key, -1, isTTLValue = true, ttlValue = v) :: results
      }
    }

    results.iterator
  }
}
class TransformWithListStateTTLSuite extends TransformWithStateTTLTest {

  import testImplicits._
  override def getProcessor(ttlConfig: TTLConfig):
    StatefulProcessor[String, InputEvent, OutputEvent] = {
    new ListStateTTLProcessor(ttlConfig)
  }

  test("verify iterator works with expired values in middle of list") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { checkpointLocation =>
        val ttlConfig1 = TTLConfig(ttlDuration = Duration.ofMinutes(3))
        val inputStream = MemoryStream[InputEvent]
        val result1 = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            getProcessor(ttlConfig1),
            TimeoutMode.NoTimeouts(),
            TTLMode.ProcessingTimeTTL(),
            OutputMode.Append())

        val clock = new StreamManualClock
        // add 3 elements with a duration of a minute
        testStream(result1)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = checkpointLocation.getAbsolutePath),
          AddData(inputStream, InputEvent("k1", "put", 1)),
          AdvanceManualClock(1 * 1000),
          AddData(inputStream, InputEvent("k1", "append", 2)),
          AddData(inputStream, InputEvent("k1", "append", 3)),
          // advance clock to trigger processing
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          // get ttl values
          AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", -1, isTTLValue = true, 181000),
            OutputEvent("k1", -1, isTTLValue = true, 182000),
            OutputEvent("k1", -1, isTTLValue = true, 182000)
          ),
          AddData(inputStream, InputEvent("k1", "get", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", 1, isTTLValue = false, -1),
            OutputEvent("k1", 2, isTTLValue = false, -1),
            OutputEvent("k1", 3, isTTLValue = false, -1)
          ),
          StopStream
        )

        val ttlConfig2 = TTLConfig(ttlDuration = Duration.ofSeconds(15))
        val result2 = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            getProcessor(ttlConfig2),
            TimeoutMode.NoTimeouts(),
            TTLMode.ProcessingTimeTTL(),
            OutputMode.Append())
        // add 3 elements with a duration of 15 seconds
        testStream(result2)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = checkpointLocation.getAbsolutePath),
          AddData(inputStream, InputEvent("k1", "append", 4)),
          AddData(inputStream, InputEvent("k1", "append", 5)),
          AddData(inputStream, InputEvent("k1", "append", 6)),
          // advance clock to trigger processing
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          // get all elements without enforcing ttl
          AddData(inputStream, InputEvent("k1", "get_without_enforcing_ttl", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", 1, isTTLValue = false, -1),
            OutputEvent("k1", 2, isTTLValue = false, -1),
            OutputEvent("k1", 3, isTTLValue = false, -1),
            OutputEvent("k1", 4, isTTLValue = false, -1),
            OutputEvent("k1", 5, isTTLValue = false, -1),
            OutputEvent("k1", 6, isTTLValue = false, -1)
          ),
          StopStream
        )
        // add 3 more elements with a duration of a minute
        testStream(result1)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = checkpointLocation.getAbsolutePath),
          AddData(inputStream, InputEvent("k1", "append", 7)),
          AddData(inputStream, InputEvent("k1", "append", 8)),
          AddData(inputStream, InputEvent("k1", "append", 9)),
          // advance clock to trigger processing
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          // advance clock to expire the middle three elements
          AdvanceManualClock(45 * 1000),
          // Get all elements in the list
          AddData(inputStream, InputEvent("k1", "get", -1, null)),
          AdvanceManualClock(1 * 1000),
          // validate that the expired elements are not returned
          CheckNewAnswer(
            OutputEvent("k1", 1, isTTLValue = false, -1),
            OutputEvent("k1", 2, isTTLValue = false, -1),
            OutputEvent("k1", 3, isTTLValue = false, -1),
            OutputEvent("k1", 7, isTTLValue = false, -1),
            OutputEvent("k1", 8, isTTLValue = false, -1),
            OutputEvent("k1", 9, isTTLValue = false, -1)
          ),
          StopStream
        )
      }
    }
  }


  test("verify iterator works with expired values in beginning of list") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { checkpointLocation =>
        val inputStream = MemoryStream[InputEvent]
        val ttlConfig1 = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        val result1 = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            getProcessor(ttlConfig1),
            TimeoutMode.NoTimeouts(),
            TTLMode.ProcessingTimeTTL(),
            OutputMode.Append())

        val ttlConfig2 = TTLConfig(ttlDuration = Duration.ofMinutes(2))
        val result2 = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            getProcessor(ttlConfig2),
            TimeoutMode.NoTimeouts(),
            TTLMode.ProcessingTimeTTL(),
            OutputMode.Append())

        val clock = new StreamManualClock
        // add 3 elements with a duration of a minute
        testStream(result1)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = checkpointLocation.getAbsolutePath),
          AddData(inputStream, InputEvent("k1", "put", 1)),
          AdvanceManualClock(1 * 1000),
          AddData(inputStream, InputEvent("k1", "append", 2)),
          AddData(inputStream, InputEvent("k1", "append", 3)),
          // advance clock to trigger processing
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          // get ttl values
          AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", -1, isTTLValue = true, 61000),
            OutputEvent("k1", -1, isTTLValue = true, 62000),
            OutputEvent("k1", -1, isTTLValue = true, 62000)
          ),
          AddData(inputStream, InputEvent("k1", "get", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", 1, isTTLValue = false, -1),
            OutputEvent("k1", 2, isTTLValue = false, -1),
            OutputEvent("k1", 3, isTTLValue = false, -1)
          ),
          StopStream
        )

        // add 3 elements with a duration of two minutes
        testStream(result2)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = checkpointLocation.getAbsolutePath),
          AddData(inputStream, InputEvent("k1", "append", 4)),
          AddData(inputStream, InputEvent("k1", "append", 5)),
          AddData(inputStream, InputEvent("k1", "append", 6)),
          // advance clock to trigger processing
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          // get ttl values
          AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", -1, isTTLValue = true, 61000),
            OutputEvent("k1", -1, isTTLValue = true, 62000),
            OutputEvent("k1", -1, isTTLValue = true, 62000),
            OutputEvent("k1", -1, isTTLValue = true, 125000),
            OutputEvent("k1", -1, isTTLValue = true, 125000),
            OutputEvent("k1", -1, isTTLValue = true, 125000)
          ),
          // expire beginning values
          AdvanceManualClock(60 * 1000),
          AddData(inputStream, InputEvent("k1", "get", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", 4, isTTLValue = false, -1),
            OutputEvent("k1", 5, isTTLValue = false, -1),
            OutputEvent("k1", 6, isTTLValue = false, -1)
          ),
          StopStream
        )
      }
    }
  }
}
