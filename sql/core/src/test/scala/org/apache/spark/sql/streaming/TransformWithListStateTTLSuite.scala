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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.{ListStateImplWithTTL, MemoryStream}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

class ListStateTTLProcessor(ttlConfig: TTLConfig)
  extends StatefulProcessor[String, InputEvent, OutputEvent] {

  @transient private var _listState: ListStateImplWithTTL[Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
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
      val ttlValues = listState.getTTLValues()
      ttlValues.foreach { ttlValue =>
        results = OutputEvent(key, ttlValue._1, isTTLValue = true, ttlValue._2) :: results
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

/**
 * Test suite for testing list state with TTL.
 * We use the base TTL suite with a list state processor.
 */
class TransformWithListStateTTLSuite extends TransformWithStateTTLTest {

  import testImplicits._

  override def getProcessor(ttlConfig: TTLConfig):
    StatefulProcessor[String, InputEvent, OutputEvent] = {
      new ListStateTTLProcessor(ttlConfig)
  }

  override def getStateTTLMetricName: String = "numListStateWithTTLVars"

  test("verify iterator works with expired values in beginning of list") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {

      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          getProcessor(ttlConfig),
          TimeMode.ProcessingTime(),
          OutputMode.Append())
      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream, InputEvent("k1", "put", 1)),
        AdvanceManualClock(1 * 1000),
        AddData(inputStream,
          InputEvent("k1", "append", 2),
          InputEvent("k1", "append", 3)
        ),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // get ttl values
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          OutputEvent("k1", 1, isTTLValue = true, 61000),
          OutputEvent("k1", 2, isTTLValue = true, 62000),
          OutputEvent("k1", 3, isTTLValue = true, 62000)
        ),
        // advance clock to add elements with later TTL
        AdvanceManualClock(45 * 1000), // batch timestamp: 48000
        AddData(inputStream,
          InputEvent("k1", "append", 4),
          InputEvent("k1", "append", 5),
          InputEvent("k1", "append", 6)
        ),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // get ttl values
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          OutputEvent("k1", 1, isTTLValue = true, 61000),
          OutputEvent("k1", 2, isTTLValue = true, 62000),
          OutputEvent("k1", 3, isTTLValue = true, 62000),
          OutputEvent("k1", 4, isTTLValue = true, 109000),
          OutputEvent("k1", 5, isTTLValue = true, 109000),
          OutputEvent("k1", 6, isTTLValue = true, 109000)
        ),
        // advance clock to expire the first three elements
        AdvanceManualClock(15 * 1000), // batch timestamp: 65000
        AddData(inputStream, InputEvent("k1", "get", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          OutputEvent("k1", 4, isTTLValue = false, -1),
          OutputEvent("k1", 5, isTTLValue = false, -1),
          OutputEvent("k1", 6, isTTLValue = false, -1)
        ),
        // ensure that expired elements are no longer in state
        AddData(inputStream, InputEvent("k1", "get_without_enforcing_ttl", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          OutputEvent("k1", 4, isTTLValue = false, -1),
          OutputEvent("k1", 5, isTTLValue = false, -1),
          OutputEvent("k1", 6, isTTLValue = false, -1)
        ),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          OutputEvent("k1", -1, isTTLValue = true, 109000)
        )
      )
    }
  }

  // We can only change the TTL of a state variable upon query restart.
  // Therefore, only on query restart, will elements not be stored in
  // ascending order of TTL.
  // The following test cases will test the case where the elements are not stored in
  // ascending order of TTL by stopping the query, setting the new TTL, and restarting
  // the query to check that the expired elements in the middle or end of the list
  // are not returned.
  test("verify iterator works with expired values in middle of list") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { checkpointLocation =>
        // starting the query with a TTL of 3 minutes
        val ttlConfig1 = TTLConfig(ttlDuration = Duration.ofMinutes(3))
        val inputStream = MemoryStream[InputEvent]
        val result1 = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            getProcessor(ttlConfig1),
            TimeMode.ProcessingTime(),
            OutputMode.Append())

        val clock = new StreamManualClock
        // add 3 elements with a duration of 3 minutes
        // batch timestamp at the end of this block will be 4000
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
            OutputEvent("k1", 1, isTTLValue = true, 181000),
            OutputEvent("k1", 2, isTTLValue = true, 182000),
            OutputEvent("k1", 3, isTTLValue = true, 182000)
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

        // Here, we are restarting the query with a new TTL of 15 seconds
        // so that we can add elements to the middle of the list that will
        // expire quickly
        // batch timestamp at the end of this block will be 7000
        val ttlConfig2 = TTLConfig(ttlDuration = Duration.ofSeconds(15))
        val result2 = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            getProcessor(ttlConfig2),
            TimeMode.ProcessingTime(),
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
          AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", 1, isTTLValue = true, 181000),
            OutputEvent("k1", 2, isTTLValue = true, 182000),
            OutputEvent("k1", 3, isTTLValue = true, 182000),
            OutputEvent("k1", 4, isTTLValue = true, 20000),
            OutputEvent("k1", 5, isTTLValue = true, 20000),
            OutputEvent("k1", 6, isTTLValue = true, 20000)
          ),
          StopStream
        )

        // Restart the stream with the first TTL config to add elements to the end
        // with a TTL of 3 minutes
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
          AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", -1, isTTLValue = true, 20000),
            OutputEvent("k1", -1, isTTLValue = true, 181000),
            OutputEvent("k1", -1, isTTLValue = true, 182000),
            OutputEvent("k1", -1, isTTLValue = true, 188000)
          ),
          // progress batch timestamp from 9000 to 54000, expiring the middle
          // three elements.
          AdvanceManualClock(45 * 1000),
          // Get all elements in the list
          AddData(inputStream, InputEvent("k1", "get", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", 1, isTTLValue = false, -1),
            OutputEvent("k1", 2, isTTLValue = false, -1),
            OutputEvent("k1", 3, isTTLValue = false, -1),
            OutputEvent("k1", 7, isTTLValue = false, -1),
            OutputEvent("k1", 8, isTTLValue = false, -1),
            OutputEvent("k1", 9, isTTLValue = false, -1)
          ),
          AddData(inputStream, InputEvent("k1", "get_without_enforcing_ttl", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", 1, isTTLValue = false, -1),
            OutputEvent("k1", 2, isTTLValue = false, -1),
            OutputEvent("k1", 3, isTTLValue = false, -1),
            OutputEvent("k1", 7, isTTLValue = false, -1),
            OutputEvent("k1", 8, isTTLValue = false, -1),
            OutputEvent("k1", 9, isTTLValue = false, -1)
          ),
          AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", -1, isTTLValue = true, 181000),
            OutputEvent("k1", -1, isTTLValue = true, 182000),
            OutputEvent("k1", -1, isTTLValue = true, 188000)
          ),
          StopStream
        )
      }
    }
  }

  test("verify iterator works with expired values in end of list") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { checkpointLocation =>
        // first TTL config to start the query with a TTL of 2 minutes
        val inputStream = MemoryStream[InputEvent]
        val ttlConfig1 = TTLConfig(ttlDuration = Duration.ofMinutes(2))
        val result1 = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            getProcessor(ttlConfig1),
            TimeMode.ProcessingTime(),
            OutputMode.Append())

        // second TTL config we will use to start the query with a TTL of 1 minute
        val ttlConfig2 = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        val result2 = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            getProcessor(ttlConfig2),
            TimeMode.ProcessingTime(),
            OutputMode.Append())

        val clock = new StreamManualClock
        // add 3 elements with a duration of a minute
        // expected batch timestamp at the end of the stream is 4000
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
            OutputEvent("k1", 1, isTTLValue = true, 121000),
            OutputEvent("k1", 2, isTTLValue = true, 122000),
            OutputEvent("k1", 3, isTTLValue = true, 122000)
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

        // Here, we are restarting the query with a new TTL of 1 minutes
        // so that the elements at the end will expire before the beginning
        // batch timestamp at the end of this block will be 7000
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
            OutputEvent("k1", 1, isTTLValue = true, 121000),
            OutputEvent("k1", 2, isTTLValue = true, 122000),
            OutputEvent("k1", 3, isTTLValue = true, 122000),
            OutputEvent("k1", 4, isTTLValue = true, 65000),
            OutputEvent("k1", 5, isTTLValue = true, 65000),
            OutputEvent("k1", 6, isTTLValue = true, 65000)
          ),
          AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", -1, isTTLValue = true, 121000),
            OutputEvent("k1", -1, isTTLValue = true, 122000),
            OutputEvent("k1", -1, isTTLValue = true, 65000)
          ),
          // expire end values, batch timestamp from 7000 to 67000
          AdvanceManualClock(60 * 1000),
          AddData(inputStream, InputEvent("k1", "get", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", 1, isTTLValue = false, -1),
            OutputEvent("k1", 2, isTTLValue = false, -1),
            OutputEvent("k1", 3, isTTLValue = false, -1)
          ),
          AddData(inputStream, InputEvent("k1", "get_without_enforcing_ttl", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", 1, isTTLValue = false, -1),
            OutputEvent("k1", 2, isTTLValue = false, -1),
            OutputEvent("k1", 3, isTTLValue = false, -1)
          ),
          AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1, null)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(
            OutputEvent("k1", -1, isTTLValue = true, 121000),
            OutputEvent("k1", -1, isTTLValue = true, 122000)
          ),
          StopStream
        )
      }
    }
  }
}
