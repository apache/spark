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

import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

case class InputEvent(
    key: String,
    action: String,
    value: Int,
    eventTime: Timestamp = null)

case class OutputEvent(
    key: String,
    value: Int,
    isTTLValue: Boolean,
    ttlValue: Long)

/**
 * Test suite base for TransformWithState with TTL support.
 */
abstract class TransformWithStateTTLTest
  extends StreamTest {
  import testImplicits._

  def getProcessor(ttlConfig: TTLConfig): StatefulProcessor[String, InputEvent, OutputEvent]

  def getStateTTLMetricName: String

  test("validate state is evicted at ttl expiry") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { dir =>
        val inputStream = MemoryStream[InputEvent]
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        val result = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            getProcessor(ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Append())

        val clock = new StreamManualClock
        testStream(result)(
          StartStream(
            Trigger.ProcessingTime("1 second"),
            triggerClock = clock,
            checkpointLocation = dir.getAbsolutePath),
          AddData(inputStream, InputEvent("k1", "put", 1)),
          // advance clock to trigger processing
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          StopStream,
          StartStream(
            Trigger.ProcessingTime("1 second"),
            triggerClock = clock,
            checkpointLocation = dir.getAbsolutePath),
          // get this state, and make sure we get unexpired value
          AddData(inputStream, InputEvent("k1", "get", -1)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
          StopStream,
          StartStream(
            Trigger.ProcessingTime("1 second"),
            triggerClock = clock,
            checkpointLocation = dir.getAbsolutePath),
          // ensure ttl values were added correctly
          AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = true, 61000)),
          AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
          StopStream,
          StartStream(
            Trigger.ProcessingTime("1 second"),
            triggerClock = clock,
            checkpointLocation = dir.getAbsolutePath),
          // advance clock so that state expires
          AdvanceManualClock(60 * 1000),
          AddData(inputStream, InputEvent("k1", "get", -1, null)),
          AdvanceManualClock(1 * 1000),
          // validate expired value is not returned
          CheckNewAnswer(),
          // ensure this state does not exist any longer in State
          AddData(inputStream, InputEvent("k1", "get_without_enforcing_ttl", -1)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1)),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          Execute { q =>
            // Filter for idle progress events and then verify the custom metrics
            // for stateful operator
            val progData = q.recentProgress.filter(prog => prog.stateOperators.size > 0)
            assert(progData.filter(prog =>
              prog.stateOperators(0).customMetrics.get(getStateTTLMetricName) > 0).size > 0)
            assert(progData.filter(prog =>
              prog.stateOperators(0).customMetrics
                .get("numValuesRemovedDueToTTLExpiry") > 0).size > 0)
          }
        )
      }
    }
  }

  test("validate state update updates the expiration timestamp") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputStream = MemoryStream[InputEvent]
      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
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
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // get this state, and make sure we get unexpired value
        AddData(inputStream, InputEvent("k1", "get", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // ensure ttl values were added correctly
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = true, 61000)),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
        // advance clock and update expiration time
        AdvanceManualClock(30 * 1000),
        AddData(inputStream, InputEvent("k1", "put", 1)),
        AddData(inputStream, InputEvent("k1", "get", -1)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        // validate value is not expired
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // validate ttl value is updated in the state
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = true, 95000)),
        // validate ttl state has both ttl values present
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000),
          OutputEvent("k1", -1, isTTLValue = true, 95000)
        ),
        // advance clock after older expiration value
        AdvanceManualClock(30 * 1000),
        // ensure unexpired value is still present in the state
        AddData(inputStream, InputEvent("k1", "get", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // validate that the older expiration value is removed from ttl state
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 95000))
      )
    }
  }

  test("validate state is evicted at ttl expiry for no data batch") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
    classOf[RocksDBStateStoreProvider].getName) {
      val inputStream = MemoryStream[InputEvent]
      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          getProcessor(ttlConfig),
          TimeMode.ProcessingTime(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(
          Trigger.ProcessingTime("1 second"),
          triggerClock = clock),
        AddData(inputStream, InputEvent("k1", "put", 1)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // get this state, and make sure we get unexpired value
        AddData(inputStream, InputEvent("k1", "get", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = false, -1)),
        // ensure ttl values were added correctly
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", 1, isTTLValue = true, 61000)),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent("k1", -1, isTTLValue = true, 61000)),
        // advance clock so that state expires
        AdvanceManualClock(60 * 1000),
        // run a no data batch
        CheckNewAnswer(),
        AddData(inputStream, InputEvent("k1", "get", -1)),
        AdvanceManualClock(1 * 1000),
        // validate expired value is not returned
        CheckNewAnswer(),
        // ensure this state does not exist any longer in State
        AddData(inputStream, InputEvent("k1", "get_without_enforcing_ttl", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer()
      )
    }
  }

  test("validate only expired keys are removed from the state") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputStream = MemoryStream[InputEvent]
      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
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
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // advance clock halfway to expiration ttl, and add another key
        AdvanceManualClock(30 * 1000),
        AddData(inputStream, InputEvent("k2", "put", 2)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // advance clock so that key k1 is expired
        AdvanceManualClock(30 * 1000),
        AddData(inputStream, InputEvent("k1", "get", 1)),
        AddData(inputStream, InputEvent("k2", "get", -1)),
        AdvanceManualClock(1 * 1000),
        // validate k1 is expired and k2 is not
        CheckNewAnswer(OutputEvent("k2", 2, isTTLValue = false, -1)),
        // validate k1 is deleted from state
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1)),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // validate k2 exists in state
        AddData(inputStream, InputEvent("k2", "get_ttl_value_from_state", -1)),
        AddData(inputStream, InputEvent("k2", "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          OutputEvent("k2", 2, isTTLValue = true, 92000),
          OutputEvent("k2", -1, isTTLValue = true, 92000))
      )
    }
  }
}
