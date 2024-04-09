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
import org.apache.spark.sql.execution.streaming.{MapStateImplWithTTL, MemoryStream}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

class MapStateSingleKeyTTLProcessor(ttlConfig: TTLConfig)
  extends StatefulProcessor[String, InputEvent, OutputEvent]
    with Logging {

  @transient private var _mapState: MapStateImplWithTTL[String, Int] = _

  override def init(
     outputMode: OutputMode,
     timeoutMode: TimeoutMode,
     ttlMode: TTLMode): Unit = {
    _mapState = getHandle
      .getMapState("mapState", Encoders.STRING, Encoders.scalaInt, ttlConfig)
      .asInstanceOf[MapStateImplWithTTL[String, Int]]
  }
  override def handleInputRows(
    key: String,
    inputRows: Iterator[InputEvent],
    timerValues: TimerValues,
    expiredTimerInfo: ExpiredTimerInfo): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()

    for (row <- inputRows) {
      val resultIter = processRow(row, _mapState)
      resultIter.foreach { r =>
        results = r :: results
      }
    }

    results.iterator
  }

  def processRow(
    row: InputEvent,
    mapState: MapStateImplWithTTL[String, Int]): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()
    val key = row.key
    val userKey = "key"
    if (row.action == "get") {
      if (mapState.containsKey(userKey)) {
        results = OutputEvent(key, mapState.getValue(userKey), isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_without_enforcing_ttl") {
      val currState = mapState.getWithoutEnforcingTTL(userKey)
      if (currState.isDefined) {
        results = OutputEvent(key, currState.get, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_ttl_value_from_state") {
      val ttlExpiration = mapState.getTTLValue(userKey)
      if (ttlExpiration.isDefined) {
        results = OutputEvent(key, -1, isTTLValue = true, ttlExpiration.get) :: results
      }
    } else if (row.action == "put") {
      mapState.updateValue(userKey, row.value)
    } else if (row.action == "get_values_in_ttl_state") {
      val ttlValues = mapState.getValuesInTTLState()
      ttlValues.foreach { v =>
        results = OutputEvent(key, -1, isTTLValue = true, ttlValue = v) :: results
      }
    }

    results.iterator
  }
}


case class MapInputEvent(
  key: String,
  userKey: String,
  action: String,
  value: Int)

case class MapOutputEvent(
   key: String,
   userKey: String,
   value: Int,
   isTTLValue: Boolean,
   ttlValue: Long)


class MapStateTTLProcessor(ttlConfig: TTLConfig)
  extends StatefulProcessor[String, MapInputEvent, MapOutputEvent]
    with Logging {

  @transient private var _mapState: MapStateImplWithTTL[String, Int] = _

  override def init(
     outputMode: OutputMode,
     timeoutMode: TimeoutMode,
     ttlMode: TTLMode): Unit = {
    _mapState = getHandle
      .getMapState("mapState", Encoders.STRING, Encoders.scalaInt, ttlConfig)
      .asInstanceOf[MapStateImplWithTTL[String, Int]]
  }

  override def handleInputRows(
    key: String,
    inputRows: Iterator[MapInputEvent],
    timerValues: TimerValues,
    expiredTimerInfo: ExpiredTimerInfo): Iterator[MapOutputEvent] = {
    var results = List[MapOutputEvent]()

    for (row <- inputRows) {
      val resultIter = processRow(row, _mapState)
      resultIter.foreach { r =>
        results = r :: results
      }
    }

    results.iterator
  }

  def processRow(
    row: MapInputEvent,
    mapState: MapStateImplWithTTL[String, Int]): Iterator[MapOutputEvent] = {
    var results = List[MapOutputEvent]()
    val key = row.key
    val userKey = row.userKey
    if (row.action == "get") {
      if (mapState.containsKey(userKey)) {
        results = MapOutputEvent(key, userKey, mapState.getValue(userKey),
          isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_without_enforcing_ttl") {
      val currState = mapState.getWithoutEnforcingTTL(userKey)
      if (currState.isDefined) {
        results = MapOutputEvent(key, userKey, currState.get, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_ttl_value_from_state") {
      val ttlExpiration = mapState.getTTLValue(userKey)
      if (ttlExpiration.isDefined) {
        results = MapOutputEvent(key, userKey, -1, isTTLValue = true, ttlExpiration.get) :: results
      }
    } else if (row.action == "put") {
        mapState.updateValue(userKey, row.value)
    } else if (row.action == "get_values_in_ttl_state") {
      val ttlValues = mapState.getValuesInTTLState()
      ttlValues.foreach { v =>
        results = MapOutputEvent(key, userKey, -1, isTTLValue = true, ttlValue = v) :: results
      }
    }

    results.iterator
  }
}

class TransformWithMapStateTTLSuite extends TransformWithStateTTLTest {

  import testImplicits._
  override def getProcessor(ttlConfig: TTLConfig):
  StatefulProcessor[String, InputEvent, OutputEvent] = {
    new MapStateSingleKeyTTLProcessor(ttlConfig)
  }

  test("validate state is evicted with multiple user keys") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {

      val inputStream = MemoryStream[MapInputEvent]
      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          new MapStateTTLProcessor(ttlConfig),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream, MapInputEvent("k1", "key1", "put", 1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        AddData(inputStream, MapInputEvent("k1", "key1", "get", -1)),
        AdvanceManualClock(30 * 1000),
        CheckNewAnswer(MapOutputEvent("k1", "key1", 1, isTTLValue = false, -1)),
        AddData(inputStream, MapInputEvent("k1", "key2", "put", 2)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // advance clock to expire first key
        AdvanceManualClock(30 * 1000),
        AddData(inputStream, MapInputEvent("k1", "key1", "get", -1),
          MapInputEvent("k1", "key2", "get", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(MapOutputEvent("k1", "key2", 2, isTTLValue = false, -1)),
        StopStream
      )
    }
  }
}
