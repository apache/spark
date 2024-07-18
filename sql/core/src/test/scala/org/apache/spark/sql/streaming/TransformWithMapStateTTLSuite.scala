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
import org.apache.spark.sql.execution.streaming.{MapStateImplWithTTL, MemoryStream}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

class MapStateSingleKeyTTLProcessor(ttlConfig: TTLConfig)
  extends StatefulProcessor[String, InputEvent, OutputEvent] {

  @transient private var _mapState: MapStateImplWithTTL[String, Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
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
      val ttlValue = mapState.getTTLValue(userKey)
      if (ttlValue.isDefined) {
        val value = ttlValue.get._1
        val ttlExpiration = ttlValue.get._2
        results = OutputEvent(key, value, isTTLValue = true, ttlExpiration) :: results
      }
    } else if (row.action == "put") {
      mapState.updateValue(userKey, row.value)
    } else if (row.action == "get_values_in_ttl_state") {
      val ttlValues = mapState.getKeyValuesInTTLState()
      ttlValues.foreach { v =>
        results = OutputEvent(key, -1, isTTLValue = true, ttlValue = v._2) :: results
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
  extends StatefulProcessor[String, MapInputEvent, MapOutputEvent] {

  @transient private var _mapState: MapStateImplWithTTL[String, Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
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
      val ttlValue = mapState.getTTLValue(userKey)
      if (ttlValue.isDefined) {
        val value = ttlValue.get._1
        val ttlExpiration = ttlValue.get._2
        results = MapOutputEvent(key, userKey, value, isTTLValue = true, ttlExpiration) :: results
      }
    } else if (row.action == "put") {
      mapState.updateValue(userKey, row.value)
    } else if (row.action == "get_values_in_ttl_state") {
      val ttlValues = mapState.getKeyValuesInTTLState()
      ttlValues.foreach { elem =>
        results = MapOutputEvent(key, elem._1, -1, isTTLValue = true, ttlValue = elem._2) :: results
      }
    } else if (row.action == "iterator") {
      val iter = mapState.iterator()
      iter.foreach { elem =>
        results = MapOutputEvent(key, elem._1, elem._2, isTTLValue = false, -1) :: results
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

  override def getStateTTLMetricName: String = "numMapStateWithTTLVars"

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
          TimeMode.ProcessingTime(),
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

  test("verify iterator doesn't return expired keys") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {

      val inputStream = MemoryStream[MapInputEvent]
      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          new MapStateTTLProcessor(ttlConfig),
          TimeMode.ProcessingTime(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream,
          MapInputEvent("k1", "key1", "put", 1),
          MapInputEvent("k1", "key2", "put", 2)
        ),
        AdvanceManualClock(1 * 1000), // batch timestamp: 1000
        CheckNewAnswer(),
        AddData(inputStream,
          MapInputEvent("k1", "key1", "get", -1),
          MapInputEvent("k1", "key2", "get", -1)
        ),
        AdvanceManualClock(30 * 1000), // batch timestamp: 31000
        CheckNewAnswer(
          MapOutputEvent("k1", "key1", 1, isTTLValue = false, -1),
          MapOutputEvent("k1", "key2", 2, isTTLValue = false, -1)
        ),
        // get values from ttl state
        AddData(inputStream,
          MapInputEvent("k1", "", "get_values_in_ttl_state", -1)
        ),
        AdvanceManualClock(1 * 1000), // batch timestamp: 32000
        CheckNewAnswer(
          MapOutputEvent("k1", "key1", -1, isTTLValue = true, 61000),
          MapOutputEvent("k1", "key2", -1, isTTLValue = true, 61000)
        ),
        // advance clock to expire first two values
        AdvanceManualClock(30 * 1000), // batch timestamp: 62000
        AddData(inputStream,
          MapInputEvent("k1", "key3", "put", 3),
          MapInputEvent("k1", "key4", "put", 4),
          MapInputEvent("k1", "key5", "put", 5),
          MapInputEvent("k1", "", "iterator", -1)
        ),
        AdvanceManualClock(1 * 1000), // batch timestamp: 63000
        CheckNewAnswer(
          MapOutputEvent("k1", "key3", 3, isTTLValue = false, -1),
          MapOutputEvent("k1", "key4", 4, isTTLValue = false, -1),
          MapOutputEvent("k1", "key5", 5, isTTLValue = false, -1)
        ),
        AddData(inputStream,
          MapInputEvent("k1", "", "get_values_in_ttl_state", -1)
        ),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          MapOutputEvent("k1", "key3", -1, isTTLValue = true, 123000),
          MapOutputEvent("k1", "key4", -1, isTTLValue = true, 123000),
          MapOutputEvent("k1", "key5", -1, isTTLValue = true, 123000)
        ),
        // get all values without enforcing ttl
        AddData(inputStream,
          MapInputEvent("k1", "key1", "get_without_enforcing_ttl", -1),
          MapInputEvent("k1", "key2", "get_without_enforcing_ttl", -1),
          MapInputEvent("k1", "key3", "get_without_enforcing_ttl", -1),
          MapInputEvent("k1", "key4", "get_without_enforcing_ttl", -1),
          MapInputEvent("k1", "key5", "get_without_enforcing_ttl", -1)
        ),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          MapOutputEvent("k1", "key3", 3, isTTLValue = false, -1),
          MapOutputEvent("k1", "key4", 4, isTTLValue = false, -1),
          MapOutputEvent("k1", "key5", 5, isTTLValue = false, -1)
        ),
        // check that updating a key updates its TTL
        AddData(inputStream, MapInputEvent("k1", "key3", "put", 3)),
        AdvanceManualClock(1 * 1000),
        AddData(inputStream, MapInputEvent("k1", "", "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          MapOutputEvent("k1", "key3", -1, isTTLValue = true, 123000),
          MapOutputEvent("k1", "key3", -1, isTTLValue = true, 126000),
          MapOutputEvent("k1", "key4", -1, isTTLValue = true, 123000),
          MapOutputEvent("k1", "key5", -1, isTTLValue = true, 123000)
        ),
        AddData(inputStream, MapInputEvent("k1", "key3", "get_ttl_value_from_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          MapOutputEvent("k1", "key3", 3, isTTLValue = true, 126000)
        ),
        StopStream
      )
    }
  }
}
