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

import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, MemoryStream, StatefulProcessorHandleImpl}
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateStore, StateStoreConf, StateStoreId, StateStoreProvider, StateStoreTestsHelper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, StructType}

case class InputMapRow(key: String, action: String, value: (String, String))

class TestMapStateProcessor
  extends StatefulProcessor[String, InputMapRow, (String, String, String)] {

  @transient var _mapState: MapState[String, String] = _

  override def init(outputMode: OutputMode): Unit = {
    _mapState = getHandle.getMapState("sessionState", Encoders.STRING)
  }

  override def handleInputRows(
    key: String,
    inputRows: Iterator[InputMapRow],
    timerValues: TimerValues): Iterator[(String, String, String)] = {

    var output = List[(String, String, String)]()

    for (row <- inputRows) {
      if (row.action == "exists") {
        output = (key, "exists", _mapState.exists().toString) :: output
      } else if (row.action == "getValue") {
        output = (key, row.value._1, _mapState.getValue(row.value._1)) :: output
      } else if (row.action == "containsKey") {
        output = (key, row.value._1,
          if (_mapState.containsKey(row.value._1)) "true" else "false") :: output
      } else if (row.action == "updateValue") {
        _mapState.updateValue(row.value._1, row.value._2)
      } else if (row.action == "getMap") {
        val res = _mapState.getMap()
        res.foreach { pair =>
          output = (key, pair._1, pair._2) :: output
        }
      } else if (row.action == "getKeys") {
        _mapState.getKeys().foreach { key =>
          output = (row.key, key, row.value._2) :: output
        }
      } else if (row.action == "getValues") {
        _mapState.getValues().foreach { value =>
          output = (row.key, row.value._1, value) :: output
        }
      } else if (row.action == "removeKey") {
        _mapState.removeKey(row.value._1)
      } else if (row.action == "clear") {
        _mapState.clear()
      }
    }
    output.iterator
  }

  override def close(): Unit = {}
}

/** Pure unit tests for MapState */
class MapStateSuite extends SharedSparkSession
  with BeforeAndAfter {

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  import StateStoreTestsHelper._

  val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)
  val schemaForValueRow: StructType = new StructType().add("value", BinaryType)

  private def newStoreProviderWithValueState(useColumnFamilies: Boolean):
  RocksDBStateStoreProvider = {
    newStoreProviderWithValueState(StateStoreId(newDir(), Random.nextInt(), 0),
      numColsPrefixKey = 0,
      useColumnFamilies = useColumnFamilies)
  }

  private def newStoreProviderWithValueState(
      storeId: StateStoreId,
      numColsPrefixKey: Int,
      sqlConf: SQLConf = SQLConf.get,
      conf: Configuration = new Configuration,
      useColumnFamilies: Boolean = false): RocksDBStateStoreProvider = {
    val provider = new RocksDBStateStoreProvider()
    provider.init(
      storeId, schemaForKeyRow, schemaForValueRow, numColsPrefixKey = numColsPrefixKey,
      useColumnFamilies,
      new StateStoreConf(sqlConf), conf)
    provider
  }

  private def tryWithProviderResource[T](
      provider: StateStoreProvider)(f: StateStoreProvider => T): T = {
    try {
      f(provider)
    } finally {
      provider.close()
    }
  }

  test("Map state operations for single instance") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val testState: MapState[String, Double] =
        handle.getMapState[String, Double]("testState", Encoders.STRING)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      // put initial value
      testState.updateValue("k1", 1.0)
      assert(testState.getValue("k1") === 1.0)
      // update existing value, append new key-value pairs
      testState.updateValue("k1", 1.0)
      testState.updateValue("k2", 2.0)
      assert(testState.getValue("k1") === 1.0)
      assert(testState.getValue("k2") === 2.0)
      testState.updateValue("k1", 3.0)
      assert(testState.getValue("k1") === 3.0)

      assert(testState.getKeys().toSeq === Seq("k1", "k2"))
      assert(testState.getValues().toSeq === Seq(3.0, 2.0))

      // test remove
      testState.removeKey("k1")
      assert(testState.getValue("k1") === null)
      assert(!testState.containsKey("k1"))

      testState.clear()
      assert(!testState.exists())
      assert(testState.getMap() === Map.empty)
    }
  }

  test("Map state operations for multiple map instances") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val testState1: MapState[Long, Double] =
        handle.getMapState[Long, Double]("testState1", Encoders.scalaLong)
      val testState2: MapState[Long, Int] =
        handle.getMapState[Long, Int]("testState2", Encoders.scalaLong)
      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      // put initial value
      testState1.updateValue(1L, 1.0)
      assert(testState1.getValue(1L) === 1.0)
      assert(!testState2.containsKey(1L))
      // update existing values, append new key-value pairs
      testState1.updateValue(1L, 2.0)
      testState2.updateValue(2L, 3)
      assert(testState1.getValue(1L) === 2.0)
      assert(testState2.getValue(2L) === 3)

      assert(testState1.getKeys().toSeq === Seq(1L))
      assert(testState2.getKeys().toSeq === Seq(2L))
      assert(testState1.getValues().toSeq === Seq(2.0))
      assert(testState2.getValues().toSeq === Seq(3))

      // test remove
      testState1.removeKey(1L)
      assert(testState1.getValue(1L) === null)
      assert(!testState1.containsKey(1L))

      testState2.clear()
      assert(!testState1.exists())
      assert(!testState2.exists())
      assert(testState1.getMap() === Map.empty)
      assert(testState2.getMap() === Map.empty)
    }
  }

  test("Map state operations with list, value, another map instances") {
    tryWithProviderResource(newStoreProviderWithValueState(true)) { provider =>
      val store = provider.getStore(0)
      val handle = new StatefulProcessorHandleImpl(store, UUID.randomUUID(),
        Encoders.STRING.asInstanceOf[ExpressionEncoder[Any]])

      val mapTestState1: MapState[String, Int] =
        handle.getMapState[String, Int]("mapTestState1", Encoders.STRING)
      val mapTestState2: MapState[String, Int] =
        handle.getMapState[String, Int]("mapTestState2", Encoders.STRING)
      val valueTestState: ValueState[String] =
        handle.getValueState[String]("valueTestState")
      val listTestState: ListState[String] =
        handle.getListState[String]("listTestState")

      ImplicitGroupingKeyTracker.setImplicitKey("test_key")
      // put initial values
      valueTestState.update("v1")
      listTestState.put(Seq("v1").toArray)
      mapTestState1.updateValue("k1", 1)
      mapTestState2.updateValue("k2", 2)
      assert(valueTestState.get() === "v1")
      assert(listTestState.get().toSeq === Seq("v1"))
      assert(mapTestState1.getValue("k1") === 1)
      assert(mapTestState2.getValue("k2") === 2)
      // update existing values, append
      valueTestState.update("v2")
      listTestState.appendValue("v3")
      mapTestState1.updateValue("k1", 3)
      mapTestState2.updateValue("k2", 4)

      assert(valueTestState.get() === "v2")
      assert(listTestState.get().toSeq === Seq("v1", "v3"))
      assert(mapTestState1.getValue("k1") === 3)
      assert(mapTestState2.getValue("k2") === 4)

      // advanced append/get operations
      listTestState.appendList(Seq("v4").toArray)
      mapTestState1.updateValue("k3", 5)
      mapTestState2.updateValue("k4", 6)

      assert(valueTestState.get() === "v2")
      assert(listTestState.get().toSeq === Seq("v1", "v3", "v4"))
      assert(mapTestState1.getKeys().toSeq === Seq("k1", "k3"))
      assert(mapTestState2.getValues().toSeq === Seq(4, 6))

      // test remove
      valueTestState.clear()
      listTestState.clear()
      mapTestState1.clear()
      mapTestState2.removeKey("k4")

      assert(!valueTestState.exists())
      assert(!listTestState.exists())
      assert(!mapTestState1.exists())
      assert(mapTestState2.exists())
      assert(mapTestState2.getMap() === Map("k2" -> 4))
    }
  }
}

class TransformWithMapStateSuite extends StreamTest {
  import testImplicits._

  private def testMapStateWithNullUserKey(inputMapRow: InputMapRow): Unit = {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputMapRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestMapStateProcessor(),
          TimeoutMode.NoTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, inputMapRow),
        ExpectFailure[SparkIllegalArgumentException](e => {
          assert(e.getMessage.contains("ILLEGAL_STATE_STORE_VALUE.NULL_VALUE"))
        })
      )
    }
  }

  test("Test retrieving value with non-existing user key") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputMapRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestMapStateProcessor(),
          TimeoutMode.NoTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputMapRow("k1", "getValue", ("v1", ""))),
        CheckAnswer(("k1", "v1", null))
      )
    }
  }

  Seq("getValue", "containsKey", "updateValue", "removeKey").foreach { mapImplFunc =>
    test(s"Test $mapImplFunc with null user key") {
      testMapStateWithNullUserKey(InputMapRow("k1", mapImplFunc, (null, "")))
    }
  }

  test("Test put value with null value") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[InputMapRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestMapStateProcessor(),
          TimeoutMode.NoTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, InputMapRow("k1", "updateValue", ("k1", null))),
        ExpectFailure[SparkIllegalArgumentException](e => {
          assert(e.getMessage.contains("ILLEGAL_STATE_STORE_VALUE.NULL_VALUE"))
        })
      )
    }
  }

  test("Test map state correctness") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputData = MemoryStream[InputMapRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestMapStateProcessor(),
          TimeoutMode.NoTimeouts(),
          OutputMode.Append())
      testStream(result, OutputMode.Append())(
        // Test exists()
        AddData(inputData, InputMapRow("k1", "updateValue", ("v1", "10"))),
        AddData(inputData, InputMapRow("k1", "exists", ("", ""))),
        AddData(inputData, InputMapRow("k2", "exists", ("", ""))),
        CheckNewAnswer(("k1", "exists", "true"), ("k2", "exists", "false")),

        // Test get and put with composite key
        AddData(inputData, InputMapRow("k1", "updateValue", ("v2", "5"))),

        AddData(inputData, InputMapRow("k2", "updateValue", ("v2", "3"))),
        AddData(inputData, InputMapRow("k2", "updateValue", ("v2", "12"))),
        AddData(inputData, InputMapRow("k2", "updateValue", ("v4", "1"))),

        // Different grouping key, same user key
        AddData(inputData, InputMapRow("k1", "getValue", ("v2", ""))),
        CheckNewAnswer(("k1", "v2", "5")),
        // Same grouping key, same user key, update value should reflect
        AddData(inputData, InputMapRow("k2", "getValue", ("v2", ""))),
        CheckNewAnswer(("k2", "v2", "12")),

        // Test get full map for a given grouping key - prefixScan
        AddData(inputData, InputMapRow("k2", "getMap", ("", ""))),
        CheckNewAnswer(("k2", "v2", "12"), ("k2", "v4", "1")),

        AddData(inputData, InputMapRow("k2", "getKeys", ("", ""))),
        CheckNewAnswer(("k2", "v2", ""), ("k2", "v4", "")),

        AddData(inputData, InputMapRow("k2", "getValues", ("", ""))),
        CheckNewAnswer(("k2", "", "12"), ("k2", "", "1")),

        // Test remove functionalities
        AddData(inputData, InputMapRow("k1", "removeKey", ("v2", ""))),
        AddData(inputData, InputMapRow("k1", "containsKey", ("v2", ""))),
        CheckNewAnswer(("k1", "v2", "false")),

        AddData(inputData, InputMapRow("k2", "clear", ("", ""))),
        AddData(inputData, InputMapRow("k2", "getMap", ("", ""))),
        CheckNewAnswer(),
        AddData(inputData, InputMapRow("k2", "exists", ("", ""))),
        CheckNewAnswer(("k2", "exists", "false"))
      )
    }
  }
}
