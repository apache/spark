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
package org.apache.spark.sql.execution.python

import java.io.DataOutputStream
import java.net.ServerSocket

import scala.collection.mutable

import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers.{any, argThat}
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.streaming.{StatefulProcessorHandleImpl, StatefulProcessorHandleState}
import org.apache.spark.sql.execution.streaming.state.StateMessage
import org.apache.spark.sql.execution.streaming.state.StateMessage.{AppendList, AppendValue, Clear, ContainsKey, Exists, Get, GetValue, HandleState, Keys, ListStateCall, ListStateGet, ListStatePut, MapStateCall, RemoveKey, SetHandleState, StateCallCommand, StatefulProcessorCall, UpdateValue, Values, ValueStateCall, ValueStateUpdate}
import org.apache.spark.sql.streaming.{ListState, MapState, TTLConfig, ValueState}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class TransformWithStateInPandasStateServerSuite extends SparkFunSuite with BeforeAndAfterEach {
  val stateName = "test"
  val iteratorId = "testId"
  val serverSocket: ServerSocket = mock(classOf[ServerSocket])
  val groupingKeySchema: StructType = StructType(Seq())
  val stateSchema: StructType = StructType(Array(StructField("value", IntegerType)))
  // Below byte array is a serialized row with a single integer value 1.
  val byteArray: Array[Byte] = Array(0x80.toByte, 0x05.toByte, 0x95.toByte, 0x05.toByte,
    0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte,
    'K'.toByte, 0x01.toByte, 0x85.toByte, 0x94.toByte, '.'.toByte
  )

  var statefulProcessorHandle: StatefulProcessorHandleImpl =
    mock(classOf[StatefulProcessorHandleImpl])
  var outputStream: DataOutputStream = _
  var valueState: ValueState[Row] = _
  var listState: ListState[Row] = _
  var mapState: MapState[Row, Row] = _
  var stateServer: TransformWithStateInPandasStateServer = _
  var stateDeserializer: ExpressionEncoder.Deserializer[Row] = _
  var stateSerializer: ExpressionEncoder.Serializer[Row] = _
  var transformWithStateInPandasDeserializer: TransformWithStateInPandasDeserializer = _
  var arrowStreamWriter: BaseStreamingArrowWriter = _
  var valueStateMap: mutable.HashMap[String, ValueStateInfo] = mutable.HashMap()
  var listStateMap: mutable.HashMap[String, ListStateInfo] = mutable.HashMap()
  var mapStateMap: mutable.HashMap[String, MapStateInfo] = mutable.HashMap()

  override def beforeEach(): Unit = {
    statefulProcessorHandle = mock(classOf[StatefulProcessorHandleImpl])
    outputStream = mock(classOf[DataOutputStream])
    valueState = mock(classOf[ValueState[Row]])
    listState = mock(classOf[ListState[Row]])
    mapState = mock(classOf[MapState[Row, Row]])
    stateDeserializer = ExpressionEncoder(stateSchema).resolveAndBind().createDeserializer()
    stateSerializer = ExpressionEncoder(stateSchema).resolveAndBind().createSerializer()
    valueStateMap = mutable.HashMap[String, ValueStateInfo](stateName ->
      ValueStateInfo(valueState, stateSchema, stateDeserializer))
    listStateMap = mutable.HashMap[String, ListStateInfo](stateName ->
      ListStateInfo(listState, stateSchema, stateDeserializer, stateSerializer))
    mapStateMap = mutable.HashMap[String, MapStateInfo](stateName ->
      MapStateInfo(mapState, stateSchema, stateSchema, stateDeserializer,
        stateSerializer, stateDeserializer, stateSerializer))

    // Iterator map for list/map state. Please note that `handleImplicitGroupingKeyRequest` would
    // reset the iterator map to empty so be careful to call it if you want to access the iterator
    // map later.
    val testRow = getIntegerRow(1)
    val iteratorMap = mutable.HashMap[String, Iterator[Row]](iteratorId -> Iterator(testRow))
    val keyValueIteratorMap = mutable.HashMap[String, Iterator[(Row, Row)]](iteratorId ->
      Iterator((testRow, testRow)))
    transformWithStateInPandasDeserializer = mock(classOf[TransformWithStateInPandasDeserializer])
    arrowStreamWriter = mock(classOf[BaseStreamingArrowWriter])
    stateServer = new TransformWithStateInPandasStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false, 2,
      outputStream, valueStateMap, transformWithStateInPandasDeserializer, arrowStreamWriter,
      listStateMap, iteratorMap, mapStateMap, keyValueIteratorMap)
    when(transformWithStateInPandasDeserializer.readArrowBatches(any))
      .thenReturn(Seq(getIntegerRow(1)))
  }

  test("set handle state") {
    val message = StatefulProcessorCall.newBuilder().setSetHandleState(
      SetHandleState.newBuilder().setState(HandleState.CREATED).build()).build()
    stateServer.handleStatefulProcessorCall(message)
    verify(statefulProcessorHandle).setHandleState(StatefulProcessorHandleState.CREATED)
    verify(outputStream).writeInt(0)
  }

  Seq(true, false).foreach { useTTL =>
    test(s"get value state, useTTL=$useTTL") {
      val stateCallCommandBuilder = StateCallCommand.newBuilder()
        .setStateName("newName")
        .setSchema("StructType(List(StructField(value,IntegerType,true)))")
      if (useTTL) {
        stateCallCommandBuilder.setTtl(StateMessage.TTLConfig.newBuilder().setDurationMs(1000))
      }
      val message = StatefulProcessorCall
        .newBuilder()
        .setGetValueState(stateCallCommandBuilder.build())
        .build()
      stateServer.handleStatefulProcessorCall(message)
      if (useTTL) {
        verify(statefulProcessorHandle)
          .getValueState[Row](any[String], any[Encoder[Row]], any[TTLConfig])
      } else {
        verify(statefulProcessorHandle).getValueState[Row](any[String], any[Encoder[Row]])
      }
      verify(outputStream).writeInt(0)
    }
  }

  test("value state exists") {
    val message = ValueStateCall.newBuilder().setStateName(stateName)
      .setExists(Exists.newBuilder().build()).build()
    stateServer.handleValueStateRequest(message)
    verify(valueState).exists()
  }

  test("value state get") {
    val message = ValueStateCall.newBuilder().setStateName(stateName)
      .setGet(Get.newBuilder().build()).build()
    val schema = new StructType().add("value", "int")
    when(valueState.getOption()).thenReturn(Some(getIntegerRow(1)))
    stateServer.handleValueStateRequest(message)
    verify(valueState).getOption()
    verify(outputStream).writeInt(argThat((x: Int) => x > 0))
  }

  test("value state get - not exist") {
    val message = ValueStateCall.newBuilder().setStateName(stateName)
      .setGet(Get.newBuilder().build()).build()
    when(valueState.getOption()).thenReturn(None)
    stateServer.handleValueStateRequest(message)
    verify(valueState).getOption()
    // We don't throw exception when value doesn't exist.
    verify(outputStream).writeInt(0)
  }

  test("value state get - not initialized") {
    val nonExistMessage = ValueStateCall.newBuilder().setStateName("nonExist")
      .setGet(Get.newBuilder().build()).build()
    stateServer.handleValueStateRequest(nonExistMessage)
    verify(valueState, times(0)).getOption()
    verify(outputStream).writeInt(argThat((x: Int) => x > 0))
  }

  test("value state clear") {
    val message = ValueStateCall.newBuilder().setStateName(stateName)
      .setClear(Clear.newBuilder().build()).build()
    stateServer.handleValueStateRequest(message)
    verify(valueState).clear()
    verify(outputStream).writeInt(0)
  }

  test("value state update") {
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    val message = ValueStateCall.newBuilder().setStateName(stateName)
      .setValueStateUpdate(ValueStateUpdate.newBuilder().setValue(byteString).build()).build()
    stateServer.handleValueStateRequest(message)
    verify(valueState).update(any[Row])
    verify(outputStream).writeInt(0)
  }

  test("list state exists") {
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setExists(Exists.newBuilder().build()).build()
    stateServer.handleListStateRequest(message)
    verify(listState).exists()
  }

  test("list state get - iterator in map") {
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setListStateGet(ListStateGet.newBuilder().setIteratorId(iteratorId).build()).build()
    stateServer.handleListStateRequest(message)
    verify(listState, times(0)).get()
    verify(arrowStreamWriter).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("list state get - iterator in map with multiple batches") {
    val maxRecordsPerBatch = 2
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setListStateGet(ListStateGet.newBuilder().setIteratorId(iteratorId).build()).build()
    val iteratorMap = mutable.HashMap[String, Iterator[Row]](iteratorId ->
      Iterator(getIntegerRow(1), getIntegerRow(2), getIntegerRow(3), getIntegerRow(4)))
    stateServer = new TransformWithStateInPandasStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, outputStream, valueStateMap,
      transformWithStateInPandasDeserializer, arrowStreamWriter, listStateMap, iteratorMap)
    // First call should send 2 records.
    stateServer.handleListStateRequest(message)
    verify(listState, times(0)).get()
    verify(arrowStreamWriter, times(maxRecordsPerBatch)).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
    // Second call should send the remaining 2 records.
    stateServer.handleListStateRequest(message)
    verify(listState, times(0)).get()
    // Since Mockito's verify counts the total number of calls, the expected number of writeRow call
    // should be 2 * maxRecordsPerBatch.
    verify(arrowStreamWriter, times(2 * maxRecordsPerBatch)).writeRow(any)
    verify(arrowStreamWriter, times(2)).finalizeCurrentArrowBatch()
  }

  test("list state get - iterator not in map") {
    val maxRecordsPerBatch = 2
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setListStateGet(ListStateGet.newBuilder().setIteratorId(iteratorId).build()).build()
    val iteratorMap: mutable.HashMap[String, Iterator[Row]] = mutable.HashMap()
    stateServer = new TransformWithStateInPandasStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, outputStream, valueStateMap,
      transformWithStateInPandasDeserializer, arrowStreamWriter, listStateMap, iteratorMap)
    when(listState.get()).thenReturn(Iterator(getIntegerRow(1), getIntegerRow(2), getIntegerRow(3)))
    stateServer.handleListStateRequest(message)
    verify(listState).get()
    // Verify that only maxRecordsPerBatch (2) rows are written to the output stream while still
    // having 1 row left in the iterator.
    verify(arrowStreamWriter, times(maxRecordsPerBatch)).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("list state put") {
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setListStatePut(ListStatePut.newBuilder().build()).build()
    stateServer.handleListStateRequest(message)
    verify(transformWithStateInPandasDeserializer).readArrowBatches(any)
    verify(listState).put(any)
  }

  test("list state append value") {
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setAppendValue(AppendValue.newBuilder().setValue(byteString).build()).build()
    stateServer.handleListStateRequest(message)
    verify(listState).appendValue(any[Row])
  }

  test("list state append list") {
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setAppendList(AppendList.newBuilder().build()).build()
    stateServer.handleListStateRequest(message)
    verify(transformWithStateInPandasDeserializer).readArrowBatches(any)
    verify(listState).appendList(any)
  }

  test("map state exists") {
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setExists(Exists.newBuilder().build()).build()
    stateServer.handleMapStateRequest(message)
    verify(mapState).exists()
  }

  test("map state get") {
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setGetValue(GetValue.newBuilder().setKey(byteString).build()).build()
    val schema = new StructType().add("value", "int")
    when(mapState.getValue(any[Row])).thenReturn(getIntegerRow(1))
    stateServer.handleMapStateRequest(message)
    verify(mapState).getValue(any[Row])
    verify(outputStream).writeInt(argThat((x: Int) => x > 0))
  }

  test("map state contains key") {
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setContainsKey(ContainsKey.newBuilder().setKey(byteString).build()).build()
    stateServer.handleMapStateRequest(message)
    verify(mapState).containsKey(any[Row])
  }

  test("map state update value") {
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setUpdateValue(UpdateValue.newBuilder().setKey(byteString).setValue(byteString).build())
      .build()
    stateServer.handleMapStateRequest(message)
    verify(mapState).updateValue(any[Row], any[Row])
  }

  test("map state iterator - iterator in map") {
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setIterator(StateMessage.Iterator.newBuilder().setIteratorId(iteratorId).build()).build()
    stateServer.handleMapStateRequest(message)
    verify(mapState, times(0)).iterator()
    verify(arrowStreamWriter).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("map state iterator - iterator in map with multiple batches") {
    val maxRecordsPerBatch = 2
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setIterator(StateMessage.Iterator.newBuilder().setIteratorId(iteratorId).build()).build()
    val keyValueIteratorMap = mutable.HashMap[String, Iterator[(Row, Row)]](iteratorId ->
      Iterator((getIntegerRow(1), getIntegerRow(1)), (getIntegerRow(2), getIntegerRow(2)),
        (getIntegerRow(3), getIntegerRow(3)), (getIntegerRow(4), getIntegerRow(4))))
    stateServer = new TransformWithStateInPandasStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, outputStream, valueStateMap, transformWithStateInPandasDeserializer,
      arrowStreamWriter, listStateMap, null, mapStateMap, keyValueIteratorMap)
    // First call should send 2 records.
    stateServer.handleMapStateRequest(message)
    verify(mapState, times(0)).iterator()
    verify(arrowStreamWriter, times(maxRecordsPerBatch)).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
    // Second call should send the remaining 2 records.
    stateServer.handleMapStateRequest(message)
    verify(mapState, times(0)).iterator()
    // Since Mockito's verify counts the total number of calls, the expected number of writeRow call
    // should be 2 * maxRecordsPerBatch.
    verify(arrowStreamWriter, times(2 * maxRecordsPerBatch)).writeRow(any)
    verify(arrowStreamWriter, times(2)).finalizeCurrentArrowBatch()
  }

  test("map state iterator - iterator not in map") {
    val maxRecordsPerBatch = 2
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setIterator(StateMessage.Iterator.newBuilder().setIteratorId(iteratorId).build()).build()
    val keyValueIteratorMap: mutable.HashMap[String, Iterator[(Row, Row)]] = mutable.HashMap()
    stateServer = new TransformWithStateInPandasStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, outputStream, valueStateMap, transformWithStateInPandasDeserializer,
      arrowStreamWriter, listStateMap, null, mapStateMap, keyValueIteratorMap)
    when(mapState.iterator()).thenReturn(Iterator((getIntegerRow(1), getIntegerRow(1)),
      (getIntegerRow(2), getIntegerRow(2)), (getIntegerRow(3), getIntegerRow(3))))
    stateServer.handleMapStateRequest(message)
    verify(mapState).iterator()
    // Verify that only maxRecordsPerBatch (2) rows are written to the output stream while still
    // having 1 row left in the iterator.
    verify(arrowStreamWriter, times(maxRecordsPerBatch)).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("map state keys - iterator in map") {
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setKeys(Keys.newBuilder().setIteratorId(iteratorId).build()).build()
    stateServer.handleMapStateRequest(message)
    verify(mapState, times(0)).keys()
    verify(arrowStreamWriter).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("map state keys - iterator not in map") {
    val maxRecordsPerBatch = 2
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setKeys(Keys.newBuilder().setIteratorId(iteratorId).build()).build()
    val iteratorMap: mutable.HashMap[String, Iterator[Row]] = mutable.HashMap()
    stateServer = new TransformWithStateInPandasStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, outputStream, valueStateMap, transformWithStateInPandasDeserializer,
      arrowStreamWriter, listStateMap, iteratorMap, mapStateMap)
    when(mapState.keys()).thenReturn(Iterator(getIntegerRow(1), getIntegerRow(2), getIntegerRow(3)))
    stateServer.handleMapStateRequest(message)
    verify(mapState).keys()
    // Verify that only maxRecordsPerBatch (2) rows are written to the output stream while still
    // having 1 row left in the iterator.
    verify(arrowStreamWriter, times(maxRecordsPerBatch)).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("map state values - iterator in map") {
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setValues(Values.newBuilder().setIteratorId(iteratorId).build()).build()
    stateServer.handleMapStateRequest(message)
    verify(mapState, times(0)).values()
    verify(arrowStreamWriter).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("map state values - iterator not in map") {
    val maxRecordsPerBatch = 2
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setValues(Values.newBuilder().setIteratorId(iteratorId).build()).build()
    val iteratorMap: mutable.HashMap[String, Iterator[Row]] = mutable.HashMap()
    stateServer = new TransformWithStateInPandasStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, outputStream, valueStateMap, transformWithStateInPandasDeserializer,
      arrowStreamWriter, listStateMap, iteratorMap, mapStateMap)
    when(mapState.values()).thenReturn(Iterator(getIntegerRow(1), getIntegerRow(2),
      getIntegerRow(3)))
    stateServer.handleMapStateRequest(message)
    verify(mapState).values()
    // Verify that only maxRecordsPerBatch (2) rows are written to the output stream while still
    // having 1 row left in the iterator.
    verify(arrowStreamWriter, times(maxRecordsPerBatch)).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("remove key") {
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setRemoveKey(RemoveKey.newBuilder().setKey(byteString).build()).build()
    stateServer.handleMapStateRequest(message)
    verify(mapState).removeKey(any[Row])
  }

  private def getIntegerRow(value: Int): Row = {
    new GenericRowWithSchema(Array(value), stateSchema)
  }
}
