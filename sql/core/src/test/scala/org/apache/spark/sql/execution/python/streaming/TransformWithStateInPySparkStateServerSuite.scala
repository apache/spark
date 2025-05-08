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
package org.apache.spark.sql.execution.python.streaming

import java.io.DataOutputStream
import java.nio.channels.ServerSocketChannel

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
import org.apache.spark.sql.execution.streaming.state.StateMessage.{AppendList, AppendValue, Clear, ContainsKey, DeleteTimer, Exists, ExpiryTimerRequest, Get, GetProcessingTime, GetValue, GetWatermark, HandleState, Keys, ListStateCall, ListStateGet, ListStatePut, ListTimers, MapStateCall, ParseStringSchema, RegisterTimer, RemoveKey, SetHandleState, StateCallCommand, StatefulProcessorCall, TimerRequest, TimerStateCallCommand, TimerValueRequest, UpdateValue, UtilsRequest, Values, ValueStateCall, ValueStateUpdate}
import org.apache.spark.sql.streaming.{ListState, MapState, TTLConfig, ValueState}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class TransformWithStateInPySparkStateServerSuite extends SparkFunSuite with BeforeAndAfterEach {
  val stateName = "test"
  val iteratorId = "testId"
  val serverSocket: ServerSocketChannel = mock(classOf[ServerSocketChannel])
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
  var stateServer: TransformWithStateInPySparkStateServer = _
  var stateDeserializer: ExpressionEncoder.Deserializer[Row] = _
  var stateSerializer: ExpressionEncoder.Serializer[Row] = _
  var transformWithStateInPySparkDeserializer: TransformWithStateInPySparkDeserializer = _
  var arrowStreamWriter: BaseStreamingArrowWriter = _
  var batchTimestampMs: Option[Long] = _
  var eventTimeWatermarkForEviction: Option[Long] = _
  var valueStateMap: mutable.HashMap[String, ValueStateInfo] = mutable.HashMap()
  var listStateMap: mutable.HashMap[String, ListStateInfo] = mutable.HashMap()
  var mapStateMap: mutable.HashMap[String, MapStateInfo] = mutable.HashMap()
  var expiryTimerIter: Iterator[(Any, Long)] = _
  var listTimerMap: mutable.HashMap[String, Iterator[Long]] = mutable.HashMap()

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
    expiryTimerIter = Iterator.single(testRow, 1L /* a random long type value */)
    val iteratorMap = mutable.HashMap[String, Iterator[Row]](iteratorId -> Iterator(testRow))
    val keyValueIteratorMap = mutable.HashMap[String, Iterator[(Row, Row)]](iteratorId ->
      Iterator((testRow, testRow)))
    listTimerMap = mutable.HashMap[String, Iterator[Long]](iteratorId -> Iterator(1L))
    transformWithStateInPySparkDeserializer = mock(classOf[TransformWithStateInPySparkDeserializer])
    arrowStreamWriter = mock(classOf[BaseStreamingArrowWriter])
    batchTimestampMs = mock(classOf[Option[Long]])
    eventTimeWatermarkForEviction = mock(classOf[Option[Long]])
    stateServer = new TransformWithStateInPySparkStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false, 2,
      batchTimestampMs, eventTimeWatermarkForEviction,
      outputStream, valueStateMap, transformWithStateInPySparkDeserializer, arrowStreamWriter,
      listStateMap, iteratorMap, mapStateMap, keyValueIteratorMap, expiryTimerIter, listTimerMap)
    when(transformWithStateInPySparkDeserializer.readArrowBatches(any))
      .thenReturn(Seq(getIntegerRow(1)))
    when(transformWithStateInPySparkDeserializer.readListElements(any, any))
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
        verify(statefulProcessorHandle).getValueState[Row](any[String], any[Encoder[Row]],
          any[TTLConfig])
      }
      verify(outputStream).writeInt(0)
    }
  }

  Seq(true, false).foreach { useTTL =>
    test(s"get list state, useTTL=$useTTL") {
      val stateCallCommandBuilder = StateCallCommand.newBuilder()
        .setStateName("newName")
        .setSchema("StructType(List(StructField(value,IntegerType,true)))")
      if (useTTL) {
        stateCallCommandBuilder.setTtl(StateMessage.TTLConfig.newBuilder().setDurationMs(1000))
      }
      val message = StatefulProcessorCall
        .newBuilder()
        .setGetListState(stateCallCommandBuilder.build())
        .build()
      stateServer.handleStatefulProcessorCall(message)
      if (useTTL) {
        verify(statefulProcessorHandle)
          .getListState[Row](any[String], any[Encoder[Row]], any[TTLConfig])
      } else {
        verify(statefulProcessorHandle).getListState[Row](any[String], any[Encoder[Row]],
          any[TTLConfig])
      }
      verify(outputStream).writeInt(0)
    }
  }

  Seq(true, false).foreach { useTTL =>
    test(s"get map state, useTTL=$useTTL") {
      val stateCallCommandBuilder = StateCallCommand.newBuilder()
        .setStateName("newName")
        .setSchema("StructType(List(StructField(value,IntegerType,true)))")
        .setMapStateValueSchema("StructType(List(StructField(value,IntegerType,true)))")
      if (useTTL) {
        stateCallCommandBuilder.setTtl(StateMessage.TTLConfig.newBuilder().setDurationMs(1000))
      }
      val message = StatefulProcessorCall
        .newBuilder()
        .setGetMapState(stateCallCommandBuilder.build())
        .build()
      stateServer.handleStatefulProcessorCall(message)
      if (useTTL) {
        verify(statefulProcessorHandle)
          .getMapState[Row, Row](any[String], any[Encoder[Row]], any[Encoder[Row]], any[TTLConfig])
      } else {
        verify(statefulProcessorHandle).getMapState[Row, Row](any[String], any[Encoder[Row]],
          any[Encoder[Row]], any[TTLConfig])
      }
      verify(outputStream).writeInt(0)
    }
  }

  test("delete if exists") {
    val stateCallCommandBuilder = StateCallCommand.newBuilder()
      .setStateName("stateName")
    val message = StatefulProcessorCall
      .newBuilder()
      .setDeleteIfExists(stateCallCommandBuilder.build())
      .build()
    stateServer.handleStatefulProcessorCall(message)
    verify(statefulProcessorHandle).deleteIfExists(any[String])
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
    when(valueState.exists()).thenReturn(true)
    when(valueState.get()).thenReturn(getIntegerRow(1))
    stateServer.handleValueStateRequest(message)
    verify(valueState).get()
    verify(outputStream).writeInt(argThat((x: Int) => x > 0))
  }

  test("value state get - not exist") {
    val message = ValueStateCall.newBuilder().setStateName(stateName)
      .setGet(Get.newBuilder().build()).build()
    stateServer.handleValueStateRequest(message)
    verify(valueState).get()
    // We don't throw exception when value doesn't exist.
    verify(outputStream).writeInt(0)
  }

  test("value state get - not initialized") {
    val nonExistMessage = ValueStateCall.newBuilder().setStateName("nonExist")
      .setGet(Get.newBuilder().build()).build()
    stateServer.handleValueStateRequest(nonExistMessage)
    verify(valueState, times(0)).get()
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
    // 1 for proto response
    verify(outputStream).writeInt(any)
    // 1 for sending proto message
    verify(outputStream).write(any[Array[Byte]])
  }

  test("list state get - iterator in map with multiple batches") {
    val maxRecordsPerBatch = 2
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setListStateGet(ListStateGet.newBuilder().setIteratorId(iteratorId).build()).build()
    val iteratorMap = mutable.HashMap[String, Iterator[Row]](iteratorId ->
      Iterator(getIntegerRow(1), getIntegerRow(2), getIntegerRow(3), getIntegerRow(4)))
    stateServer = new TransformWithStateInPySparkStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, batchTimestampMs, eventTimeWatermarkForEviction, outputStream,
      valueStateMap, transformWithStateInPySparkDeserializer, arrowStreamWriter,
      listStateMap, iteratorMap)
    // First call should send 2 records.
    stateServer.handleListStateRequest(message)
    verify(listState, times(0)).get()
    // 1 for proto response
    verify(outputStream).writeInt(any)
    // 1 for proto message
    verify(outputStream).write(any[Array[Byte]])
    // Second call should send the remaining 2 records.
    stateServer.handleListStateRequest(message)
    verify(listState, times(0)).get()
    // Since Mockito's verify counts the total number of calls, the expected number of writeInt
    // and write should be accumulated from the prior count; the number of calls are the same
    // with prior one.
    // 1 for proto response
    verify(outputStream, times(2)).writeInt(any)
    // 1 for sending proto message
    verify(outputStream, times(2)).write(any[Array[Byte]])
  }

  test("list state get - iterator not in map") {
    val maxRecordsPerBatch = 2
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setListStateGet(ListStateGet.newBuilder().setIteratorId(iteratorId).build()).build()
    val iteratorMap: mutable.HashMap[String, Iterator[Row]] = mutable.HashMap()
    stateServer = new TransformWithStateInPySparkStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, batchTimestampMs, eventTimeWatermarkForEviction, outputStream,
      valueStateMap, transformWithStateInPySparkDeserializer, arrowStreamWriter,
      listStateMap, iteratorMap)
    when(listState.get()).thenReturn(Iterator(getIntegerRow(1), getIntegerRow(2), getIntegerRow(3)))
    stateServer.handleListStateRequest(message)
    verify(listState).get()

    // Verify that only maxRecordsPerBatch (2) rows are written to the output stream while still
    // having 1 row left in the iterator.
    // 1 for proto response
    verify(outputStream, times(1)).writeInt(any)
    // 1 for proto message
    verify(outputStream, times(1)).write(any[Array[Byte]])
  }

  test("list state put - inlined data") {
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setListStatePut(ListStatePut.newBuilder().setFetchWithArrow(false).build()).build()
    stateServer.handleListStateRequest(message)
    // Verify that the data is not read from Arrow stream. It is inlined.
    verify(transformWithStateInPySparkDeserializer, times(0)).readArrowBatches(any)
    verify(listState).put(any)
  }

  test("list state put - data via Arrow batch") {
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setListStatePut(ListStatePut.newBuilder().setFetchWithArrow(true).build()).build()
    stateServer.handleListStateRequest(message)
    verify(transformWithStateInPySparkDeserializer).readArrowBatches(any)
    verify(listState).put(any)
  }

  test("list state append value") {
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setAppendValue(AppendValue.newBuilder().setValue(byteString).build()).build()
    stateServer.handleListStateRequest(message)
    verify(listState).appendValue(any[Row])
  }

  test("list state append list - inlined data") {
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setAppendList(AppendList.newBuilder().setFetchWithArrow(false).build()).build()
    stateServer.handleListStateRequest(message)
    // Verify that the data is not read from Arrow stream. It is inlined.
    verify(transformWithStateInPySparkDeserializer, times(0)).readArrowBatches(any)
    verify(listState).appendList(any)
  }

  test("list state append list - data via Arrow batch") {
    val message = ListStateCall.newBuilder().setStateName(stateName)
      .setAppendList(AppendList.newBuilder().setFetchWithArrow(true).build()).build()
    stateServer.handleListStateRequest(message)
    verify(transformWithStateInPySparkDeserializer).readArrowBatches(any)
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
      .setGetValue(GetValue.newBuilder().setUserKey(byteString).build()).build()
    val schema = new StructType().add("value", "int")
    when(mapState.getValue(any[Row])).thenReturn(getIntegerRow(1))
    stateServer.handleMapStateRequest(message)
    verify(mapState).getValue(any[Row])
    verify(outputStream).writeInt(argThat((x: Int) => x > 0))
  }

  test("map state contains key") {
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setContainsKey(ContainsKey.newBuilder().setUserKey(byteString).build()).build()
    stateServer.handleMapStateRequest(message)
    verify(mapState).containsKey(any[Row])
  }

  test("map state update value") {
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    val message = MapStateCall.newBuilder().setStateName(stateName)
      .setUpdateValue(UpdateValue.newBuilder().setUserKey(byteString).setValue(byteString).build())
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
    stateServer = new TransformWithStateInPySparkStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, batchTimestampMs, eventTimeWatermarkForEviction, outputStream,
      valueStateMap, transformWithStateInPySparkDeserializer, arrowStreamWriter,
      listStateMap, null, mapStateMap, keyValueIteratorMap)
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
    stateServer = new TransformWithStateInPySparkStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, batchTimestampMs, eventTimeWatermarkForEviction,
      outputStream, valueStateMap, transformWithStateInPySparkDeserializer,
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
    stateServer = new TransformWithStateInPySparkStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, batchTimestampMs, eventTimeWatermarkForEviction,
      outputStream, valueStateMap, transformWithStateInPySparkDeserializer,
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
    stateServer = new TransformWithStateInPySparkStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      maxRecordsPerBatch, batchTimestampMs, eventTimeWatermarkForEviction, outputStream,
      valueStateMap, transformWithStateInPySparkDeserializer,
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
      .setRemoveKey(RemoveKey.newBuilder().setUserKey(byteString).build()).build()
    stateServer.handleMapStateRequest(message)
    verify(mapState).removeKey(any[Row])
  }

  test("timer value get processing time") {
    val message = TimerRequest.newBuilder().setTimerValueRequest(
      TimerValueRequest.newBuilder().setGetProcessingTimer(
        GetProcessingTime.newBuilder().build()
      ).build()
    ).build()
    stateServer.handleTimerRequest(message)
    verify(batchTimestampMs).isDefined
    verify(outputStream).writeInt(argThat((x: Int) => x > 0))
  }

  test("timer value get watermark") {
    val message = TimerRequest.newBuilder().setTimerValueRequest(
      TimerValueRequest.newBuilder().setGetWatermark(
        GetWatermark.newBuilder().build()
      ).build()
    ).build()
    stateServer.handleTimerRequest(message)
    verify(eventTimeWatermarkForEviction).isDefined
    verify(outputStream).writeInt(argThat((x: Int) => x > 0))
  }

  test("get expiry timers") {
    val message = TimerRequest.newBuilder().setExpiryTimerRequest(
      ExpiryTimerRequest.newBuilder().setExpiryTimestampMs(
        10L
      ).build()
    ).build()
    stateServer.handleTimerRequest(message)
    verify(arrowStreamWriter).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("stateful processor register timer") {
    val message = StatefulProcessorCall.newBuilder().setTimerStateCall(
      TimerStateCallCommand.newBuilder()
        .setRegister(RegisterTimer.newBuilder().setExpiryTimestampMs(10L).build())
        .build()
    ).build()
    stateServer.handleStatefulProcessorCall(message)
    verify(statefulProcessorHandle).registerTimer(any[Long])
    verify(outputStream).writeInt(0)
  }

  test("stateful processor delete timer") {
    val message = StatefulProcessorCall.newBuilder().setTimerStateCall(
      TimerStateCallCommand.newBuilder()
        .setDelete(DeleteTimer.newBuilder().setExpiryTimestampMs(10L).build())
        .build()
    ).build()
    stateServer.handleStatefulProcessorCall(message)
    verify(statefulProcessorHandle).deleteTimer(any[Long])
    verify(outputStream).writeInt(0)
  }

  test("stateful processor list timer - iterator in map") {
    val message = StatefulProcessorCall.newBuilder().setTimerStateCall(
      TimerStateCallCommand.newBuilder()
        .setList(ListTimers.newBuilder().setIteratorId(iteratorId).build())
        .build()
    ).build()
    stateServer.handleStatefulProcessorCall(message)
    verify(statefulProcessorHandle, times(0)).listTimers()
    verify(arrowStreamWriter).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("stateful processor list timer - iterator not in map") {
    val message = StatefulProcessorCall.newBuilder().setTimerStateCall(
      TimerStateCallCommand.newBuilder()
        .setList(ListTimers.newBuilder().setIteratorId("non-exist").build())
        .build()
    ).build()
    stateServer = new TransformWithStateInPySparkStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      2, batchTimestampMs, eventTimeWatermarkForEviction, outputStream,
      valueStateMap, transformWithStateInPySparkDeserializer,
      arrowStreamWriter, listStateMap, null, mapStateMap, null,
      null, listTimerMap)
    when(statefulProcessorHandle.listTimers()).thenReturn(Iterator(1))
    stateServer.handleStatefulProcessorCall(message)
    verify(statefulProcessorHandle, times(1)).listTimers()
    verify(arrowStreamWriter).writeRow(any)
    verify(arrowStreamWriter).finalizeCurrentArrowBatch()
  }

  test("utils request - parse string schema") {
    val message = UtilsRequest.newBuilder().setParseStringSchema(
      ParseStringSchema.newBuilder().setSchema(
        "value int"
      ).build()
    ).build()
    stateServer.handleUtilsRequest(message)
    verify(outputStream).writeInt(argThat((x: Int) => x > 0))
  }

  private def getIntegerRow(value: Int): Row = {
    new GenericRowWithSchema(Array(value), stateSchema)
  }
}
