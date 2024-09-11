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
import org.apache.spark.sql.execution.streaming.state.StateMessage.{Clear, DeleteTimer, Exists, ExpiryTimerRequest, Get, GetProcessingTime, GetWatermark, HandleState, ListTimers, RegisterTimer, SetHandleState, StateCallCommand, StatefulProcessorCall, TimerRequest, TimerStateCallCommand, TimerValueRequest, ValueStateCall, ValueStateUpdate}
import org.apache.spark.sql.streaming.{TTLConfig, ValueState}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class TransformWithStateInPandasStateServerSuite extends SparkFunSuite with BeforeAndAfterEach {
  val valueStateName = "test"
  var statefulProcessorHandle: StatefulProcessorHandleImpl = _
  var outputStream: DataOutputStream = _
  var valueState: ValueState[Row] = _
  var stateServer: TransformWithStateInPandasStateServer = _
  var valueSchema: StructType = _
  var valueDeserializer: ExpressionEncoder.Deserializer[Row] = _
  var batchTimestampMs: Option[Long] = _
  var eventTimeWatermarkForEviction: Option[Long] = _

  override def beforeEach(): Unit = {
    val serverSocket = mock(classOf[ServerSocket])
    statefulProcessorHandle = mock(classOf[StatefulProcessorHandleImpl])
    val groupingKeySchema = StructType(Seq())
    outputStream = mock(classOf[DataOutputStream])
    valueState = mock(classOf[ValueState[Row]])
    valueSchema = StructType(Array(StructField("value", IntegerType)))
    valueDeserializer = ExpressionEncoder(valueSchema).resolveAndBind().createDeserializer()
    batchTimestampMs = mock(classOf[Option[Long]])
    eventTimeWatermarkForEviction = mock(classOf[Option[Long]])
    val valueStateMap = mutable.HashMap[String,
      (ValueState[Row], StructType, ExpressionEncoder.Deserializer[Row])](valueStateName ->
      (valueState, valueSchema, valueDeserializer))
    stateServer = new TransformWithStateInPandasStateServer(serverSocket,
      statefulProcessorHandle, groupingKeySchema, "", false, false,
      2, outputStream, valueStateMap, batchTimestampMs, eventTimeWatermarkForEviction)
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
    val message = ValueStateCall.newBuilder().setStateName(valueStateName)
      .setExists(Exists.newBuilder().build()).build()
    stateServer.handleValueStateRequest(message)
    verify(valueState).exists()
  }

  test("value state get") {
    val message = ValueStateCall.newBuilder().setStateName(valueStateName)
      .setGet(Get.newBuilder().build()).build()
    val schema = new StructType().add("value", "int")
    when(valueState.getOption()).thenReturn(Some(new GenericRowWithSchema(Array(1), schema)))
    stateServer.handleValueStateRequest(message)
    verify(valueState).getOption()
    verify(outputStream).writeInt(argThat((x: Int) => x > 0))
  }

  test("value state get - not exist") {
    val message = ValueStateCall.newBuilder().setStateName(valueStateName)
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
    val message = ValueStateCall.newBuilder().setStateName(valueStateName)
      .setClear(Clear.newBuilder().build()).build()
    stateServer.handleValueStateRequest(message)
    verify(valueState).clear()
    verify(outputStream).writeInt(0)
  }

  test("value state update") {
    // Below byte array is a serialized row with a single integer value 1.
    val byteArray: Array[Byte] = Array(0x80.toByte, 0x05.toByte, 0x95.toByte, 0x05.toByte,
      0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte,
      'K'.toByte, 0x01.toByte, 0x85.toByte, 0x94.toByte, '.'.toByte
    )
    val byteString: ByteString = ByteString.copyFrom(byteArray)
    val message = ValueStateCall.newBuilder().setStateName(valueStateName)
      .setValueStateUpdate(ValueStateUpdate.newBuilder().setValue(byteString).build()).build()
    stateServer.handleValueStateRequest(message)
    verify(valueState).update(any[Row])
    verify(outputStream).writeInt(0)
  }


  // TODO change this after returning correct format
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
    verify(statefulProcessorHandle).getExpiredTimers(any[Long])
    verify(outputStream).writeInt(0)
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

  test("stateful processor list timer") {
    val message = StatefulProcessorCall.newBuilder().setTimerStateCall(
      TimerStateCallCommand.newBuilder()
        .setList(ListTimers.newBuilder().build())
        .build()
    ).build()
    stateServer.handleStatefulProcessorCall(message)
    verify(statefulProcessorHandle).listTimers()
    verify(outputStream).writeInt(0)
  }
}
