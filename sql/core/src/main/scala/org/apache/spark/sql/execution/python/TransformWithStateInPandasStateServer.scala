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

import java.io.{DataInputStream, DataOutputStream, EOFException}
import java.nio.channels.Channels

import scala.collection.mutable

import com.google.protobuf.ByteString
import jnr.unixsocket.UnixServerSocketChannel

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl, StatefulProcessorHandleState}
import org.apache.spark.sql.execution.streaming.state.StateMessage.{HandleState, ImplicitGroupingKeyRequest, StatefulProcessorCall, StateRequest, StateResponse, StateVariableRequest, ValueStateCall}
import org.apache.spark.sql.streaming.ValueState
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StructType}

/**
 * This class is used to handle the state requests from the Python side.
 */
class TransformWithStateInPandasStateServer(
    private val serverChannel: UnixServerSocketChannel,
    private val statefulProcessorHandle: StatefulProcessorHandleImpl,
    private val groupingKeySchema: StructType)
  extends Runnable
  with Logging{

  private var inputStream: DataInputStream = _
  private var outputStream: DataOutputStream = _

  private val valueStates = mutable.HashMap[String, ValueState[Any]]()

  def run(): Unit = {
    logWarning(s"Waiting for connection from Python worker")
    val channel = serverChannel.accept()
    logWarning(s"listening on channel - ${channel.getLocalAddress}")

    inputStream = new DataInputStream(
      Channels.newInputStream(channel))
    outputStream = new DataOutputStream(
      Channels.newOutputStream(channel)
    )

    while (channel.isConnected &&
      statefulProcessorHandle.getHandleState != StatefulProcessorHandleState.CLOSED) {

      try {
        logWarning(s"reading the version")
        val version = inputStream.readInt()

        if (version != -1) {
          logWarning(s"version = ${version}")
          assert(version == 0)
          val messageLen = inputStream.readInt()
          logWarning(s"parsing a message of ${messageLen} bytes")

          val messageBytes = new Array[Byte](messageLen)
          inputStream.read(messageBytes)
          logWarning(s"read bytes = ${messageBytes.mkString("Array(", ", ", ")")}")

          val message = StateRequest.parseFrom(ByteString.copyFrom(messageBytes))

          logWarning(s"read message = $message")
          handleRequest(message)
          logWarning(s"flush output stream")

          outputStream.flush()
        }
      } catch {
        case _: EOFException =>
          logWarning(s"No more data to read from the socket")
          statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
          return
        case e: Exception =>
          logWarning(s"Error reading message: ${e.getMessage}")
          sendResponse(1, e.getMessage)
          outputStream.flush()
          statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
          return
      }
    }
    logWarning(s"done from the state server thread")
  }

  private def handleRequest(message: StateRequest): Unit = {
    if (message.getMethodCase == StateRequest.MethodCase.STATEFULPROCESSORCALL) {
      val statefulProcessorHandleRequest = message.getStatefulProcessorCall
      if (statefulProcessorHandleRequest.getMethodCase ==
        StatefulProcessorCall.MethodCase.SETHANDLESTATE) {
        val requestedState = statefulProcessorHandleRequest.getSetHandleState.getState
        requestedState match {
          case HandleState.CREATED =>
            logWarning(s"set handle state to Created")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CREATED)
          case HandleState.INITIALIZED =>
            logWarning(s"set handle state to Initialized")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
          case HandleState.CLOSED =>
            logWarning(s"set handle state to Closed")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
          case _ =>
        }
        sendResponse(0)
      } else if (statefulProcessorHandleRequest.getMethodCase ==
        StatefulProcessorCall.MethodCase.GETVALUESTATE) {
        val stateName = statefulProcessorHandleRequest.getGetValueState.getStateName
        val schema = statefulProcessorHandleRequest.getGetValueState.getSchema
        logWarning(s"initializing value state $stateName")
        initializeState("ValueState", stateName, schema)
      } else {
        throw new IllegalArgumentException("Invalid method call")
      }
    } else if (message.getMethodCase == StateRequest.MethodCase.STATEVARIABLEREQUEST) {
      if (message.getStateVariableRequest.getMethodCase ==
        StateVariableRequest.MethodCase.VALUESTATECALL) {
        if (message.getStateVariableRequest.getValueStateCall.getMethodCase ==
          ValueStateCall.MethodCase.EXISTS) {
          val stateName = message.getStateVariableRequest.getValueStateCall.getExists.getStateName
          if (valueStates.contains(stateName) && valueStates(stateName).exists()) {
            logWarning(s"state $stateName exists")
            sendResponse(0)
          } else {
            logWarning(s"state $stateName doesn't exist")
            sendResponse(-1)
          }
        } else if (message.getStateVariableRequest.getValueStateCall.getMethodCase ==
          ValueStateCall.MethodCase.GET) {
          val stateName = message.getStateVariableRequest.getValueStateCall.getGet.getStateName
          if (valueStates.contains(stateName)) {
            val valueState = valueStates(stateName)
            val valueOption = valueState.getOption()
            if (valueOption.isDefined) {
              sendResponse(0)
              val value = valueOption.get.toString
              logWarning("got state value " + value)
              val valueBytes = value.getBytes("UTF-8")
              val byteLength = valueBytes.length
              logWarning(s"writing value bytes of length $byteLength")
              outputStream.writeInt(byteLength)
              logWarning(s"writing value bytes: ${valueBytes.mkString("Array(", ", ", ")")}")
              outputStream.write(valueBytes)
            } else {
              logWarning(s"state $stateName doesn't exist")
              sendResponse(1, s"state $stateName doesn't exist")
            }
          } else {
            logWarning(s"state $stateName doesn't exist")
            sendResponse(1, s"state $stateName doesn't exist")
          }
        } else if (message.getStateVariableRequest.getValueStateCall.getMethodCase ==
          ValueStateCall.MethodCase.UPDATE) {
          val updateRequest = message.getStateVariableRequest.getValueStateCall.getUpdate
          val stateName = updateRequest.getStateName
          val updateValueString = updateRequest.getValue.toStringUtf8
          val dataType = StructType.fromString(updateRequest.getSchema).fields(0).dataType
          val updatedValue = castToType(updateValueString, dataType)
          logWarning(s"updating state $stateName with value $updatedValue and" +
            s" type ${updatedValue.getClass}")
          if (valueStates.contains(stateName)) {
            valueStates(stateName).update(updatedValue)
            sendResponse(0)
          } else {
            logWarning(s"state $stateName doesn't exist")
            sendResponse(1, s"state $stateName doesn't exist")
          }
        } else if (message.getStateVariableRequest.getValueStateCall.getMethodCase ==
          ValueStateCall.MethodCase.CLEAR) {
          val stateName = message.getStateVariableRequest.getValueStateCall.getClear.getStateName
          if (valueStates.contains(stateName)) {
            valueStates(stateName).clear()
            sendResponse(0)
          } else {
            logWarning(s"state $stateName doesn't exist")
            sendResponse(1, s"state $stateName doesn't exist")
          }
        } else {
          throw new IllegalArgumentException("Invalid method call")
        }
      }
    } else if (message.getMethodCase == StateRequest.MethodCase.IMPLICITGROUPINGKEYREQUEST) {
      if (message.getImplicitGroupingKeyRequest.getMethodCase ==
        ImplicitGroupingKeyRequest.MethodCase.SETIMPLICITKEY) {
        val key = message.getImplicitGroupingKeyRequest.getSetImplicitKey.getKey
        val groupingKeyType = groupingKeySchema.fields(0).dataType
        val castedData = castToType(key, groupingKeyType)
        logWarning(s"setting implicit key to $castedData with type ${castedData.getClass}")
        val row = Row(castedData)
        ImplicitGroupingKeyTracker.setImplicitKey(row)
        sendResponse(0)
      } else if (message.getImplicitGroupingKeyRequest.getMethodCase ==
        ImplicitGroupingKeyRequest.MethodCase.REMOVEIMPLICITKEY) {
        logWarning(s"removing implicit key")
        ImplicitGroupingKeyTracker.removeImplicitKey()
        logWarning(s"removed implicit key")
        sendResponse(0)
      } else {
        throw new IllegalArgumentException("Invalid method call")
      }
    } else {
      throw new IllegalArgumentException("Invalid method call")
    }
  }

  private def sendResponse(status: Int, errorMessage: String = null): Unit = {
    val responseMessageBuilder = StateResponse.newBuilder().setStatusCode(status)
    if (status != 0 && errorMessage != null) {
      responseMessageBuilder.setErrorMessage(errorMessage)
    }
    val responseMessage = responseMessageBuilder.build()
    logWarning(s"sending state response: $responseMessage with size" +
      s" ${responseMessage.getSerializedSize}, status: ${responseMessage.getStatusCode}," +
      s" error: ${responseMessage.getErrorMessage}")
    val responseMessageBytes = responseMessage.toByteArray
    val byteLength = responseMessageBytes.length
    logWarning(s"writing state response bytes of length $byteLength")
    outputStream.writeInt(byteLength)
    logWarning(s"writing state response bytes:" +
      s" ${responseMessageBytes.mkString("Array(", ", ", ")")}")
    outputStream.write(responseMessageBytes)
  }

  private def initializeState(stateType: String, stateName: String, schema: String): Unit = {
    if (stateType == "ValueState") {
      if (!valueStates.contains(stateName)) {
        val structType = StructType.fromString(schema)
        val field = structType.fields(0)
        val encoder = getEncoder(field.dataType)
        val state = statefulProcessorHandle.getValueState(stateName, encoder)
          .asInstanceOf[ValueState[Any]]
        valueStates.put(stateName, state)
        sendResponse(0)
      } else {
        sendResponse(1, s"state $stateName already exists")
      }
    } else {
      sendResponse(1, s"state type $stateType is not supported")
    }
  }

  private def castToType(value: String, dataType: DataType): Any = {
    dataType match {
      case IntegerType => value.toInt
      case LongType => value.toLong
      case DoubleType => value.toDouble
      case FloatType => value.toFloat
      case BooleanType => value.toBoolean
      case _ => value
    }
  }

  private def getEncoder(dataType: DataType): Encoder[_] = {
    dataType match {
      case IntegerType => Encoders.INT
      case LongType => Encoders.LONG
      case DoubleType => Encoders.DOUBLE
      case FloatType => Encoders.FLOAT
      case BooleanType => Encoders.BOOLEAN
      case _ => Encoders.STRING
    }
  }
}

object TransformWithStateInPandasStateServer {
  @volatile private var id = 0

  def allocateServerId(): Int = synchronized {
    id = id + 1
    return id
  }
}
