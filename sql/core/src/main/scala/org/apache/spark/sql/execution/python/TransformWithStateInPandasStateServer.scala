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

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException}
import java.net.ServerSocket

import scala.collection.mutable

import com.google.protobuf.ByteString

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl, StatefulProcessorHandleState}
import org.apache.spark.sql.execution.streaming.state.StateMessage.{HandleState, ImplicitGroupingKeyRequest, StatefulProcessorCall, StateRequest, StateResponse, StateVariableRequest, ValueStateCall}
import org.apache.spark.sql.streaming.ValueState
import org.apache.spark.sql.types.StructType

/**
 * This class is used to handle the state requests from the Python side. It runs on a separate
 * thread spawned by TransformWithStateInPandasStateRunner per task. It opens a dedicated socket
 * to process/transfer state related info which is shut down when task finishes or there's an error
 * on opening the socket. It run It processes following state requests and return responses to the
 * Python side:
 * - Requests for managing explicit grouping key.
 * - Stateful processor requests.
 * - Requests for managing state variables (e.g. valueState).
 */
class TransformWithStateInPandasStateServer(
    private val stateServerSocket: ServerSocket,
    private val statefulProcessorHandle: StatefulProcessorHandleImpl,
    private val groupingKeySchema: StructType,
    private val outputStreamForTest: DataOutputStream = null,
    private val valueStateMapForTest: mutable.HashMap[String, ValueState[Row]] = null)
  extends Runnable with Logging {
  private var inputStream: DataInputStream = _
  private var outputStream: DataOutputStream = outputStreamForTest
  private val valueStates = if (valueStateMapForTest != null) {
    valueStateMapForTest
  } else {
    new mutable.HashMap[String, ValueState[Row]]()
  }

  def run(): Unit = {
    val listeningSocket = stateServerSocket.accept()
    inputStream = new DataInputStream(
      new BufferedInputStream(listeningSocket.getInputStream))
    outputStream = new DataOutputStream(
      new BufferedOutputStream(listeningSocket.getOutputStream)
    )

    while (listeningSocket.isConnected &&
      statefulProcessorHandle.getHandleState != StatefulProcessorHandleState.CLOSED) {
      try {
        val version = inputStream.readInt()
        if (version != -1) {
          assert(version == 0)
          val message = parseProtoMessage()
          handleRequest(message)
          outputStream.flush()
        }
      } catch {
        case _: EOFException =>
          logWarning(log"No more data to read from the socket")
          statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
          return
        case e: Exception =>
          logError(log"Error reading message: ${MDC(LogKeys.ERROR, e.getMessage)}", e)
          sendResponse(1, e.getMessage)
          outputStream.flush()
          statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
          return
      }
    }
    logInfo(log"Done from the state server thread")
  }

  private def parseProtoMessage(): StateRequest = {
    val messageLen = inputStream.readInt()
    val messageBytes = new Array[Byte](messageLen)
    inputStream.read(messageBytes)
    StateRequest.parseFrom(ByteString.copyFrom(messageBytes))
  }

  private def handleRequest(message: StateRequest): Unit = {
    message.getMethodCase match {
      case StateRequest.MethodCase.IMPLICITGROUPINGKEYREQUEST =>
        handleImplicitGroupingKeyRequest(message.getImplicitGroupingKeyRequest)
      case StateRequest.MethodCase.STATEFULPROCESSORCALL =>
        handleStatefulProcessorCall(message.getStatefulProcessorCall)
      case StateRequest.MethodCase.STATEVARIABLEREQUEST =>
        handleStateVariableRequest(message.getStateVariableRequest)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private def handleImplicitGroupingKeyRequest(message: ImplicitGroupingKeyRequest): Unit = {
    message.getMethodCase match {
      case ImplicitGroupingKeyRequest.MethodCase.SETIMPLICITKEY =>
        val keyBytes = message.getSetImplicitKey.getKey.toByteArray
        // The key row is serialized as a byte array, we need to convert it back to a Row
        val keyRow = PythonSQLUtils.toJVMRow(keyBytes, groupingKeySchema,
          ExpressionEncoder(groupingKeySchema).resolveAndBind().createDeserializer())
        ImplicitGroupingKeyTracker.setImplicitKey(keyRow)
        sendResponse(0)
      case ImplicitGroupingKeyRequest.MethodCase.REMOVEIMPLICITKEY =>
        ImplicitGroupingKeyTracker.removeImplicitKey()
        sendResponse(0)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private[sql] def handleStatefulProcessorCall(message: StatefulProcessorCall): Unit = {
    message.getMethodCase match {
      case StatefulProcessorCall.MethodCase.SETHANDLESTATE =>
        val requestedState = message.getSetHandleState.getState
        requestedState match {
          case HandleState.CREATED =>
            logInfo(log"set handle state to Created")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CREATED)
          case HandleState.INITIALIZED =>
            logInfo(log"set handle state to Initialized")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
          case HandleState.CLOSED =>
            logInfo(log"set handle state to Closed")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
          case _ =>
        }
        sendResponse(0)
      case StatefulProcessorCall.MethodCase.GETVALUESTATE =>
        val stateName = message.getGetValueState.getStateName
        val schema = message.getGetValueState.getSchema
        initializeValueState(stateName, schema)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private def handleStateVariableRequest(message: StateVariableRequest): Unit = {
    message.getMethodCase match {
      case StateVariableRequest.MethodCase.VALUESTATECALL =>
        handleValueStateRequest(message.getValueStateCall)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private[sql] def handleValueStateRequest(message: ValueStateCall): Unit = {
    val stateName = message.getStateName
    message.getMethodCase match {
      case ValueStateCall.MethodCase.EXISTS =>
        if (valueStates.contains(stateName) && valueStates(stateName).exists()) {
          sendResponse(0)
        } else {
          sendResponse(1, s"state $stateName doesn't exist")
        }
      case ValueStateCall.MethodCase.GET =>
        if (valueStates.contains(stateName)) {
          val valueOption = valueStates(stateName).getOption()
          if (valueOption.isDefined) {
            sendResponse(0)
            // Serialize the value row as a byte array
            val valueBytes = PythonSQLUtils.toPyRow(valueOption.get)
            outputStream.writeInt(valueBytes.length)
            outputStream.write(valueBytes)
          } else {
            logWarning(log"state ${MDC(LogKeys.STATE_NAME, stateName)} doesn't exist")
            sendResponse(1, s"state $stateName doesn't exist")
          }
        } else {
          logWarning(log"state ${MDC(LogKeys.STATE_NAME, stateName)} doesn't exist")
          sendResponse(1, s"state $stateName doesn't exist")
        }
      case ValueStateCall.MethodCase.VALUESTATEUPDATE =>
        val byteArray = message.getValueStateUpdate.getValue.toByteArray
        val schema = StructType.fromString(message.getValueStateUpdate.getSchema)
        // The value row is serialized as a byte array, we need to convert it back to a Row
        val valueRow = PythonSQLUtils.toJVMRow(byteArray, schema,
          ExpressionEncoder(schema).resolveAndBind().createDeserializer())
        if (valueStates.contains(stateName)) {
          valueStates(stateName).update(valueRow)
          sendResponse(0)
        } else {
          logWarning(log"state ${MDC(LogKeys.STATE_NAME, stateName)} doesn't exist")
          sendResponse(1, s"state $stateName doesn't exist")
        }
      case ValueStateCall.MethodCase.CLEAR =>
        if (valueStates.contains(stateName)) {
          valueStates(stateName).clear()
          sendResponse(0)
        } else {
          logWarning(log"state ${MDC(LogKeys.STATE_NAME, stateName)} doesn't exist")
          sendResponse(1, s"state $stateName doesn't exist")
        }
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private def sendResponse(status: Int, errorMessage: String = null): Unit = {
    val responseMessageBuilder = StateResponse.newBuilder().setStatusCode(status)
    if (status != 0 && errorMessage != null) {
      responseMessageBuilder.setErrorMessage(errorMessage)
    }
    val responseMessage = responseMessageBuilder.build()
    val responseMessageBytes = responseMessage.toByteArray
    val byteLength = responseMessageBytes.length
    outputStream.writeInt(byteLength)
    outputStream.write(responseMessageBytes)
  }

  private def initializeValueState(stateName: String, schema: String): Unit = {
    if (!valueStates.contains(stateName)) {
      val state = statefulProcessorHandle.getValueState[Row](stateName,
        Encoders.row(StructType.fromString(schema)))
      valueStates.put(stateName, state)
      sendResponse(0)
    } else {
      sendResponse(1, s"state $stateName already exists")
    }
  }
}
