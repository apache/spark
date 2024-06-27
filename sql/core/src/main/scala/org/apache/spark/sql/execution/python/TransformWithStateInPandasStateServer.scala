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

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.ServerSocket

import scala.collection.mutable
import scala.util.control.Breaks.break

import com.google.protobuf.ByteString

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl, StatefulProcessorHandleState}
import org.apache.spark.sql.execution.streaming.state.StateMessage.{HandleState, ImplicitGroupingKeyRequest, StatefulProcessorCall, StateRequest, StateVariableRequest, ValueStateCall}
import org.apache.spark.sql.streaming.ValueState
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType}

/**
 * This class is used to handle the state requests from the Python side.
 */
class TransformWithStateInPandasStateServer(
    private val stateServerSocket: ServerSocket,
    private val statefulProcessorHandle: StatefulProcessorHandleImpl,
    private val groupingKeySchema: StructType)
  extends Runnable
  with Logging{

  private var inputStream: DataInputStream = _
  private var outputStream: DataOutputStream = _

  private val valueStates = mutable.HashMap[String, ValueState[String]]()

  def run(): Unit = {
    logWarning(s"Waiting for connection from Python worker")
    val listeningSocket = stateServerSocket.accept()
    logWarning(s"listening on socket - ${listeningSocket.getLocalAddress}")

    inputStream = new DataInputStream(
      new BufferedInputStream(listeningSocket.getInputStream))
    outputStream = new DataOutputStream(
      new BufferedOutputStream(listeningSocket.getOutputStream)
    )

    while (listeningSocket.isConnected &&
      statefulProcessorHandle.getHandleState != StatefulProcessorHandleState.CLOSED) {

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
        outputStream.writeInt(0)
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
          if (valueStates.contains(stateName)) {
            logWarning(s"state $stateName exists")
            outputStream.writeInt(0)
          } else {
            logWarning(s"state $stateName doesn't exist")
            outputStream.writeInt(1)
          }
        } else if (message.getStateVariableRequest.getValueStateCall.getMethodCase ==
          ValueStateCall.MethodCase.GET) {
          val stateName = message.getStateVariableRequest.getValueStateCall.getGet.getStateName
          val valueState = valueStates.get(stateName).get
          val valueOption = valueState.getOption()
          if (valueOption.isDefined) {
            outputStream.writeInt(0)
            val value = valueOption.get
            logWarning("got state value " + value)
            val valueBytes = value.getBytes("UTF-8")
            val byteLength = valueBytes.length
            logWarning(s"writing value bytes of length $byteLength")
            outputStream.writeInt(byteLength)
            logWarning(s"writing value bytes: ${valueBytes.mkString("Array(", ", ", ")")}")
            outputStream.write(valueBytes)
          } else {
            logWarning("didn't get state value")
            outputStream.writeInt(1)
          }
        } else if (message.getStateVariableRequest.getValueStateCall.getMethodCase ==
          ValueStateCall.MethodCase.UPDATE) {
          val stateName = message.getStateVariableRequest.getValueStateCall.getUpdate.getStateName
          val schema = message.getStateVariableRequest.getValueStateCall.getUpdate.getSchema
          val value = message.getStateVariableRequest.getValueStateCall.getUpdate.getValue
          val updateValueString = value.toStringUtf8
          val structType = StructType.fromString(schema)
          val field = structType.fields(0)
          val updatedValue = castToType(updateValueString, field.dataType)
          logWarning(s"updating state $stateName with value $updatedValue and" +
            s" type ${updatedValue.getClass}")
          valueStates(stateName).update(updateValueString)
          outputStream.writeInt(0)
        } else if (message.getStateVariableRequest.getValueStateCall.getMethodCase ==
          ValueStateCall.MethodCase.CLEAR) {
          val stateName = message.getStateVariableRequest.getValueStateCall.getClear.getStateName
          valueStates(stateName).clear()
          outputStream.writeInt(0)
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
        outputStream.writeInt(0)
      } else if (message.getImplicitGroupingKeyRequest.getMethodCase ==
        ImplicitGroupingKeyRequest.MethodCase.REMOVEIMPLICITKEY) {
        logWarning(s"removing implicit key")
        ImplicitGroupingKeyTracker.removeImplicitKey()
        logWarning(s"removed implicit key")
        outputStream.writeInt(0)
      } else {
        throw new IllegalArgumentException("Invalid method call")
      }
    } else {
      throw new IllegalArgumentException("Invalid method call")
    }
  }

  private def initializeState(stateType: String, stateName: String, schema: String): Unit = {
    if (stateType == "ValueState") {
      if (!valueStates.contains(stateName)) {
        val structType = StructType.fromString(schema)
        val field = structType.fields(0)
        val encoder = getEncoder(field.dataType)
        val state = statefulProcessorHandle.getValueState[String](stateName, Encoders.STRING)
//        val state = statefulProcessorHandle.getValueState(stateName, encoder)
        valueStates.put(stateName, state)
        outputStream.writeInt(0)
      } else {
        outputStream.writeInt(1)
      }
    } else {
      outputStream.writeInt(1)
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
      case StringType => Encoders.STRING
      case _ => Encoders.STRING
    }
  }
}
