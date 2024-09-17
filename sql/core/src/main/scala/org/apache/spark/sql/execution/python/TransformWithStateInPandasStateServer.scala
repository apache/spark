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
import java.time.Duration

import scala.collection.mutable

import com.google.protobuf.ByteString
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl, StatefulProcessorHandleState}
import org.apache.spark.sql.execution.streaming.state.StateMessage.{HandleState, ImplicitGroupingKeyRequest, StatefulProcessorCall, StateRequest, StateResponse, StateVariableRequest, TimerRequest, TimerStateCallCommand, TimerValueRequest, ValueStateCall}
import org.apache.spark.sql.streaming.{TTLConfig, ValueState}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils

/**
 * This class is used to handle the state requests from the Python side. It runs on a separate
 * thread spawned by TransformWithStateInPandasStateRunner per task. It opens a dedicated socket
 * to process/transfer state related info which is shut down when task finishes or there's an error
 * on opening the socket. It processes following state requests and return responses to the
 * Python side:
 * - Requests for managing explicit grouping key.
 * - Stateful processor requests.
 * - Requests for managing state variables (e.g. valueState).
 */
class TransformWithStateInPandasStateServer(
    stateServerSocket: ServerSocket,
    statefulProcessorHandle: StatefulProcessorHandleImpl,
    groupingKeySchema: StructType,
    timeZoneId: String,
    errorOnDuplicatedFieldNames: Boolean,
    largeVarTypes: Boolean,
    arrowTransformWithStateInPandasMaxRecordsPerBatch: Int,
    outputStreamForTest: DataOutputStream = null,
    valueStateMapForTest: mutable.HashMap[String,
      (ValueState[Row], StructType, ExpressionEncoder.Deserializer[Row])] = null,
    batchTimestampMs: Option[Long] = None,
    eventTimeWatermarkForEviction: Option[Long] = None)
  extends Runnable with Logging {

  import PythonResponseWriterUtils._

  private val keyRowDeserializer: ExpressionEncoder.Deserializer[Row] =
    ExpressionEncoder(groupingKeySchema).resolveAndBind().createDeserializer()
  private var inputStream: DataInputStream = _
  private var outputStream: DataOutputStream = outputStreamForTest
  // A map to store the value state name -> (value state, schema, value row deserializer) mapping.
  private val valueStates = if (valueStateMapForTest != null) {
    valueStateMapForTest
  } else {
    new mutable.HashMap[String, (ValueState[Row], StructType,
      ExpressionEncoder.Deserializer[Row])]()
  }

  private var expiryTimestampIter: Option[Iterator[(Any, Long)]] = None

  private var listTimerIter: Option[Iterator[Long]] = None

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
      case StateRequest.MethodCase.TIMERREQUEST =>
        handleTimerRequest(message.getTimerRequest)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private[sql] def handleTimerRequest(message: TimerRequest): Unit = {
    message.getMethodCase match {
      case TimerRequest.MethodCase.TIMERVALUEREQUEST =>
        val timerRequest = message.getTimerValueRequest()
        timerRequest.getMethodCase match {
          case TimerValueRequest.MethodCase.GETPROCESSINGTIMER =>
            val procTimestamp: Long =
              if (batchTimestampMs.isDefined) batchTimestampMs.get else -1L
            val byteString = PythonResponseWriterUtils.serializeLongToByteString(procTimestamp)
            sendResponse(0, null, byteString)
          case TimerValueRequest.MethodCase.GETWATERMARK =>
            val eventTimestamp: Long =
              if (eventTimeWatermarkForEviction.isDefined) eventTimeWatermarkForEviction.get
              else -1L
            val byteString = PythonResponseWriterUtils.serializeLongToByteString(eventTimestamp)
            sendResponse(0, null, byteString)
          case _ =>
            throw new IllegalArgumentException("Invalid timer value method call")
        }

      case TimerRequest.MethodCase.EXPIRYTIMERREQUEST =>
        val expiryRequest = message.getExpiryTimerRequest()
        val expiryTimestamp = expiryRequest.getExpiryTimestampMs
        if (!expiryTimestampIter.isDefined) {
          expiryTimestampIter =
            Option(statefulProcessorHandle.getExpiredTimersWithKeyRow(expiryTimestamp))
        }
        // expiryTimestampIter could be None in the TWSPandasServerSuite
        if (!expiryTimestampIter.isDefined || !expiryTimestampIter.get.hasNext) {
          // iterator is exhausted, signal the end of iterator on python client
          sendResponse(1)
        } else {
          sendResponse(0)
          val outputSchema = new StructType()
            .add("key", groupingKeySchema)
            .add(StructField("timestamp", LongType))
          sendIteratorAsArrowBatches(expiryTimestampIter.get, outputSchema) { data =>
            InternalRow(data._1, data._2)
          }
        }

      case _ =>
        throw new IllegalArgumentException("Invalid timer request method call")
    }
  }

  private def handleImplicitGroupingKeyRequest(message: ImplicitGroupingKeyRequest): Unit = {
    message.getMethodCase match {
      case ImplicitGroupingKeyRequest.MethodCase.SETIMPLICITKEY =>
        val keyBytes = message.getSetImplicitKey.getKey.toByteArray
        // The key row is serialized as a byte array, we need to convert it back to a Row
        val keyRow = PythonSQLUtils.toJVMRow(keyBytes, groupingKeySchema, keyRowDeserializer)
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
          case HandleState.DATA_PROCESSED =>
            logInfo(log"set handle state to Data Processed")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.DATA_PROCESSED)
          case HandleState.TIMER_PROCESSED =>
            logInfo(log"set handle state to Timer Processed")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.TIMER_PROCESSED)
          case HandleState.CLOSED =>
            logInfo(log"set handle state to Closed")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
          case _ =>
        }
        sendResponse(0)
      case StatefulProcessorCall.MethodCase.GETVALUESTATE =>
        val stateName = message.getGetValueState.getStateName
        val schema = message.getGetValueState.getSchema
        val ttlDurationMs = if (message.getGetValueState.hasTtl) {
          Some(message.getGetValueState.getTtl.getDurationMs)
        } else None
        initializeValueState(stateName, schema, ttlDurationMs)

      case StatefulProcessorCall.MethodCase.TIMERSTATECALL =>
        message.getTimerStateCall.getMethodCase match {
          case TimerStateCallCommand.MethodCase.REGISTER =>
            val expiryTimestamp =
              message.getTimerStateCall.getRegister.getExpiryTimestampMs
            statefulProcessorHandle.registerTimer(expiryTimestamp)
            sendResponse(0)
          case TimerStateCallCommand.MethodCase.DELETE =>
            val expiryTimestamp =
              message.getTimerStateCall.getDelete.getExpiryTimestampMs
            statefulProcessorHandle.deleteTimer(expiryTimestamp)
            sendResponse(0)
          case TimerStateCallCommand.MethodCase.LIST =>
            if (!listTimerIter.isDefined) {
              listTimerIter = Option(statefulProcessorHandle.listTimers())
            }
            // listTimerIter could be None in the TWSPandasServerSuite
            if (!listTimerIter.isDefined || !listTimerIter.get.hasNext) {
              // avoid sending over empty batch
              sendResponse(1)
            } else {
              sendResponse(0)
              val outputSchema = new StructType()
                .add(StructField("timestamp", LongType))
              sendIteratorAsArrowBatches(listTimerIter.get, outputSchema) { data =>
                InternalRow(data)
              }
            }

          case _ =>
            throw new IllegalArgumentException("Invalid timer state method call")
        }

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
    if (!valueStates.contains(stateName)) {
      logWarning(log"Value state ${MDC(LogKeys.STATE_NAME, stateName)} is not initialized.")
      sendResponse(1, s"Value state $stateName is not initialized.")
      return
    }
    message.getMethodCase match {
      case ValueStateCall.MethodCase.EXISTS =>
        if (valueStates(stateName)._1.exists()) {
          sendResponse(0)
        } else {
          // Send status code 2 to indicate that the value state doesn't have a value yet.
          sendResponse(2, s"state $stateName doesn't exist")
        }
      case ValueStateCall.MethodCase.GET =>
        val valueOption = valueStates(stateName)._1.getOption()
        if (valueOption.isDefined) {
          // Serialize the value row as a byte array
          val valueBytes = PythonSQLUtils.toPyRow(valueOption.get)
          val byteString = ByteString.copyFrom(valueBytes)
          sendResponse(0, null, byteString)
        } else {
          logWarning(log"Value state ${MDC(LogKeys.STATE_NAME, stateName)} doesn't contain" +
            log" a value.")
          sendResponse(0)
        }
      case ValueStateCall.MethodCase.VALUESTATEUPDATE =>
        val byteArray = message.getValueStateUpdate.getValue.toByteArray
        val valueStateTuple = valueStates(stateName)
        // The value row is serialized as a byte array, we need to convert it back to a Row
        val valueRow = PythonSQLUtils.toJVMRow(byteArray, valueStateTuple._2, valueStateTuple._3)
        valueStateTuple._1.update(valueRow)
        sendResponse(0)
      case ValueStateCall.MethodCase.CLEAR =>
        valueStates(stateName)._1.clear()
        sendResponse(0)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private def initializeValueState(
      stateName: String,
      schemaString: String,
      ttlDurationMs: Option[Int]): Unit = {
    if (!valueStates.contains(stateName)) {
      val schema = StructType.fromString(schemaString)
      val state = if (ttlDurationMs.isEmpty) {
        statefulProcessorHandle.getValueState[Row](stateName, Encoders.row(schema))
      } else {
        statefulProcessorHandle.getValueState(
          stateName, Encoders.row(schema), TTLConfig(Duration.ofMillis(ttlDurationMs.get)))
      }
      val valueRowDeserializer = ExpressionEncoder(schema).resolveAndBind().createDeserializer()
      valueStates.put(stateName, (state, schema, valueRowDeserializer))
      sendResponse(0)
    } else {
      sendResponse(1, s"state $stateName already exists")
    }
  }

  /** Utils object for sending response to Python client. */
  private object PythonResponseWriterUtils {
    def sendResponse(
        status: Int,
        errorMessage: String = null,
        byteString: ByteString = null): Unit = {
      val responseMessageBuilder = StateResponse.newBuilder().setStatusCode(status)
      if (status != 0 && errorMessage != null) {
        responseMessageBuilder.setErrorMessage(errorMessage)
      }
      if (byteString != null) {
        responseMessageBuilder.setValue(byteString)
      }
      val responseMessage = responseMessageBuilder.build()
      val responseMessageBytes = responseMessage.toByteArray
      val byteLength = responseMessageBytes.length
      outputStream.writeInt(byteLength)
      outputStream.write(responseMessageBytes)
    }

    def serializeLongToByteString(longValue: Long): ByteString = {
      // ByteBuffer defaults to big-endian byte order.
      // This is accordingly deserialized as big-endian on Python client.
      val valueBytes =
      java.nio.ByteBuffer.allocate(8).putLong(longValue).array()
      ByteString.copyFrom(valueBytes)
    }

    def sendIteratorAsArrowBatches[T](
        iter: Iterator[T], outputSchema: StructType)(func: T => InternalRow): Unit = {
      outputStream.flush()
      val arrowStreamWriter = {
        val arrowSchema = ArrowUtils.toArrowSchema(outputSchema, timeZoneId,
          errorOnDuplicatedFieldNames, largeVarTypes)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for transformWithStateInPandas state socket", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        new BaseStreamingArrowWriter(root, new ArrowStreamWriter(root, null, outputStream),
          arrowTransformWithStateInPandasMaxRecordsPerBatch)
      }
      while (iter.hasNext) {
        val data = iter.next()
        val internalRow = func(data)
        arrowStreamWriter.writeRow(internalRow)
      }
      arrowStreamWriter.finalizeCurrentArrowBatch()
    }
  }
}
