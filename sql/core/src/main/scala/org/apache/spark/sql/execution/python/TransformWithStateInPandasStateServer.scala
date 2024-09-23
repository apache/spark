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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl, StatefulProcessorHandleState}
import org.apache.spark.sql.execution.streaming.state.StateMessage.{HandleState, ImplicitGroupingKeyRequest, ListStateCall, MapStateCall, StatefulProcessorCall, StateRequest, StateResponse, StateVariableRequest, ValueStateCall}
import org.apache.spark.sql.streaming.{ListState, MapState, TTLConfig, ValueState}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
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
    valueStateMapForTest: mutable.HashMap[String, ValueStateInfo] = null,
    deserializerForTest: TransformWithStateInPandasDeserializer = null,
    arrowStreamWriterForTest: BaseStreamingArrowWriter = null,
    listStatesMapForTest : mutable.HashMap[String, ListStateInfo] = null,
    iteratorMapForTest: mutable.HashMap[String, Iterator[Row]] = null,
    mapStatesMapForTest : mutable.HashMap[String, MapStateInfo] = null,
    keyValueIteratorMapForTest: mutable.HashMap[String, Iterator[(Row, Row)]] = null)
  extends Runnable with Logging {
  private val keyRowDeserializer: ExpressionEncoder.Deserializer[Row] =
    ExpressionEncoder(groupingKeySchema).resolveAndBind().createDeserializer()
  private var inputStream: DataInputStream = _
  private var outputStream: DataOutputStream = outputStreamForTest
  // A map to store the value state name -> (value state, schema, value row deserializer) mapping.
  private val valueStates = if (valueStateMapForTest != null) {
    valueStateMapForTest
  } else {
    new mutable.HashMap[String, ValueStateInfo]()
  }
  // A map to store the list state name -> (list state, schema, list state row deserializer,
  // list state row serializer) mapping.
  private val listStates = if (listStatesMapForTest != null) {
    listStatesMapForTest
  } else {
    new mutable.HashMap[String, ListStateInfo]()
  }
  // A map to store the iterator id -> Iterator[Row] mapping. This is to keep track of the
  // current iterator position for each iterator id in a state variable for a grouping key in case
  // user tries to fetch another state variable before the current iterator is exhausted. This is
  // mainly used for list state and map state.
  private var iterators = if (iteratorMapForTest != null) {
    iteratorMapForTest
  } else {
    new mutable.HashMap[String, Iterator[Row]]()
  }
  // A map to store the map state name -> (map state, schema, map state row deserializer,
  // map state row serializer) mapping.
  private val mapStates = if (mapStatesMapForTest != null) {
    mapStatesMapForTest
  } else {
    new mutable.HashMap[String, MapStateInfo]()
  }

  // A map to store the iterator id -> Iterator[(Row, Row)] mapping. This is to keep track of the
  // current key-value iterator position for each iterator id in a map state for a grouping key in
  // case user tries to fetch another state variable before the current iterator is exhausted.
  private var keyValueIterators = if (keyValueIteratorMapForTest != null) {
    keyValueIteratorMapForTest
  } else {
    new mutable.HashMap[String, Iterator[(Row, Row)]]()
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
        val keyRow = PythonSQLUtils.toJVMRow(keyBytes, groupingKeySchema, keyRowDeserializer)
        ImplicitGroupingKeyTracker.setImplicitKey(keyRow)
        // Reset the list state iterators for a new grouping key.
        iterators = new mutable.HashMap[String, Iterator[Row]]()
        sendResponse(0)
      case ImplicitGroupingKeyRequest.MethodCase.REMOVEIMPLICITKEY =>
        ImplicitGroupingKeyTracker.removeImplicitKey()
        // Reset the list state iterators for a new grouping key.
        iterators = new mutable.HashMap[String, Iterator[Row]]()
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
        val ttlDurationMs = if (message.getGetValueState.hasTtl) {
          Some(message.getGetValueState.getTtl.getDurationMs)
        } else None
        initializeStateVariable(stateName, schema, "valueState", ttlDurationMs)
      case StatefulProcessorCall.MethodCase.GETLISTSTATE =>
        val stateName = message.getGetListState.getStateName
        val schema = message.getGetListState.getSchema
        initializeStateVariable(stateName, schema, "listState", None)
      case StatefulProcessorCall.MethodCase.GETMAPSTATE =>
        val stateName = message.getGetMapState.getStateName
        val keySchema = message.getGetMapState.getSchema
        val valueSchema = message.getGetMapState.getMapStateValueSchema
        initializeStateVariable(stateName, keySchema, "mapState", None, valueSchema)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private def handleStateVariableRequest(message: StateVariableRequest): Unit = {
    message.getMethodCase match {
      case StateVariableRequest.MethodCase.VALUESTATECALL =>
        handleValueStateRequest(message.getValueStateCall)
      case StateVariableRequest.MethodCase.LISTSTATECALL =>
        handleListStateRequest(message.getListStateCall)
      case StateVariableRequest.MethodCase.MAPSTATECALL =>
        handleMapStateRequest(message.getMapStateCall)
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
    val valueStateInfo = valueStates(stateName)
    message.getMethodCase match {
      case ValueStateCall.MethodCase.EXISTS =>
        if (valueStateInfo.valueState.exists()) {
          sendResponse(0)
        } else {
          // Send status code 2 to indicate that the value state doesn't have a value yet.
          sendResponse(2, s"state $stateName doesn't exist")
        }
      case ValueStateCall.MethodCase.GET =>
        val valueOption = valueStateInfo.valueState.getOption()
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
        // The value row is serialized as a byte array, we need to convert it back to a Row
        val valueRow = PythonSQLUtils.toJVMRow(byteArray, valueStateInfo.schema,
          valueStateInfo.deserializer)
        valueStateInfo.valueState.update(valueRow)
        sendResponse(0)
      case ValueStateCall.MethodCase.CLEAR =>
        valueStateInfo.valueState.clear()
        sendResponse(0)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private[sql] def handleListStateRequest(message: ListStateCall): Unit = {
    val stateName = message.getStateName
    if (!listStates.contains(stateName)) {
      logWarning(log"List state ${MDC(LogKeys.STATE_NAME, stateName)} is not initialized.")
      sendResponse(1, s"List state $stateName is not initialized.")
      return
    }
    val listStateInfo = listStates(stateName)
    val deserializer = if (deserializerForTest != null) {
      deserializerForTest
    } else {
      new TransformWithStateInPandasDeserializer(listStateInfo.deserializer)
    }
    message.getMethodCase match {
      case ListStateCall.MethodCase.EXISTS =>
        if (listStateInfo.listState.exists()) {
          sendResponse(0)
        } else {
          // Send status code 2 to indicate that the list state doesn't have a value yet.
          sendResponse(2, s"state $stateName doesn't exist")
        }
      case ListStateCall.MethodCase.LISTSTATEPUT =>
        val rows = deserializer.readArrowBatches(inputStream)
        listStateInfo.listState.put(rows.toArray)
        sendResponse(0)
      case ListStateCall.MethodCase.LISTSTATEGET =>
        val iteratorId = message.getListStateGet.getIteratorId
        var iteratorOption = iterators.get(iteratorId)
        if (iteratorOption.isEmpty) {
          iteratorOption = Some(listStateInfo.listState.get())
          iterators.put(iteratorId, iteratorOption.get)
        }
        if (!iteratorOption.get.hasNext) {
          sendResponse(2, s"List state $stateName doesn't contain any value.")
          return
        } else {
          sendResponse(0)
        }
        outputStream.flush()
        val arrowStreamWriter = if (arrowStreamWriterForTest != null) {
          arrowStreamWriterForTest
        } else {
          val arrowSchema = ArrowUtils.toArrowSchema(listStateInfo.schema, timeZoneId,
            errorOnDuplicatedFieldNames, largeVarTypes)
          val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for transformWithStateInPandas state socket", 0, Long.MaxValue)
          val root = VectorSchemaRoot.create(arrowSchema, allocator)
          new BaseStreamingArrowWriter(root, new ArrowStreamWriter(root, null, outputStream),
            arrowTransformWithStateInPandasMaxRecordsPerBatch)
        }
        val listRowSerializer = listStateInfo.serializer
        // Only write a single batch in each GET request. Stops writing row if rowCount reaches
        // the arrowTransformWithStateInPandasMaxRecordsPerBatch limit. This is to avoid a case
        // when there are multiple state variables, user tries to access a different state variable
        // while the current state variable is not exhausted yet.
        var rowCount = 0
        while (iteratorOption.get.hasNext &&
          rowCount < arrowTransformWithStateInPandasMaxRecordsPerBatch) {
          val row = iteratorOption.get.next()
          val internalRow = listRowSerializer(row)
          arrowStreamWriter.writeRow(internalRow)
          rowCount += 1
        }
        arrowStreamWriter.finalizeCurrentArrowBatch()
      case ListStateCall.MethodCase.APPENDVALUE =>
        val byteArray = message.getAppendValue.getValue.toByteArray
        val newRow = PythonSQLUtils.toJVMRow(byteArray, listStateInfo.schema,
          listStateInfo.deserializer)
        listStateInfo.listState.appendValue(newRow)
        sendResponse(0)
      case ListStateCall.MethodCase.APPENDLIST =>
        val rows = deserializer.readArrowBatches(inputStream)
        listStateInfo.listState.appendList(rows.toArray)
        sendResponse(0)
      case ListStateCall.MethodCase.CLEAR =>
        listStates(stateName).listState.clear()
        sendResponse(0)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private[sql] def handleMapStateRequest(message: MapStateCall): Unit = {
    val stateName = message.getStateName
    if (!mapStates.contains(stateName)) {
      logWarning(log"Map state ${MDC(LogKeys.STATE_NAME, stateName)} is not initialized.")
      sendResponse(1, s"Map state $stateName is not initialized.")
      return
    }
    val mapStateInfo = mapStates(stateName)
    message.getMethodCase match {
      case MapStateCall.MethodCase.EXISTS =>
        if (mapStateInfo.mapState.exists()) {
          sendResponse(0)
        } else {
          // Send status code 2 to indicate that the list state doesn't have a value yet.
          sendResponse(2, s"state $stateName doesn't exist")
        }
      case MapStateCall.MethodCase.GETVALUE =>
        val keyBytes = message.getGetValue.getKey.toByteArray
        val keyRow = PythonSQLUtils.toJVMRow(keyBytes, mapStateInfo.keySchema,
          mapStateInfo.keyDeserializer)
        val value = mapStateInfo.mapState.getValue(keyRow)
        if (value != null) {
          val valueBytes = PythonSQLUtils.toPyRow(value)
          val byteString = ByteString.copyFrom(valueBytes)
          sendResponse(0, null, byteString)
        } else {
          logWarning(log"Map state ${MDC(LogKeys.STATE_NAME, stateName)} doesn't contain" +
            log" key ${MDC(LogKeys.KEY, keyRow.toString)}.")
          sendResponse(0)
        }
      case MapStateCall.MethodCase.CONTAINSKEY =>
        val keyBytes = message.getContainsKey.getKey.toByteArray
        val keyRow = PythonSQLUtils.toJVMRow(keyBytes, mapStateInfo.keySchema,
          mapStateInfo.keyDeserializer)
        if (mapStateInfo.mapState.containsKey(keyRow)) {
          sendResponse(0)
        } else {
          sendResponse(2, s"Map state $stateName doesn't contain key ${keyRow.toString()}")
        }
      case MapStateCall.MethodCase.UPDATEVALUE =>
        val keyBytes = message.getUpdateValue.getKey.toByteArray
        val keyRow = PythonSQLUtils.toJVMRow(keyBytes, mapStateInfo.keySchema,
          mapStateInfo.keyDeserializer)
        val valueBytes = message.getUpdateValue.getValue.toByteArray
        val valueRow = PythonSQLUtils.toJVMRow(valueBytes, mapStateInfo.valueSchema,
          mapStateInfo.valueDeserializer)
        mapStateInfo.mapState.updateValue(keyRow, valueRow)
        val value = mapStateInfo.mapState.getValue(keyRow)
        sendResponse(0)
      case MapStateCall.MethodCase.ITERATOR =>
        val iteratorId = message.getIterator.getIteratorId
        var iteratorOption = keyValueIterators.get(iteratorId)
        if (iteratorOption.isEmpty) {
          iteratorOption = Some(mapStateInfo.mapState.iterator())
          keyValueIterators.put(iteratorId, iteratorOption.get)
        }
        if (!iteratorOption.get.hasNext) {
          sendResponse(2, s"Map state $stateName doesn't contain any entry.")
          return
        } else {
          sendResponse(0)
        }
        outputStream.flush()
        val arrowStreamWriter = if (arrowStreamWriterForTest != null) {
          arrowStreamWriterForTest
        } else {
          val keyValueStateSchema: StructType = StructType(
            Array(
              // key row serialized as a byte array.
              StructField("keyRow", BinaryType),
              // value row serialized as a byte array.
              StructField("valueRow", BinaryType)
            )
          )
          val arrowSchema = ArrowUtils.toArrowSchema(keyValueStateSchema, timeZoneId,
            errorOnDuplicatedFieldNames, largeVarTypes)
          val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for transformWithStateInPandas state socket", 0, Long.MaxValue)
          val root = VectorSchemaRoot.create(arrowSchema, allocator)
          new BaseStreamingArrowWriter(root, new ArrowStreamWriter(root, null, outputStream),
            arrowTransformWithStateInPandasMaxRecordsPerBatch)
        }
        // Only write a single batch in each GET request. Stops writing row if rowCount reaches
        // the arrowTransformWithStateInPandasMaxRecordsPerBatch limit. This is to avoid a case
        // when there are multiple state variables, user tries to access a different state variable
        // while the current state variable is not exhausted yet.
        var rowCount = 0
        while (iteratorOption.get.hasNext &&
          rowCount < arrowTransformWithStateInPandasMaxRecordsPerBatch) {
          val (keyRow, valueRow) = iteratorOption.get.next()
          val keyBytes = PythonSQLUtils.toPyRow(keyRow)
          val valueBytes = PythonSQLUtils.toPyRow(valueRow)
          val internalRow = new GenericInternalRow(
            Array[Any](
              keyBytes,
              valueBytes
            )
          )
          arrowStreamWriter.writeRow(internalRow)
          rowCount += 1
        }
        arrowStreamWriter.finalizeCurrentArrowBatch()
      case MapStateCall.MethodCase.KEYS =>
        val iteratorId = message.getKeys.getIteratorId
        var iteratorOption = iterators.get(iteratorId)
        if (iteratorOption.isEmpty) {
          iteratorOption = Some(mapStateInfo.mapState.keys())
          iterators.put(iteratorId, iteratorOption.get)
        }
        if (!iteratorOption.get.hasNext) {
          sendResponse(2, s"Map state $stateName doesn't contain any key.")
          return
        } else {
          sendResponse(0)
        }
        outputStream.flush()
        val arrowStreamWriter = if (arrowStreamWriterForTest != null) {
          arrowStreamWriterForTest
        } else {
          val arrowSchema = ArrowUtils.toArrowSchema(mapStateInfo.keySchema, timeZoneId,
            errorOnDuplicatedFieldNames, largeVarTypes)
          val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for transformWithStateInPandas state socket", 0, Long.MaxValue)
          val root = VectorSchemaRoot.create(arrowSchema, allocator)
          new BaseStreamingArrowWriter(root, new ArrowStreamWriter(root, null, outputStream),
            arrowTransformWithStateInPandasMaxRecordsPerBatch)
        }
        val keyRowSerializer = mapStateInfo.keySerializer
        // Only write a single batch in each GET request. Stops writing row if rowCount reaches
        // the arrowTransformWithStateInPandasMaxRecordsPerBatch limit. This is to avoid a case
        // when there are multiple state variables, user tries to access a different state variable
        // while the current state variable is not exhausted yet.
        var rowCount = 0
        while (iteratorOption.get.hasNext &&
          rowCount < arrowTransformWithStateInPandasMaxRecordsPerBatch) {
          val row = iteratorOption.get.next()
          val internalRow = keyRowSerializer(row)
          arrowStreamWriter.writeRow(internalRow)
          rowCount += 1
        }
        arrowStreamWriter.finalizeCurrentArrowBatch()
      case MapStateCall.MethodCase.VALUES =>
        val iteratorId = message.getValues.getIteratorId
        var iteratorOption = iterators.get(iteratorId)
        if (iteratorOption.isEmpty) {
          iteratorOption = Some(mapStateInfo.mapState.values())
          iterators.put(iteratorId, iteratorOption.get)
        }
        if (!iteratorOption.get.hasNext) {
          sendResponse(2, s"Map state $stateName doesn't contain any value.")
          return
        } else {
          sendResponse(0)
        }
        outputStream.flush()
        val arrowStreamWriter = if (arrowStreamWriterForTest != null) {
          arrowStreamWriterForTest
        } else {
          val arrowSchema = ArrowUtils.toArrowSchema(mapStateInfo.valueSchema, timeZoneId,
            errorOnDuplicatedFieldNames, largeVarTypes)
          val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for transformWithStateInPandas state socket", 0, Long.MaxValue)
          val root = VectorSchemaRoot.create(arrowSchema, allocator)
          new BaseStreamingArrowWriter(root, new ArrowStreamWriter(root, null, outputStream),
            arrowTransformWithStateInPandasMaxRecordsPerBatch)
        }
        val valueRowSerializer = mapStateInfo.valueSerializer
        // Only write a single batch in each GET request. Stops writing row if rowCount reaches
        // the arrowTransformWithStateInPandasMaxRecordsPerBatch limit. This is to avoid a case
        // when there are multiple state variables, user tries to access a different state variable
        // while the current state variable is not exhausted yet.
        var rowCount = 0
        while (iteratorOption.get.hasNext &&
          rowCount < arrowTransformWithStateInPandasMaxRecordsPerBatch) {
          val row = iteratorOption.get.next()
          val internalRow = valueRowSerializer(row)
          arrowStreamWriter.writeRow(internalRow)
          rowCount += 1
        }
        arrowStreamWriter.finalizeCurrentArrowBatch()
      case MapStateCall.MethodCase.REMOVEKEY =>
        val keyBytes = message.getRemoveKey.getKey.toByteArray
        val keyRow = PythonSQLUtils.toJVMRow(keyBytes, mapStateInfo.keySchema,
          mapStateInfo.keyDeserializer)
        mapStateInfo.mapState.removeKey(keyRow)
        sendResponse(0)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private def sendResponse(
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

  private def initializeStateVariable(
    stateName: String,
    schemaString: String,
    stateVariable: String,
    ttlDurationMs: Option[Int],
    mapStateValueSchemaString: String = null): Unit = {
    val schema = StructType.fromString(schemaString)
    val expressionEncoder = ExpressionEncoder(schema).resolveAndBind()
    stateVariable match {
        case "valueState" => if (!valueStates.contains(stateName)) {
          val state = if (ttlDurationMs.isEmpty) {
            statefulProcessorHandle.getValueState[Row](stateName, Encoders.row(schema))
          } else {
            statefulProcessorHandle.getValueState(
              stateName, Encoders.row(schema), TTLConfig(Duration.ofMillis(ttlDurationMs.get)))
          }
          valueStates.put(stateName,
            ValueStateInfo(state, schema, expressionEncoder.createDeserializer()))
          sendResponse(0)
        } else {
          sendResponse(1, s"Value state $stateName already exists")
        }
        case "listState" => if (!listStates.contains(stateName)) {
          listStates.put(stateName,
            ListStateInfo(statefulProcessorHandle.getListState[Row](stateName,
              Encoders.row(schema)), schema, expressionEncoder.createDeserializer(),
              expressionEncoder.createSerializer()))
          sendResponse(0)
        } else {
          sendResponse(1, s"List state $stateName already exists")
        }
        case "mapState" => if (!mapStates.contains(stateName)) {
          val valueSchema = StructType.fromString(mapStateValueSchemaString)
          val valueExpressionEncoder = ExpressionEncoder(valueSchema).resolveAndBind()
          mapStates.put(stateName,
            MapStateInfo(statefulProcessorHandle.getMapState[Row, Row](stateName,
              Encoders.row(schema), Encoders.row(valueSchema)), schema, valueSchema,
              expressionEncoder.createDeserializer(), expressionEncoder.createSerializer(),
              valueExpressionEncoder.createDeserializer(),
              valueExpressionEncoder.createSerializer()))
          sendResponse(0)
        } else {
          sendResponse(1, s"Map state $stateName already exists")
        }
    }
  }
}

/**
 * Case class to store the information of a value state.
 */
case class ValueStateInfo(
    valueState: ValueState[Row],
    schema: StructType,
    deserializer: ExpressionEncoder.Deserializer[Row])

/**
 * Case class to store the information of a list state.
 */
case class ListStateInfo(
    listState: ListState[Row],
    schema: StructType,
    deserializer: ExpressionEncoder.Deserializer[Row],
    serializer: ExpressionEncoder.Serializer[Row])

/**
 * Case class to store the information of a map state.
 */
case class MapStateInfo(
    mapState: MapState[Row, Row],
    keySchema: StructType,
    valueSchema: StructType,
    keyDeserializer: ExpressionEncoder.Deserializer[Row],
    keySerializer: ExpressionEncoder.Serializer[Row],
    valueDeserializer: ExpressionEncoder.Deserializer[Row],
    valueSerializer: ExpressionEncoder.Serializer[Row])
