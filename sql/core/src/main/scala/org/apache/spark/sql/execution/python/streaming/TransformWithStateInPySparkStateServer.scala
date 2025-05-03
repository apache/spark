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

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException}
import java.nio.channels.{Channels, ServerSocketChannel}
import java.time.Duration

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.config.Python.PYTHON_UNIX_DOMAIN_SOCKET_ENABLED
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.streaming.{ImplicitGroupingKeyTracker, StatefulProcessorHandleImpl, StatefulProcessorHandleImplBase, StatefulProcessorHandleState, StateVariableType}
import org.apache.spark.sql.execution.streaming.state.StateMessage.{HandleState, ImplicitGroupingKeyRequest, ListStateCall, MapStateCall, StatefulProcessorCall, StateRequest, StateResponse, StateResponseWithLongTypeVal, StateResponseWithStringTypeVal, StateVariableRequest, TimerRequest, TimerStateCallCommand, TimerValueRequest, UtilsRequest, ValueStateCall}
import org.apache.spark.sql.execution.streaming.state.StateMessage.StateResponseWithListGet
import org.apache.spark.sql.streaming.{ListState, MapState, TTLConfig, ValueState}
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

/**
 * This class is used to handle the state requests from the Python side. It runs on a separate
 * thread spawned by TransformWithStateInPySparkStateRunner per task. It opens a dedicated socket
 * to process/transfer state related info which is shut down when task finishes or there's an error
 * on opening the socket. It processes following state requests and return responses to the
 * Python side:
 * - Requests for managing explicit grouping key.
 * - Stateful processor requests.
 * - Requests for managing state variables (e.g. valueState).
 */
class TransformWithStateInPySparkStateServer(
    stateServerSocket: ServerSocketChannel,
    statefulProcessorHandle: StatefulProcessorHandleImplBase,
    groupingKeySchema: StructType,
    timeZoneId: String,
    errorOnDuplicatedFieldNames: Boolean,
    largeVarTypes: Boolean,
    arrowTransformWithStateInPySparkMaxRecordsPerBatch: Int,
    batchTimestampMs: Option[Long] = None,
    eventTimeWatermarkForEviction: Option[Long] = None,
    outputStreamForTest: DataOutputStream = null,
    valueStateMapForTest: mutable.HashMap[String, ValueStateInfo] = null,
    deserializerForTest: TransformWithStateInPySparkDeserializer = null,
    arrowStreamWriterForTest: BaseStreamingArrowWriter = null,
    listStatesMapForTest : mutable.HashMap[String, ListStateInfo] = null,
    iteratorMapForTest: mutable.HashMap[String, Iterator[Row]] = null,
    mapStatesMapForTest : mutable.HashMap[String, MapStateInfo] = null,
    keyValueIteratorMapForTest: mutable.HashMap[String, Iterator[(Row, Row)]] = null,
    expiryTimerIterForTest: Iterator[(Any, Long)] = null,
    listTimerMapForTest: mutable.HashMap[String, Iterator[Long]] = null)
  extends Runnable with Logging {

  import PythonResponseWriterUtils._

  private val keyRowDeserializer: ExpressionEncoder.Deserializer[Row] =
    ExpressionEncoder(groupingKeySchema).resolveAndBind().createDeserializer()
  private var inputStream: DataInputStream = _
  private var outputStream: DataOutputStream = outputStreamForTest

  private val isUnixDomainSock = Option(SparkEnv.get)
    .map(_.conf.get(PYTHON_UNIX_DOMAIN_SOCKET_ENABLED))
    .getOrElse(PYTHON_UNIX_DOMAIN_SOCKET_ENABLED.defaultValue.get)

  /** State variable related class variables */
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
  private val keyValueIterators = if (keyValueIteratorMapForTest != null) {
    keyValueIteratorMapForTest
  } else {
    new mutable.HashMap[String, Iterator[(Row, Row)]]()
  }

  /** Timer related class variables */
  // An iterator to store all expired timer info. This is meant to be consumed only once per
  // partition. This should be called after finishing handling all input rows.
  private var expiryTimestampIter: Option[Iterator[(Any, Long)]] =
    if (expiryTimerIterForTest != null) {
      Option(expiryTimerIterForTest)
    } else None

  // A map to store the iterator id -> Iterator[Long] mapping. This is to keep track of the
  // current iterator position for each iterator id in the same partition for a grouping key in case
  // user tries to fetch multiple iterators before the current iterator is exhausted. This is
  // used for list timer function call
  private var listTimerIters = if (listTimerMapForTest != null) {
    listTimerMapForTest
  } else new mutable.HashMap[String, Iterator[Long]]()

  def run(): Unit = {
    val listeningSocket = stateServerSocket.accept()

    // SPARK-51667: We have a pattern of sending messages continuously from one side
    // (Python -> JVM, and vice versa) before getting response from other side. Since most
    // messages we are sending are small, this triggers the bad combination of Nagle's algorithm
    // and delayed ACKs, which can cause a significant delay on the latency.
    // See SPARK-51667 for more details on how this can be a problem.
    //
    // Disabling either would work, but it's more common to disable Nagle's algorithm; there is
    // lot less reference to disabling delayed ACKs, while there are lots of resources to
    // disable Nagle's algorithm.
    if (!isUnixDomainSock) listeningSocket.socket().setTcpNoDelay(true)

    inputStream = new DataInputStream(
      new BufferedInputStream(Channels.newInputStream(listeningSocket)))
    outputStream = new DataOutputStream(
      new BufferedOutputStream(Channels.newOutputStream(listeningSocket))
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
        case _: InterruptedException =>
          logInfo(log"Thread interrupted, shutting down state server")
          Thread.currentThread().interrupt()
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
      case StateRequest.MethodCase.UTILSREQUEST =>
        handleUtilsRequest(message.getUtilsRequest)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private[sql] def handleUtilsRequest(message: UtilsRequest): Unit = {
    message.getMethodCase match {
      case UtilsRequest.MethodCase.PARSESTRINGSCHEMA =>
        val stringSchema = message.getParseStringSchema.getSchema
        val schema = CatalystSqlParser.parseTableSchema(stringSchema)
        sendResponseWithStringVal(0, null, schema.json)
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
            sendResponseWithLongVal(0, null, procTimestamp)
          case TimerValueRequest.MethodCase.GETWATERMARK =>
            val eventTimestamp: Long =
              if (eventTimeWatermarkForEviction.isDefined) eventTimeWatermarkForEviction.get
              else -1L
            sendResponseWithLongVal(0, null, eventTimestamp)
          case _ =>
            throw new IllegalArgumentException("Invalid timer value method call")
        }

      case TimerRequest.MethodCase.EXPIRYTIMERREQUEST =>
        // Note that for `getExpiryTimers` python call, as this is not a public
        // API and it will only be used by `group_ops` once per partition, we won't
        // need to worry about different function calls will interleaved and hence
        // this implementation is safe
        assert(statefulProcessorHandle.isInstanceOf[StatefulProcessorHandleImpl])
        val expiryRequest = message.getExpiryTimerRequest()
        val expiryTimestamp = expiryRequest.getExpiryTimestampMs
        if (!expiryTimestampIter.isDefined) {
          expiryTimestampIter =
            Option(statefulProcessorHandle
              .asInstanceOf[StatefulProcessorHandleImpl].getExpiredTimers(expiryTimestamp))
        }
        // expiryTimestampIter could be None in the TWSPySparkServerSuite
        if (!expiryTimestampIter.isDefined || !expiryTimestampIter.get.hasNext) {
          // iterator is exhausted, signal the end of iterator on python client
          sendResponse(1)
        } else {
          sendResponse(0)
          val outputSchema = new StructType()
            .add("key", BinaryType)
            .add(StructField("timestamp", LongType))
          sendIteratorAsArrowBatches(expiryTimestampIter.get, outputSchema,
            arrowStreamWriterForTest) { data =>
            InternalRow(PythonSQLUtils.toPyRow(data._1.asInstanceOf[Row]), data._2)
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
        // Reset the list/map state iterators for a new grouping key.
        iterators = new mutable.HashMap[String, Iterator[Row]]()
        listTimerIters = new mutable.HashMap[String, Iterator[Long]]()
        sendResponse(0)
      case ImplicitGroupingKeyRequest.MethodCase.REMOVEIMPLICITKEY =>
        ImplicitGroupingKeyTracker.removeImplicitKey()
        // Reset the list/map state iterators for a new grouping key.
        iterators = new mutable.HashMap[String, Iterator[Row]]()
        listTimerIters = new mutable.HashMap[String, Iterator[Long]]()
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
          case HandleState.PRE_INIT =>
            logInfo(log"set handle state to Pre-init")
            statefulProcessorHandle.setHandleState(StatefulProcessorHandleState.PRE_INIT)
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
        initializeStateVariable(stateName, schema, StateVariableType.ValueState, ttlDurationMs)
      case StatefulProcessorCall.MethodCase.GETLISTSTATE =>
        val stateName = message.getGetListState.getStateName
        val schema = message.getGetListState.getSchema
        val ttlDurationMs = if (message.getGetListState.hasTtl) {
          Some(message.getGetListState.getTtl.getDurationMs)
        } else {
            None
        }
        initializeStateVariable(stateName, schema, StateVariableType.ListState, ttlDurationMs)
      case StatefulProcessorCall.MethodCase.GETMAPSTATE =>
        val stateName = message.getGetMapState.getStateName
        val userKeySchema = message.getGetMapState.getSchema
        val valueSchema = message.getGetMapState.getMapStateValueSchema
        val ttlDurationMs = if (message.getGetMapState.hasTtl) {
          Some(message.getGetMapState.getTtl.getDurationMs)
        } else None
        initializeStateVariable(stateName, userKeySchema, StateVariableType.MapState, ttlDurationMs,
          valueSchema)
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
            val iteratorId = message.getTimerStateCall.getList.getIteratorId
            var iteratorOption = listTimerIters.get(iteratorId)
            if (iteratorOption.isEmpty) {
              iteratorOption = Option(statefulProcessorHandle.listTimers())
              listTimerIters.put(iteratorId, iteratorOption.get)
            }
            if (!iteratorOption.get.hasNext) {
              sendResponse(2, s"List timer iterator doesn't contain any value.")
              return
            } else {
              sendResponse(0)
            }
            val outputSchema = new StructType()
              .add(StructField("timestamp", LongType))
            sendIteratorAsArrowBatches(iteratorOption.get, outputSchema,
              arrowStreamWriterForTest) { data =>
              InternalRow(data)
            }

          case _ =>
            throw new IllegalArgumentException("Invalid timer state method call")
        }
      case StatefulProcessorCall.MethodCase.DELETEIFEXISTS =>
        val stateName = message.getDeleteIfExists.getStateName
        statefulProcessorHandle.deleteIfExists(stateName)
        if (valueStates.contains(stateName)) {
          valueStates.remove(stateName)
        } else if (listStates.contains(stateName)) {
          listStates.remove(stateName)
        } else if (mapStates.contains(stateName)) {
          mapStates.remove(stateName)
        }
        sendResponse(0)
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
        val valueOption = Option(valueStateInfo.valueState.get())
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
      new TransformWithStateInPySparkDeserializer(listStateInfo.deserializer)
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
        val rows = if (message.getListStatePut.getFetchWithArrow) {
          deserializer.readArrowBatches(inputStream)
        } else {
          val elements = message.getListStatePut.getValueList.asScala
          elements.map { e =>
            PythonSQLUtils.toJVMRow(
              e.toByteArray,
              listStateInfo.schema,
              listStateInfo.deserializer)
          }
        }
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
        } else {
          sendResponseWithListGet(0, iter = iteratorOption.get)
        }
      case ListStateCall.MethodCase.APPENDVALUE =>
        val byteArray = message.getAppendValue.getValue.toByteArray
        val newRow = PythonSQLUtils.toJVMRow(byteArray, listStateInfo.schema,
          listStateInfo.deserializer)
        listStateInfo.listState.appendValue(newRow)
        sendResponse(0)
      case ListStateCall.MethodCase.APPENDLIST =>
        val rows = if (message.getAppendList.getFetchWithArrow) {
          deserializer.readArrowBatches(inputStream)
        } else {
          val elements = message.getAppendList.getValueList.asScala
          elements.map { e =>
            PythonSQLUtils.toJVMRow(
              e.toByteArray,
              listStateInfo.schema,
              listStateInfo.deserializer)
          }
        }
        listStateInfo.listState.appendList(rows.toArray)
        sendResponse(0)
      case ListStateCall.MethodCase.CLEAR =>
        listStates(stateName).listState.clear()
        sendResponse(0)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private def sendIteratorForListState(iter: Iterator[Row]): Unit = {
    // Only write a single batch in each GET request. Stops writing row if rowCount reaches
    // the arrowTransformWithStateInPandasMaxRecordsPerBatch limit. This is to handle a case
    // when there are multiple state variables, user tries to access a different state variable
    // while the current state variable is not exhausted yet.
    var rowCount = 0
    while (iter.hasNext && rowCount < arrowTransformWithStateInPySparkMaxRecordsPerBatch) {
      val data = iter.next()

      // Serialize the value row as a byte array
      val valueBytes = PythonSQLUtils.toPyRow(data)
      val lenBytes = valueBytes.length

      outputStream.writeInt(lenBytes)
      outputStream.write(valueBytes)

      rowCount += 1
    }
    outputStream.writeInt(-1)
    outputStream.flush()
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
        val keyBytes = message.getGetValue.getUserKey.toByteArray
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
        val keyBytes = message.getContainsKey.getUserKey.toByteArray
        val keyRow = PythonSQLUtils.toJVMRow(keyBytes, mapStateInfo.keySchema,
          mapStateInfo.keyDeserializer)
        if (mapStateInfo.mapState.containsKey(keyRow)) {
          sendResponse(0)
        } else {
          sendResponse(2, s"Map state $stateName doesn't contain key ${keyRow.toString()}")
        }
      case MapStateCall.MethodCase.UPDATEVALUE =>
        val keyBytes = message.getUpdateValue.getUserKey.toByteArray
        val keyRow = PythonSQLUtils.toJVMRow(keyBytes, mapStateInfo.keySchema,
          mapStateInfo.keyDeserializer)
        val valueBytes = message.getUpdateValue.getValue.toByteArray
        val valueRow = PythonSQLUtils.toJVMRow(valueBytes, mapStateInfo.valueSchema,
          mapStateInfo.valueDeserializer)
        mapStateInfo.mapState.updateValue(keyRow, valueRow)
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
        } else {
          sendResponse(0)
          val keyValueStateSchema: StructType = StructType(
            Array(
              // key row serialized as a byte array.
              StructField("keyRow", BinaryType),
              // value row serialized as a byte array.
              StructField("valueRow", BinaryType)
            )
          )
          sendIteratorAsArrowBatches(iteratorOption.get, keyValueStateSchema,
            arrowStreamWriterForTest) {tuple =>
            val keyBytes = PythonSQLUtils.toPyRow(tuple._1)
            val valueBytes = PythonSQLUtils.toPyRow(tuple._2)
            new GenericInternalRow(
              Array[Any](
                keyBytes,
                valueBytes
              )
            )
          }
        }
      case MapStateCall.MethodCase.KEYS =>
        val iteratorId = message.getKeys.getIteratorId
        var iteratorOption = iterators.get(iteratorId)
        if (iteratorOption.isEmpty) {
          iteratorOption = Some(mapStateInfo.mapState.keys())
          iterators.put(iteratorId, iteratorOption.get)
        }
        if (!iteratorOption.get.hasNext) {
          sendResponse(2, s"Map state $stateName doesn't contain any key.")
        } else {
          sendResponse(0)
          sendIteratorAsArrowBatches(iteratorOption.get, mapStateInfo.keySchema,
            arrowStreamWriterForTest) {data => mapStateInfo.keySerializer(data)}
        }
      case MapStateCall.MethodCase.VALUES =>
        val iteratorId = message.getValues.getIteratorId
        var iteratorOption = iterators.get(iteratorId)
        if (iteratorOption.isEmpty) {
          iteratorOption = Some(mapStateInfo.mapState.values())
          iterators.put(iteratorId, iteratorOption.get)
        }
        if (!iteratorOption.get.hasNext) {
          sendResponse(2, s"Map state $stateName doesn't contain any value.")
        } else {
          sendResponse(0)
          sendIteratorAsArrowBatches(iteratorOption.get, mapStateInfo.valueSchema,
            arrowStreamWriterForTest) {data => mapStateInfo.valueSerializer(data)}
        }
      case MapStateCall.MethodCase.REMOVEKEY =>
        val keyBytes = message.getRemoveKey.getUserKey.toByteArray
        val keyRow = PythonSQLUtils.toJVMRow(keyBytes, mapStateInfo.keySchema,
          mapStateInfo.keyDeserializer)
        mapStateInfo.mapState.removeKey(keyRow)
        sendResponse(0)
      case MapStateCall.MethodCase.CLEAR =>
        mapStateInfo.mapState.clear()
        sendResponse(0)
      case _ =>
        throw new IllegalArgumentException("Invalid method call")
    }
  }

  private def initializeStateVariable(
      stateName: String,
      schemaString: String,
      stateType: StateVariableType.StateVariableType,
      ttlDurationMs: Option[Long],
      mapStateValueSchemaString: String = null): Unit = {
    val schema = StructType.fromString(schemaString)
    val expressionEncoder = ExpressionEncoder(schema).resolveAndBind()
    stateType match {
      case StateVariableType.ValueState => if (!valueStates.contains(stateName)) {
        val state = if (ttlDurationMs.isEmpty) {
          statefulProcessorHandle.getValueState[Row](stateName, Encoders.row(schema),
            TTLConfig.NONE)
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

      case StateVariableType.ListState => if (!listStates.contains(stateName)) {
        val state = if (ttlDurationMs.isEmpty) {
          statefulProcessorHandle.getListState[Row](stateName, Encoders.row(schema),
            TTLConfig.NONE)
        } else {
          statefulProcessorHandle.getListState(
            stateName, Encoders.row(schema), TTLConfig(Duration.ofMillis(ttlDurationMs.get)))
        }
        listStates.put(stateName,
          ListStateInfo(state, schema, expressionEncoder.createDeserializer(),
            expressionEncoder.createSerializer()))
        sendResponse(0)
      } else {
        sendResponse(1, s"List state $stateName already exists")
      }

      case StateVariableType.MapState => if (!mapStates.contains(stateName)) {
        val valueSchema = StructType.fromString(mapStateValueSchemaString)
        val valueExpressionEncoder = ExpressionEncoder(valueSchema).resolveAndBind()
        val state = if (ttlDurationMs.isEmpty) {
          statefulProcessorHandle.getMapState[Row, Row](stateName,
            Encoders.row(schema), Encoders.row(valueSchema), TTLConfig.NONE)
        } else {
          statefulProcessorHandle.getMapState[Row, Row](stateName, Encoders.row(schema),
            Encoders.row(valueSchema), TTLConfig(Duration.ofMillis(ttlDurationMs.get)))
        }
        mapStates.put(stateName,
          MapStateInfo(state, schema, valueSchema, expressionEncoder.createDeserializer(),
            expressionEncoder.createSerializer(), valueExpressionEncoder.createDeserializer(),
            valueExpressionEncoder.createSerializer()))
        sendResponse(0)
      } else {
        sendResponse(1, s"Map state $stateName already exists")
      }
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

    def sendResponseWithLongVal(
        status: Int,
        errorMessage: String = null,
        longVal: Long): Unit = {
      val responseMessageBuilder = StateResponseWithLongTypeVal.newBuilder().setStatusCode(status)
      if (status != 0 && errorMessage != null) {
        responseMessageBuilder.setErrorMessage(errorMessage)
      }
      responseMessageBuilder.setValue(longVal)
      val responseMessage = responseMessageBuilder.build()
      val responseMessageBytes = responseMessage.toByteArray
      val byteLength = responseMessageBytes.length
      outputStream.writeInt(byteLength)
      outputStream.write(responseMessageBytes)
    }

    def sendResponseWithStringVal(
        status: Int,
        errorMessage: String = null,
        stringVal: String): Unit = {
      val responseMessageBuilder = StateResponseWithStringTypeVal.newBuilder().setStatusCode(status)
      if (status != 0 && errorMessage != null) {
        responseMessageBuilder.setErrorMessage(errorMessage)
      }
      responseMessageBuilder.setValue(stringVal)
      val responseMessage = responseMessageBuilder.build()
      val responseMessageBytes = responseMessage.toByteArray
      val byteLength = responseMessageBytes.length
      outputStream.writeInt(byteLength)
      outputStream.write(responseMessageBytes)
    }

    def sendResponseWithListGet(
        status: Int,
        errorMessage: String = null,
        iter: Iterator[Row] = null): Unit = {
      val responseMessageBuilder = StateResponseWithListGet.newBuilder()
        .setStatusCode(status)
      if (status != 0 && errorMessage != null) {
        responseMessageBuilder.setErrorMessage(errorMessage)
      }

      if (status == 0) {
        // Only write a single batch in each GET request. Stops writing row if rowCount reaches
        // the arrowTransformWithStateInPySparkMaxRecordsPerBatch limit. This is to handle a case
        // when there are multiple state variables, user tries to access a different state variable
        // while the current state variable is not exhausted yet.
        var rowCount = 0
        while (iter.hasNext && rowCount < arrowTransformWithStateInPySparkMaxRecordsPerBatch) {
          val data = iter.next()

          // Serialize the value row as a byte array
          val valueBytes = PythonSQLUtils.toPyRow(data)

          responseMessageBuilder.addValue(ByteString.copyFrom(valueBytes))

          rowCount += 1
        }

        assert(rowCount > 0, s"rowCount should be greater than 0 when status code is 0, " +
          s"iter.hasNext ${iter.hasNext}")

        responseMessageBuilder.setRequireNextFetch(iter.hasNext)
      }

      val responseMessage = responseMessageBuilder.build()
      val responseMessageBytes = responseMessage.toByteArray
      val byteLength = responseMessageBytes.length
      outputStream.writeInt(byteLength)
      outputStream.write(responseMessageBytes)
    }

    def sendIteratorAsArrowBatches[T](
        iter: Iterator[T],
        outputSchema: StructType,
        arrowStreamWriterForTest: BaseStreamingArrowWriter = null)(func: T => InternalRow): Unit = {
      outputStream.flush()
      val arrowSchema = ArrowUtils.toArrowSchema(outputSchema, timeZoneId,
        errorOnDuplicatedFieldNames, largeVarTypes)
      val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdout writer for transformWithStateInPySpark state socket", 0, Long.MaxValue)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      val writer = new ArrowStreamWriter(root, null, outputStream)
      val arrowStreamWriter = if (arrowStreamWriterForTest != null) {
        arrowStreamWriterForTest
      } else {
        new BaseStreamingArrowWriter(root, writer,
          arrowTransformWithStateInPySparkMaxRecordsPerBatch)
      }
      // Only write a single batch in each GET request. Stops writing row if rowCount reaches
      // the arrowTransformWithStateInPySparkMaxRecordsPerBatch limit. This is to handle a case
      // when there are multiple state variables, user tries to access a different state variable
      // while the current state variable is not exhausted yet.
      var rowCount = 0
      while (iter.hasNext && rowCount < arrowTransformWithStateInPySparkMaxRecordsPerBatch) {
        val data = iter.next()
        val internalRow = func(data)
        arrowStreamWriter.writeRow(internalRow)
        rowCount += 1
      }
      arrowStreamWriter.finalizeCurrentArrowBatch()
      Utils.tryWithSafeFinally {
        // end writes footer to the output stream and doesn't clean any resources.
        // It could throw exception if the output stream is closed, so it should be
        // in the try block.
        writer.end()
      } {
        root.close()
        allocator.close()
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
