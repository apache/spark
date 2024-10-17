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
package org.apache.spark.sql.execution.streaming

import java.util
import java.util.UUID

import scala.collection.mutable

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.StatefulProcessorHandleState.PRE_INIT
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{ListState, MapState, QueryInfo, TimeMode, TTLConfig, ValueState}
import org.apache.spark.util.Utils

/**
 * Object used to assign/retrieve/remove grouping key passed implicitly for various state
 * manipulation actions using the store handle.
 */
object ImplicitGroupingKeyTracker {
  val implicitKey: InheritableThreadLocal[Any] = new InheritableThreadLocal[Any]

  def getImplicitKeyOption: Option[Any] = Option(implicitKey.get())

  def setImplicitKey(key: Any): Unit = implicitKey.set(key)

  def removeImplicitKey(): Unit = implicitKey.remove()
}

/**
 * Enum used to track valid states for the StatefulProcessorHandle
 */
object StatefulProcessorHandleState extends Enumeration {
  type StatefulProcessorHandleState = Value
  val CREATED, PRE_INIT, INITIALIZED, DATA_PROCESSED, TIMER_PROCESSED, CLOSED = Value
}

class QueryInfoImpl(
    val queryId: UUID,
    val runId: UUID,
    val batchId: Long) extends QueryInfo {

  override def getQueryId: UUID = queryId

  override def getRunId: UUID = runId

  override def getBatchId: Long = batchId

  override def toString: String = {
    s"QueryInfo(queryId=$queryId, runId=$runId, batchId=$batchId)"
  }
}

/**
 * Class that provides a concrete implementation of a StatefulProcessorHandle. Note that we keep
 * track of valid transitions as various functions are invoked to track object lifecycle.
 * @param store - instance of state store
 * @param runId - unique id for the current run
 * @param keyEncoder - encoder for the key
 * @param isStreaming - defines whether the query is streaming or batch
 * @param batchTimestampMs - timestamp for the current batch if available
 * @param metrics - metrics to be updated as part of stateful processing
 */
class StatefulProcessorHandleImpl(
    store: StateStore,
    runId: UUID,
    keyEncoder: ExpressionEncoder[Any],
    timeMode: TimeMode,
    isStreaming: Boolean = true,
    batchTimestampMs: Option[Long] = None,
    metrics: Map[String, SQLMetric] = Map.empty,
    schemas: Map[String, StateStoreColFamilySchema] = Map.empty)
  extends StatefulProcessorHandleImplBase(timeMode, keyEncoder) with Logging {
  import StatefulProcessorHandleState._

  /**
   * Stores all the active ttl states, and is used to cleanup expired values
   * in [[doTtlCleanup()]] function.
   */
  private[sql] val ttlStates: util.List[TTLState] = new util.ArrayList[TTLState]()

  private val BATCH_QUERY_ID = "00000000-0000-0000-0000-000000000000"

  currState = CREATED

  private def buildQueryInfo(): QueryInfo = {
    val taskCtxOpt = Option(TaskContext.get())
    val (queryId, batchId) = if (!isStreaming) {
      (BATCH_QUERY_ID, 0L)
    } else if (taskCtxOpt.isDefined) {
      (taskCtxOpt.get.getLocalProperty(StreamExecution.QUERY_ID_KEY),
        taskCtxOpt.get.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY).toLong)
    } else {
      assert(Utils.isTesting, "Failed to find query id/batch Id in task context")
      (UUID.randomUUID().toString, 0L)
    }

    new QueryInfoImpl(UUID.fromString(queryId), runId, batchId)
  }

  private lazy val currQueryInfo: QueryInfo = buildQueryInfo()

  private def incrementMetric(metricName: String): Unit = {
    metrics.get(metricName).foreach(_.add(1))
  }

  override def getValueState[T](
      stateName: String,
      valEncoder: Encoder[T]): ValueState[T] = {
    verifyStateVarOperations("get_value_state", CREATED)
    incrementMetric("numValueStateVars")
    val resultState = new ValueStateImpl[T](
      store, stateName, keyEncoder, valEncoder, schemas(stateName).avroSerde)
    resultState
  }

  // For testing

  private[sql] def getValueStateWithSerde[T](
      stateName: String,
      valEncoder: Encoder[T]): ValueState[T] = {
    verifyStateVarOperations("get_value_state", CREATED)
    incrementMetric("numValueStateVars")
    val avroSerde = new StateStoreColumnFamilySchemaUtils(true).getValueStateSchema[T](
      stateName, keyEncoder, valEncoder, hasTtl = false).avroSerde
    val resultState = new ValueStateImpl[T](
      store, stateName, keyEncoder, valEncoder, avroSerde)
    resultState
  }

  override def getValueState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ValueState[T] = {
    verifyStateVarOperations("get_value_state", CREATED)
    validateTTLConfig(ttlConfig, stateName)

    assert(batchTimestampMs.isDefined)
    val valueStateWithTTL = new ValueStateImplWithTTL[T](store, stateName,
      keyEncoder, valEncoder, ttlConfig, batchTimestampMs.get, avroSerde = None)
    incrementMetric("numValueStateWithTTLVars")
    ttlStates.add(valueStateWithTTL)
    valueStateWithTTL
  }

  override def getQueryInfo(): QueryInfo = currQueryInfo

  private lazy val timerState = new TimerStateImpl(store, timeMode, keyEncoder)

  /**
   * Function to register a timer for the given expiryTimestampMs
   * @param expiryTimestampMs - timestamp in milliseconds for the timer to expire
   */
  override def registerTimer(expiryTimestampMs: Long): Unit = {
    verifyTimerOperations("register_timer")
    incrementMetric("numRegisteredTimers")
    timerState.registerTimer(expiryTimestampMs)
  }

  /**
   * Function to delete a timer for the given expiryTimestampMs
   * @param expiryTimestampMs - timestamp in milliseconds for the timer to delete
   */
  override def deleteTimer(expiryTimestampMs: Long): Unit = {
    verifyTimerOperations("delete_timer")
    incrementMetric("numDeletedTimers")
    timerState.deleteTimer(expiryTimestampMs)
  }

  /**
   * Function to retrieve all expired registered timers for all grouping keys
   * @param expiryTimestampMs Threshold for expired timestamp in milliseconds, this function
   *                          will return all timers that have timestamp less than passed threshold
   * @return - iterator of registered timers for all grouping keys
   */
  def getExpiredTimers(expiryTimestampMs: Long): Iterator[(Any, Long)] = {
    verifyTimerOperations("get_expired_timers")
    timerState.getExpiredTimers(expiryTimestampMs)
  }

  /**
   * Function to list all the registered timers for given implicit key
   * Note: calling listTimers() within the `handleInputRows` method of the StatefulProcessor
   * will return all the unprocessed registered timers, including the one being fired within the
   * invocation of `handleInputRows`.
   * @return - iterator of all the registered timers for given implicit key
   */
  def listTimers(): Iterator[Long] = {
    verifyTimerOperations("list_timers")
    timerState.listTimers()
  }

  /**
   * Performs the user state cleanup based on assigned TTl values. Any state
   * which is expired will be cleaned up from StateStore.
   */
  def doTtlCleanup(): Unit = {
    val numValuesRemovedDueToTTLExpiry = metrics.get("numValuesRemovedDueToTTLExpiry").get
    ttlStates.forEach { s =>
      numValuesRemovedDueToTTLExpiry += s.clearExpiredState()
    }
  }

  /**
   * Function to delete and purge state variable if defined previously
   *
   * @param stateName - name of the state variable
   */
  override def deleteIfExists(stateName: String): Unit = {
    verifyStateVarOperations("delete_if_exists", CREATED)
    if (store.removeColFamilyIfExists(stateName)) {
      incrementMetric("numDeletedStateVars")
    }
  }

  override def getListState[T](stateName: String, valEncoder: Encoder[T]): ListState[T] = {
    verifyStateVarOperations("get_list_state", CREATED)
    incrementMetric("numListStateVars")
    val resultState = new ListStateImpl[T](
      store, stateName, keyEncoder, valEncoder, avroSerde = None)
    resultState
  }

  /**
   * Function to create new or return existing list state variable of given type
   * with ttl. State values will not be returned past ttlDuration, and will be eventually removed
   * from the state store. Any values in listState which have expired after ttlDuration will not
   * returned on get() and will be eventually removed from the state.
   *
   * The user must ensure to call this function only within the `init()` method of the
   * StatefulProcessor.
   *
   * @param stateName  - name of the state variable
   * @param valEncoder - SQL encoder for state variable
   * @param ttlConfig  - the ttl configuration (time to live duration etc.)
   * @tparam T - type of state variable
   * @return - instance of ListState of type T that can be used to store state persistently
   */
  override def getListState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ListState[T] = {

    verifyStateVarOperations("get_list_state", CREATED)
    validateTTLConfig(ttlConfig, stateName)

    assert(batchTimestampMs.isDefined)
    val listStateWithTTL = new ListStateImplWithTTL[T](store, stateName,
      keyEncoder, valEncoder, ttlConfig, batchTimestampMs.get, avroSerde = None)
    incrementMetric("numListStateWithTTLVars")
    ttlStates.add(listStateWithTTL)

    listStateWithTTL
  }

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V]): MapState[K, V] = {
    verifyStateVarOperations("get_map_state", CREATED)
    incrementMetric("numMapStateVars")
    val resultState = new MapStateImpl[K, V](
      store, stateName, keyEncoder, userKeyEnc, valEncoder, avroSerde = None)
    resultState
  }

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      ttlConfig: TTLConfig): MapState[K, V] = {
    verifyStateVarOperations("get_map_state", CREATED)
    validateTTLConfig(ttlConfig, stateName)

    assert(batchTimestampMs.isDefined)
    val mapStateWithTTL = new MapStateImplWithTTL[K, V](store, stateName, keyEncoder, userKeyEnc,
      valEncoder, ttlConfig, batchTimestampMs.get, avroSerde = None)
    incrementMetric("numMapStateWithTTLVars")
    ttlStates.add(mapStateWithTTL)

    mapStateWithTTL
  }

  private def validateTTLConfig(ttlConfig: TTLConfig, stateName: String): Unit = {
    val ttlDuration = ttlConfig.ttlDuration
    if (timeMode != TimeMode.ProcessingTime()) {
      throw StateStoreErrors.cannotProvideTTLConfigForTimeMode(stateName, timeMode.toString)
    } else if (ttlDuration == null || ttlDuration.isNegative || ttlDuration.isZero) {
      throw StateStoreErrors.ttlMustBePositive("update", stateName)
    }
  }
}

/**
 * This DriverStatefulProcessorHandleImpl is used within TransformWithExec
 * on the driver side to collect the columnFamilySchemas before any processing is
 * actually done. We need this class because we can only collect the schemas after
 * the StatefulProcessor is initialized.
 */
class DriverStatefulProcessorHandleImpl(
    timeMode: TimeMode, keyExprEnc: ExpressionEncoder[Any], initializeAvroSerde: Boolean)
  extends StatefulProcessorHandleImplBase(timeMode, keyExprEnc) {

  // Because this is only happening on the driver side, there is only
  // one task modifying and accessing these maps at a time
  private[sql] val columnFamilySchemas: mutable.Map[String, StateStoreColFamilySchema] =
    new mutable.HashMap[String, StateStoreColFamilySchema]()

  private val stateVariableInfos: mutable.Map[String, TransformWithStateVariableInfo] =
    new mutable.HashMap[String, TransformWithStateVariableInfo]()

  // If we want use Avro serializers and deserializers, the schemaUtils will create and populate
  // these objects as a part of the schema, and will add this to the map
  // These serde objects will eventually be passed to the executors
  private val schemaUtils: StateStoreColumnFamilySchemaUtils =
    new StateStoreColumnFamilySchemaUtils(initializeAvroSerde)

  // If timeMode is not None, add a timer column family schema to the operator metadata so that
  // registered timers can be read using the state data source reader.
  if (timeMode != TimeMode.None()) {
    addTimerColFamily()
  }

  def getColumnFamilySchemas: Map[String, StateStoreColFamilySchema] = columnFamilySchemas.toMap

  def getStateVariableInfos: Map[String, TransformWithStateVariableInfo] = stateVariableInfos.toMap

  private def checkIfDuplicateVariableDefined(stateVarName: String): Unit = {
    if (columnFamilySchemas.contains(stateVarName)) {
      throw StateStoreErrors.duplicateStateVariableDefined(stateVarName)
    }
  }

  private def addTimerColFamily(): Unit = {
    val stateName = TimerStateUtils.getTimerStateVarName(timeMode.toString)
    val timerEncoder = new TimerKeyEncoder(keyExprEnc)
    val colFamilySchema = schemaUtils.
      getTimerStateSchema(stateName, timerEncoder.schemaForKeyRow, timerEncoder.schemaForValueRow)
    columnFamilySchemas.put(stateName, colFamilySchema)
    val stateVariableInfo = TransformWithStateVariableUtils.getTimerState(stateName)
    stateVariableInfos.put(stateName, stateVariableInfo)
  }

  override def getValueState[T](stateName: String, valEncoder: Encoder[T]): ValueState[T] = {
    verifyStateVarOperations("get_value_state", PRE_INIT)
    val colFamilySchema = schemaUtils.
      getValueStateSchema(stateName, keyExprEnc, valEncoder, false)
    checkIfDuplicateVariableDefined(stateName)
    columnFamilySchemas.put(stateName, colFamilySchema)
    val stateVariableInfo = TransformWithStateVariableUtils.
      getValueState(stateName, ttlEnabled = false)
    stateVariableInfos.put(stateName, stateVariableInfo)
    null.asInstanceOf[ValueState[T]]
  }

  override def getValueState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ValueState[T] = {
    verifyStateVarOperations("get_value_state", PRE_INIT)
    val colFamilySchema = schemaUtils.
      getValueStateSchema(stateName, keyExprEnc, valEncoder, true)
    checkIfDuplicateVariableDefined(stateName)
    columnFamilySchemas.put(stateName, colFamilySchema)
    val stateVariableInfo = TransformWithStateVariableUtils.
      getValueState(stateName, ttlEnabled = true)
    stateVariableInfos.put(stateName, stateVariableInfo)
    null.asInstanceOf[ValueState[T]]
  }

  override def getListState[T](stateName: String, valEncoder: Encoder[T]): ListState[T] = {
    verifyStateVarOperations("get_list_state", PRE_INIT)
    val colFamilySchema = schemaUtils.
      getListStateSchema(stateName, keyExprEnc, valEncoder, false)
    checkIfDuplicateVariableDefined(stateName)
    columnFamilySchemas.put(stateName, colFamilySchema)
    val stateVariableInfo = TransformWithStateVariableUtils.
      getListState(stateName, ttlEnabled = false)
    stateVariableInfos.put(stateName, stateVariableInfo)
    null.asInstanceOf[ListState[T]]
  }

  override def getListState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ListState[T] = {
    verifyStateVarOperations("get_list_state", PRE_INIT)
    val colFamilySchema = schemaUtils.
      getListStateSchema(stateName, keyExprEnc, valEncoder, true)
    checkIfDuplicateVariableDefined(stateName)
    columnFamilySchemas.put(stateName, colFamilySchema)
    val stateVariableInfo = TransformWithStateVariableUtils.
      getListState(stateName, ttlEnabled = true)
    stateVariableInfos.put(stateName, stateVariableInfo)
    null.asInstanceOf[ListState[T]]
  }

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V]): MapState[K, V] = {
    verifyStateVarOperations("get_map_state", PRE_INIT)
    val colFamilySchema = schemaUtils.
      getMapStateSchema(stateName, keyExprEnc, userKeyEnc, valEncoder, false)
    checkIfDuplicateVariableDefined(stateName)
    columnFamilySchemas.put(stateName, colFamilySchema)
    val stateVariableInfo = TransformWithStateVariableUtils.
      getMapState(stateName, ttlEnabled = false)
    stateVariableInfos.put(stateName, stateVariableInfo)
    null.asInstanceOf[MapState[K, V]]
  }

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      ttlConfig: TTLConfig): MapState[K, V] = {
    verifyStateVarOperations("get_map_state", PRE_INIT)
    val colFamilySchema = schemaUtils.
      getMapStateSchema(stateName, keyExprEnc, userKeyEnc, valEncoder, true)
    columnFamilySchemas.put(stateName, colFamilySchema)
    val stateVariableInfo = TransformWithStateVariableUtils.
      getMapState(stateName, ttlEnabled = true)
    stateVariableInfos.put(stateName, stateVariableInfo)
    null.asInstanceOf[MapState[K, V]]
  }

  /** Function to return queryInfo for currently running task */
  override def getQueryInfo(): QueryInfo = {
    new QueryInfoImpl(UUID.randomUUID(), UUID.randomUUID(), 0L)
  }

  /**
   * Methods that are only included to satisfy the interface.
   * These methods will fail if called from the driver side, as the handle
   * will be in the PRE_INIT phase, and all these timer operations need to be
   * called from the INITIALIZED phase.
   */
  override def registerTimer(expiryTimestampMs: Long): Unit = {
    verifyTimerOperations("register_timer")
  }

  override def deleteTimer(expiryTimestampMs: Long): Unit = {
    verifyTimerOperations("delete_timer")
  }

  override def listTimers(): Iterator[Long] = {
    verifyTimerOperations("list_timers")
    Iterator.empty
  }

  override def deleteIfExists(stateName: String): Unit = {
    verifyStateVarOperations("delete_if_exists", PRE_INIT)
  }
}
