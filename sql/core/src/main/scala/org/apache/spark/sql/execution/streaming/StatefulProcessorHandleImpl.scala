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
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.StatefulProcessorHandleState.PRE_INIT
import org.apache.spark.sql.execution.streaming.StateVariableType._
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils.{getExpirationMsRowSchema, getTTLRowKeySchema}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{ListState, MapState, QueryInfo, TimeMode, TTLConfig, ValueState}
import org.apache.spark.sql.types._
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
 * Utility object to perform metrics updates
 */
object TWSMetricsUtils {
  def resetMetric(
      metrics: Map[String, SQLMetric],
      metricName: String): Unit = {
    metrics.get(metricName).foreach(_.reset())
  }

  def incrementMetric(
      metrics: Map[String, SQLMetric],
      metricName: String,
      countValue: Long = 1L): Unit = {
    metrics.get(metricName).foreach(_.add(countValue))
  }
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
    metrics: Map[String, SQLMetric] = Map.empty)
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

  override def getQueryInfo(): QueryInfo = currQueryInfo

  private lazy val timerState = new TimerStateImpl(store, timeMode, keyEncoder)

  /**
   * Function to register a timer for the given expiryTimestampMs
   * @param expiryTimestampMs - timestamp in milliseconds for the timer to expire
   */
  override def registerTimer(expiryTimestampMs: Long): Unit = {
    verifyTimerOperations("register_timer")
    timerState.registerTimer(expiryTimestampMs)
    TWSMetricsUtils.incrementMetric(metrics, "numRegisteredTimers")
  }

  /**
   * Function to delete a timer for the given expiryTimestampMs
   * @param expiryTimestampMs - timestamp in milliseconds for the timer to delete
   */
  override def deleteTimer(expiryTimestampMs: Long): Unit = {
    verifyTimerOperations("delete_timer")
    timerState.deleteTimer(expiryTimestampMs)
    TWSMetricsUtils.incrementMetric(metrics, "numDeletedTimers")
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
      numValuesRemovedDueToTTLExpiry += s.clearExpiredStateForAllKeys()
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
      TWSMetricsUtils.incrementMetric(metrics, "numDeletedStateVars")
    }
  }

  override def getValueState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ValueState[T] = {
    getValueState(stateName, ttlConfig)(valEncoder)
  }

  override def getValueState[T: Encoder](
      stateName: String,
      ttlConfig: TTLConfig): ValueState[T] = {
    verifyStateVarOperations("get_value_state", CREATED)
    val ttlEnabled = if (ttlConfig.ttlDuration != null && ttlConfig.ttlDuration.isZero) {
      false
    } else {
      true
    }

    val stateEncoder = encoderFor[T].asInstanceOf[ExpressionEncoder[Any]]
    val result = if (ttlEnabled) {
      validateTTLConfig(ttlConfig, stateName)
      assert(batchTimestampMs.isDefined)
      val valueStateWithTTL = new ValueStateImplWithTTL[T](store, stateName,
        keyEncoder, stateEncoder, ttlConfig, batchTimestampMs.get, metrics)
      ttlStates.add(valueStateWithTTL)
      TWSMetricsUtils.incrementMetric(metrics, "numValueStateWithTTLVars")
      valueStateWithTTL
    } else {
      val valueStateWithoutTTL = new ValueStateImpl[T](store, stateName,
        keyEncoder, stateEncoder, metrics)
      TWSMetricsUtils.incrementMetric(metrics, "numValueStateVars")
      valueStateWithoutTTL
    }
    result
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
    getListState(stateName, ttlConfig)(valEncoder)
  }

  override def getListState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ListState[T] = {
    verifyStateVarOperations("get_list_state", CREATED)

    val ttlEnabled = if (ttlConfig.ttlDuration != null && ttlConfig.ttlDuration.isZero) {
      false
    } else {
      true
    }

    val stateEncoder = encoderFor[T].asInstanceOf[ExpressionEncoder[Any]]
    val result = if (ttlEnabled) {
      validateTTLConfig(ttlConfig, stateName)
      assert(batchTimestampMs.isDefined)
      val listStateWithTTL = new ListStateImplWithTTL[T](store, stateName,
        keyEncoder, stateEncoder, ttlConfig, batchTimestampMs.get, metrics)
      TWSMetricsUtils.incrementMetric(metrics, "numListStateWithTTLVars")
      ttlStates.add(listStateWithTTL)
      listStateWithTTL
    } else {
      val listStateWithoutTTL = new ListStateImpl[T](store, stateName, keyEncoder,
        stateEncoder, metrics)
      TWSMetricsUtils.incrementMetric(metrics, "numListStateVars")
      listStateWithoutTTL
    }
    result
  }

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      ttlConfig: TTLConfig): MapState[K, V] = {
    getMapState(stateName, ttlConfig)(userKeyEnc, valEncoder)
  }

  override def getMapState[K: Encoder, V: Encoder](
      stateName: String,
      ttlConfig: TTLConfig): MapState[K, V] = {
    verifyStateVarOperations("get_map_state", CREATED)

    val ttlEnabled = if (ttlConfig.ttlDuration != null && ttlConfig.ttlDuration.isZero) {
      false
    } else {
      true
    }

    val userKeyEnc = encoderFor[K].asInstanceOf[ExpressionEncoder[Any]]
    val valEncoder = encoderFor[V].asInstanceOf[ExpressionEncoder[Any]]
    val result = if (ttlEnabled) {
      validateTTLConfig(ttlConfig, stateName)
      assert(batchTimestampMs.isDefined)
      val mapStateWithTTL = new MapStateImplWithTTL[K, V](store, stateName, keyEncoder, userKeyEnc,
        valEncoder, ttlConfig, batchTimestampMs.get, metrics)
      TWSMetricsUtils.incrementMetric(metrics, "numMapStateWithTTLVars")
      ttlStates.add(mapStateWithTTL)
      mapStateWithTTL
    } else {
      val mapStateWithoutTTL = new MapStateImpl[K, V](store, stateName, keyEncoder,
        userKeyEnc, valEncoder, metrics)
      TWSMetricsUtils.incrementMetric(metrics, "numMapStateVars")
      mapStateWithoutTTL
    }
    result
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
class DriverStatefulProcessorHandleImpl(timeMode: TimeMode, keyExprEnc: ExpressionEncoder[Any])
  extends StatefulProcessorHandleImplBase(timeMode, keyExprEnc) {

  // Because this is only happening on the driver side, there is only
  // one task modifying and accessing these maps at a time
  private[sql] val columnFamilySchemas: mutable.Map[String, StateStoreColFamilySchema] =
    new mutable.HashMap[String, StateStoreColFamilySchema]()

  private val stateVariableInfos: mutable.Map[String, TransformWithStateVariableInfo] =
    new mutable.HashMap[String, TransformWithStateVariableInfo]()

  // If timeMode is not None, add a timer column family schema to the operator metadata so that
  // registered timers can be read using the state data source reader.
  if (timeMode != TimeMode.None()) {
    addTimerColFamily()
  }

  def getColumnFamilySchemas(
      setNullableFields: Boolean
  ): Map[String, StateStoreColFamilySchema] = {
    val schemas = columnFamilySchemas.toMap
    if (setNullableFields) {
      schemas.map { case (colFamilyName, stateStoreColFamilySchema) =>
        colFamilyName -> stateStoreColFamilySchema.copy(
          valueSchema = stateStoreColFamilySchema.valueSchema.toNullable
        )
      }
    } else {
      schemas
    }
  }

  def getStateVariableInfos: Map[String, TransformWithStateVariableInfo] = stateVariableInfos.toMap

  private def checkIfDuplicateVariableDefined(stateVarName: String): Unit = {
    if (columnFamilySchemas.contains(stateVarName)) {
      throw StateStoreErrors.duplicateStateVariableDefined(stateVarName)
    }
  }

  private def addTimerColFamily(): Unit = {
    val stateNames = TimerStateUtils.getTimerStateVarNames(timeMode.toString)
    val primaryIndex = stateNames._1
    val secondaryIndex = stateNames._2
    val timerEncoder = new TimerKeyEncoder(keyExprEnc)
    val colFamilySchema = StateStoreColumnFamilySchemaUtils.
      getTimerStateSchema(
        primaryIndex,
        timerEncoder.schemaForKeyRow,
        timerEncoder.schemaForValueRow
      )
    columnFamilySchemas.put(primaryIndex, colFamilySchema)
    val stateVariableInfo = TransformWithStateVariableUtils.getTimerState(primaryIndex)
    stateVariableInfos.put(primaryIndex, stateVariableInfo)

    val secondaryColFamilySchema = StateStoreColumnFamilySchemaUtils.
      getSecIndexTimerStateSchema(
        secondaryIndex, timerEncoder.keySchemaForSecIndex, timerEncoder.schemaForValueRow)
    columnFamilySchemas.put(secondaryIndex, secondaryColFamilySchema)
  }

  override def getValueState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ValueState[T] = {
    getValueState(stateName, ttlConfig)(valEncoder)
  }

  override def getValueState[T: Encoder](
      stateName: String,
      ttlConfig: TTLConfig): ValueState[T] = {
    verifyStateVarOperations("get_value_state", PRE_INIT)
    val ttlEnabled = if (ttlConfig.ttlDuration != null && ttlConfig.ttlDuration.isZero) {
      false
    } else {
      true
    }

    val stateEncoder = encoderFor[T]
    val colFamilySchema = StateStoreColumnFamilySchemaUtils.
      getValueStateSchema(stateName, keyExprEnc, stateEncoder, ttlEnabled)
    checkIfDuplicateVariableDefined(stateName)
    columnFamilySchemas ++= colFamilySchema
    val stateVariableInfo = TransformWithStateVariableUtils.
      getValueState(stateName, ttlEnabled = ttlEnabled)
    stateVariableInfos.put(stateName, stateVariableInfo)
    addTTLSchemas(
      columnFamilySchemas,
      stateVariableInfo,
      stateName,
      keyExprEnc.schema
    )
    null.asInstanceOf[ValueState[T]]
  }

  override def getListState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ListState[T] = {
    getListState(stateName, ttlConfig)(valEncoder)
  }

  override def getListState[T: Encoder](
      stateName: String,
      ttlConfig: TTLConfig): ListState[T] = {
    verifyStateVarOperations("get_list_state", PRE_INIT)
    val ttlEnabled = if (ttlConfig.ttlDuration != null && ttlConfig.ttlDuration.isZero) {
      false
    } else {
      true
    }

    val stateEncoder = encoderFor[T]
    val colFamilySchema = StateStoreColumnFamilySchemaUtils.
      getListStateSchema(stateName, keyExprEnc, stateEncoder, ttlEnabled)
    checkIfDuplicateVariableDefined(stateName)
    columnFamilySchemas ++= colFamilySchema
    val stateVariableInfo = TransformWithStateVariableUtils.
      getListState(stateName, ttlEnabled = ttlEnabled)
    stateVariableInfos.put(stateName, stateVariableInfo)
    addTTLSchemas(
      columnFamilySchemas,
      stateVariableInfo,
      stateName,
      keyExprEnc.schema
    )
    null.asInstanceOf[ListState[T]]
  }

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      ttlConfig: TTLConfig): MapState[K, V] = {
    getMapState(stateName, ttlConfig)(userKeyEnc, valEncoder)
  }

  override def getMapState[K: Encoder, V: Encoder](
      stateName: String,
      ttlConfig: TTLConfig): MapState[K, V] = {
    verifyStateVarOperations("get_map_state", PRE_INIT)

    val ttlEnabled = if (ttlConfig.ttlDuration != null && ttlConfig.ttlDuration.isZero) {
      false
    } else {
      true
    }

    val userKeyEnc = encoderFor[K]
    val valEncoder = encoderFor[V]
    val colFamilySchema = StateStoreColumnFamilySchemaUtils.
      getMapStateSchema(stateName, keyExprEnc, userKeyEnc, valEncoder, ttlEnabled)
    columnFamilySchemas ++= colFamilySchema
    val stateVariableInfo = TransformWithStateVariableUtils.
      getMapState(stateName, ttlEnabled = ttlEnabled)
    stateVariableInfos.put(stateName, stateVariableInfo)
    null.asInstanceOf[MapState[K, V]]
  }

  /**
   * Gets the schema for TTL index column family which maps (expirationMs, elementKey) -> EMPTY_ROW.
   * This is used by both one-to-one and one-to-many TTL states.
   */
  def getTTLIndexSchema(
      stateName: String,
      elementKeySchema: StructType): StateStoreColFamilySchema = {
    val ttlIndexName = s"$$ttl_$stateName"
    val ttlIndexSchema = getTTLRowKeySchema(elementKeySchema)
    val emptyValueSchema = StructType(Array(StructField("__empty__", NullType)))

    StateStoreColFamilySchema(
      ttlIndexName, 0,
      ttlIndexSchema, 0,
      emptyValueSchema,
      Some(RangeKeyScanStateEncoderSpec(ttlIndexSchema, Seq(0))))
  }

  /**
   * Gets the schema for min expiry index column family which maps elementKey -> minExpirationMs.
   * This is used by one-to-many TTL states.
   */
  def getMinExpiryIndexSchema(
      stateName: String,
      elementKeySchema: StructType): StateStoreColFamilySchema = {
    val minIndexName = s"$$min_$stateName"
    val minValueSchema = getExpirationMsRowSchema()

    StateStoreColFamilySchema(
      minIndexName, 0,
      elementKeySchema, 0,
      minValueSchema,
      Some(NoPrefixKeyStateEncoderSpec(elementKeySchema)))
  }

  /**
   * Gets the schema for count index column family which maps elementKey -> count.
   * This is used by one-to-many TTL states to track number of entries.
   */
  def getCountIndexSchema(
      stateName: String,
      elementKeySchema: StructType): StateStoreColFamilySchema = {
    val countIndexName = s"$$count_$stateName"
    val countValueSchema = StructType(Seq(
      StructField("count", LongType, nullable = false)
    ))

    StateStoreColFamilySchema(
      countIndexName, 0,
      elementKeySchema, 0,
      countValueSchema,
      Some(NoPrefixKeyStateEncoderSpec(elementKeySchema)))
  }

  /**
   * Adds TTL-related column families to the schema map for value state with TTL.
   * Value state uses one-to-one TTL state which only needs the TTL index.
   */
  private def addValueStateTTLSchemas(
      columnFamilySchemas: mutable.Map[String, StateStoreColFamilySchema],
      stateName: String,
      keySchema: StructType): Unit = {
    val ttlIndexSchema = getTTLIndexSchema(stateName, keySchema)
    columnFamilySchemas.put(ttlIndexSchema.colFamilyName, ttlIndexSchema)
  }

  /**
   * Adds TTL-related column families to the schema map for list state with TTL.
   * List state uses one-to-many TTL state which needs TTL, min expiry and count indexes.
   */
  private def addListStateTTLSchemas(
      columnFamilySchemas: mutable.Map[String, StateStoreColFamilySchema],
      stateName: String,
      keySchema: StructType): Unit = {
    val ttlIndexSchema = getTTLIndexSchema(stateName, keySchema)
    val minExpirySchema = getMinExpiryIndexSchema(stateName, keySchema)
    val countSchema = getCountIndexSchema(stateName, keySchema)

    columnFamilySchemas.put(ttlIndexSchema.colFamilyName, ttlIndexSchema)
    columnFamilySchemas.put(minExpirySchema.colFamilyName, minExpirySchema)
    columnFamilySchemas.put(countSchema.colFamilyName, countSchema)
  }

  /**
   * Updates the column family schemas map to handle TTL column families.
   */
  def addTTLSchemas(
      columnFamilySchemas: mutable.Map[String, StateStoreColFamilySchema],
      stateVariableInfo: TransformWithStateVariableInfo,
      stateName: String,
      keySchema: StructType): Unit = {

    if (stateVariableInfo.ttlEnabled) {
      stateVariableInfo.stateVariableType match {
        case ValueState => addValueStateTTLSchemas(columnFamilySchemas, stateName, keySchema)
        case ListState => addListStateTTLSchemas(columnFamilySchemas, stateName, keySchema)
        case MapState => addValueStateTTLSchemas(columnFamilySchemas, stateName, keySchema)
        case other => throw new IllegalArgumentException(s"Unsupported state type: $other")
      }
    }
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
