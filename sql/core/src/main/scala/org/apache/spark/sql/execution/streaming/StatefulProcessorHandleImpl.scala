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

import java.util.UUID

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{ListState, MapState, QueryInfo, StatefulProcessorHandle, TimeoutMode, ValueState}
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
  val CREATED, INITIALIZED, DATA_PROCESSED, TIMER_PROCESSED, CLOSED = Value
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
 */
class StatefulProcessorHandleImpl(
    store: StateStore,
    runId: UUID,
    keyEncoder: ExpressionEncoder[Any],
    timeoutMode: TimeoutMode,
    isStreaming: Boolean = true)
  extends StatefulProcessorHandle with Logging {
  import StatefulProcessorHandleState._

  private val BATCH_QUERY_ID = "00000000-0000-0000-0000-000000000000"
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

  private var currState: StatefulProcessorHandleState = CREATED

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(msg)
    }
  }

  def setHandleState(newState: StatefulProcessorHandleState): Unit = {
    currState = newState
  }

  def getHandleState: StatefulProcessorHandleState = currState

  override def getValueState[T](stateName: String, valEncoder: Encoder[T]): ValueState[T] = {
    verifyStateVarOperations("get_value_state")
    val resultState = new ValueStateImpl[T](store, stateName, keyEncoder, valEncoder)
    resultState
  }

  override def getQueryInfo(): QueryInfo = currQueryInfo

  private lazy val timerState = new TimerStateImpl(store, timeoutMode, keyEncoder)

  private def verifyStateVarOperations(operationType: String): Unit = {
    if (currState != CREATED) {
      throw StateStoreErrors.cannotPerformOperationWithInvalidHandleState(operationType,
        currState.toString)
    }
  }

  private def verifyTimerOperations(operationType: String): Unit = {
    if (timeoutMode == NoTimeouts) {
      throw StateStoreErrors.cannotPerformOperationWithInvalidTimeoutMode(operationType,
        timeoutMode.toString)
    }

    if (currState < INITIALIZED || currState >= TIMER_PROCESSED) {
      throw StateStoreErrors.cannotPerformOperationWithInvalidHandleState(operationType,
        currState.toString)
    }
  }

  /**
   * Function to register a timer for the given expiryTimestampMs
   * @param expiryTimestampMs - timestamp in milliseconds for the timer to expire
   */
  override def registerTimer(expiryTimestampMs: Long): Unit = {
    verifyTimerOperations("register_timer")
    timerState.registerTimer(expiryTimestampMs)
  }

  /**
   * Function to delete a timer for the given expiryTimestampMs
   * @param expiryTimestampMs - timestamp in milliseconds for the timer to delete
   */
  override def deleteTimer(expiryTimestampMs: Long): Unit = {
    verifyTimerOperations("delete_timer")
    timerState.deleteTimer(expiryTimestampMs)
  }

  /**
   * Function to retrieve all registered timers for all grouping keys
   * @return - iterator of registered timers for all grouping keys
   */
  def getExpiredTimers(): Iterator[(Any, Long)] = {
    verifyTimerOperations("get_expired_timers")
    timerState.getExpiredTimers()
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
   * Function to delete and purge state variable if defined previously
   *
   * @param stateName - name of the state variable
   */
  override def deleteIfExists(stateName: String): Unit = {
    verifyStateVarOperations("delete_if_exists")
    store.removeColFamilyIfExists(stateName)
  }

  override def getListState[T](stateName: String, valEncoder: Encoder[T]): ListState[T] = {
    verifyStateVarOperations("get_list_state")
    val resultState = new ListStateImpl[T](store, stateName, keyEncoder, valEncoder)
    resultState
  }

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V]): MapState[K, V] = {
    verifyStateVarOperations("get_map_state")
    val resultState = new MapStateImpl[K, V](store, stateName, keyEncoder, userKeyEnc, valEncoder)
    resultState
  }
}
