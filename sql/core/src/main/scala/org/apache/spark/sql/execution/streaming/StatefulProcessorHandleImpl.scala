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
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.streaming.{ListState, MapState, QueryInfo, StatefulProcessorHandle, ValueState}
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
  val CREATED, INITIALIZED, DATA_PROCESSED, CLOSED = Value
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
    verify(currState == CREATED, s"Cannot create state variable with name=$stateName after " +
      "initialization is complete")
    val resultState = new ValueStateImpl[T](store, stateName, keyEncoder, valEncoder)
    resultState
  }

  override def getQueryInfo(): QueryInfo = currQueryInfo

  /**
   * Function to delete and purge state variable if defined previously
   *
   * @param stateName - name of the state variable
   */
  override def deleteIfExists(stateName: String): Unit = {
    verify(currState == CREATED, s"Cannot delete state variable with name=$stateName after " +
      "initialization is complete")
    store.removeColFamilyIfExists(stateName)
  }

  override def getListState[T](stateName: String, valEncoder: Encoder[T]): ListState[T] = {
    verify(currState == CREATED, s"Cannot create state variable with name=$stateName after " +
      "initialization is complete")
    val resultState = new ListStateImpl[T](store, stateName, keyEncoder, valEncoder)
    resultState
  }

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V]): MapState[K, V] = {
    verify(currState == CREATED, s"Cannot create state variable with name=$stateName after " +
      "initialization is complete")
    val resultState = new MapStateImpl[K, V](store, stateName, keyEncoder, userKeyEnc, valEncoder)
    resultState
  }
}
