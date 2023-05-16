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

package org.apache.spark.sql.connect.service

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.connect.service.SparkConnectForeachBatchStateManager.StateValue
import org.apache.spark.sql.connect.service.State.State

private[connect] class SparkConnectForeachBatchStateManager {
  // key is (queryId, runId)
  private val stateMap = new ConcurrentHashMap[(String, String), StateValue]()

  def updateToProcessing(queryId: String, runId: String, dfId: String, batchId: Long): Unit = {
    assert(stateMap.containsKey((queryId, runId)))
    assert(stateMap.get((queryId, runId)).state == State.WAITING)

    stateMap.put((queryId, runId), StateValue(State.PROCESSING, Option(dfId), Option(batchId)))
  }

  def updateToWaiting(queryId: String, runId: String): Unit = {
    stateMap.put((queryId, runId), StateValue(State.WAITING, None, None))
  }

  def updateToFinished(queryId: String, runId: String, batchId: Long): Unit = {
    assert(stateMap.containsKey((queryId, runId)))

    val stateValue = stateMap.get((queryId, runId))
    assert(stateValue.state == State.PROCESSING)
    assert(stateValue.batchId.isDefined && stateValue.batchId.get == batchId)

    stateMap.put((queryId, runId),
      StateValue(State.FINISHED, stateValue.dfId, stateValue.batchId))
  }

  def isFinished(queryId: String, runId: String): Boolean = {
    assert(stateMap.containsKey((queryId, runId)))

    stateMap.get((queryId, runId)).state == State.FINISHED
  }

  def isProcessing(queryId: String, runId: String): Boolean = {
    stateMap.containsKey((queryId, runId)) &&
      (stateMap.get((queryId, runId)).state == State.PROCESSING)
  }

  def getStateValue(queryId: String, runId: String): StateValue = {
    assert(stateMap.containsKey((queryId, runId)))
    stateMap.get((queryId, runId))
  }

  // TODO: clean up state
}


private[connect] object SparkConnectForeachBatchStateManager {
  case class StateValue(state: State, dfId: Option[String], batchId: Option[Long])
}

private[connect] object State extends Enumeration {
  type State = Value

  val WAITING, PROCESSING, FINISHED = Value
}