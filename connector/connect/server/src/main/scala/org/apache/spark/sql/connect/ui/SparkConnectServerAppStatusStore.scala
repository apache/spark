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

package org.apache.spark.sql.connect.ui

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.annotation.JsonIgnore

import org.apache.spark.status.KVUtils
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.{KVIndex, KVStore}

class SparkConnectServerAppStatusStore(store: KVStore) {
  def getSessionList: Seq[SessionInfo] = {
    KVUtils.viewToSeq(store.view(classOf[SessionInfo]))
  }

  def getExecutionList: Seq[ExecutionInfo] = {
    KVUtils.viewToSeq(store.view(classOf[ExecutionInfo]))
  }

  def getOnlineSessionNum: Int = {
    KVUtils.count(store.view(classOf[SessionInfo]))(_.finishTimestamp == 0)
  }

  def getSession(sessionId: String): Option[SessionInfo] = {
    try {
      Some(store.read(classOf[SessionInfo], sessionId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def getExecution(executionId: String): Option[ExecutionInfo] = {
    try {
      Some(store.read(classOf[ExecutionInfo], executionId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  /**
   * When an error or a cancellation occurs, we set the finishTimestamp of the statement.
   * Therefore, when we count the number of running statements, we need to exclude errors and
   * cancellations and count all statements that have not been closed so far.
   */
  def getTotalRunning: Int = {
    KVUtils.count(store.view(classOf[ExecutionInfo]))(_.isExecutionActive)
  }

  def getSessionCount: Long = {
    store.count(classOf[SessionInfo])
  }

  def getExecutionCount: Long = {
    store.count(classOf[ExecutionInfo])
  }
}

private[connect] class SessionInfo(
    @KVIndexParam val sessionId: String,
    val startTimestamp: Long,
    val userId: String,
    val finishTimestamp: Long,
    val totalExecution: Long) {
  @JsonIgnore @KVIndex("finishTime")
  private def finishTimeIndex: Long = if (finishTimestamp > 0L) finishTimestamp else -1L
  def totalTime: Long = {
    if (finishTimestamp == 0L) {
      System.currentTimeMillis - startTimestamp
    } else {
      finishTimestamp - startTimestamp
    }
  }
}

private[connect] class ExecutionInfo(
    @KVIndexParam val jobTag: String,
    val statement: String,
    val sessionId: String,
    val startTimestamp: Long,
    val userId: String,
    val operationId: String,
    val sparkSessionTags: Set[String],
    val finishTimestamp: Long,
    val closeTimestamp: Long,
    val detail: String,
    val state: ExecutionState.Value,
    val jobId: ArrayBuffer[String],
    val sqlExecId: mutable.Set[String]) {
  @JsonIgnore @KVIndex("finishTime")
  private def finishTimeIndex: Long = if (finishTimestamp > 0L && !isExecutionActive) {
    finishTimestamp
  } else -1L

  @JsonIgnore @KVIndex("isExecutionActive")
  def isExecutionActive: Boolean = {
    state == ExecutionState.STARTED ||
    state == ExecutionState.COMPILED ||
    state == ExecutionState.READY
  }

  def totalTime(endTime: Long): Long = {
    if (endTime == 0L) {
      System.currentTimeMillis - startTimestamp
    } else {
      endTime - startTimestamp
    }
  }
}

private[connect] object ExecutionState extends Enumeration {
  val STARTED, COMPILED, READY, CANCELED, FAILED, FINISHED, CLOSED = Value
  type ExecutionState = Value
}
