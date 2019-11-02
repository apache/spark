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

package org.apache.spark.sql.hive.thriftserver.ui

import com.fasterxml.jackson.annotation.JsonIgnore

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.{ExecutionState, HiveThriftServer2Listener}
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.KVStore

class HiveThriftServer2AppStatusStore(
  store: KVStore,
  val listener: Option[HiveThriftServer2Listener] = None) {

  def getSessionList: Seq[LiveSessionData] = {
    store.view(classOf[LiveSessionData]).asScala.toSeq
  }

  def getExecutionList: Seq[LiveExecutionData] = {
    store.view(classOf[LiveExecutionData]).asScala.toSeq
  }

  def getOnlineSessionNum: Int = {
    store.view(classOf[LiveSessionData]).asScala.count(_.finishTimestamp == 0)
  }

  def getSession(sessionId: String): Option[LiveSessionData] = {
    try {
      Some(store.read(classOf[LiveSessionData], sessionId))
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
    store.view(classOf[LiveExecutionData]).asScala.count(isExecutionActive)
  }

  def isExecutionActive(execInfo: LiveExecutionData): Boolean = {
    !(execInfo.state == ExecutionState.FAILED ||
      execInfo.state == ExecutionState.CANCELED ||
      execInfo.state == ExecutionState.CLOSED)
  }
}

private[thriftserver] class LiveSessionData(
  @KVIndexParam val sessionId: String,
  val startTimestamp: Long,
  val ip: String,
  val userName: String,
  val finishTimestamp: Long,
  val totalExecution: Long) {

  @JsonIgnore @KVIndexParam("sessionId")
  def session: String = sessionId

  def totalTime: Long = {
    if (finishTimestamp == 0L) {
      System.currentTimeMillis - startTimestamp
    } else {
      finishTimestamp - startTimestamp
    }
  }
}

private[thriftserver] class LiveExecutionData(
  @KVIndexParam val execId: String,
  val statement: String,
  val sessionId: String,
  val startTimestamp: Long,
  val userName: String,
  val finishTimestamp: Long,
  val closeTimestamp: Long,
  val executePlan: String,
  val detail: String,
  val state: ExecutionState.Value,
  val jobId: ArrayBuffer[String],
  val groupId: String) {

  @JsonIgnore @KVIndexParam("execId")
  def exec(): String = execId

  def totalTime(endTime: Long): Long = {
    if (endTime == 0L) {
      System.currentTimeMillis - startTimestamp
    } else {
      endTime - startTimestamp
    }
  }
}