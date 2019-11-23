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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hive.service.server.HiveServer2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler._
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.ExecutionState
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.status.{ElementTrackingStore, KVUtils, LiveEntity}

/**
 * An inner sparkListener called in sc.stop to clean up the HiveThriftServer2
 */
private[thriftserver] class HiveThriftServer2Listener(
  kvstore: ElementTrackingStore,
  sparkConf: SparkConf,
  server: Option[HiveServer2],
  live: Boolean = true) extends SparkListener {

  private val sessionList = new ConcurrentHashMap[String, LiveSessionData]()
  private val executionList = new ConcurrentHashMap[String, LiveExecutionData]()

  private val (retainedStatements: Int, retainedSessions: Int) = {
    (sparkConf.get(SQLConf.THRIFTSERVER_UI_STATEMENT_LIMIT),
      sparkConf.get(SQLConf.THRIFTSERVER_UI_SESSION_LIMIT))
  }

  // Returns true if this listener has no live data. Exposed for tests only.
  private[thriftserver] def noLiveData(): Boolean = {
    sessionList.isEmpty && executionList.isEmpty
  }

  kvstore.addTrigger(classOf[SessionInfo], retainedSessions) { count =>
    cleanupSession(count)
  }

  kvstore.addTrigger(classOf[ExecutionInfo], retainedStatements) { count =>
    cleanupExecutions(count)
  }

  kvstore.onFlush {
    if (!live) {
      val now = System.nanoTime()
      flush(update(_, now))
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (live) {
      server.foreach(_.stop())
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val properties = jobStart.properties
    if (properties != null) {
      val groupId = properties.getProperty(SparkContext.SPARK_JOB_GROUP_ID)
      if (groupId != null) {
        updateJobDetails(jobStart.jobId.toString, groupId)
        }
      }
    }

  /**
   * This method is to handle out of order events. ie. if Job event come after execution end event.
   * @param jobId
   * @param groupId
   */
  private def updateJobDetails(jobId: String, groupId: String): Unit = {
    val execList = executionList.values().asScala.filter(_.groupId == groupId).toSeq
    if (execList.nonEmpty) {
      execList.foreach { exec =>
        exec.jobId += jobId.toString
        updateLiveStore(exec)
      }
    } else {
      // Here will come only if JobStart event comes after Execution End event.
      val storeExecInfo = kvstore.view(classOf[ExecutionInfo]).asScala.filter(_.groupId == groupId)
      storeExecInfo.foreach { exec =>
        val liveExec = getOrCreateExecution(exec.execId, exec.statement, exec.sessionId,
          exec.startTimestamp, exec.userName)
        liveExec.jobId += jobId.toString
        updateLiveStore(liveExec, true)
        executionList.remove(liveExec.execId)
      }
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSessionCreated => onSessionCreated(e)
      case e: SparkListenerSessionClosed => onSessionClosed(e)
      case e: SparkListenerOperationStart => onOperationStart(e)
      case e: SparkListenerOperationParsed => onOperationParsed(e)
      case e: SparkListenerOperationCanceled => onOperationCanceled(e)
      case e: SparkListenerOperationError => onOperationError(e)
      case e: SparkListenerOperationFinish => onOperationFinished(e)
      case e: SparkListenerOperationClosed => onOperationClosed(e)
      case _ => // Ignore
    }
  }

  private def onSessionCreated(e: SparkListenerSessionCreated): Unit = {
    val session = getOrCreateSession(e.sessionId, e.startTime, e.ip, e.userName)
    sessionList.put(e.sessionId, session)
    updateLiveStore(session, true)
  }

  private def onSessionClosed(e: SparkListenerSessionClosed): Unit = {
    val session = sessionList.get(e.sessionId)
    session.finishTimestamp = e.finishTime
    updateLiveStore(session, true)
    sessionList.remove(e.sessionId)
  }

  private def onOperationStart(e: SparkListenerOperationStart): Unit = {
    val info = getOrCreateExecution(
      e.id,
      e.statement,
      e.sessionId,
      e.startTime,
      e.userName)

    info.state = ExecutionState.STARTED
    executionList.put(e.id, info)
    sessionList.get(e.sessionId).totalExecution += 1
    executionList.get(e.id).groupId = e.groupId
    updateLiveStore(executionList.get(e.id))
    updateLiveStore(sessionList.get(e.sessionId), true)
  }

  private def onOperationParsed(e: SparkListenerOperationParsed): Unit = {
    executionList.get(e.id).executePlan = e.executionPlan
    executionList.get(e.id).state = ExecutionState.COMPILED
    updateLiveStore(executionList.get(e.id))
  }

  private def onOperationCanceled(e: SparkListenerOperationCanceled): Unit = {
    executionList.get(e.id).finishTimestamp = e.finishTime
    executionList.get(e.id).state = ExecutionState.CANCELED
    updateLiveStore(executionList.get(e.id))
  }

  private def onOperationError(e: SparkListenerOperationError): Unit = {
    executionList.get(e.id).finishTimestamp = e.finishTime
    executionList.get(e.id).detail = e.errorMsg
    executionList.get(e.id).state = ExecutionState.FAILED
    updateLiveStore(executionList.get(e.id))
  }

  private def onOperationFinished(e: SparkListenerOperationFinish): Unit = {
    executionList.get(e.id).finishTimestamp = e.finishTime
    executionList.get(e.id).state = ExecutionState.FINISHED
    updateLiveStore(executionList.get(e.id))
  }

  private def onOperationClosed(e: SparkListenerOperationClosed): Unit = {
    executionList.get(e.id).closeTimestamp = e.closeTime
    executionList.get(e.id).state = ExecutionState.CLOSED
    updateLiveStore(executionList.get(e.id), true)
    executionList.remove(e.id)
  }

  /** Go through all `LiveEntity`s and use `entityFlushFunc(entity)` to flush them. */
  private def flush(entityFlushFunc: LiveEntity => Unit): Unit = {
    sessionList.values.asScala.foreach(entityFlushFunc)
    executionList.values.asScala.foreach(entityFlushFunc)
  }

  private def update(entity: LiveEntity, now: Long): Unit = {
    entity.write(kvstore, now)
  }

  def updateLiveStore(session: LiveEntity, force: Boolean = false): Unit = {
    if (live || force == true) {
      session.write(kvstore, System.nanoTime())
    }
  }

  private def getOrCreateSession(
     sessionId: String,
     startTime: Long,
     ip: String,
     username: String): LiveSessionData = {
    sessionList.computeIfAbsent(sessionId,
      (_: String) => new LiveSessionData(sessionId, startTime, ip, username))
  }

  private def getOrCreateExecution(
    execId: String, statement: String,
    sessionId: String, startTimestamp: Long,
    userName: String): LiveExecutionData = {
    executionList.computeIfAbsent(execId,
      (_: String) => new LiveExecutionData(execId, statement, sessionId, startTimestamp, userName))
  }

  private def cleanupExecutions(count: Long): Unit = {
    val countToDelete = calculateNumberToRemove(count, retainedStatements)
    if (countToDelete <= 0L) {
      return
    }
    val view = kvstore.view(classOf[ExecutionInfo]).index("finishTime").first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt) { j =>
      j.finishTimestamp != 0
    }
    toDelete.foreach { j => kvstore.delete(j.getClass, j.execId) }
  }

  private def cleanupSession(count: Long): Unit = {
    val countToDelete = calculateNumberToRemove(count, retainedSessions)
    if (countToDelete <= 0L) {
      return
    }
    val view = kvstore.view(classOf[SessionInfo]).index("finishTime").first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt) { j =>
      j.finishTimestamp != 0L
    }

    toDelete.foreach { j => kvstore.delete(j.getClass, j.sessionId) }
  }

  /**
   * Remove at least (retainedSize / 10) items to reduce friction. Because tracking may be done
   * asynchronously, this method may return 0 in case enough items have been deleted already.
   */
  private def calculateNumberToRemove(dataSize: Long, retainedSize: Long): Long = {
    if (dataSize > retainedSize) {
      math.max(retainedSize / 10L, dataSize - retainedSize)
    } else {
      0L
    }
  }
}

private[thriftserver] class LiveExecutionData(
    val execId: String,
    val statement: String,
    val sessionId: String,
    val startTimestamp: Long,
    val userName: String) extends LiveEntity {

    var finishTimestamp: Long = 0L
    var closeTimestamp: Long = 0L
    var executePlan: String = ""
    var detail: String = ""
    var state: ExecutionState.Value = ExecutionState.STARTED
    val jobId: ArrayBuffer[String] = ArrayBuffer[String]()
    var groupId: String = ""

  override protected def doUpdate(): Any = {
    new ExecutionInfo(
      execId,
      statement,
      sessionId,
      startTimestamp,
      userName,
      finishTimestamp,
      closeTimestamp,
      executePlan,
      detail,
      state,
      jobId,
      groupId)
  }
}


private[thriftserver] class LiveSessionData(
    val sessionId: String,
    val startTimeStamp: Long,
    val ip: String,
    val username: String) extends LiveEntity {

  var finishTimestamp: Long = 0L
  var totalExecution: Int = 0

  override protected def doUpdate(): Any = {
    new SessionInfo(
      sessionId,
      startTimeStamp,
      ip,
      username,
      finishTimestamp,
      totalExecution)
  }
}
