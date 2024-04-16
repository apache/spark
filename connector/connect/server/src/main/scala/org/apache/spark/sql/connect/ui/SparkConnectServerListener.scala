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

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{OP_ID, SESSION_ID}
import org.apache.spark.internal.config.Status.LIVE_ENTITY_UPDATE_PERIOD
import org.apache.spark.scheduler._
import org.apache.spark.sql.connect.config.Connect.{CONNECT_UI_SESSION_LIMIT, CONNECT_UI_STATEMENT_LIMIT}
import org.apache.spark.sql.connect.service._
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.status.{ElementTrackingStore, KVUtils, LiveEntity}

private[connect] class SparkConnectServerListener(
    kvstore: ElementTrackingStore,
    sparkConf: SparkConf,
    live: Boolean = true)
    extends SparkListener
    with Logging {

  private val sessionList = new mutable.LinkedHashMap[String, LiveSessionData]
  private val executionList = new mutable.LinkedHashMap[String, LiveExecutionData]

  private val (retainedStatements: Int, retainedSessions: Int) = {
    (
      SparkEnv.get.conf.get(CONNECT_UI_STATEMENT_LIMIT),
      SparkEnv.get.conf.get(CONNECT_UI_SESSION_LIMIT))
  }

  // How often to update live entities. -1 means "never update" when replaying applications,
  // meaning only the last write will happen. For live applications, this avoids a few
  // operations that we can live without when rapidly processing incoming events.
  private val liveUpdatePeriodNs = if (live) sparkConf.get(LIVE_ENTITY_UPDATE_PERIOD) else -1L

  // Returns true if this listener has no live data. Exposed for tests only.
  private[connect] def noLiveData(): Boolean = synchronized {
    sessionList.isEmpty && executionList.isEmpty
  }

  kvstore.addTrigger(classOf[SessionInfo], retainedSessions) { count =>
    cleanupSession(count)
  }

  kvstore.addTrigger(classOf[ExecutionInfo], retainedStatements) { count =>
    cleanupExecutions(count)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobTags = Option(jobStart.properties)
      .flatMap { p => Option(p.getProperty(SparkContext.SPARK_JOB_TAGS)) }
      .map(_.split(SparkContext.SPARK_JOB_TAGS_SEP).toSet)
      .getOrElse(Set())
      .toSeq
      .filter(!_.isEmpty)
      .sorted
    val executeJobTagOpt = jobTags.find {
      case ExecuteJobTag(_) => true
      case _ => false
    }
    if (executeJobTagOpt.isEmpty) {
      return
    }
    val executeJobTag = executeJobTagOpt.get
    val exec = executionList.get(executeJobTag)
    if (exec.nonEmpty) {
      exec.foreach { exec =>
        exec.jobId += jobStart.jobId.toString
        updateLiveStore(exec)
      }
    } else {
      // It may possible that event reordering happens, such a way that JobStart event come after
      // Execution end event (Refer SPARK-27019). To handle that situation, if occurs in
      // Spark Connect Server, following code will take care. Here will come only if JobStart
      // event comes after Execution End event.
      val storeExecInfo =
        KVUtils.viewToSeq(kvstore.view(classOf[ExecutionInfo]), Int.MaxValue)(exec =>
          exec.jobTag == executeJobTag)
      storeExecInfo.foreach { exec =>
        val liveExec = getOrCreateExecution(
          exec.jobTag,
          exec.statement,
          exec.sessionId,
          exec.startTimestamp,
          exec.userId,
          exec.operationId,
          exec.sparkSessionTags)
        liveExec.sqlExecId = exec.sqlExecId
        liveExec.jobId += jobStart.jobId.toString
        updateStoreWithTriggerEnabled(liveExec)
        executionList.remove(liveExec.jobTag)
      }
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart => onSQLExecutionStart(e)
      case e: SparkListenerConnectOperationStarted => onOperationStarted(e)
      case e: SparkListenerConnectOperationAnalyzed => onOperationAnalyzed(e)
      case e: SparkListenerConnectOperationReadyForExecution => onOperationReadyForExecution(e)
      case e: SparkListenerConnectOperationCanceled => onOperationCanceled(e)
      case e: SparkListenerConnectOperationFailed => onOperationFailed(e)
      case e: SparkListenerConnectOperationFinished => onOperationFinished(e)
      case e: SparkListenerConnectOperationClosed => onOperationClosed(e)
      case e: SparkListenerConnectSessionStarted => onSessionStarted(e)
      case e: SparkListenerConnectSessionClosed => onSessionClosed(e)
      case _ => // Ignore
    }
  }

  def onSQLExecutionStart(e: SparkListenerSQLExecutionStart): Unit = {
    val executeJobTagOpt = e.jobTags.find {
      case ExecuteJobTag(_) => true
      case _ => false
    }
    if (executeJobTagOpt.isEmpty) {
      return
    }
    val executeJobTag = executeJobTagOpt.get
    val exec = executionList.get(executeJobTag)
    if (exec.nonEmpty) {
      exec.foreach { exec =>
        exec.sqlExecId += e.executionId.toString
        updateLiveStore(exec)
      }
    } else {
      // This block guards against potential event re-ordering where a SQLExecutionStart
      // event is processed after a ConnectOperationClosed event, in which case the Execution
      // has already been evicted from the executionList.
      val storeExecInfo =
        KVUtils.viewToSeq(kvstore.view(classOf[ExecutionInfo]), Int.MaxValue)(exec =>
          exec.jobTag == executeJobTag)
      storeExecInfo.foreach { exec =>
        val liveExec = getOrCreateExecution(
          exec.jobTag,
          exec.statement,
          exec.sessionId,
          exec.startTimestamp,
          exec.userId,
          exec.operationId,
          exec.sparkSessionTags)
        liveExec.jobId = exec.jobId
        liveExec.sqlExecId += e.executionId.toString
        updateStoreWithTriggerEnabled(liveExec)
        executionList.remove(liveExec.jobTag)
      }
    }
  }

  private def onOperationStarted(e: SparkListenerConnectOperationStarted) = synchronized {
    val executionData = getOrCreateExecution(
      e.jobTag,
      e.statementText,
      e.sessionId,
      e.eventTime,
      e.userId,
      e.operationId,
      e.sparkSessionTags)
    executionData.state = ExecutionState.STARTED
    executionList.put(e.jobTag, executionData)
    updateLiveStore(executionData)
    sessionList.get(e.sessionId) match {
      case Some(sessionData) =>
        sessionData.totalExecution += 1
        updateLiveStore(sessionData)
      case None =>
        logWarning(
          log"onOperationStart called with unknown session id: ${MDC(SESSION_ID, e.sessionId)}." +
            log"Regardless, the operation has been registered.")
    }
  }

  private def onOperationAnalyzed(e: SparkListenerConnectOperationAnalyzed) = synchronized {
    executionList.get(e.jobTag) match {
      case Some(executionData) =>
        executionData.state = ExecutionState.COMPILED
        updateLiveStore(executionData)
      case None =>
        logWarning(
          log"onOperationAnalyzed called with " +
            log"unknown operation id: ${MDC(OP_ID, e.jobTag)}")
    }
  }

  private def onOperationReadyForExecution(
      e: SparkListenerConnectOperationReadyForExecution): Unit = synchronized {
    executionList.get(e.jobTag) match {
      case Some(executionData) =>
        executionData.state = ExecutionState.READY
        updateLiveStore(executionData)
      case None =>
        logWarning(
          log"onOperationReadyForExecution called with " +
            log"unknown operation id: ${MDC(OP_ID, e.jobTag)}")
    }
  }

  private def onOperationCanceled(e: SparkListenerConnectOperationCanceled) = synchronized {
    executionList.get(e.jobTag) match {
      case Some(executionData) =>
        executionData.finishTimestamp = e.eventTime
        executionData.state = ExecutionState.CANCELED
        updateLiveStore(executionData)
      case None =>
        logWarning(
          log"onOperationCanceled called with " +
            log"unknown operation id: ${MDC(OP_ID, e.jobTag)}")
    }
  }
  private def onOperationFailed(e: SparkListenerConnectOperationFailed) = synchronized {
    executionList.get(e.jobTag) match {
      case Some(executionData) =>
        executionData.finishTimestamp = e.eventTime
        executionData.detail = e.errorMessage
        executionData.state = ExecutionState.FAILED
        updateLiveStore(executionData)
      case None =>
        logWarning(
          log"onOperationFailed called with " +
            log"unknown operation id: ${MDC(OP_ID, e.jobTag)}")
    }
  }
  private def onOperationFinished(e: SparkListenerConnectOperationFinished) = synchronized {
    executionList.get(e.jobTag) match {
      case Some(executionData) =>
        executionData.finishTimestamp = e.eventTime
        executionData.state = ExecutionState.FINISHED
        updateLiveStore(executionData)
      case None =>
        logWarning(
          log"onOperationFinished called with " +
            log"unknown operation id: ${MDC(OP_ID, e.jobTag)}")
    }
  }
  private def onOperationClosed(e: SparkListenerConnectOperationClosed) = synchronized {
    executionList.get(e.jobTag) match {
      case Some(executionData) =>
        executionData.closeTimestamp = e.eventTime
        executionData.state = ExecutionState.CLOSED
        updateStoreWithTriggerEnabled(executionData)
        executionList.remove(e.jobTag)
      case None =>
        logWarning(
          log"onOperationClosed called with " +
            log"unknown operation id: ${MDC(OP_ID, e.jobTag)}")
    }
  }

  private def onSessionStarted(e: SparkListenerConnectSessionStarted) = synchronized {
    val session = getOrCreateSession(e.sessionId, e.userId, e.eventTime)
    sessionList.put(e.sessionId, session)
    updateLiveStore(session)
  }

  private def onSessionClosed(e: SparkListenerConnectSessionClosed) = synchronized {
    sessionList.get(e.sessionId) match {
      case Some(sessionData) =>
        sessionData.finishTimestamp = e.eventTime
        updateStoreWithTriggerEnabled(sessionData)
        sessionList.remove(e.sessionId)

      case None =>
        logWarning(
          log"onSessionClosed called with " +
            log"unknown session id: ${MDC(SESSION_ID, e.sessionId)}")
    }
  }

  // Update both live and history stores. Trigger is enabled by default, hence
  // it will cleanup the entity which exceeds the threshold.
  def updateStoreWithTriggerEnabled(entity: LiveEntity): Unit = synchronized {
    entity.write(kvstore, System.nanoTime(), checkTriggers = true)
  }

  // Update only live stores. If trigger is enabled, it will cleanup entity
  // which exceeds the threshold.
  def updateLiveStore(entity: LiveEntity, trigger: Boolean = false): Unit = synchronized {
    val now = System.nanoTime()
    if (live && liveUpdatePeriodNs >= 0 && now - entity.lastWriteTime > liveUpdatePeriodNs) {
      entity.write(kvstore, now, checkTriggers = trigger)
    }
  }

  private def getOrCreateSession(
      sessionId: String,
      userName: String,
      startTime: Long): LiveSessionData = synchronized {
    sessionList.getOrElseUpdate(sessionId, new LiveSessionData(sessionId, startTime, userName))
  }

  private def getOrCreateExecution(
      jobTag: String,
      statement: String,
      sessionId: String,
      startTimestamp: Long,
      userId: String,
      operationId: String,
      sparkSessionTags: Set[String]): LiveExecutionData = synchronized {
    executionList.getOrElseUpdate(
      jobTag,
      new LiveExecutionData(
        jobTag,
        statement,
        sessionId,
        startTimestamp,
        userId,
        operationId,
        sparkSessionTags))
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
    toDelete.foreach { j => kvstore.delete(j.getClass, j.jobTag) }
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

private[connect] class LiveExecutionData(
    val jobTag: String,
    val statement: String,
    val sessionId: String,
    val startTimestamp: Long,
    val userId: String,
    val operationId: String,
    val sparkSessionTags: Set[String])
    extends LiveEntity {

  var finishTimestamp: Long = 0L
  var closeTimestamp: Long = 0L
  var detail: String = ""
  var state: ExecutionState.Value = ExecutionState.STARTED
  var jobId: ArrayBuffer[String] = ArrayBuffer[String]()
  var sqlExecId: mutable.Set[String] = mutable.Set[String]()

  override protected def doUpdate(): Any = {
    new ExecutionInfo(
      jobTag,
      statement,
      sessionId,
      startTimestamp,
      userId,
      operationId,
      sparkSessionTags,
      finishTimestamp,
      closeTimestamp,
      detail,
      state,
      jobId,
      sqlExecId)
  }

  def totalTime(endTime: Long): Long = {
    if (endTime == 0L) {
      System.currentTimeMillis - startTimestamp
    } else {
      endTime - startTimestamp
    }
  }
}

private[connect] class LiveSessionData(
    val sessionId: String,
    val startTimestamp: Long,
    val userName: String)
    extends LiveEntity {

  var finishTimestamp: Long = 0L
  var totalExecution: Int = 0

  override protected def doUpdate(): Any = {
    new SessionInfo(sessionId, startTimestamp, userName, finishTimestamp, totalExecution)
  }
  def totalTime: Long = {
    if (finishTimestamp == 0L) {
      System.currentTimeMillis - startTimestamp
    } else {
      finishTimestamp - startTimestamp
    }
  }
}
