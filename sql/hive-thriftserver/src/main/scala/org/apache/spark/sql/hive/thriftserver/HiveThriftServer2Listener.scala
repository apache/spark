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

package org.apache.spark.sql.hive.thriftserver

import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.cli.session.HiveSession
import org.apache.hive.service.server.HiveServer2
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.scheduler.{SparkListenerJobStart, SparkListenerApplicationEnd, SparkListener}

import scala.collection.mutable.HashMap

private[thriftserver] trait HiveThriftServerEventListener {
  /**
   * Called when a session created.
   */
  def onSessionCreated(ip: String, session: HiveSession): Unit = {}

  /**
   * Called when a session closed.
   */
  def onSessionClosed(session: HiveSession): Unit = {}

  /**
   * Called when a statement started to run.
   */
  def onStatementStart(
      id: String,
      session: HiveSession,
      statement: String,
      groupId: String): Unit = {}

  /**
   * Called when a statement completed compilation.
   */
  def onStatementParse(id: String, executePlan: String): Unit = {}

  /**
   * Called when a statement got a error during running.
   */
  def onStatementError(id: String, errorMessage: String, errorTrace: String): Unit = {}

  /**
   * Called when a statement ran success.
   */
  def onStatementFinish(id: String): Unit = {}
}

private[thriftserver] class SessionInfo(
                                         val session: HiveSession,
                                         val startTimestamp: Long,
                                         val ip: String) {
  val sessionID = session.getSessionHandle.getSessionId.toString
  val userName = if (session.getUserName == null) "UNKNOWN" else session.getUserName
  var finishTimestamp = 0L
  var totalExecute = 0

  def totalTime = {
    if (finishTimestamp == 0L) {
      System.currentTimeMillis() - startTimestamp
    } else {
      finishTimestamp - startTimestamp
    }
  }
}

private[thriftserver] object ExecutionState extends Enumeration {
  val STARTED, COMPILED, FAILED, FINISHED = Value
  type ExecutionState = Value
}

private[thriftserver] class ExecutionInfo(
    val statement: String,
    val session: HiveSession,
    val startTimestamp: Long) {
  val userName = if(session.getUserName == null) "UNKNOWN" else session.getUserName
  var finishTimestamp = 0L
  var executePlan = ""
  var detail = ""
  var state: ExecutionState.Value = ExecutionState.STARTED
  var jobId = scala.collection.mutable.ArrayBuffer[String]()
  var groupId = ""
  def totalTime = {
    if (finishTimestamp == 0L) {
      System.currentTimeMillis() - startTimestamp
    } else {
      finishTimestamp - startTimestamp
    }
  }
}

/**
 * A listener for HiveThriftServer2
 */
class HiveThriftServer2Listener(
    val server: HiveServer2,
    val conf:SparkConf) extends SparkListener with HiveThriftServerEventListener{

  // called in sc.stop to clean up the HiveThriftServer2
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    server.stop()
  }

  var sessionList = new HashMap[SessionHandle, SessionInfo]
  var executeList = new HashMap[String, ExecutionInfo]
  val retainedStatements =
    conf.getInt("spark.thriftserver.ui.retainedStatements", 1000)
  val retainedSessions =
    conf.getInt("spark.thriftserver.ui.retainedSessions", 1000)
  var totalRunning = 0

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobGroup = for (
      props <- Option(jobStart.properties);
      statement <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
    ) yield statement

    jobGroup match {
      case Some(groupId: String) => {
        val ret = executeList.find( _ match {
          case (id: String, info: ExecutionInfo) => {
            info.groupId == groupId
          }
        })
        if(ret.isDefined) {
          ret.get._2.jobId += jobStart.jobId.toString
          ret.get._2.groupId = groupId
        }
      }
    }
  }

  override def onSessionCreated(ip: String, session: HiveSession): Unit = {
    val info = new SessionInfo(session, System.currentTimeMillis(), ip)
    sessionList(session.getSessionHandle) = info
    trimSessionIfNecessary()
  }

  override def onSessionClosed(session: HiveSession): Unit = {
    sessionList(session.getSessionHandle).finishTimestamp = System.currentTimeMillis()
  }

  override def onStatementStart(id: String, session: HiveSession,
      statement: String, groupId: String): Unit = {
    val info = new ExecutionInfo(statement, session, System.currentTimeMillis())
    info.state = ExecutionState.STARTED
    executeList(id) = info
    trimExecutionIfNecessary()
    sessionList(session.getSessionHandle).totalExecute += 1
    executeList(id).groupId = groupId
    totalRunning += 1
  }

  override def onStatementParse(id: String, executePlan: String): Unit = {
    executeList(id).executePlan = executePlan
    executeList(id).state = ExecutionState.COMPILED
  }

  override def onStatementError(id: String, errorMessage: String, errorTrace: String): Unit = {
    executeList(id).finishTimestamp = System.currentTimeMillis()
    executeList(id).detail = errorMessage
    executeList(id).state = ExecutionState.FAILED
    totalRunning -= 1
  }

  override def onStatementFinish(id: String): Unit = {
    executeList(id).finishTimestamp = System.currentTimeMillis()
    executeList(id).state = ExecutionState.FINISHED
    totalRunning -= 1
  }

  private def trimExecutionIfNecessary() = synchronized {
    if (executeList.size > retainedStatements) {
      val toRemove = math.max(retainedStatements / 10, 1)
      executeList.toList.sortBy(_._2.startTimestamp).take(toRemove).foreach { s =>
        executeList.remove(s._1)
      }
    }
  }

  private def trimSessionIfNecessary() = synchronized {
    if (sessionList.size > retainedSessions) {
      val toRemove = math.max(retainedSessions / 10, 1)
      sessionList.toList.sortBy(_._2.startTimestamp).take(toRemove).foreach { s =>
        sessionList.remove(s._1)
      }
    }
  }
}
