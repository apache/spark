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

import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.cli.session.HiveSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.{SparkListenerJobStart, SparkListener}

import scala.collection.mutable.HashMap

trait ThriftServerEventListener {

  def onConnected(session: HiveSession) { }
  def onDisconnected(session: HiveSession) { }

  def onStart(id: String, session: HiveSession, statement: String) { }
  def onParse(id: String, executePlan: String, groupId: String) { }
  def onError(id: String, errorMessage: String, errorTrace: String) { }
  def onFinish(id: String) { }
}

class SessionInfo(val session: HiveSession, val startTimestamp: Long) {
  val sessionID = session.getSessionHandle.getSessionId.toString
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

object ExecutionState extends Enumeration {
  val STARTED, COMPILED, FAILED, FINISHED = Value
  type ExecutionState = Value
}

class ExecutionInfo(val statement: String, val session: HiveSession, val startTimestamp: Long) {
  var finishTimestamp = 0L
  var executePlan = ""
  var detail = ""
  var state: ExecutionState.Value = ExecutionState.STARTED
  var groupId = ""
  var jobId = ""
  def totalTime = {
    if (finishTimestamp == 0L) {
      System.currentTimeMillis() - startTimestamp
    } else {
      finishTimestamp - startTimestamp
    }
  }
}

class ThriftServerUIEventListener(val conf: SparkConf)
  extends ThriftServerEventListener with SparkListener {

  import ThriftServerUIEventListener._

  var sessionList = new HashMap[SessionHandle, SessionInfo]
  var executeList = new HashMap[String, ExecutionInfo]
  val retainedStatements =
    conf.getInt("spark.thriftserver.ui.retainedStatements", DEFAULT_RETAINED_STATEMENTS)
  val retainedSessions =
    conf.getInt("spark.thriftserver.ui.retainedSessions", DEFAULT_RETAINED_SESSIONS)
  var totalRunning = 0

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobGroup = for (
      props <- Option(jobStart.properties);
      group <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
    ) yield group

    executeList.foreach {
      case (id: String, info: ExecutionInfo) if info.groupId == jobGroup.get => {
        executeList(id).jobId = jobStart.jobId.toString
      }
    }
  }

  override def onConnected(session: HiveSession): Unit = {
    val info = new SessionInfo(session, System.currentTimeMillis())
    sessionList(session.getSessionHandle) = info
    trimSessionIfNecessary()
  }

  override def onDisconnected(session: HiveSession): Unit = {
    if(!sessionList.contains(session.getSessionHandle)) {
      onConnected(session)
    }
    sessionList(session.getSessionHandle).finishTimestamp = System.currentTimeMillis()

  }

  override def onStart(id: String, session: HiveSession, statement: String): Unit = {
    // TODO: Due to the incompatible interface between different hive version,
    // we can't get the session start event.
    // So we have to update session information from here.
    if(!sessionList.contains(session.getSessionHandle)) {
      onConnected(session)
    }
    val info = new ExecutionInfo(statement, session, System.currentTimeMillis())
    info.state = ExecutionState.STARTED
    executeList(id) = info
    trimExecutionIfNecessary()
    sessionList(session.getSessionHandle).totalExecute += 1
    totalRunning += 1
  }

  override def onParse(id: String, executePlan: String, groupId: String): Unit = {
    executeList(id).executePlan = executePlan
    executeList(id).groupId = groupId
    executeList(id).state = ExecutionState.COMPILED
  }

  override def onError(id: String, errorMessage: String, errorTrace: String): Unit = {
    executeList(id).finishTimestamp = System.currentTimeMillis()
    executeList(id).detail = errorMessage
    //+ "<br></br>" + errorTrace
    executeList(id).state = ExecutionState.FAILED
    totalRunning -= 1
  }

  override def onFinish(id: String): Unit = {
    executeList(id).finishTimestamp = System.currentTimeMillis()
    executeList(id).state = ExecutionState.FINISHED
    totalRunning -= 1
  }

  private def trimExecutionIfNecessary() = synchronized {
    if (executeList.size > retainedStatements) {
      val toRemove = math.max(retainedStatements / 10, 1)
      executeList.toList.sortWith(compareExecutionDesc).take(toRemove).foreach { s =>
        executeList.remove(s._1)
      }
    }
  }

  private def compareExecutionDesc(
      l:(String, ExecutionInfo),
      r:(String, ExecutionInfo)): Boolean = {
    l._2.startTimestamp < r._2.startTimestamp
  }

  private def compareSessionDesc(
      l:(SessionHandle, SessionInfo),
      r:(SessionHandle, SessionInfo)): Boolean = {
    l._2.startTimestamp < r._2.startTimestamp
  }

  private def trimSessionIfNecessary() = synchronized {
    if (sessionList.size > retainedSessions) {
      val toRemove = math.max(retainedSessions / 10, 1)
      sessionList.toList.sortWith(compareSessionDesc).take(toRemove).foreach { s =>
        sessionList.remove(s._1)
      }
    }
  }
}

private object ThriftServerUIEventListener {
  val DEFAULT_RETAINED_SESSIONS = 1000
  val DEFAULT_RETAINED_STATEMENTS = 1000
}
