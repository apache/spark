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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.thrift.{ThriftBinaryCLIService, ThriftHttpCLIService}
import org.apache.hive.service.server.{HiveServer2, ServerOptionsProcessor}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobStart}
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.sql.hive.{HiveContext, HiveShim}
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkContext}

/**
 * The main entry point for the Spark SQL port of HiveServer2.  Starts up a `SparkSQLContext` and a
 * `HiveThriftServer2` thrift server.
 */
object HiveThriftServer2 extends Logging {
  var LOG = LogFactory.getLog(classOf[HiveServer2])
  var uiTab: Option[ThriftServerTab] = _
  var listener: HiveThriftServer2Listener = _

  /**
   * :: DeveloperApi ::
   * Starts a new thrift server with the given context.
   */
  @DeveloperApi
  def startWithContext(sqlContext: HiveContext): Unit = {
    val server = new HiveThriftServer2(sqlContext)
    sqlContext.setConf("spark.sql.hive.version", HiveShim.version)
    server.init(sqlContext.hiveconf)
    server.start()
    listener = new HiveThriftServer2Listener(server, sqlContext.conf)
    sqlContext.sparkContext.addSparkListener(listener)
    uiTab = if (sqlContext.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
      Some(new ThriftServerTab(sqlContext.sparkContext))
    } else {
      None
    }
  }

  def main(args: Array[String]) {
    val optionsProcessor = new ServerOptionsProcessor("HiveThriftServer2")
    if (!optionsProcessor.process(args)) {
      System.exit(-1)
    }

    logInfo("Starting SparkContext")
    SparkSQLEnv.init()

    Utils.addShutdownHook { () =>
      SparkSQLEnv.stop()
      uiTab.foreach(_.detach())
    }

    try {
      val server = new HiveThriftServer2(SparkSQLEnv.hiveContext)
      server.init(SparkSQLEnv.hiveContext.hiveconf)
      server.start()
      logInfo("HiveThriftServer2 started")
      listener = new HiveThriftServer2Listener(server, SparkSQLEnv.hiveContext.conf)
      SparkSQLEnv.sparkContext.addSparkListener(listener)
      uiTab = if (SparkSQLEnv.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
        Some(new ThriftServerTab(SparkSQLEnv.sparkContext))
      } else {
        None
      }
    } catch {
      case e: Exception =>
        logError("Error starting HiveThriftServer2", e)
        System.exit(-1)
    }
  }

  private[thriftserver] class SessionInfo(
      val sessionId: String,
      val startTimestamp: Long,
      val ip: String,
      val userName: String) {
    var finishTimestamp: Long = 0L
    var totalExecution: Int = 0
    def totalTime: Long = {
      if (finishTimestamp == 0L) {
        System.currentTimeMillis - startTimestamp
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
      val sessionId: String,
      val startTimestamp: Long,
      val userName: String) {
    var finishTimestamp: Long = 0L
    var executePlan: String = ""
    var detail: String = ""
    var state: ExecutionState.Value = ExecutionState.STARTED
    val jobId: ArrayBuffer[String] = ArrayBuffer[String]()
    var groupId: String = ""
    def totalTime: Long = {
      if (finishTimestamp == 0L) {
        System.currentTimeMillis - startTimestamp
      } else {
        finishTimestamp - startTimestamp
      }
    }
  }


  /**
   * A inner sparkListener called in sc.stop to clean up the HiveThriftServer2
   */
  private[thriftserver] class HiveThriftServer2Listener(
      val server: HiveServer2,
      val conf: SQLConf) extends SparkListener {

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      server.stop()
    }
    var onlineSessionNum: Int = 0
    val sessionList = new mutable.LinkedHashMap[String, SessionInfo]
    val executionList = new mutable.LinkedHashMap[String, ExecutionInfo]
    val retainedStatements =
      conf.getConf(SQLConf.THRIFTSERVER_UI_STATEMENT_LIMIT, "200").toInt
    val retainedSessions =
      conf.getConf(SQLConf.THRIFTSERVER_UI_SESSION_LIMIT, "200").toInt
    var totalRunning = 0

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      for {
        props <- Option(jobStart.properties)
        groupId <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
        (_, info) <- executionList if info.groupId == groupId
      } {
        info.jobId += jobStart.jobId.toString
        info.groupId = groupId
      }
    }

    def onSessionCreated(ip: String, sessionId: String, userName: String = "UNKNOWN"): Unit = {
      val info = new SessionInfo(sessionId, System.currentTimeMillis, ip, userName)
      sessionList.put(sessionId, info)
      onlineSessionNum += 1
      trimSessionIfNecessary()
    }

    def onSessionClosed(sessionId: String): Unit = {
      val info = sessionList(sessionId)
      if(info != null) {
        sessionList(sessionId).finishTimestamp = System.currentTimeMillis
      }
      onlineSessionNum -= 1
    }

    def onStatementStart(
        id: String,
        sessionId: String,
        statement: String,
        groupId: String,
        userName: String = "UNKNOWN"): Unit = {
      val info = new ExecutionInfo(statement, sessionId, System.currentTimeMillis, userName)
      info.state = ExecutionState.STARTED
      executionList.put(id, info)
      trimExecutionIfNecessary()
      sessionList(sessionId).totalExecution += 1
      executionList(id).groupId = groupId
      totalRunning += 1
    }

    def onStatementParsed(id: String, executionPlan: String): Unit = {
      executionList(id).executePlan = executionPlan
      executionList(id).state = ExecutionState.COMPILED
    }

    def onStatementError(id: String, errorMessage: String, errorTrace: String): Unit = {
      executionList(id).finishTimestamp = System.currentTimeMillis
      executionList(id).detail = errorMessage
      executionList(id).state = ExecutionState.FAILED
      totalRunning -= 1
    }

    def onStatementFinish(id: String): Unit = {
      executionList(id).finishTimestamp = System.currentTimeMillis
      executionList(id).state = ExecutionState.FINISHED
      totalRunning -= 1
    }

    private def trimExecutionIfNecessary() = synchronized {
      if (executionList.size > retainedStatements) {
        val toRemove = math.max(retainedStatements / 10, 1)
        executionList.take(toRemove).foreach { s =>
          executionList.remove(s._1)
        }
      }
    }

    private def trimSessionIfNecessary() = synchronized {
      if (sessionList.size > retainedSessions) {
        val toRemove = math.max(retainedSessions / 10, 1)
        sessionList.take(toRemove).foreach { s =>
          sessionList.remove(s._1)
        }
      }

    }
  }
}

private[hive] class HiveThriftServer2(hiveContext: HiveContext)
  extends HiveServer2
  with ReflectedCompositeService {

  override def init(hiveConf: HiveConf) {
    val sparkSqlCliService = new SparkSQLCLIService(hiveContext)
    setSuperField(this, "cliService", sparkSqlCliService)
    addService(sparkSqlCliService)

    val thriftCliService = if (isHTTPTransportMode(hiveConf)) {
      new ThriftHttpCLIService(sparkSqlCliService)
    } else {
      new ThriftBinaryCLIService(sparkSqlCliService)
    }

    setSuperField(this, "thriftCLIService", thriftCliService)
    addService(thriftCliService)
    initCompositeService(hiveConf)
  }

  private def isHTTPTransportMode(hiveConf: HiveConf): Boolean = {
    val transportMode: String = hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
    transportMode.equalsIgnoreCase("http")
  }

}
