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

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.thrift.{ThriftBinaryCLIService, ThriftHttpCLIService}
import org.apache.hive.service.server.{HiveServer2, ServerOptionsProcessor}

import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.scheduler.{SparkListenerJobStart, SparkListenerApplicationEnd, SparkListener}
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
    server.init(sqlContext.hiveconf)
    server.start()
    listener = new HiveThriftServer2Listener(server, sqlContext.sparkContext.conf)
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
      listener = new HiveThriftServer2Listener(server, SparkSQLEnv.sparkContext.conf)
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
    var totalExecute: Int = 0
    def totalTime: Long = {
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
  class HiveThriftServer2Listener(
      val server: HiveServer2,
      val conf: SparkConf) extends SparkListener {

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      server.stop()
    }

    val sessionList = new mutable.HashMap[String, SessionInfo]
    val executeList = new mutable.HashMap[String, ExecutionInfo]
    val retainedStatements =
      conf.getInt("spark.thriftserver.ui.retainedStatements", 200)
    val retainedSessions =
      conf.getInt("spark.thriftserver.ui.retainedSessions", 200)
    var totalRunning = 0

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      val jobGroup = for (
        props <- Option(jobStart.properties);
        statement <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
      ) yield statement

      jobGroup.map( groupId => {
        val ret = executeList.find( _ match {
          case (id: String, info: ExecutionInfo) => info.groupId == groupId
        })
        if (ret.isDefined) {
          ret.get._2.jobId += jobStart.jobId.toString
          ret.get._2.groupId = groupId
        }
      })
    }

    def onSessionCreated(ip: String, sessionId: String, userName: String = "UNKNOWN"): Unit = {
      val info = new SessionInfo(sessionId, System.currentTimeMillis, ip, userName)
      sessionList(sessionId) = info
      trimSessionIfNecessary
    }

    def onSessionClosed(sessionId: String): Unit = {
      sessionList(sessionId).finishTimestamp = System.currentTimeMillis
    }

    def onStatementStart(
        id: String,
        sessionId: String,
        statement: String,
        groupId: String,
        userName: String = "UNKNOWN"): Unit = {
      val info = new ExecutionInfo(statement, sessionId, System.currentTimeMillis, userName)
      info.state = ExecutionState.STARTED
      executeList(id) = info
      trimExecutionIfNecessary
      sessionList(sessionId).totalExecute += 1
      executeList(id).groupId = groupId
      totalRunning += 1
    }

    def onStatementParse(id: String, executePlan: String): Unit = {
      executeList(id).executePlan = executePlan
      executeList(id).state = ExecutionState.COMPILED
    }

    def onStatementError(id: String, errorMessage: String, errorTrace: String): Unit = {
      executeList(id).finishTimestamp = System.currentTimeMillis
      executeList(id).detail = errorMessage
      executeList(id).state = ExecutionState.FAILED
      totalRunning -= 1
    }

    def onStatementFinish(id: String): Unit = {
      executeList(id).finishTimestamp = System.currentTimeMillis
      executeList(id).state = ExecutionState.FINISHED
      totalRunning -= 1
    }

    private def trimExecutionIfNecessary = synchronized {
      if (executeList.size > retainedStatements) {
        val toRemove = math.max(retainedStatements / 10, 1)
        executeList.toList.sortBy(_._2.startTimestamp).take(toRemove).foreach { s =>
          executeList.remove(s._1)
        }
      }
    }

    private def trimSessionIfNecessary = synchronized {
      if (sessionList.size > retainedSessions) {
        val toRemove = math.max(retainedSessions / 10, 1)
        sessionList.toList.sortBy(_._2.startTimestamp).take(toRemove).foreach { s =>
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
