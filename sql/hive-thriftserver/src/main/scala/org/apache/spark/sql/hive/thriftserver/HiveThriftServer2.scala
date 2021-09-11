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

import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.hive.common.ServerUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.thrift.{ThriftBinaryCLIService, ThriftHttpCLIService}
import org.apache.hive.service.server.HiveServer2

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.thriftserver.ui._
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * The main entry point for the Spark SQL port of HiveServer2.  Starts up a `SparkSQLContext` and a
 * `HiveThriftServer2` thrift server.
 */
object HiveThriftServer2 extends Logging {
  var uiTab: Option[ThriftServerTab] = None
  var listener: HiveThriftServer2Listener = _
  var eventManager: HiveThriftServer2EventManager = _

  /**
   * :: DeveloperApi ::
   * Starts a new thrift server with the given context.
   */
  @DeveloperApi
  def startWithContext(sqlContext: SQLContext): HiveThriftServer2 = {
    val executionHive = HiveUtils.newClientForExecution(
      sqlContext.sparkContext.conf,
      sqlContext.sessionState.newHadoopConf())

    // Cleanup the scratch dir before starting
    ServerUtils.cleanUpScratchDir(executionHive.conf)
    val server = new HiveThriftServer2(sqlContext)

    server.init(executionHive.conf)
    server.start()
    logInfo("HiveThriftServer2 started")
    createListenerAndUI(server, sqlContext.sparkContext)
    server
  }

  private def createListenerAndUI(server: HiveThriftServer2, sc: SparkContext): Unit = {
    val kvStore = sc.statusStore.store.asInstanceOf[ElementTrackingStore]
    eventManager = new HiveThriftServer2EventManager(sc)
    listener = new HiveThriftServer2Listener(kvStore, sc.conf, Some(server))
    sc.listenerBus.addToStatusQueue(listener)
    uiTab = if (sc.getConf.get(UI_ENABLED)) {
      Some(new ThriftServerTab(new HiveThriftServer2AppStatusStore(kvStore),
        ThriftServerTab.getSparkUI(sc)))
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    // If the arguments contains "-h" or "--help", print out the usage and exit.
    if (args.contains("-h") || args.contains("--help")) {
      HiveServer2.main(args)
      // The following code should not be reachable. It is added to ensure the main function exits.
      return
    }

    Utils.initDaemon(log)
    val optionsProcessor = new HiveServer2.ServerOptionsProcessor("HiveThriftServer2")
    optionsProcessor.parse(args)

    logInfo("Starting SparkContext")
    SparkSQLEnv.init()

    ShutdownHookManager.addShutdownHook { () =>
      SparkSQLEnv.stop()
      uiTab.foreach(_.detach())
    }

    try {
      startWithContext(SparkSQLEnv.sqlContext)
      // If application was killed before HiveThriftServer2 start successfully then SparkSubmit
      // process can not exit, so check whether if SparkContext was stopped.
      if (SparkSQLEnv.sparkContext.stopped.get()) {
        logError("SparkContext has stopped even if HiveServer2 has started, so exit")
        System.exit(-1)
      }
    } catch {
      case e: Exception =>
        logError("Error starting HiveThriftServer2", e)
        System.exit(-1)
    }
  }

  private[thriftserver] object ExecutionState extends Enumeration {
    val STARTED, COMPILED, CANCELED, TIMEDOUT, FAILED, FINISHED, CLOSED = Value
    type ExecutionState = Value
  }
}

private[hive] class HiveThriftServer2(sqlContext: SQLContext)
  extends HiveServer2
  with ReflectedCompositeService {
  // state is tracked internally so that the server only attempts to shut down if it successfully
  // started, and then once only.
  private val started = new AtomicBoolean(false)

  override def init(hiveConf: HiveConf): Unit = {
    val sparkSqlCliService = new SparkSQLCLIService(this, sqlContext)
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
    val transportMode = hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
    transportMode.toLowerCase(Locale.ROOT).equals("http")
  }


  override def start(): Unit = {
    super.start()
    started.set(true)
  }

  override def stop(): Unit = {
    if (started.getAndSet(false)) {
       super.stop()
    }
  }
}
