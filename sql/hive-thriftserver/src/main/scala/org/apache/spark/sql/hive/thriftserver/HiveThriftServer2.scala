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
import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.thrift.{ThriftBinaryCLIService, ThriftHttpCLIService}
import org.apache.hive.service.server.{HiveServer2, ServerOptionsProcessor}

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListener}

/**
 * The main entry point for the Spark SQL port of HiveServer2.  Starts up a `SparkSQLContext` and a
 * `HiveThriftServer2` thrift server.
 */
object HiveThriftServer2 extends Logging {
  var LOG = LogFactory.getLog(classOf[HiveServer2])

  /**
   * :: DeveloperApi ::
   * Starts a new thrift server with the given context.
   */
  @DeveloperApi
  def startWithContext(sqlContext: HiveContext): Unit = {
    val server = new HiveThriftServer2(sqlContext)
    server.init(sqlContext.hiveconf)
    server.start()
    sqlContext.sparkContext.addSparkListener(new HiveThriftServer2Listener(server))
  }

  def main(args: Array[String]) {
    val optionsProcessor = new ServerOptionsProcessor("HiveThriftServer2")
    if (!optionsProcessor.process(args)) {
      System.exit(-1)
    }

    LogUtils.initHiveLog4j()

    logInfo("Starting SparkContext")
    SparkSQLEnv.init()

    Runtime.getRuntime.addShutdownHook(
      new Thread() {
        override def run() {
          SparkSQLEnv.stop()
        }
      }
    )

    try {
      val server = new HiveThriftServer2(SparkSQLEnv.hiveContext)
      server.init(SparkSQLEnv.hiveContext.hiveconf)
      server.start()
      logInfo("HiveThriftServer2 started")
      SparkSQLEnv.sparkContext.addSparkListener(new HiveThriftServer2Listener(server))
    } catch {
      case e: Exception =>
        logError("Error starting HiveThriftServer2", e)
        System.exit(-1)
    }
  }

  /**
   * A inner sparkListener called in sc.stop to clean up the HiveThriftServer2
   */
  class HiveThriftServer2Listener(val server: HiveServer2) extends SparkListener {
    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      server.stop()
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

    if (isHTTPTransportMode(hiveConf)) {
      val thriftCliService = new ThriftHttpCLIService(sparkSqlCliService)
      setSuperField(this, "thriftCLIService", thriftCliService)
      addService(thriftCliService)
    } else {
      val thriftCliService = new ThriftBinaryCLIService(sparkSqlCliService)
      setSuperField(this, "thriftCLIService", thriftCliService)
      addService(thriftCliService)
    }

    initCompositeService(hiveConf)
  }

  private def isHTTPTransportMode(hiveConf: HiveConf): Boolean = {
    val transportMode: String = hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
    transportMode.equalsIgnoreCase("http")
  }

}
