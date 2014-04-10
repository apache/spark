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

package org.apache.spark.ui

import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.EnvironmentUI
import org.apache.spark.ui.exec.ExecutorsUI
import org.apache.spark.ui.jobs.JobProgressUI
import org.apache.spark.ui.storage.BlockManagerUI
import org.apache.spark.util.Utils

/** Top level user interface for Spark */
private[spark] class SparkUI(
    val sc: SparkContext,
    conf: SparkConf,
    val listenerBus: SparkListenerBus,
    val appName: String,
    val basePath: String = "")
  extends Logging {

  def this(sc: SparkContext) = this(sc, sc.conf, sc.listenerBus, sc.appName)
  def this(conf: SparkConf, listenerBus: SparkListenerBus, appName: String, basePath: String) =
    this(null, conf, listenerBus, appName, basePath)

  // If SparkContext is not provided, assume the associated application is not live
  val live = sc != null

  val securityManager = if (live) sc.env.securityManager else new SecurityManager(conf)
  val killEnabled = conf.getBoolean("spark.ui.killEnabled", true)

  private val bindHost = Utils.localHostName()
  private val publicHost = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(bindHost)
  private val port = conf.get("spark.ui.port", SparkUI.DEFAULT_PORT).toInt
  private var serverInfo: Option[ServerInfo] = None

  private val storage = new BlockManagerUI(this)
  private val jobs = new JobProgressUI(this)
  private val env = new EnvironmentUI(this)
  private val exec = new ExecutorsUI(this)

  val handlers: Seq[ServletContextHandler] = {
    val metricsServletHandlers = if (live) {
      SparkEnv.get.metricsSystem.getServletHandlers
    } else {
      Array[ServletContextHandler]()
    }
    storage.getHandlers ++
    jobs.getHandlers ++
    env.getHandlers ++
    exec.getHandlers ++
    metricsServletHandlers ++
    Seq[ServletContextHandler] (
      createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"),
      createRedirectHandler("/", "/stages", basePath)
    )
  }

  // Maintain executor storage status through Spark events
  val storageStatusListener = new StorageStatusListener

  /** Bind the HTTP server which backs this web interface */
  def bind() {
    try {
      serverInfo = Some(startJettyServer("0.0.0.0", port, handlers, sc.conf))
      logInfo("Started Spark Web UI at http://%s:%d".format(publicHost, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to create Spark JettyUtils", e)
        System.exit(1)
    }
  }

  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  /** Initialize all components of the server */
  def start() {
    storage.start()
    jobs.start()
    env.start()
    exec.start()

    // Storage status listener must receive events first, as other listeners depend on its state
    listenerBus.addListener(storageStatusListener)
    listenerBus.addListener(storage.listener)
    listenerBus.addListener(jobs.listener)
    listenerBus.addListener(env.listener)
    listenerBus.addListener(exec.listener)
  }

  def stop() {
    assert(serverInfo.isDefined, "Attempted to stop a SparkUI that was not bound to a server!")
    serverInfo.get.server.stop()
    logInfo("Stopped Spark Web UI at %s".format(appUIAddress))
  }

  private[spark] def appUIAddress = "http://" + publicHost + ":" + boundPort

}

private[spark] object SparkUI {
  val DEFAULT_PORT = "4040"
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
}
