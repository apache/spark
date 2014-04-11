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

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext}
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.EnvironmentTab
import org.apache.spark.ui.exec.ExecutorsTab
import org.apache.spark.ui.jobs.JobProgressTab
import org.apache.spark.ui.storage.BlockManagerTab

/**
 * Top level user interface for Spark.
 */
private[spark] class SparkUI(
    val sc: SparkContext,
    val conf: SparkConf,
    val securityManager: SecurityManager,
    val listenerBus: SparkListenerBus,
    var appName: String,
    val basePath: String = "")
  extends WebUI(securityManager, SparkUI.getUIPort(conf), conf, basePath)
  with Logging {

  def this(sc: SparkContext) = this(sc, sc.conf, sc.env.securityManager, sc.listenerBus, sc.appName)
  def this(conf: SparkConf, listenerBus: SparkListenerBus, appName: String, basePath: String) =
    this(null, conf, new SecurityManager(conf), listenerBus, appName, basePath)

  // If SparkContext is not provided, assume the associated application is not live
  val live = sc != null

  // Maintain executor storage status through Spark events
  val storageStatusListener = new StorageStatusListener

  initialize()

  /** Initialize all components of the server. */
  def initialize() {
    listenerBus.addListener(storageStatusListener)
    attachTab(new JobProgressTab(this))
    attachTab(new BlockManagerTab(this))
    attachTab(new EnvironmentTab(this))
    attachTab(new ExecutorsTab(this))
    attachHandler(createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"))
    attachHandler(createRedirectHandler("/", "/stages", basePath))
    if (live) {
      sc.env.metricsSystem.getServletHandlers.foreach(attachHandler)
    }
  }

  /** Set the app name for this UI. */
  def setAppName(name: String) {
    appName = name
  }

  /** Register the given listener with the listener bus. */
  def registerListener(listener: SparkListener) {
    listenerBus.addListener(listener)
  }

  /** Stop the server behind this web interface. Only valid after bind(). */
  override def stop() {
    super.stop()
    logInfo("Stopped Spark web UI at %s".format(appUIAddress))
  }

  private[spark] def appUIAddress = "http://" + publicHostName + ":" + boundPort
}

private[spark] object SparkUI {
  val DEFAULT_PORT = 4040
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"

  def getUIPort(conf: SparkConf): Int = {
    conf.getInt("spark.ui.port", SparkUI.DEFAULT_PORT)
  }
}
