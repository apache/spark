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

import java.util.Date

import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo, JsonRootResource, UIRoot}
import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext}
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.{EnvironmentListener, EnvironmentTab}
import org.apache.spark.ui.exec.{ExecutorsListener, ExecutorsTab}
import org.apache.spark.ui.jobs.{JobsTab, JobProgressListener, StagesTab}
import org.apache.spark.ui.storage.{StorageListener, StorageTab}
import org.apache.spark.ui.scope.RDDOperationGraphListener

/**
 * Top level user interface for a Spark application.
 */
private[spark] class SparkUI private (
    val sc: Option[SparkContext],
    val conf: SparkConf,
    securityManager: SecurityManager,
    val environmentListener: EnvironmentListener,
    val storageStatusListener: StorageStatusListener,
    val executorsListener: ExecutorsListener,
    val jobProgressListener: JobProgressListener,
    val storageListener: StorageListener,
    val operationGraphListener: RDDOperationGraphListener,
    var appName: String,
    val basePath: String,
    val startTime: Long)
  extends WebUI(securityManager, SparkUI.getUIPort(conf), conf, basePath, "SparkUI")
  with Logging
  with UIRoot {

  val killEnabled = sc.map(_.conf.getBoolean("spark.ui.killEnabled", true)).getOrElse(false)


  val stagesTab = new StagesTab(this)

  /** Initialize all components of the server. */
  def initialize() {
    attachTab(new JobsTab(this))
    attachTab(stagesTab)
    attachTab(new StorageTab(this))
    attachTab(new EnvironmentTab(this))
    attachTab(new ExecutorsTab(this))
    attachHandler(createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"))
    attachHandler(createRedirectHandler("/", "/jobs", basePath = basePath))
    attachHandler(JsonRootResource.getJsonServlet(this))
    // This should be POST only, but, the YARN AM proxy won't proxy POSTs
    attachHandler(createRedirectHandler(
      "/stages/stage/kill", "/stages", stagesTab.handleKillRequest,
      httpMethods = Set("GET", "POST")))
  }
  initialize()

  def getAppName: String = appName

  /** Set the app name for this UI. */
  def setAppName(name: String) {
    appName = name
  }

  /** Stop the server behind this web interface. Only valid after bind(). */
  override def stop() {
    super.stop()
    logInfo("Stopped Spark web UI at %s".format(appUIAddress))
  }

  /**
   * Return the application UI host:port. This does not include the scheme (http://).
   */
  private[spark] def appUIHostPort = publicHostName + ":" + boundPort

  private[spark] def appUIAddress = s"http://$appUIHostPort"

  def getSparkUI(appId: String): Option[SparkUI] = {
    if (appId == appName) Some(this) else None
  }

  def getApplicationInfoList: Iterator[ApplicationInfo] = {
    Iterator(new ApplicationInfo(
      id = appName,
      name = appName,
      attempts = Seq(new ApplicationAttemptInfo(
        attemptId = None,
        startTime = new Date(startTime),
        endTime = new Date(-1),
        sparkUser = "",
        completed = false
      ))
    ))
  }
}

private[spark] abstract class SparkUITab(parent: SparkUI, prefix: String)
  extends WebUITab(parent, prefix) {

  def appName: String = parent.getAppName

}

private[spark] object SparkUI {
  val DEFAULT_PORT = 4040
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
  val DEFAULT_POOL_NAME = "default"
  val DEFAULT_RETAINED_STAGES = 1000
  val DEFAULT_RETAINED_JOBS = 1000

  def getUIPort(conf: SparkConf): Int = {
    conf.getInt("spark.ui.port", SparkUI.DEFAULT_PORT)
  }

  def createLiveUI(
      sc: SparkContext,
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      jobProgressListener: JobProgressListener,
      securityManager: SecurityManager,
      appName: String,
      startTime: Long): SparkUI =  {
    create(Some(sc), conf, listenerBus, securityManager, appName,
      jobProgressListener = Some(jobProgressListener), startTime = startTime)
  }

  def createHistoryUI(
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      securityManager: SecurityManager,
      appName: String,
      basePath: String,
      startTime: Long): SparkUI = {
    create(None, conf, listenerBus, securityManager, appName, basePath, startTime = startTime)
  }

  /**
   * Create a new Spark UI.
   *
   * @param sc optional SparkContext; this can be None when reconstituting a UI from event logs.
   * @param jobProgressListener if supplied, this JobProgressListener will be used; otherwise, the
   *                            web UI will create and register its own JobProgressListener.
   */
  private def create(
      sc: Option[SparkContext],
      conf: SparkConf,
      listenerBus: SparkListenerBus,
      securityManager: SecurityManager,
      appName: String,
      basePath: String = "",
      jobProgressListener: Option[JobProgressListener] = None,
      startTime: Long): SparkUI = {

    val _jobProgressListener: JobProgressListener = jobProgressListener.getOrElse {
      val listener = new JobProgressListener(conf)
      listenerBus.addListener(listener)
      listener
    }

    val environmentListener = new EnvironmentListener
    val storageStatusListener = new StorageStatusListener
    val executorsListener = new ExecutorsListener(storageStatusListener)
    val storageListener = new StorageListener(storageStatusListener)
    val operationGraphListener = new RDDOperationGraphListener(conf)

    listenerBus.addListener(environmentListener)
    listenerBus.addListener(storageStatusListener)
    listenerBus.addListener(executorsListener)
    listenerBus.addListener(storageListener)
    listenerBus.addListener(operationGraphListener)

    new SparkUI(sc, conf, securityManager, environmentListener, storageStatusListener,
      executorsListener, _jobProgressListener, storageListener, operationGraphListener,
      appName, basePath, startTime)
  }
}
