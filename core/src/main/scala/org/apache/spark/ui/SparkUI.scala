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

import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{SecurityManager, SparkConf, SparkContext}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CLASS_NAME, WEB_URL}
import org.apache.spark.internal.config.DRIVER_LOG_LOCAL_DIR
import org.apache.spark.internal.config.UI._
import org.apache.spark.scheduler._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.EnvironmentTab
import org.apache.spark.ui.exec.ExecutorsTab
import org.apache.spark.ui.jobs.{JobsTab, StagesTab}
import org.apache.spark.ui.storage.StorageTab

/**
 * Top level user interface for a Spark application.
 */
private[spark] class SparkUI private (
    val store: AppStatusStore,
    val sc: Option[SparkContext],
    val conf: SparkConf,
    securityManager: SecurityManager,
    var appName: String,
    val basePath: String,
    val startTime: Long,
    val appSparkVersion: String)
  extends WebUI(securityManager, securityManager.getSSLOptions("ui"), SparkUI.getUIPort(conf),
    conf, basePath, "SparkUI")
  with Logging
  with UIRoot {

  val killEnabled = sc.map(_.conf.get(UI_KILL_ENABLED)).getOrElse(false)

  var appId: String = _

  private var streamingJobProgressListener: Option[SparkListener] = None

  private val initHandler: ServletContextHandler = {
    val servlet = new HttpServlet() {
      override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        res.setContentType("text/html;charset=utf-8")
        res.getWriter.write("Spark is starting up. Please wait a while until it's ready.")
      }
    }
    createServletHandler("/", servlet, basePath)
  }

  private var readyToAttachHandlers = false

  /**
   * Attach all existing handlers to ServerInfo.
   */
  def attachAllHandlers(): Unit = {
    // Attach all handlers that have been added already, but not yet attached.
    serverInfo.foreach { server =>
      server.removeHandler(initHandler)
      handlers.foreach(server.addHandler(_, securityManager))
    }
    // Handlers attached after this can be directly started.
    readyToAttachHandlers = true
  }

  /** Attaches a handler to this UI.
   *  Note: The handler will not be attached until readyToAttachHandlers is true,
   *  handlers added before that will be attached by attachAllHandlers */
  override def attachHandler(handler: ServletContextHandler): Unit = synchronized {
    handlers += handler
    if (readyToAttachHandlers) {
      serverInfo.foreach(_.addHandler(handler, securityManager))
    }
  }

  /** Initialize all components of the server. */
  def initialize(): Unit = {
    val jobsTab = new JobsTab(this, store)
    attachTab(jobsTab)
    val stagesTab = new StagesTab(this, store)
    attachTab(stagesTab)
    attachTab(new StorageTab(this, store))
    attachTab(new EnvironmentTab(this, store))
    if (sc.map(_.conf.get(DRIVER_LOG_LOCAL_DIR).nonEmpty).getOrElse(false)) {
      val driverLogTab = new DriverLogTab(this)
      attachTab(driverLogTab)
      attachHandler(createServletHandler("/log",
        (request: HttpServletRequest) => driverLogTab.getPage.renderLog(request),
        sc.get.conf))
    }
    attachTab(new ExecutorsTab(this))
    addStaticHandler(SparkUI.STATIC_RESOURCE_DIR)
    attachHandler(createRedirectHandler("/", "/jobs/", basePath = basePath))
    attachHandler(ApiRootResource.getServletHandler(this))
    if (sc.map(_.conf.get(UI_PROMETHEUS_ENABLED)).getOrElse(false)) {
      attachHandler(PrometheusResource.getServletHandler(this))
    }

    // These should be POST only, but, the YARN AM proxy won't proxy POSTs
    attachHandler(createRedirectHandler(
      "/jobs/job/kill", "/jobs/", jobsTab.handleKillRequest, httpMethods = Set("GET", "POST")))
    attachHandler(createRedirectHandler(
      "/stages/stage/kill", "/stages/", stagesTab.handleKillRequest,
      httpMethods = Set("GET", "POST")))
  }

  initialize()

  def getSparkUser: String = {
    try {
      Option(store.applicationInfo().attempts.head.sparkUser)
        .orElse(store.environmentInfo().systemProperties.toMap.get("user.name"))
        .getOrElse("<unknown>")
    } catch {
      case _: NoSuchElementException => "<unknown>"
    }
  }

  def getAppName: String = appName

  def setAppId(id: String): Unit = {
    appId = id
  }

  /**
   * To start SparUI, Spark starts Jetty Server first to bind address.
   * After the Spark application is fully started, call [attachAllHandlers]
   * to start all existing handlers.
   */
  override def bind(): Unit = {
    assert(serverInfo.isEmpty, s"Attempted to bind $className more than once!")
    try {
      val server = initServer()
      server.addHandler(initHandler, securityManager)
      serverInfo = Some(server)
    } catch {
      case e: Exception =>
        logError(log"Failed to bind ${MDC(CLASS_NAME, className)}", e)
        System.exit(1)
    }
  }

  /** Stop the server behind this web interface. Only valid after bind(). */
  override def stop(): Unit = {
    super.stop()
    logInfo(log"Stopped Spark web UI at ${MDC(WEB_URL, webUrl)}")
  }

  override def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T = {
    if (appId == this.appId) {
      fn(this)
    } else {
      throw new NoSuchElementException()
    }
  }

  override def checkUIViewPermissions(appId: String, attemptId: Option[String],
      user: String): Boolean = {
    securityManager.checkUIViewPermissions(user)
  }

  def getApplicationInfoList: Iterator[ApplicationInfo] = {
    Iterator(new ApplicationInfo(
      id = appId,
      name = appName,
      coresGranted = None,
      maxCores = None,
      coresPerExecutor = None,
      memoryPerExecutorMB = None,
      attempts = Seq(new ApplicationAttemptInfo(
        attemptId = None,
        startTime = new Date(startTime),
        endTime = new Date(-1),
        duration = System.currentTimeMillis() - startTime,
        lastUpdated = new Date(startTime),
        sparkUser = getSparkUser,
        completed = false,
        appSparkVersion = appSparkVersion
      ))
    ))
  }

  def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    getApplicationInfoList.find(_.id == appId)
  }

  def getStreamingJobProgressListener: Option[SparkListener] = streamingJobProgressListener

  def setStreamingJobProgressListener(sparkListener: SparkListener): Unit = {
    streamingJobProgressListener = Option(sparkListener)
  }

  def clearStreamingJobProgressListener(): Unit = {
    streamingJobProgressListener = None
  }
}

private[spark] abstract class SparkUITab(parent: SparkUI, prefix: String)
  extends WebUITab(parent, prefix) {

  def appName: String = parent.appName

  def appSparkVersion: String = parent.appSparkVersion
}

private[spark] object SparkUI {
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
  val DEFAULT_POOL_NAME = "default"

  def getUIPort(conf: SparkConf): Int = {
    conf.get(UI_PORT)
  }

  /**
   * Create a new UI backed by an AppStatusStore.
   */
  def create(
      sc: Option[SparkContext],
      store: AppStatusStore,
      conf: SparkConf,
      securityManager: SecurityManager,
      appName: String,
      basePath: String,
      startTime: Long,
      appSparkVersion: String = org.apache.spark.SPARK_VERSION): SparkUI = {

    new SparkUI(store, sc, conf, securityManager, appName, basePath, startTime, appSparkVersion)
  }

}
