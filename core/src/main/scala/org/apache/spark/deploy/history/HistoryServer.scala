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

package org.apache.spark.deploy.history

import java.util.NoSuchElementException
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.google.common.cache._
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.ui.{SparkUI, UIUtils, WebUI}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.SignalLogger

/**
 * A web server that renders SparkUIs of completed applications.
 *
 * For the standalone mode, MasterWebUI already achieves this functionality. Thus, the
 * main use case of the HistoryServer is in other deploy modes (e.g. Yarn or Mesos).
 *
 * The logging directory structure is as follows: Within the given base directory, each
 * application's event logs are maintained in the application's own sub-directory. This
 * is the same structure as maintained in the event log write code path in
 * EventLoggingListener.
 */
class HistoryServer(
    conf: SparkConf,
    provider: ApplicationHistoryProvider,
    securityManager: SecurityManager,
    port: Int)
  extends WebUI(securityManager, port, conf) with Logging {

  // How many applications to retain
  private val retainedApplications = conf.getInt("spark.history.retainedApplications", 50)

  private val appLoader = new CacheLoader[String, SparkUI] {
    override def load(key: String): SparkUI = {
      val ui = provider.getAppUI(key)
      if (ui == null) {
        throw new NoSuchElementException()
      }
      attachSparkUI(ui)
      ui
    }
  }

  private val appCache = CacheBuilder.newBuilder()
    .maximumSize(retainedApplications)
    .removalListener(new RemovalListener[String, SparkUI] {
      override def onRemoval(rm: RemovalNotification[String, SparkUI]) = {
        detachSparkUI(rm.getValue())
      }
    })
    .build(appLoader)

  private val loaderServlet = new HttpServlet {
    protected override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
      val parts = Option(req.getPathInfo()).getOrElse("").split("/")
      if (parts.length < 2) {
        res.sendError(HttpServletResponse.SC_BAD_REQUEST,
          s"Unexpected path info in request (URI = ${req.getRequestURI()}")
        return
      }

      val appId = parts(1)

      // Note we don't use the UI retrieved from the cache; the cache loader above will register
      // the app's UI, and all we need to do is redirect the user to the same URI that was
      // requested, and the proper data should be served at that point.
      try {
        appCache.get(appId)
        res.sendRedirect(res.encodeRedirectURL(req.getRequestURI()))
      } catch {
        case e: Exception => e.getCause() match {
          case nsee: NoSuchElementException =>
            val msg = <div class="row-fluid">Application {appId} not found.</div>
            res.setStatus(HttpServletResponse.SC_NOT_FOUND)
            UIUtils.basicSparkPage(msg, "Not Found").foreach(
              n => res.getWriter().write(n.toString))

          case cause: Exception => throw cause
        }
      }
    }
  }

  initialize()

  /**
   * Initialize the history server.
   *
   * This starts a background thread that periodically synchronizes information displayed on
   * this UI with the event logs in the provided base directory.
   */
  def initialize() {
    attachPage(new HistoryPage(this))
    attachHandler(createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"))

    val contextHandler = new ServletContextHandler
    contextHandler.setContextPath(HistoryServer.UI_PATH_PREFIX)
    contextHandler.addServlet(new ServletHolder(loaderServlet), "/*")
    attachHandler(contextHandler)
  }

  /** Bind to the HTTP server behind this web interface. */
  override def bind() {
    super.bind()
  }

  /** Stop the server and close the file system. */
  override def stop() {
    super.stop()
    provider.stop()
  }

  /** Attach a reconstructed UI to this server. Only valid after bind(). */
  private def attachSparkUI(ui: SparkUI) {
    assert(serverInfo.isDefined, "HistoryServer must be bound before attaching SparkUIs")
    ui.getHandlers.foreach(attachHandler)
    addFilters(ui.getHandlers, conf)
  }

  /** Detach a reconstructed UI from this server. Only valid after bind(). */
  private def detachSparkUI(ui: SparkUI) {
    assert(serverInfo.isDefined, "HistoryServer must be bound before detaching SparkUIs")
    ui.getHandlers.foreach(detachHandler)
  }

  /**
   * Returns a list of available applications, in descending order according to their end time.
   *
   * @return List of all known applications.
   */
  def getApplicationList() = provider.getListing()

  /**
   * Returns the provider configuration to show in the listing page.
   *
   * @return A map with the provider's configuration.
   */
  def getProviderConfig() = provider.getConfig()

}

/**
 * The recommended way of starting and stopping a HistoryServer is through the scripts
 * start-history-server.sh and stop-history-server.sh. The path to a base log directory
 * is must be specified, while the requested UI port is optional. For example:
 *
 *   ./sbin/spark-history-server.sh /tmp/spark-events
 *   ./sbin/spark-history-server.sh hdfs://1.2.3.4:9000/spark-events
 *
 * This launches the HistoryServer as a Spark daemon.
 */
object HistoryServer extends Logging {
  private val conf = new SparkConf

  val UI_PATH_PREFIX = "/history"

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    initSecurity()
    new HistoryServerArguments(conf, argStrings)
    val securityManager = new SecurityManager(conf)

    val providerName = conf.getOption("spark.history.provider")
      .getOrElse(classOf[FsHistoryProvider].getName())
    val provider = Class.forName(providerName)
      .getConstructor(classOf[SparkConf])
      .newInstance(conf)
      .asInstanceOf[ApplicationHistoryProvider]

    val port = conf.getInt("spark.history.ui.port", 18080)

    val server = new HistoryServer(conf, provider, securityManager, port)
    server.bind()

    Runtime.getRuntime().addShutdownHook(new Thread("HistoryServerStopper") {
      override def run() = {
        server.stop()
      }
    })

    // Wait until the end of the world... or if the HistoryServer process is manually stopped
    while(true) { Thread.sleep(Int.MaxValue) }
  }

  def initSecurity() {
    // If we are accessing HDFS and it has security enabled (Kerberos), we have to login
    // from a keytab file so that we can access HDFS beyond the kerberos ticket expiration.
    // As long as it is using Hadoop rpc (hdfs://), a relogin will automatically
    // occur from the keytab.
    if (conf.getBoolean("spark.history.kerberos.enabled", false)) {
      // if you have enabled kerberos the following 2 params must be set
      val principalName = conf.get("spark.history.kerberos.principal")
      val keytabFilename = conf.get("spark.history.kerberos.keytab")
      SparkHadoopUtil.get.loginUserFromKeytab(principalName, keytabFilename)
    }
  }

}
