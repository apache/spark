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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.ZipOutputStream
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.xml.Node

import com.codahale.metrics.{Counter, Counting, Gauge, Metric, MetricFilter, MetricRegistry, Timer}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.status.api.v1.{ApiRootResource, ApplicationInfo, ApplicationsListResource, UIRoot}
import org.apache.spark.ui.{SparkUI, UIUtils, WebUI}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.{ShutdownHookManager, SystemClock, Utils}

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
  extends WebUI(securityManager, securityManager.getSSLOptions("historyServer"), port, conf)
  with Logging with UIRoot with ApplicationCacheOperations {

  // How many applications to retain
  private val retainedApplications = conf.getInt("spark.history.retainedApplications", 50)

  // How many applications the summary ui displays
  private[history] val maxApplications = conf.get(HISTORY_UI_MAX_APPS);

  // application
  private val appCache = new ApplicationCache(this, retainedApplications, new SystemClock())
  private val initialized = new AtomicBoolean(false)

  private[history] val metricsSystem = MetricsSystem.createMetricsSystem("history",
    conf, securityManager)
  private[history] var metricsRegistry = metricsSystem.getMetricRegistry

  // and its metrics, for testing as well as monitoring
  val cacheMetrics = appCache.metrics

  private val loaderServlet = new HttpServlet {
    protected override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
      // Parse the URI created by getAttemptURI(). It contains an app ID and an optional
      // attempt ID (separated by a slash).
      val parts = Option(req.getPathInfo()).getOrElse("").split("/")
      if (parts.length < 2) {
        res.sendError(HttpServletResponse.SC_BAD_REQUEST,
          s"Unexpected path info in request (URI = ${req.getRequestURI()}")
        return
      }

      val appId = parts(1)
      val attemptId = if (parts.length >= 3) Some(parts(2)) else None

      // Since we may have applications with multiple attempts mixed with applications with a
      // single attempt, we need to try both. Try the single-attempt route first, and if an
      // error is raised, then try the multiple attempt route.
      if (!loadAppUi(appId, None) && (!attemptId.isDefined || !loadAppUi(appId, attemptId))) {
        val msg = <div class="row-fluid">Application {appId} not found.</div>
        res.setStatus(HttpServletResponse.SC_NOT_FOUND)
        UIUtils.basicSparkPage(msg, "Not Found").foreach { n =>
          res.getWriter().write(n.toString)
        }
        return
      }

      // Note we don't use the UI retrieved from the cache; the cache loader above will register
      // the app's UI, and all we need to do is redirect the user to the same URI that was
      // requested, and the proper data should be served at that point.
      // Also, make sure that the redirect url contains the query string present in the request.
      val requestURI = req.getRequestURI + Option(req.getQueryString).map("?" + _).getOrElse("")
      res.sendRedirect(res.encodeRedirectURL(requestURI))
    }

    // SPARK-5983 ensure TRACE is not supported
    protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
      res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
    }
  }

  def getSparkUI(appKey: String): Option[SparkUI] = {
    appCache.getSparkUI(appKey)
  }

  val historyMetrics = new HistoryMetrics(this, "history.server")
  // provider metrics are None until the provider is started, and only after that
  // point if the provider returns any.
  var providerMetrics: Option[Source] = None

  initialize()

  /**
   * Initialize the history server.
   *
   * This starts a background thread that periodically synchronizes information displayed on
   * this UI with the event logs in the provided base directory.
   */
  def initialize() {
    if (!initialized.getAndSet(true)) {
      attachPage(new HistoryPage(this))

      attachHandler(ApiRootResource.getServletHandler(this))

      attachHandler(createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"))

      val contextHandler = new ServletContextHandler
      contextHandler.setContextPath(HistoryServer.UI_PATH_PREFIX)
      contextHandler.addServlet(new ServletHolder(loaderServlet), "/*")
      attachHandler(contextHandler)

      // hook up metrics
      metricsSystem.registerSource(historyMetrics)
      metricsSystem.registerSource(appCache.metrics)
      providerMetrics = provider.start()
      providerMetrics.foreach(metricsSystem.registerSource)
      metricsSystem.start()
      metricsSystem.getServletHandlers.foreach(attachHandler)
    }
  }

  /** Bind to the HTTP server behind this web interface. */
  override def bind() {
    super.bind()
  }

  /** Stop the server and close the history provider. */
  override def stop() {
    try {
      super.stop()
    } finally {
      appCache.stop()
      if (provider != null) {
        provider.stop()
      }
      if (metricsSystem != null) {
        metricsSystem.stop()
      }
    }
  }

  /** Attach a reconstructed UI to this server. Only valid after bind(). */
  override def attachSparkUI(
      appId: String,
      attemptId: Option[String],
      ui: SparkUI,
      completed: Boolean) {
    assert(serverInfo.isDefined, "HistoryServer must be bound before attaching SparkUIs")
    ui.getHandlers.foreach(attachHandler)
    addFilters(ui.getHandlers, conf)
  }

  /** Detach a reconstructed UI from this server. Only valid after bind(). */
  override def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit = {
    assert(serverInfo.isDefined, "HistoryServer must be bound before detaching SparkUIs")
    ui.getHandlers.foreach(detachHandler)
  }

  /**
   * Get the application UI and whether or not it is completed
   * @param appId application ID
   * @param attemptId attempt ID
   * @return If found, the Spark UI and any history information to be used in the cache
   */
  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    provider.getAppUI(appId, attemptId)
  }

  /**
   * Returns a list of available applications, in descending order according to their end time.
   *
   * @return List of all known applications.
   */
  def getApplicationList(): Iterator[ApplicationHistoryInfo] = {
    provider.getListing()
  }

  def getEventLogsUnderProcess(): Int = {
    provider.getEventLogsUnderProcess()
  }

  def getLastUpdatedTime(): Long = {
    provider.getLastUpdatedTime()
  }

  def getApplicationInfoList: Iterator[ApplicationInfo] = {
    getApplicationList().map(ApplicationsListResource.appHistoryInfoToPublicAppInfo)
  }

  def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    provider.getApplicationInfo(appId).map(ApplicationsListResource.appHistoryInfoToPublicAppInfo)
  }

  override def writeEventLogs(
      appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {
    provider.writeEventLogs(appId, attemptId, zipStream)
  }

  /**
   * @return html text to display when the application list is empty
   */
  def emptyListingHtml(): Seq[Node] = {
    provider.getEmptyListingHtml()
  }

  /**
   * Returns the provider configuration to show in the listing page.
   *
   * @return A map with the provider's configuration.
   */
  def getProviderConfig(): Map[String, String] = provider.getConfig()

  /**
   * Load an application UI and attach it to the web server.
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return true if the application was found and loaded.
   */
  private def loadAppUi(appId: String, attemptId: Option[String]): Boolean = {
    try {
      appCache.get(appId, attemptId)
      true
    } catch {
      case NonFatal(e) => e.getCause() match {
        case nsee: NoSuchElementException =>
          false

        case cause: Exception => throw cause
      }
    }
  }

  /**
   * String value for diagnostics.
   * @return a multi-line description of the server state.
   */
  override def toString: String = {
    s"""
      | History Server;
      | provider = $provider
      | cache = $appCache
    """.stripMargin
  }
}

/**
 * An abstract implementation of the metrics [[Source]] trait with some common operations.
 */
private[history] abstract class HistoryMetricSource(val prefix: String) extends Source {
  override val metricRegistry = new MetricRegistry()

  /**
   * Register a sequence of metrics
   * @param metrics sequence of metrics to register
   */
  def register(metrics: Seq[(String, Metric)]): Unit = {
    metrics.foreach { case (name, metric) =>
      metricRegistry.register(fullname(name), metric)
    }
  }

  /**
   * Create the full name of a metric by prepending the prefix to the name
   * @param name short name
   * @return the full name to use in registration
   */
  def fullname(name: String): String = {
    MetricRegistry.name(prefix, name)
  }

  /**
   * Dump the counters and gauges.
   * @return a string for logging and diagnostics —not for parsing by machines.
   */
  override def toString: String = {
    val sb = new StringBuilder(s"Metrics for $sourceName:\n")
    sb.append("  Counters\n")
    metricRegistry.getCounters.asScala.foreach { entry =>
        sb.append("    ").append(entry._1).append(" = ").append(entry._2.getCount).append('\n')
    }
    sb.append("  Gauges\n")
    metricRegistry.getGauges.asScala.foreach { entry =>
      sb.append("    ").append(entry._1).append(" = ").append(entry._2.getValue).append('\n')
    }
    sb.toString()
  }

  /**
   * Get a named counter.
   * @param counterName name of the counter
   * @return the counter, if found
   */
  def getCounter(counterName: String): Option[Counter] = {
    Option(metricRegistry.getCounters(new MetricFilter {
      def matches(name: String, metric: Metric): Boolean = name == counterName
    }).get(counterName))
  }

  /**
   * Get a gauge of an unknown numeric type.
   * @param gaugeName name of the gauge
   * @return gauge, if found
   */
  def getGauge(gaugeName: String): Option[Gauge[_]] = {
    Option(metricRegistry.getGauges(new MetricFilter {
      def matches(name: String, metric: Metric): Boolean = name == gaugeName
    }).get(gaugeName))
  }

  /**
   * Get a Long gauge.
   * @param gaugeName name of the gauge
   * @return gauge, if found
   * @throws ClassCastException if the gauge is found but of the wrong type
   */
  def getLongGauge(gaugeName: String): Option[Gauge[Long]] = {
    Option(metricRegistry.getGauges(new MetricFilter {
      def matches(name: String, metric: Metric): Boolean = name == gaugeName
    }).get(gaugeName)).asInstanceOf[Option[Gauge[Long]]]
  }

  /**
   * Get a timer.
   * @param timerName name of the timer
   * @return the timer, if found.
   */
  def getTimer(timerName: String): Option[Timer] = {
    Option(metricRegistry.getTimers(new MetricFilter {
      def matches(name: String, metric: Metric): Boolean = name == timerName
    }).get(timerName))
  }

}

/**
 * History system metrics independent of providers go in here.
 * @param owner owning instance
 */
private[history] class HistoryMetrics(val owner: HistoryServer, prefix: String)
    extends HistoryMetricSource(prefix) {
  override val sourceName = "history"

}

/**
 * A timestamp is a gauge which is set to a point in time
 * as measured in millseconds since the epoch began.
 */
private[history] class Timestamp extends Gauge[Long] {
  var time = 0L

  /** Current value. */
  override def getValue: Long = time

  /** Set a new value. */
  def setValue(t: Long): Unit = {
    time = t
  }

  /** Set to the current system time. */
  def touch(): Unit = {
    setValue(System.currentTimeMillis())
  }
}

/**
 * A Long gauge from a lambda expression; the expression is evaluated
 * whenever the metrics are queried
 * @param expression expression which generates the value.
 */
private[history] class LambdaLongGauge(expression: (() => Long)) extends Gauge[Long] {
  override def getValue: Long = expression()
}

/**
 * The recommended way of starting and stopping a HistoryServer is through the scripts
 * start-history-server.sh and stop-history-server.sh. The path to a base log directory,
 * as well as any other relevant history server configuration, should be specified via
 * the $SPARK_HISTORY_OPTS environment variable. For example:
 *
 *   export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/tmp/spark-events"
 *   ./sbin/start-history-server.sh
 *
 * This launches the HistoryServer as a Spark daemon.
 */
object HistoryServer extends Logging {
  private val conf = new SparkConf

  val UI_PATH_PREFIX = "/history"

  def main(argStrings: Array[String]): Unit = {
    Utils.initDaemon(log)
    new HistoryServerArguments(conf, argStrings)
    initSecurity()
    val securityManager = createSecurityManager(conf)

    val providerName = conf.getOption("spark.history.provider")
      .getOrElse(classOf[FsHistoryProvider].getName())
    val provider = Utils.classForName(providerName)
      .getConstructor(classOf[SparkConf])
      .newInstance(conf)
      .asInstanceOf[ApplicationHistoryProvider]

    val port = conf.getInt("spark.history.ui.port", 18080)

    val server = new HistoryServer(conf, provider, securityManager, port)
    server.bind()

    ShutdownHookManager.addShutdownHook { () => server.stop() }

    // Wait until the end of the world... or if the HistoryServer process is manually stopped
    while(true) { Thread.sleep(Int.MaxValue) }
  }

  /**
   * Create a security manager.
   * This turns off security in the SecurityManager, so that the History Server can start
   * in a Spark cluster where security is enabled.
   * @param config configuration for the SecurityManager constructor
   * @return the security manager for use in constructing the History Server.
   */
  private[history] def createSecurityManager(config: SparkConf): SecurityManager = {
    if (config.getBoolean(SecurityManager.SPARK_AUTH_CONF, false)) {
      logDebug(s"Clearing ${SecurityManager.SPARK_AUTH_CONF}")
      config.set(SecurityManager.SPARK_AUTH_CONF, "false")
    }
    new SecurityManager(config)
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

  private[history] def getAttemptURI(appId: String, attemptId: Option[String]): String = {
    val attemptSuffix = attemptId.map { id => s"/$id" }.getOrElse("")
    s"${HistoryServer.UI_PATH_PREFIX}/${appId}${attemptSuffix}"
  }

}
