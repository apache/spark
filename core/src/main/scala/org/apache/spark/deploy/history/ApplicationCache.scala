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
import javax.servlet.{DispatcherType, Filter, FilterChain, FilterConfig, ServletException, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalListener, RemovalNotification}
import org.eclipse.jetty.servlet.FilterHolder

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.Source
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Clock

/**
 * Cache for applications.
 *
 * Completed applications are cached for as long as there is capacity for them.
 * Incompleted applications have their update time checked on every
 * retrieval; if the cached entry is out of date, it is refreshed.
 *
 * @note there must be only one instance of [[ApplicationCache]] in a
 * JVM at a time. This is because a static field in [[ApplicationCacheCheckFilterRelay]]
 * keeps a reference to the cache so that HTTP requests on the attempt-specific web UIs
 * can probe the current cache to see if the attempts have changed.
 *
 * Creating multiple instances will break this routing.
 * @param operations implementation of record access operations
 * @param retainedApplications number of retained applications
 * @param clock time source
 */
private[history] class ApplicationCache(
    val operations: ApplicationCacheOperations,
    val retainedApplications: Int,
    val clock: Clock) extends Logging {

  /**
   * Services the load request from the cache.
   */
  private val appLoader = new CacheLoader[CacheKey, CacheEntry] {

    /** the cache key doesn't match a cached entry, or the entry is out-of-date, so load it. */
    override def load(key: CacheKey): CacheEntry = {
      loadApplicationEntry(key.appId, key.attemptId)
    }

  }

  /**
   * Handler for callbacks from the cache of entry removal.
   */
  private val removalListener = new RemovalListener[CacheKey, CacheEntry] {

    /**
     * Removal event notifies the provider to detach the UI.
     * @param rm removal notification
     */
    override def onRemoval(rm: RemovalNotification[CacheKey, CacheEntry]): Unit = {
      metrics.evictionCount.inc()
      val key = rm.getKey
      logDebug(s"Evicting entry ${key}")
      operations.detachSparkUI(key.appId, key.attemptId, rm.getValue().ui)
    }
  }

  /**
   * The cache of applications.
   *
   * Tagged as `protected` so as to allow subclasses in tests to access it directly
   */
  protected val appCache: LoadingCache[CacheKey, CacheEntry] = {
    CacheBuilder.newBuilder()
        .maximumSize(retainedApplications)
        .removalListener(removalListener)
        .build(appLoader)
  }

  /**
   * The metrics which are updated as the cache is used.
   */
  val metrics = new CacheMetrics("history.cache")

  init()

  /**
   * Perform any startup operations.
   *
   * This includes declaring this instance as the cache to use in the
   * [[ApplicationCacheCheckFilterRelay]].
   */
  private def init(): Unit = {
    ApplicationCacheCheckFilterRelay.setApplicationCache(this)
  }

  /**
   * Stop the cache.
   * This will reset the relay in [[ApplicationCacheCheckFilterRelay]].
   */
  def stop(): Unit = {
    ApplicationCacheCheckFilterRelay.resetApplicationCache()
  }

  /**
   * Get an entry.
   *
   * Cache fetch/refresh will have taken place by the time this method returns.
   * @param appAndAttempt application to look up in the format needed by the history server web UI,
   *                      `appId/attemptId` or `appId`.
   * @return the entry
   */
  def get(appAndAttempt: String): SparkUI = {
    val parts = splitAppAndAttemptKey(appAndAttempt)
    get(parts._1, parts._2)
  }

  /**
   * Get the Spark UI, converting a lookup failure from an exception to `None`.
   * @param appAndAttempt application to look up in the format needed by the history server web UI,
   *                      `appId/attemptId` or `appId`.
   * @return the entry
   */
  def getSparkUI(appAndAttempt: String): Option[SparkUI] = {
    try {
      val ui = get(appAndAttempt)
      Some(ui)
    } catch {
      case NonFatal(e) => e.getCause() match {
        case nsee: NoSuchElementException =>
          None
        case cause: Exception => throw cause
      }
    }
  }

  /**
   * Get the associated spark UI.
   *
   * Cache fetch/refresh will have taken place by the time this method returns.
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return the entry
   */
  def get(appId: String, attemptId: Option[String]): SparkUI = {
    lookupAndUpdate(appId, attemptId)._1.ui
  }

  /**
   * Look up the entry; update it if needed.
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return the underlying cache entry -which can have its timestamp changed, and a flag to
   *         indicate that the entry has changed
   */
  private def lookupAndUpdate(appId: String, attemptId: Option[String]): (CacheEntry, Boolean) = {
    metrics.lookupCount.inc()
    val cacheKey = CacheKey(appId, attemptId)
    var entry = appCache.getIfPresent(cacheKey)
    var updated = false
    if (entry == null) {
      // no entry, so fetch without any post-fetch probes for out-of-dateness
      // this will trigger a callback to loadApplicationEntry()
      entry = appCache.get(cacheKey)
    } else if (!entry.completed) {
      val now = clock.getTimeMillis()
      log.debug(s"Probing at time $now for updated application $cacheKey -> $entry")
      metrics.updateProbeCount.inc()
      updated = time(metrics.updateProbeTimer) {
        entry.updateProbe()
      }
      if (updated) {
        logDebug(s"refreshing $cacheKey")
        metrics.updateTriggeredCount.inc()
        appCache.refresh(cacheKey)
        // and repeat the lookup
        entry = appCache.get(cacheKey)
      } else {
        // update the probe timestamp to the current time
        entry.probeTime = now
      }
    }
    (entry, updated)
  }

  /**
   * This method is visible for testing.
   *
   * It looks up the cached entry *and returns a clone of it*.
   * This ensures that the cached entries never leak
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return a new entry with shared SparkUI, but copies of the other fields.
   */
  def lookupCacheEntry(appId: String, attemptId: Option[String]): CacheEntry = {
    val entry = lookupAndUpdate(appId, attemptId)._1
    new CacheEntry(entry.ui, entry.completed, entry.updateProbe, entry.probeTime)
  }

  /**
   * Probe for an application being updated.
   * @param appId application ID
   * @param attemptId attempt ID
   * @return true if an update has been triggered
   */
  def checkForUpdates(appId: String, attemptId: Option[String]): Boolean = {
    val (entry, updated) = lookupAndUpdate(appId, attemptId)
    updated
  }

  /**
   * Size probe, primarily for testing.
   * @return size
   */
  def size(): Long = appCache.size()

  /**
   * Emptiness predicate, primarily for testing.
   * @return true if the cache is empty
   */
  def isEmpty: Boolean = appCache.size() == 0

  /**
   * Time a closure, returning its output.
   * @param t timer
   * @param f function
   * @tparam T type of return value of time
   * @return the result of the function.
   */
  private def time[T](t: Timer)(f: => T): T = {
    val timeCtx = t.time()
    try {
      f
    } finally {
      timeCtx.close()
    }
  }

  /**
   * Load the Spark UI via [[ApplicationCacheOperations.getAppUI()]],
   * then attach it to the web UI via [[ApplicationCacheOperations.attachSparkUI()]].
   *
   * If the application is incomplete, it has the [[ApplicationCacheCheckFilter]]
   * added as a filter to the HTTP requests, so that queries on the UI will trigger
   * update checks.
   *
   * The generated entry contains the UI and the current timestamp.
   * The timer [[metrics.loadTimer]] tracks the time taken to load the UI.
   *
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return the cache entry
   * @throws NoSuchElementException if there is no matching element
   */
  @throws[NoSuchElementException]
  def loadApplicationEntry(appId: String, attemptId: Option[String]): CacheEntry = {

    logDebug(s"Loading application Entry $appId/$attemptId")
    metrics.loadCount.inc()
    time(metrics.loadTimer) {
      operations.getAppUI(appId, attemptId) match {
        case Some(LoadedAppUI(ui, updateState)) =>
          val completed = ui.getApplicationInfoList.exists(_.attempts.last.completed)
          if (completed) {
            // completed spark UIs are attached directly
            operations.attachSparkUI(appId, attemptId, ui, completed)
          } else {
            // incomplete UIs have the cache-check filter put in front of them.
            ApplicationCacheCheckFilterRelay.registerFilter(ui, appId, attemptId)
            operations.attachSparkUI(appId, attemptId, ui, completed)
          }
          // build the cache entry
          val now = clock.getTimeMillis()
          val entry = new CacheEntry(ui, completed, updateState, now)
          logDebug(s"Loaded application $appId/$attemptId -> $entry")
          entry
        case None =>
          metrics.lookupFailureCount.inc()
          // guava's cache logs via java.util log, so is of limited use. Hence: our own message
          logInfo(s"Failed to load application attempt $appId/$attemptId")
          throw new NoSuchElementException(s"no application with application Id '$appId'" +
              attemptId.map { id => s" attemptId '$id'" }.getOrElse(" and no attempt Id"))
      }
    }
  }

  /**
   * Split up an `applicationId/attemptId` or `applicationId` key into the separate pieces.
   *
   * @param appAndAttempt combined key
   * @return a tuple of the application ID and, if present, the attemptID
   */
  def splitAppAndAttemptKey(appAndAttempt: String): (String, Option[String]) = {
    val parts = appAndAttempt.split("/")
    require(parts.length == 1 || parts.length == 2, s"Invalid app key $appAndAttempt")
    val appId = parts(0)
    val attemptId = if (parts.length > 1) Some(parts(1)) else None
    (appId, attemptId)
  }

  /**
   * Merge an appId and optional attempt Id into a key of the form `applicationId/attemptId`.
   *
   * If there is an `attemptId`; `applicationId` if not.
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return a unified string
   */
  def mergeAppAndAttemptToKey(appId: String, attemptId: Option[String]): String = {
    appId + attemptId.map { id => s"/$id" }.getOrElse("")
  }

  /**
   * String operator dumps the cache entries and metrics.
   * @return a string value, primarily for testing and diagnostics
   */
  override def toString: String = {
    val sb = new StringBuilder(s"ApplicationCache(" +
          s" retainedApplications= $retainedApplications)")
    sb.append(s"; time= ${clock.getTimeMillis()}")
    sb.append(s"; entry count= ${appCache.size()}\n")
    sb.append("----\n")
    appCache.asMap().asScala.foreach {
      case(key, entry) => sb.append(s"  $key -> $entry\n")
    }
    sb.append("----\n")
    sb.append(metrics)
    sb.append("----\n")
    sb.toString()
  }
}

/**
 * An entry in the cache.
 *
 * @param ui Spark UI
 * @param completed Flag to indicated that the application has completed (and so
 *                 does not need refreshing).
 * @param updateProbe function to call to see if the application has been updated and
 *                    therefore that the cached value needs to be refreshed.
 * @param probeTime Times in milliseconds when the probe was last executed.
 */
private[history] final class CacheEntry(
    val ui: SparkUI,
    val completed: Boolean,
    val updateProbe: () => Boolean,
    var probeTime: Long) {

  /** string value is for test assertions */
  override def toString: String = {
    s"UI $ui, completed=$completed, probeTime=$probeTime"
  }
}

/**
 * Cache key: compares on `appId` and then, if non-empty, `attemptId`.
 * The [[hashCode()]] function uses the same fields.
 * @param appId application ID
 * @param attemptId attempt ID
 */
private[history] final case class CacheKey(appId: String, attemptId: Option[String]) {

  override def toString: String = {
    appId + attemptId.map { id => s"/$id" }.getOrElse("")
  }
}

/**
 * Metrics of the cache
 * @param prefix prefix to register all entries under
 */
private[history] class CacheMetrics(prefix: String) extends Source {

  /* metrics: counters and timers */
  val lookupCount = new Counter()
  val lookupFailureCount = new Counter()
  val evictionCount = new Counter()
  val loadCount = new Counter()
  val loadTimer = new Timer()
  val updateProbeCount = new Counter()
  val updateProbeTimer = new Timer()
  val updateTriggeredCount = new Counter()

  /** all the counters: for registration and string conversion. */
  private val counters = Seq(
    ("lookup.count", lookupCount),
    ("lookup.failure.count", lookupFailureCount),
    ("eviction.count", evictionCount),
    ("load.count", loadCount),
    ("update.probe.count", updateProbeCount),
    ("update.triggered.count", updateTriggeredCount))

  /** all metrics, including timers */
  private val allMetrics = counters ++ Seq(
    ("load.timer", loadTimer),
    ("update.probe.timer", updateProbeTimer))

  /**
   * Name of metric source
   */
  override val sourceName = "ApplicationCache"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  /**
   * Startup actions.
   * This includes registering metrics with [[metricRegistry]]
   */
  private def init(): Unit = {
    allMetrics.foreach { case (name, metric) =>
      metricRegistry.register(MetricRegistry.name(prefix, name), metric)
    }
  }

  override def toString: String = {
    val sb = new StringBuilder()
    counters.foreach { case (name, counter) =>
      sb.append(name).append(" = ").append(counter.getCount).append('\n')
    }
    sb.toString()
  }
}

/**
 * API for cache events. That is: loading an App UI; and for
 * attaching/detaching the UI to and from the Web UI.
 */
private[history] trait ApplicationCacheOperations {

  /**
   * Get the application UI and the probe needed to see if it has been updated.
   * @param appId application ID
   * @param attemptId attempt ID
   * @return If found, the Spark UI and any history information to be used in the cache
   */
  def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI]

  /**
   * Attach a reconstructed UI.
   * @param appId application ID
   * @param attemptId attempt ID
   * @param ui UI
   * @param completed flag to indicate that the UI has completed
   */
  def attachSparkUI(
      appId: String,
      attemptId: Option[String],
      ui: SparkUI,
      completed: Boolean): Unit

  /**
   * Detach a Spark UI.
   *
   * @param ui Spark UI
   */
  def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit

}

/**
 * This is a servlet filter which intercepts HTTP requests on application UIs and
 * triggers checks for updated data.
 *
 * If the application cache indicates that the application has been updated,
 * the filter returns a 302 redirect to the caller, asking them to re-request the web
 * page.
 *
 * Because the application cache will detach and then re-attach the UI, when the caller
 * repeats that request, it will now pick up the newly-updated web application.
 *
 * This does require the caller to handle 302 requests. Because of the ambiguity
 * in how POST and PUT operations are responded to (that is, should a 307 be
 * processed directly), the filter <i>does not</i> filter those requests.
 * As the current web UIs are read-only, this is not an issue. If it were ever to
 * support more HTTP verbs, then some support may be required. Perhaps, rather
 * than sending a redirect, simply updating the value so that the <i>next</i>
 * request will pick it up.
 *
 * Implementation note: there's some abuse of a shared global entry here because
 * the configuration data passed to the servlet is just a string:string map.
 */
private[history] class ApplicationCacheCheckFilter() extends Filter with Logging {

  import ApplicationCacheCheckFilterRelay._
  var appId: String = _
  var attemptId: Option[String] = _

  /**
   * Bind the app and attempt ID, throwing an exception if no application ID was provided.
   * @param filterConfig configuration
   */
  override def init(filterConfig: FilterConfig): Unit = {

    appId = Option(filterConfig.getInitParameter(APP_ID))
      .getOrElse(throw new ServletException(s"Missing Parameter $APP_ID"))
    attemptId = Option(filterConfig.getInitParameter(ATTEMPT_ID))
    logDebug(s"initializing filter $this")
  }

  /**
   * Filter the request.
   * Either the caller is given a 302 redirect to the current URL, or the
   * request is passed on to the SparkUI servlets.
   *
   * @param request HttpServletRequest
   * @param response HttpServletResponse
   * @param chain the rest of the request chain
   */
  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain): Unit = {

    // nobody has ever implemented any other kind of servlet, yet
    // this check is universal, just in case someone does exactly
    // that on your classpath
    if (!(request.isInstanceOf[HttpServletRequest])) {
      throw new ServletException("This filter only works for HTTP/HTTPS")
    }
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val httpResponse = response.asInstanceOf[HttpServletResponse]
    val requestURI = httpRequest.getRequestURI
    val operation = httpRequest.getMethod

    // if the request is for an attempt, check to see if it is in need of delete/refresh
    // and have the cache update the UI if so
    if (operation=="HEAD" || operation=="GET"
        && checkForUpdates(requestURI, appId, attemptId)) {
      // send a redirect back to the same location. This will be routed
      // to the *new* UI
      logInfo(s"Application Attempt $appId/$attemptId updated; refreshing")
      val queryStr = Option(httpRequest.getQueryString).map("?" + _).getOrElse("")
      val redirectUrl = httpResponse.encodeRedirectURL(requestURI + queryStr)
      httpResponse.sendRedirect(redirectUrl)
    } else {
      chain.doFilter(request, response)
    }
  }

  override def destroy(): Unit = {
  }

  override def toString: String = s"ApplicationCacheCheckFilter for $appId/$attemptId"
}

/**
 * Global state for the [[ApplicationCacheCheckFilter]] instances, so that they can relay cache
 * probes to the cache.
 *
 * This is an ugly workaround for the limitation of servlets and filters in the Java servlet
 * API; they are still configured on the model of a list of classnames and configuration
 * strings in a `web.xml` field, rather than a chain of instances wired up by hand or
 * via an injection framework. There is no way to directly configure a servlet filter instance
 * with a reference to the application cache which is must use: some global state is needed.
 *
 * Here, [[ApplicationCacheCheckFilter]] is that global state; it relays all requests
 * to the singleton [[ApplicationCache]]
 *
 * The field `applicationCache` must be set for the filters to work -
 * this is done during the construction of [[ApplicationCache]], which requires that there
 * is only one cache serving requests through the WebUI.
 *
 * *Important* In test runs, if there is more than one [[ApplicationCache]], the relay logic
 * will break: filters may not find instances. Tests must not do that.
 *
 */
private[history] object ApplicationCacheCheckFilterRelay extends Logging {
  // name of the app ID entry in the filter configuration. Mandatory.
  val APP_ID = "appId"

  // name of the attempt ID entry in the filter configuration. Optional.
  val ATTEMPT_ID = "attemptId"

  // name of the filter to register
  val FILTER_NAME = "org.apache.spark.deploy.history.ApplicationCacheCheckFilter"

  /** the application cache to relay requests to */
  @volatile
  private var applicationCache: Option[ApplicationCache] = None

  /**
   * Set the application cache. Logs a warning if it is overwriting an existing value
   * @param cache new cache
   */
  def setApplicationCache(cache: ApplicationCache): Unit = {
    applicationCache.foreach( c => logWarning(s"Overwriting application cache $c"))
    applicationCache = Some(cache)
  }

  /**
   * Reset the application cache
   */
  def resetApplicationCache(): Unit = {
    applicationCache = None
  }

  /**
   * Check to see if there has been an update
   * @param requestURI URI the request came in on
   * @param appId application ID
   * @param attemptId attempt ID
   * @return true if an update was loaded for the app/attempt
   */
  def checkForUpdates(requestURI: String, appId: String, attemptId: Option[String]): Boolean = {

    logDebug(s"Checking $appId/$attemptId from $requestURI")
    applicationCache match {
      case Some(cache) =>
        try {
          cache.checkForUpdates(appId, attemptId)
        } catch {
          case ex: Exception =>
            // something went wrong. Keep going with the existing UI
            logWarning(s"When checking for $appId/$attemptId from $requestURI", ex)
            false
        }

      case None =>
        logWarning("No application cache instance defined")
        false
    }
  }


  /**
   * Register a filter for the web UI which checks for updates to the given app/attempt
   * @param ui Spark UI to attach filters to
   * @param appId application ID
   * @param attemptId attempt ID
   */
  def registerFilter(
      ui: SparkUI,
      appId: String,
      attemptId: Option[String] ): Unit = {
    require(ui != null)
    val enumDispatcher = java.util.EnumSet.of(DispatcherType.ASYNC, DispatcherType.REQUEST)
    val holder = new FilterHolder()
    holder.setClassName(FILTER_NAME)
    holder.setInitParameter(APP_ID, appId)
    attemptId.foreach( id => holder.setInitParameter(ATTEMPT_ID, id))
    require(ui.getHandlers != null, "null handlers")
    ui.getHandlers.foreach { handler =>
      handler.addFilter(holder, "/*", enumDispatcher)
    }
  }
}
