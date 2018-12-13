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
import java.util.concurrent.ExecutionException
import javax.servlet.{DispatcherType, Filter, FilterChain, FilterConfig, ServletException, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.collection.JavaConverters._

import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalListener, RemovalNotification}
import com.google.common.util.concurrent.UncheckedExecutionException
import org.eclipse.jetty.servlet.FilterHolder

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.Source
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Clock

/**
 * Cache for application UIs.
 *
 * Applications are cached for as long as there is capacity for them. See [[LoadedAppUI]] for a
 * discussion of the UI lifecycle.
 *
 * @param operations implementation of record access operations
 * @param retainedApplications number of retained applications
 * @param clock time source
 */
private[history] class ApplicationCache(
    val operations: ApplicationCacheOperations,
    val retainedApplications: Int,
    val clock: Clock) extends Logging {

  private val appLoader = new CacheLoader[CacheKey, CacheEntry] {

    /** the cache key doesn't match a cached entry, or the entry is out-of-date, so load it. */
    override def load(key: CacheKey): CacheEntry = {
      loadApplicationEntry(key.appId, key.attemptId)
    }

  }

  private val removalListener = new RemovalListener[CacheKey, CacheEntry] {

    /**
     * Removal event notifies the provider to detach the UI.
     * @param rm removal notification
     */
    override def onRemoval(rm: RemovalNotification[CacheKey, CacheEntry]): Unit = {
      metrics.evictionCount.inc()
      val key = rm.getKey
      logDebug(s"Evicting entry ${key}")
      operations.detachSparkUI(key.appId, key.attemptId, rm.getValue().loadedUI.ui)
    }
  }

  private val appCache: LoadingCache[CacheKey, CacheEntry] = {
    CacheBuilder.newBuilder()
        .maximumSize(retainedApplications)
        .removalListener(removalListener)
        .build(appLoader)
  }

  /**
   * The metrics which are updated as the cache is used.
   */
  val metrics = new CacheMetrics("history.cache")

  def get(appId: String, attemptId: Option[String] = None): CacheEntry = {
    try {
      appCache.get(new CacheKey(appId, attemptId))
    } catch {
      case e @ (_: ExecutionException | _: UncheckedExecutionException) =>
        throw Option(e.getCause()).getOrElse(e)
    }
  }

  /**
   * Run a closure while holding an application's UI read lock. This prevents the history server
   * from closing the UI data store while it's being used.
   */
  def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T = {
    var entry = get(appId, attemptId)

    // If the entry exists, we need to make sure we run the closure with a valid entry. So
    // we need to re-try until we can lock a valid entry for read.
    entry.loadedUI.lock.readLock().lock()
    try {
      while (!entry.loadedUI.valid) {
        entry.loadedUI.lock.readLock().unlock()
        entry = null
        try {
          invalidate(new CacheKey(appId, attemptId))
          entry = get(appId, attemptId)
          metrics.loadCount.inc()
        } finally {
          if (entry != null) {
            entry.loadedUI.lock.readLock().lock()
          }
        }
      }

      fn(entry.loadedUI.ui)
    } finally {
      if (entry != null) {
        entry.loadedUI.lock.readLock().unlock()
      }
    }
  }

  /** @return Number of cached UIs. */
  def size(): Long = appCache.size()

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
  private def loadApplicationEntry(appId: String, attemptId: Option[String]): CacheEntry = {
    logDebug(s"Loading application Entry $appId/$attemptId")
    metrics.loadCount.inc()
    val loadedUI = time(metrics.loadTimer) {
      metrics.lookupCount.inc()
      operations.getAppUI(appId, attemptId) match {
        case Some(loadedUI) =>
          logDebug(s"Loaded application $appId/$attemptId")
          loadedUI
        case None =>
          metrics.lookupFailureCount.inc()
          // guava's cache logs via java.util log, so is of limited use. Hence: our own message
          logInfo(s"Failed to load application attempt $appId/$attemptId")
          throw new NoSuchElementException(s"no application with application Id '$appId'" +
              attemptId.map { id => s" attemptId '$id'" }.getOrElse(" and no attempt Id"))
      }
    }
    try {
      val completed = loadedUI.ui.getApplicationInfoList.exists(_.attempts.last.completed)
      if (!completed) {
        // incomplete UIs have the cache-check filter put in front of them.
        registerFilter(new CacheKey(appId, attemptId), loadedUI)
      }
      operations.attachSparkUI(appId, attemptId, loadedUI.ui, completed)
      new CacheEntry(loadedUI, completed)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to initialize application UI for $appId/$attemptId", e)
        operations.detachSparkUI(appId, attemptId, loadedUI.ui)
        throw e
    }
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

  /**
   * Register a filter for the web UI which checks for updates to the given app/attempt
   * @param ui Spark UI to attach filters to
   * @param appId application ID
   * @param attemptId attempt ID
   */
  private def registerFilter(key: CacheKey, loadedUI: LoadedAppUI): Unit = {
    require(loadedUI != null)
    val enumDispatcher = java.util.EnumSet.of(DispatcherType.ASYNC, DispatcherType.REQUEST)
    val filter = new ApplicationCacheCheckFilter(key, loadedUI, this)
    val holder = new FilterHolder(filter)
    require(loadedUI.ui.getHandlers != null, "null handlers")
    loadedUI.ui.getHandlers.foreach { handler =>
      handler.addFilter(holder, "/*", enumDispatcher)
    }
  }

  def invalidate(key: CacheKey): Unit = appCache.invalidate(key)

}

/**
 * An entry in the cache.
 *
 * @param ui Spark UI
 * @param completed Flag to indicated that the application has completed (and so
 *                 does not need refreshing).
 */
private[history] final class CacheEntry(
    val loadedUI: LoadedAppUI,
    val completed: Boolean) {

  /** string value is for test assertions */
  override def toString: String = {
    s"UI ${loadedUI.ui}, completed=$completed"
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

  /** all the counters: for registration and string conversion. */
  private val counters = Seq(
    ("lookup.count", lookupCount),
    ("lookup.failure.count", lookupFailureCount),
    ("eviction.count", evictionCount),
    ("load.count", loadCount))

  /** all metrics, including timers */
  private val allMetrics = counters ++ Seq(
    ("load.timer", loadTimer))

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
private[history] class ApplicationCacheCheckFilter(
    key: CacheKey,
    loadedUI: LoadedAppUI,
    cache: ApplicationCache)
  extends Filter with Logging {

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
    loadedUI.lock.readLock().lock()
    if (loadedUI.valid) {
      try {
        chain.doFilter(request, response)
      } finally {
        loadedUI.lock.readLock.unlock()
      }
    } else {
      loadedUI.lock.readLock.unlock()
      cache.invalidate(key)
      val queryStr = Option(httpRequest.getQueryString).map("?" + _).getOrElse("")
      val redirectUrl = httpResponse.encodeRedirectURL(requestURI + queryStr)
      httpResponse.sendRedirect(redirectUrl)
    }
  }

  override def init(config: FilterConfig): Unit = { }

  override def destroy(): Unit = { }

}
