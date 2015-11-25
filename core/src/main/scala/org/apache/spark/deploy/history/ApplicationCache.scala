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

import scala.collection.JavaConverters._

import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalListener, RemovalNotification}
import com.google.common.util.concurrent.ListenableFuture

import org.apache.spark.Logging
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
 * @param operations implementation of record access operations
 * @param refreshInterval interval between refreshes in milliseconds.
 * @param retainedApplications number of retained applications
 * @param clock time source
 */
private[history] class ApplicationCache(val operations: ApplicationCacheOperations,
    val refreshInterval: Long,
    val retainedApplications: Int,
    val clock: Clock) extends Logging {

  /**
   * Services the load request from the cache.
   */
  private val appLoader = new CacheLoader[CacheKey, CacheEntry] {

    /** the cache key doesn't match an cached entry ... attempt to load it  */
    override def load(key: CacheKey): CacheEntry = {
      loadApplicationEntry(key.appId, key.attemptId)
    }

    override def reload(key: CacheKey, oldValue: CacheEntry): ListenableFuture[CacheEntry] = super
        .reload(key, oldValue)
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
   * Tagged as protected so as to allow subclasses in tests to accesss it directly
   */
  protected val appCache: LoadingCache[CacheKey, CacheEntry] = CacheBuilder.newBuilder()
      .maximumSize(retainedApplications)
      .removalListener(removalListener)
      .build(appLoader)

  /**
   * The metrics which are updated as the cache is used
   */
  val metrics = new CacheMetrics("history.cache")

  /**
   * Get an entry. Cache fetch/refresh will have taken place by
   * the time this method returns.
   * @param appAndAttempt application to look up in the format needed by the history server web UI,
   *                      `appId/attemptId` or `appId`.
   * @return the entry
   */
  def get(appAndAttempt: String): SparkUI = {
    val parts = splitAppAndAttemptKey(appAndAttempt)
    get(parts._1, parts._2)
  }

  /**
   * Get the associated spark UI. Cache fetch/refresh will have taken place by
   * the time this method returns.
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return the entry
   */
  def get(appId: String, attemptId: Option[String]): SparkUI = {
    lookupAndUpdate(appId, attemptId).ui
  }

  /**
   * Look up the entry; update it if needed.
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return the underlying cache entry -which can have its timestamp changed.
   */
  private def lookupAndUpdate(appId: String, attemptId: Option[String]): CacheEntry = {
    metrics.lookupCount.inc()
    val cacheKey = CacheKey(appId, attemptId)
    var entry = appCache.getIfPresent(cacheKey)
    if (entry == null) {
      // no entry, so fetch without any post-fetch probes for out-of-dateness
      entry = appCache.get(cacheKey)
    } else if (!entry.completed) {
      val now = clock.getTimeMillis()
      if (now - entry.timestamp > refreshInterval) {
        log.debug(s"Probing for updated application $cacheKey")
        metrics.updateProbeCount.inc()
        val updated = time(metrics.updateProbeTimer) {
          operations.isUpdated(appId, attemptId, entry.timestamp)
        }
        if (updated) {
          logDebug(s"refreshing $cacheKey")
          metrics.updateTriggeredCount.inc()
          appCache.refresh(cacheKey)
          // and re-attempt the lookup
          entry = appCache.get(cacheKey)
        } else {
          // update the timestamp to the time of this probe
          entry.timestamp = now
        }
      }
    }
    entry
  }

  /**
   * This method is visible for testing. It looks up the cached entry *and returns a clone of it*.
   * This ensures that the cached entries never leak
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return a new entry with shared SparkUI, but copies of the other fields.
   */
  def lookupCacheEntry(appId: String, attemptId: Option[String]): CacheEntry = {
    val entry = lookupAndUpdate(appId, attemptId)
    new CacheEntry(entry.ui, entry.completed, entry.timestamp)
  }

  def size(): Long = appCache.size()


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
   * The generated entry contains the UI and the current timestamp.
   * The timer [[metrics.loadTimer]] tracks the time taken to load the UI.
   *
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return the cache entry
   * @throws NoSuchElementException if there
   */
  @throws[NoSuchElementException]
  def loadApplicationEntry(appId: String, attemptId: Option[String]): CacheEntry = {
    logDebug(s"Loading application Entry $appId/$attemptId")
    metrics.loadCount.inc()
    time(metrics.loadTimer) {
      operations.getAppUI(appId, attemptId) match {
        case Some(ui) =>
          val completed = ui.getApplicationInfoList.exists(_.attempts.last.completed)
          // attach the spark UI
          operations.attachSparkUI(appId, attemptId, ui, completed)
          // build the cache entry
          new CacheEntry(ui, completed, clock.getTimeMillis())
        case None =>
          metrics.lookupFailureCount.inc()
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
   * Merge an appId and optional attempt Id into a key of the form `applicationId/attemptId`
   * if there is an `attemptId`; `applicationId` if not.
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
    val sb = new StringBuilder(s"ApplicationCache(refreshInterval=$refreshInterval," +
          s" retainedApplications= $retainedApplications)")
    sb.append(s"; time = ${clock.getTimeMillis()}")
    sb.append(s"; entry count= ${appCache.size()}\n")
    sb.append("----\n")
    appCache.asMap().asScala.foreach {
      case(key, entry) => sb.append(s"    $key -> $entry\n")
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
 * @param completed: flag to indicated that the application has completed (and so
 *                 does not need refreshing)
 * @param timestamp timestamp in milliseconds. This may be updated during probes
 */
private[history] final class CacheEntry(val ui: SparkUI, val completed: Boolean,
    var timestamp: Long) {

  /** string value is for test assertions */
  override def toString: String = {
    s"UI $ui, completed=$completed, timestamp=$timestamp"
  }
}

/**
 * Cache key: compares on App Id and then, if non-empty, attemptId.
 * The [[hashCode()]] function uses the same fields.
 * @param appId application ID
 * @param attemptId attempt ID
 */
private[history] final case class CacheKey(appId: String, attemptId: Option[String]) {

  override def hashCode(): Int = {
    appId.hashCode() + attemptId.map(_.hashCode).getOrElse(0)
  }

  override def equals(obj: Any): Boolean = {
    val that = obj.asInstanceOf[CacheKey]
    that.appId == appId && that.attemptId == attemptId
  }

  override def toString: String = {
    appId + attemptId.map { id => s"/$id" }.getOrElse("")
  }
}

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

  override def metricRegistry: MetricRegistry = new MetricRegistry

  /**
   * Startup actions.
   * This includes registering metrics with [[metricRegistry]]
   */
  private def init(): Unit = {
    allMetrics.foreach(e =>
      metricRegistry.register(MetricRegistry.name(prefix, e._1), e._2))
  }

  override def toString: String = {
    val sb = new StringBuilder()
    counters.foreach { e =>
      sb.append(e._1).append(" = ").append(e._2.getCount).append('\n')
    }
    sb.toString()
  }
}

/**
 * API for cache events. That is: loading an APP UI; probing for it changing, and for
 * attaching/detaching the UI to and from the Web UI.
 */
private[history] trait ApplicationCacheOperations {

  /**
   * Get the application UI.
   * @param appId application ID
   * @param attemptId attempt ID
   * @return The Spark UI
   */
  def getAppUI(appId: String, attemptId: Option[String]): Option[SparkUI]

  /**
   * Attach a reconstructed UI.
   * @param appId application ID
   * @param attemptId attempt ID
   * @param ui UI
   * @param completed flag to indicate that the UI has completed
   */
  def attachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI, completed: Boolean): Unit

  /**
   *  Detach a Spark UI.
   *
   * @param ui Spark UI
   */
  def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit

  /**
   * Probe for an update to an (incompleted) application.
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @param updateTimeMillis time in milliseconds to use as the threshold for an update.
   * @return true if the application was updated since `updateTimeMillis`
   */
  def isUpdated(appId: String, attemptId: Option[String], updateTimeMillis: Long): Boolean
}
