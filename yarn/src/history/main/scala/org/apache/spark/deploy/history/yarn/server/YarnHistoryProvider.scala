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

package org.apache.spark.deploy.history.yarn.server

import java.io.{FileNotFoundException, IOException, InterruptedIOException}
import java.net.URI
import java.util.Date
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.zip.ZipOutputStream

import scala.collection.JavaConverters._

import com.codahale.metrics.{Counter, Gauge, Metric, MetricRegistry, Timer}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.service.{Service, ServiceOperations}
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.{ApplicationHistoryProvider, HistoryServer}
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.{ExtendedMetricsSource, YarnTimelineUtils}
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding
import org.apache.spark.deploy.history.yarn.server.TimelineQueryClient._
import org.apache.spark.deploy.history.yarn.server.YarnProviderUtils._
import org.apache.spark.scheduler.{ApplicationEventListener, ReplayListenerBus}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{Clock, SystemClock}

/**
 * A  History provider which reads in the history from the YARN Application Timeline Service.
 *
 * The service is a remote HTTP service, so failure modes are different from simple file IO.
 *
 * 1. Application listings are asynchronous, and made on a schedule, though
 * they can be forced (and the schedule disabled).
 * 2. The results are cached and can be retrieved with [[getApplications]].
 * 3. The most recent failure of any operation is stored,
 * The [[getLastFailure]] call will return the last exception
 * or `None`. It is shared across threads so is primarily there for
 * tests and basic diagnostics.
 * 4. Listing the details of a single application in [[getAppUI()]]
 * is synchronous and *not* cached.
 * 5. the [[maybeCheckEndpoint()]] call performs an endpoint check as the initial
 * binding operation of this instance. This call invokes [[TimelineQueryClient.endpointCheck()]]
 * for better diagnostics on binding failures -particularly configuration problems.
 * 6. Every REST call, synchronous or asynchronous, will invoke [[maybeCheckEndpoint()]] until
 * the endpoint check eventually succeeds.
 *
 * If the timeline is not enabled, the API calls used by the web UI
 * downgrade gracefully (returning empty entries), rather than fail.
 *
 * @param sparkConf configuration of the provider
 */
private[spark] class YarnHistoryProvider(sparkConf: SparkConf)
  extends ApplicationHistoryProvider with Logging {

  // import all the constants
  import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider._

  /**
   * The configuration here is a YarnConfiguration built off the spark configuration
   * supplied in the constructor; this operation ensures that `yarn-default.xml`
   * and `yarn-site.xml` are pulled in. Options in the spark conf will override
   * those in the -default and -site XML resources which are not marked as final.
   */
  private val yarnConf = {
    new YarnConfiguration(SparkHadoopUtil.get.newConfiguration(sparkConf))
  }

  /**
   * UI ACL option.
   */
  private val uiAclsEnabled = sparkConf.getBoolean("spark.history.ui.acls.enable", false)

  /**
   * Flag/option which triggers providing detailed diagnostics info on the property
   * map of the history provider.
   */
  private val detailedInfo = sparkConf.getBoolean(OPTION_DETAILED_INFO, false)

  /**
   * The minimum interval in milliseconds between each check for event log updates.
   */
  val manualRefreshInterval = sparkConf.getTimeAsSeconds(OPTION_MANUAL_REFRESH_INTERVAL,
    s"${DEFAULT_MANUAL_REFRESH_INTERVAL_SECONDS}s") * 1000

  /**
   * The interval for manual intervals.
   */
  val backgroundRefreshInterval = sparkConf.getTimeAsSeconds(OPTION_BACKGROUND_REFRESH_INTERVAL,
    s"${DEFAULT_BACKGROUND_REFRESH_INTERVAL_SECONDS}s") * 1000

  /**
   * Window limit in milliseconds.
   */
  private val windowLimitMs = sparkConf.getTimeAsSeconds(OPTION_WINDOW_LIMIT,
    DEFAULT_WINDOW_LIMIT) * 1000

  /**
   * Number of events to get.
   */
  private val eventFetchLimit = sparkConf.getLong(OPTION_EVENT_FETCH_LIMIT,
    DEFAULT_EVENT_FETCH_LIMIT)

  /**
   * Convert the limit to an option where 0 is mapped to None.
   */
  private val eventFetchOption: Option[Long] =
    if (eventFetchLimit > 0) Some(eventFetchLimit) else None

  /**
   * Clock. Subclasses may override for testing.
   */
  protected var clock: Clock = new SystemClock()

  /**
   * YARN client used to list running apps and so infer which of the incomplete apps have really
   * finished.
   */
  private var yarnClient: YarnClient = _

  /**
   * Are liveness checks enabled?
   * That is, is the option [[OPTION_YARN_LIVENESS_CHECKS]] set?
   */
  val _livenessChecksEnabled = sparkConf.getBoolean(OPTION_YARN_LIVENESS_CHECKS, true)

  /**
   * Start time in milliseconds.
   */
  val serviceStartTime = clock.getTimeMillis()

  /**
   * Timeline endpoint URI.
   */
  protected val timelineEndpoint = createTimelineEndpoint()

  /**
   * Create a [[TimelineQueryClient]] instance to talk to the timeline service running
   * at [[timelineEndpoint]].
   *
   */
  protected val timelineQueryClient = {
    createTimelineQueryClient()
  }

  /**
   * Override point: create the timeline endpoint.
   *
   * @return a URI to the timeline web service
   */
  protected def createTimelineEndpoint(): URI = {
    getTimelineEndpoint(yarnConf)
  }

  /**
   * Create the timeline query client.
   *
   * This is called during instance creation; tests may override this
   * @return a timeline query client for use for the duration
   *         of this instance
   */
  protected def createTimelineQueryClient(): TimelineQueryClient = {
    new TimelineQueryClient(timelineEndpoint, yarnConf, JerseyBinding.createClientConfig())
  }

  /**
   * The empty listing, with a timestamp to indicate that the listing
   * has never taken place.
   */
  private val EmptyListing = new ApplicationListingResults(0, Nil, None)

  /**
   * List of applications. Initial result is empty.
   */
  private var applications: ApplicationListingResults = EmptyListing

  /**
   * Last exception seen and when.
   */
  protected var lastFailureCause: Option[(Throwable, Date)] = None

  /**
   * Flag to indicate an endpoint diagnostics probe should take place.
   */
  protected val endpointCheckExecuted = new AtomicBoolean(false)

  /**
   * Flag set to indicate refresh is in progress.
   */
  private val _refreshInProgress = new AtomicBoolean(false)

  /**
   * How long did that last refresh take?
   */
  private val _lastRefreshDuration = new AtomicLong(0)

  /**
   * Enabled flag.
   */
  private val _enabled = timelineServiceEnabled(yarnConf)

  /**
   * Atomic boolean used to signal to the refresh thread that it
   * must exit its loop.
   */
  private val stopRefresh = new AtomicBoolean(false)

  /**
   * Refresher thread.
   */
  private[yarn] val refresher = new Refresher()


  /**
   * Time in milliseconds *before* the last application report to set the window
   * start on the next query. Keeping this earlier than the last report ensures that
   * changes to that application are picked up.
   */
  private val StartWindowOffsetMillis = 1000

  /**
   * Component Metrics. Accessible for the benefit of tests.
   */
  val metrics = new YarnHistoryProviderMetrics(this)

  /**
   * Initialize the provider.
   */
  init()

  /**
   * Check the configuration and log whether or not it is enabled;
   * if it is enabled then the refresh thread starts.
   *
   * Starting threads in a constructor is considered awful practice as
   * it can leak reference and cause chaos and confusion with subclasses.
   * But in the absence of some start() method for history providers, it's
   * all there is.
   */
  private def init(): Unit = {
    if (!enabled) {
      logError(TEXT_SERVICE_DISABLED)
    } else {
      logInfo(TEXT_SERVICE_ENABLED)
      logInfo(KEY_SERVICE_URL + ": " + timelineEndpoint)
      logDebug(sparkConf.toDebugString)
      // get the thread time
      logInfo(s"refresh interval $manualRefreshInterval milliseconds")
      validateInterval(OPTION_MANUAL_REFRESH_INTERVAL, manualRefreshInterval, 0)
      // check the background refresh interval if there is one
      if (backgroundRefreshInterval > 0) {
        validateInterval(OPTION_BACKGROUND_REFRESH_INTERVAL, backgroundRefreshInterval,
          MIN_BACKGROUND_REFRESH_INTERVAL)
      }
      initYarnClient()
      startRefreshThread()
    }
  }

  /**
   * Verify that a time interval is at or above the minimum allowed.
   *
   * @param prop property for use in exception text
   * @param interval interval to valide
   * @param min minimum allowed value
   */
  private def validateInterval(prop: String, interval: Long, min: Long): Unit = {
    if (interval < min) {
      throw new IllegalArgumentException(TEXT_INVALID_UPDATE_INTERVAL +
          prop + s": ${interval / 1000}; minimum allowed = ${min/1000}")
    }
  }

  /**
   * Stop the service. After this point operations will fail.
   */
  override def stop(): Unit = {
    logDebug(s"Stopping $this")
    // attempt to stop the refresh thread
    if (enabled) {
      if (!stopRefreshThread()) {
        closeQueryClient()
        ServiceOperations.stop(yarnClient)
      }
    }
  }

  /**
   * Close the query client.
   */
  private def closeQueryClient(): Unit = {
    logDebug("Stopping Timeline client")
    timelineQueryClient.close()
  }

  /**
   * Is the timeline service (and therefore this provider) enabled.
   * (override point for tests)?
   *
   * Important: this is called during construction, so test-time subclasses
   * will be invoked before their own construction has taken place.
   * Code appropriately.
   *
   * @return true if the provider/YARN configuration enables the timeline
   *         service.
   */
  def enabled: Boolean = {
    _enabled
  }

  /**
   * Are liveness checks enabled?
   * @return true if the liveness checks on the service endpoint take place.
   */
  def livenessChecksEnabled: Boolean = {
    _livenessChecksEnabled
  }

  /**
   * Is a refresh in progress?
   *
   * @return flag to indicate a refresh is in progress
   */
  def refreshInProgress: Boolean = {
    _refreshInProgress.get()
  }

  /**
   * The duration of last refresh.
   * @return a duration in millis; 0 if no refresh has taken place
   */
  def lastRefreshDuration: Long = {
    _lastRefreshDuration.get()
  }

  /**
   * Get the timeline query client. Used internally to ease testing
   * @return the client.
   */
  def getTimelineQueryClient: TimelineQueryClient = {
    timelineQueryClient
  }

  /**
   * Set the last exception.
   * @param ex exception seen
   */
  private def setLastFailure(ex: Throwable): Unit = {
    setLastFailure(ex, now())
  }

  /**
   * Set the last exception.
   *
   * @param ex exception seen
   * @param timestamp the timestamp of the failure
   */
  private def setLastFailure(ex: Throwable, timestamp: Long): Unit = {
    synchronized {
      lastFailureCause = Some((ex, new Date(timestamp)))
    }
  }

  /**
   * Reset the failure info.
   */
  private def resetLastFailure(): Unit = {
    synchronized {
      lastFailureCause = None
    }
  }

  /**
   * Get the last exception.
   *
   * @return the last exception or  null
   */
  def getLastFailure: Option[(Throwable, Date)] = {
    synchronized {
      lastFailureCause
    }
  }

  /**
   * Thread safe accessor to the application list.
   *
   * @return a list of applications
   */
  def getApplications: ApplicationListingResults = {
    synchronized {
      applications
    }
  }

  /**
   * Thread safe call to update the application results.
   *
   * @param newVal new value
   */
  private def setApplications(newVal: ApplicationListingResults): Unit = {
    synchronized {
      applications = newVal
    }
  }

  /**
   * Reachability check to call before any other operation is attempted.
   * This is atomic, using the `shouldCheckEndpoint` flag to check.
   * If the endpoint check failes then the
   * `endpointCheckExecuted`  flag is reset to false and an exception thrown.
   *
   * @return true if the check took place
   */
  protected def maybeCheckEndpoint(): Boolean = {
    if (!endpointCheckExecuted.getAndSet(true)) {
      val client = getTimelineQueryClient
      try {
        client.endpointCheck()
        true
      } catch {
        case e: Exception =>
          // failure
          logWarning(s"Endpoint check of $client failed", e)
          setLastFailure(e)
          // reset probe so another caller may attempt it.
          endpointCheckExecuted.set(false)
          // propagate the failure
          throw e
      }
    } else {
      false
    }
  }

  /**
   * Query for the endpoint check being successful.
   *
   * Note: also true if the check is in progress.
   *
   * @return true if the check has succeeded, or it is actually ongoing (it may fail)
   */
  def endpointCheckSuccess(): Boolean = {
    endpointCheckExecuted.get()
  }

  /**
   * Start the refresh thread with the given interval.
   *
   * When this thread exits, it will close the `timelineQueryClient`
   * instance
   */
  def startRefreshThread(): Unit = {
    logInfo(s"Starting timeline refresh thread")
    val thread = new Thread(refresher, s"YarnHistoryProvider Refresher")
    thread.setDaemon(true)
    refresher.start(thread)
  }

  /**
   * Stop the refresh thread if there is one.
   *
   * This does not guarantee an immediate halt to the thread.
   *
   * @return true if there was a refresh thread to stop
   */
  private def stopRefreshThread(): Boolean = {
    refresher.stopRefresher()
  }

  /**
   * Probe for the refresh thread running.
   *
   * @return true if the refresh thread has been created and is still alive
   */
  def isRefreshThreadRunning: Boolean = {
    refresher.isRunning
  }

  /**
   * Get the number of times an attempt was made to refresh the listing.
   * This is incremented on every operation, irrespective of the outcome.
   *
   * @return the current counter of refresh attempts.
   */
  def refreshCount: Long = {
    metrics.refreshCount.getCount
  }

  /**
   * Get number of failed refreshes.
   *
   * Invariant: always equal to or less than [[refreshCount]]
   *
   * @return the number of times refreshes failed
   */
  def refreshFailedCount: Long = {
    metrics.refreshFailedCount.getCount()
  }

  /**
   * List applications.
   *
   * If the timeline is not enabled, returns `emptyListing`
   * @return  the result of the last successful listing operation,
   *          or a listing with no history events if there has been a failure
   */
   def listApplications(limit: Option[Long] = None,
      windowStart: Option[Long] = None,
      windowEnd: Option[Long] = None): ApplicationListingResults = {
    if (!enabled) {
      // Timeline is disabled: return the empty listing
      return EmptyListing
    }
    try {
      maybeCheckEndpoint()
      val client = getTimelineQueryClient
      logInfo(s"getListing from: $client")
      // get the timestamp after any endpoint check
      val timestamp = now()
      // list the entities, excluding the events -this is critical for performance reasons
      val timelineEntities = client.listEntities(SPARK_EVENT_ENTITY_TYPE,
          windowStart = windowStart,
          windowEnd = windowEnd,
          limit = limit,
          fields = Seq(PRIMARY_FILTERS, OTHER_INFO)
        )

      // build one history info entry for each entity in the least (implicitly, one
      // attempt per history info entry
      val umergedHistory = timelineEntities.flatMap { en =>
        try {
          val historyInfo = toApplicationHistoryInfo(en)
          logDebug(s"${describeApplicationHistoryInfo(historyInfo)}")
          Some(historyInfo)
        } catch {
          case e: Exception =>
            logWarning(s"Failed to parse entity. ${YarnTimelineUtils.describeEntity(en)}", e)
            // skip this result
            None
        }
      }
      // merge the results so that multiple attempts are combined into
      // single history entries
      val histories = combineResults(Nil, umergedHistory)
      val incomplete = countIncompleteApplications(histories)
      logInfo(s"Found ${histories.size} application(s): " +
          s"${histories.size - incomplete} complete and $incomplete incomplete")
      new ApplicationListingResults(timestamp, histories, None)
    } catch {
      case e: IOException =>
        logWarning(s"Failed to list entities from $timelineEndpoint", e)
        new ApplicationListingResults(now(), Nil, Some(e))
    }
  }

  /**
   * List applications.
   *
   * Also updates the cached values of the listing/last failure, depending
   * upon the outcome.
   *
   * If the timeline is not enabled, returns an empty list.
   *
   * @param startup a flag to indicate this is the startup retrieval with different window policy
   * @return List of all known applications.
   */
  def listAndCacheApplications(startup: Boolean): ApplicationListingResults = {
    metrics.refreshCount.inc()
    _refreshInProgress.set(true)
    val history = getApplications.applications

    val refreshStartTime = now()
    metrics.time(metrics.refreshDuration) {
      try {
        // work out the start of the new window
        val nextWindowStart = if (startup) {
          None
        } else {
          findStartOfWindow(history) map { app =>
            // inclusive on the one retrieved last time.
            // Why? we need to include the oldest incomplete entry in our range
            val inclusiveWindow = startTime(app) - StartWindowOffsetMillis
            // sanity check on window size
            val earliestWindow = if (windowLimitMs > 0) refreshStartTime - windowLimitMs else 0
            Math.max(earliestWindow, inclusiveWindow)
          }
        }
        val results = listApplications(windowStart = nextWindowStart)
        synchronized {
          if (results.succeeded) {
            // on a success, the existing application list is merged
            // creating a new aggregate application list
            logDebug(s"Listed application count: ${results.size}")
            val merged = combineResults(history, results.applications)
            logDebug(s"Existing count: ${history.size}; merged = ${merged.size} ")
            val updated = if (livenessChecksEnabled) {
              updateAppsFromYARN(merged)
            } else {
              merged
            }
            val sorted = sortApplicationsByStartTime(updated)
            // and a final result
            setApplications(new ApplicationListingResults(results.timestamp, sorted, None))
            resetLastFailure()
          } else {
            // on a failure, the failure cause is updated
            setLastFailure(results.failureCause.get, results.timestamp)
            // and the failure counter
            metrics.refreshFailedCount.inc()
          }
        }
        results
      } finally {
        _refreshInProgress.set(false)
        _lastRefreshDuration.set(now() - refreshStartTime)
      }
    }
  }

  /**
   * List applications.
   *
   * If the timeline is not enabled, returns an empty list
   * @return List of all known applications.
   */
  override def getListing(): Seq[TimelineApplicationHistoryInfo] = {
    // get the current list
    val listing = getApplications.applications
    // and queue another refresh
    triggerRefresh()
    listing
  }

  /**
   * Trigger a refresh.
   */
  private[yarn] def triggerRefresh(): Unit = {
    refresher.refresh(now())
  }

  /**
   * Return the current time.
   * @return the time in milliseconds.
   */
  private[yarn] def now(): Long = {
    clock.getTimeMillis()
  }

  /**
   * Get the last refresh attempt (Which may or may not have been successful).
   *
   * @return the last refresh time
   */
  def lastRefreshAttemptTime: Long = {
    refresher.lastRefreshAttemptTime
  }

  /**
   * Look up the timeline entity.
   *
   * @param entityId application ID
   * @return the entity associated with the given application
   * @throws FileNotFoundException if no entry was found
   */
  def getTimelineEntity(entityId: String): TimelineEntity = {
    logDebug(s"GetTimelineEntity $entityId")
    metrics.time(metrics.attemptFetchDuration){
      maybeCheckEndpoint()
      getTimelineQueryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, entityId)
    }
  }

  /**
   * Returns the Spark UI for a specific application.
   *
   * If the timeline is not enabled, returns `None`
   *
   * @param appId The application ID.
   * @param attemptId The application attempt ID (or `None` if there is no attempt ID).
   * @return The application's UI, or `None` if application is not found.
   */
  override def getAppUI(appId: String, attemptId: Option[String]): Option[SparkUI] = {

    logInfo(s"Request UI with appId $appId attempt $attemptId")
    if (!enabled) {
      // Timeline is disabled: return nothing
      return None
    }
    maybeCheckEndpoint()
    metrics.attemptLoadCount.inc()
    if (attemptId.isEmpty) {
      // empty attempts are rejected
      metrics.attemptLoadFailureCount.inc()
      return None
    }

    // the URL generation/linking code in the History Server appears fairly brittle,
    // so provide as diagnostics information in the logs to help understand what
    // is happending
    val (foundApp, attempt, attempts) = getApplications.lookupAttempt(appId, attemptId)
    if (foundApp.isEmpty) {
      log.error(s"Application $appId not found")
      metrics.attemptLoadFailureCount.inc()
      return None
    }
    val attemptInfo = attempt.getOrElse {
      log.error(s"Attempt $attemptId not found under application $appId")
      log.error(s"${attempts.length} attempts: " + attempts.mkString("[", ", ", "]"))
      metrics.attemptLoadFailureCount.inc()
      return None
    }

    val entityId = attemptInfo.entityId
    val appInfo = foundApp.get

    metrics.time(metrics.attemptLoadDuration) {
      try {
        val attemptEntity = getTimelineEntity(entityId)
        if (log.isDebugEnabled) {
          logDebug(describeEntity(attemptEntity))
        }
        val bus = new ReplayListenerBus()
        val appListener = new ApplicationEventListener()
        bus.addListener(appListener)
        val conf = this.sparkConf.clone()
        val attemptURI = HistoryServer.getAttemptURI(appId, attemptId)
        val name = appInfo.name
        val ui = SparkUI.createHistoryUI(conf,
          bus,
          new SecurityManager(conf),
          name,
          attemptURI,
          attemptInfo.startTime)
        ui.setAppId(appId)
        logInfo(s"Building Application UI $name attempt $attemptId under ${ui.basePath}")

        // replay all the events
        val events = attemptEntity.getEvents.asScala
        logInfo(s"App $appId history contains ${events.length} events")

        metrics.time(metrics.attemptReplayDuration) {
          events.reverse.foreach { event =>
            val sparkEvent = toSparkEvent(event)
            bus.postToAll(sparkEvent)
          }
        }

        ui.getSecurityManager.setAcls(uiAclsEnabled)
        // make sure to set admin acls before view acls so they are properly picked up
        ui.getSecurityManager.setAdminAcls(appListener.adminAcls.getOrElse(""))
        ui.getSecurityManager.setViewAcls(appListener.sparkUser.getOrElse("<Not Started>"),
          appListener.viewAcls.getOrElse(""))
        Some(ui)
      } catch {

        case e: FileNotFoundException =>
          logInfo(s"Unknown attempt $entityId", e)
          setLastFailure(e)
          metrics.attemptLoadFailureCount.inc()
          None

        case e: Exception =>
          logWarning(s"Failed to get attempt information for $appId attempt $entityId", e)
          setLastFailure(e)
          metrics.attemptLoadFailureCount.inc()
          throw e
      }
    }
  }

  /**
   * Get configuration information for the Web UI.
   *
   * @return A map with the configuration data. Data is shown in the order returned by the map.
   */
  override def getConfig(): Map[String, String] = {
    val timelineURI = getEndpointURI
    logDebug(s"getConfig $timelineURI")
    synchronized {
      val applications = getApplications
      val failure = getLastFailure
      var state = Map(
        KEY_PROVIDER_NAME -> PROVIDER_DESCRIPTION,
        KEY_START_TIME -> humanDateCurrentTZ(serviceStartTime, "(not started)"),
        KEY_SERVICE_URL -> s"$timelineURI",
        KEY_ENABLED -> (if (enabled) TEXT_SERVICE_ENABLED else TEXT_SERVICE_DISABLED),
        KEY_LAST_UPDATED -> applications.updated,
        KEY_CURRENT_TIME -> humanDateCurrentTZ(now(), "unknown")
      )
      // in a secure cluster, list the user name
      if (UserGroupInformation.isSecurityEnabled) {
        state += (KEY_USERNAME -> UserGroupInformation.getCurrentUser.getUserName)
      }

      // on a failure, add failure specifics to the operations
      failure foreach {
        case (ex , date) =>
          state = state ++
            Map(
              KEY_LAST_FAILURE_TIME -> humanDateCurrentTZ(date.getTime, TEXT_NEVER_UPDATED),
              KEY_LAST_FAILURE -> ex.toString)
      }
      // add detailed information if enabled
      if (detailedInfo) {
        state = state ++ Map(
          KEY_X_TOKEN_RENEWAL ->
              humanDateCurrentTZ(timelineQueryClient.lastTokenRenewal, TEXT_NEVER_UPDATED),
          KEY_X_TOKEN_RENEWAL_COUNT -> timelineQueryClient.tokenRenewalCount.toString,
          KEY_X_INTERNAL_STATE -> s"$this",
          KEY_X_MIN_REFRESH_INTERVAL -> s"$manualRefreshInterval mS",
          KEY_X_BACKGROUND_REFRESH_INTERVAL -> s"$backgroundRefreshInterval mS",
          KEY_X_EVENT_FETCH_LIMIT -> eventFetchLimit.toString,
          KEY_X_ENTITY_LISTING ->
              (timelineQueryClient.entityResource(SPARK_EVENT_ENTITY_TYPE).getURI.toString +
               s"?fields=$PRIMARY_FILTERS,$OTHER_INFO"),
          KEY_X_REFRESH_IN_PROGRESS -> refreshInProgress.toString,
          KEY_X_LAST_REFRESH_DURATION -> s"$lastRefreshDuration ms"
        )
      }
      state
    }
  }

  /**
   * Get the URI of the root of the timeline server.
   *
   * @return URI of the timeline endpoint
   */
  def getEndpointURI: URI = {
    timelineEndpoint.resolve("/")
  }

  /**
   * Stub implementation of the "write event logs" operation, which isn't supported
   * by the timeline service
   * @throws SparkException always
   */
  override def writeEventLogs(appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {
    throw new SparkException("Unsupported Feature")
  }

  override def toString(): String = {
    s"YarnHistoryProvider bound to history server at $timelineEndpoint," +
    s" enabled = $enabled;" +
    s" refresh count = $refreshCount; failed count = $refreshFailedCount;" +
    s" last update ${applications.updated};" +
    s" history size ${applications.size};" +
    s" $refresher"
  }

  /**
   * Comparison function that defines the sort order for the application listing.
   *
   * @return Whether `i1` should precede `i2`.
   */
  private def compareAppInfo(
      i1: TimelineApplicationHistoryInfo,
      i2: TimelineApplicationHistoryInfo): Boolean = {
    val a1 = i1.attempts.head
    val a2 = i2.attempts.head
    if (a1.endTime != a2.endTime) a1.endTime >= a2.endTime else a1.startTime >= a2.startTime
  }

  /**
   * Initialize the YARN client
   */
  protected def initYarnClient(): Unit = {
    require(yarnClient == null, "YARN client already initialized")
    if (livenessChecksEnabled) {
      logDebug("Creating YARN Client")
      yarnClient = YarnClient.createYarnClient()
      yarnClient.init(yarnConf)
      yarnClient.start()
    } else {
      logInfo("YARN liveness checks disabled.")
    }
  }

  /**
   * List spark applications known by the the YARN RM.
   *
   * This includes known failed/halted/killed applications as well as those in running/scheduled
   * states. Sometimes on AM restart the application appears to drop out of the list briefly.
   *
   * @return the list of running spark applications, which can then be filtered against
   */
  private[yarn] def listYarnSparkApplications(): Map[String, ApplicationReport] = {
    if (isYarnClientRunning) {
      val reports = yarnClient.getApplications(Set(SPARK_YARN_APPLICATION_TYPE).asJava).asScala
      reportsToMap(reports)
    } else {
      Map()
    }
  }

  /**
   * Is the [[yarnClient]] running?
   *
   * @return true if it is non-null and running
   */
  private[yarn] def isYarnClientRunning: Boolean = {
    yarnClient != null && yarnClient.isInState(Service.STATE.STARTED)
  }

  /**
   * Filter out all incomplete applications that are not in the list of running YARN applications.
   *
   * This filters out apps which have failed without any notification event.
   * @param apps list of applications
   * @return list of apps which are marked as incomplete but no longer running
   */
  private[yarn] def updateAppsFromYARN(apps: Seq[TimelineApplicationHistoryInfo])
    : Seq[TimelineApplicationHistoryInfo] = {

    if (isYarnClientRunning) {
      completeAppsFromYARN(apps, listYarnSparkApplications(), now(), DEFAULT_LIVENESS_WINDOW_I)
    } else {
      apps
    }
  }

  /**
   * This is the implementation of the triggered refresh logic.
   * It awaits events, including one generated on a schedule
   */
  private[yarn] class Refresher extends Runnable {

    private[yarn] sealed trait RefreshActions

    /** start the refresh */
    private[yarn] case class Start() extends RefreshActions

    /** refresh requested at the given time */
    private[yarn] case class RefreshRequest(time: Long) extends RefreshActions

    /** stop */
    private[yarn] case class StopExecution() extends RefreshActions

    /**
     * Poll operation timed out.
     */
    private[yarn] case class PollTimeout() extends RefreshActions

    private val queue = new LinkedBlockingQueue[RefreshActions]()
    private val running = new AtomicBoolean(false)
    private var self: Thread = _
    private val _lastRefreshAttemptTime = new AtomicLong(0)

    /**
     * Bond to the thread then start it.
     * @param t thread
     */
    def start(t: Thread): Unit = {
      synchronized {
        self = t
        running.set(true)
        queue.add(Start())
        t.start()
      }
    }

    /**
     * Request a refresh. If the request queue is empty, a refresh request
     * is queued.
     * @param time time request was made
     */
    def refresh(time: Long): Unit = {
      if (queue.isEmpty) {
        queue.add(RefreshRequest(time))
      }
    }

    /**
     * Stop operation.
     * @return true if the stop was scheduled
     */
    def stopRefresher(): Boolean = {
      synchronized {
        if (isRunning) {
          // yes, more than one stop may get issued. but it will
          // replace the previous one.
          queue.clear()
          queue.add(StopExecution())
          self.interrupt()
          true
        } else {
          false
        }
      }
    }

    /**
     * Thread routine.
     */
    override def run(): Unit = {
      try {
        var stopped = false
          while (!stopped) {
            try {
              // get next event
              val event = if (backgroundRefreshInterval > 0) {
                // background interval set => poll for an event
                poll(backgroundRefreshInterval)
              } else {
                // no interval: block for manually triggered refresh
                take()
              }
              event match {
                case StopExecution() =>
                  // stop: exit the loop
                  stopped = true
                case Start() =>
                  // initial read; may be bigger
                  logDebug("Startup refresh")
                  doRefresh(true, false)
                case RefreshRequest(time) =>
                  // requested refresh operation
                  logDebug("Triggered refresh")
                  doRefresh(false, false)
                case PollTimeout() =>
                  // poll timeout. Do a refresh
                  logDebug("Background Refresh")
                  doRefresh(false, true)
              }
              // it is only after processing the
              // message that the message process counter
              // is incremented
              metrics.backgroundOperationsProcessed.inc()

            } catch {
              case ex: InterruptedException =>
                // raised during stop process to interrupt IO
                logDebug("Interrupted ", ex)
              case ex: InterruptedIOException =>
                // a common wrapper for interrupted exceptions in the Hadoop stack.
                logDebug("Interrupted ", ex)
              case ex: Exception =>
                // something else. Log at warn
                logWarning("Exception in refresh thread", ex)
            }
          }
      } finally {
        closeQueryClient()
        ServiceOperations.stop(yarnClient)
        running.set(false)
        logInfo("Background Refresh Thread exited")
      }
    }

    /**
     * Do the refresh.
     *
     * This contains the decision making as when to refresh, which can happen if
     * any of the following conditions were met:
     *
     * 1. the refresh interval == 0 (always)
     * 2. the last refresh was outside the window.
     * 3. this is a background refresh
     *
     * There isn't a special check for "never updated", as this
     * would only be inside the window in test cases with a small
     * simulated clock.
     *
     * @param startup a flag to indicate this is the startup retrieval with different window policy
     */
    private def doRefresh(startup: Boolean, background: Boolean): Unit = {
      val t = now()
      if (manualRefreshInterval == 0
          || background
          || ((t - _lastRefreshAttemptTime.get) >= manualRefreshInterval )) {
        logDebug(s"refresh triggered at $t")
        listAndCacheApplications(startup)
        // set refresh time to after the operation, so that even if the operation
        // is slow, refresh intervals don't come too often.
        _lastRefreshAttemptTime.set(now())
      }
    }

    /**
     * Get the next action -blocking until it is ready.
     *
     * @return the next action
     */
    private def take(): Refresher.this.RefreshActions = {
      queue.take
    }

    /**
     * Poll for the next action; return the head of the queue as soon as it is available,
     * or, after the timeout, a [[PollTimeout]] message.
     *
     * @param millis poll time
     * @return an action
     */
    private def poll(millis: Long): Refresher.this.RefreshActions = {
      val result = queue.poll(millis, TimeUnit.MILLISECONDS)
      if (result == null) PollTimeout() else result
    }

    /**
     * Flag to indicate the refresher thread is running.
     * @return true if the refresher is running
     */
    def isRunning: Boolean = {
      running.get
    }

    /**
     * Get the last refresh time.
     * @return the last refresh time
     */
    def lastRefreshAttemptTime: Long = {
      _lastRefreshAttemptTime.get
    }

    /**
     * Get count of messages processed.
     *
     * This will be at least equal to the number of refreshes executed
     * @return processed count
     */
    def messagesProcessed: Long = {
      metrics.backgroundOperationsProcessed.getCount
    }

    /**
     * String value is for diagnostics in tests.
     *
     * @return a description of the current state
     */
    override def toString: String = {
      s"Refresher running = $isRunning queue size = ${queue.size};" +
        s" min refresh interval = $manualRefreshInterval mS;" +
        s" background refresh interval = $backgroundRefreshInterval mS;" +
        s" processed = $messagesProcessed;" +
        s" last refresh attempt = " + timeShort(lastRefreshAttemptTime, "never") + ";\n" +
        metrics.toString
    }
  }

}

/**
 * All the metrics for the YARN history provider
 * @param owner owning class
 */
private[history] class YarnHistoryProviderMetrics(owner: YarnHistoryProvider)
    extends ExtendedMetricsSource {
  override val sourceName = "yarn.history.provider"

  override val metricRegistry = new MetricRegistry()

  /** How many applications? */
  val applicationGauge = new Gauge[Int] {
    override def getValue: Int = { owner.getApplications.size }
  }

  /** How many application attempts? */
  val applicationAttemptGauge = new Gauge[Int] {
    override def getValue: Int = {
      owner.getApplications.applications.foldLeft(0) {
        (acc, info) => acc + info.attempts.length}
    }
  }

  /** Flag to indicate whether or not a refresh is in progress. */
  val refreshInProgress = new Gauge[Int] {
    override def getValue: Int = { if (owner.refreshInProgress) 1 else 0 }
  }

  /** Timer of background refresh operations. */
  val refreshDuration = new Timer()

  /** Counter of number of application attempts that have been loaded. */
  val attemptLoadCount = new Counter()

  /** Counter of how many times applicaton attempts failed to load. */
  val attemptLoadFailureCount = new Counter()

  /** Timer of how long it it taking to load app attempt. */
  val attemptLoadDuration = new Timer()

  /** Timer of how long it has taken to fetch app attempts from ATS. */
  val attemptFetchDuration = new Timer()

  /** How long has it taken to replay fetched app attempts? */
  val attemptReplayDuration = new Timer()

  /** Counter of refresh operations. */
  val refreshCount = new Counter()

  /** Counter of failed refresh operations. */
  val refreshFailedCount = new Counter()

  /** Number of operations processed asynchronously. */
  val backgroundOperationsProcessed = new Counter()

  val metricsMap: Map[String, Metric] = Map(
    "applications" -> applicationGauge,
    "application.attempts" -> applicationAttemptGauge,
    "app.attempt.load.count" -> attemptLoadCount,
    "app.attempt.load.duration" -> attemptLoadDuration,
    "app.attempt.load.failure.count" -> attemptLoadFailureCount,
    "app.attempt.load.fetch.duration" -> attemptFetchDuration,
    "app.attempt.load.replay.duration" -> attemptReplayDuration,
    "background.operations.processed" -> backgroundOperationsProcessed,
    "refresh.count" -> refreshCount,
    "refresh.duration" -> refreshDuration,
    "refresh.failed.count" -> refreshFailedCount,
    "refresh.in.progress" -> refreshInProgress
  )

  init()
}

/**
 * Constants to go with the history provider.
 *
 * 1. Any with the prefix `KEY_` are for configuration (key, value) pairs, so can be
 * searched for after scraping the History server web page.
 *
 * 2. Any with the prefix `OPTION_` are options from the configuration.
 *
 * 3. Any with the prefix `DEFAULT_` are the default value of options
 *
 * 4. Any with the prefix `TEXT_` are text messages which may appear in web pages
 * and other messages (and so can be scanned for in tests)
 */
private[spark] object YarnHistoryProvider {

  /** Name of the class to use in configuration strings. */
  val YARN_HISTORY_PROVIDER_CLASS = classOf[YarnHistoryProvider].getName()

  /** Message when the timeline service is enabled. */
  val TEXT_SERVICE_ENABLED = "Timeline service is enabled"

  /** Message when the timeline service is disabled. */
  val TEXT_SERVICE_DISABLED =
    "Timeline service is disabled: application history cannot be retrieved"

  /** Message when a "last updated" field has never been updated. */
  val TEXT_NEVER_UPDATED = "Never"

  /** Message when reporting an invalid update interval. */
  val TEXT_INVALID_UPDATE_INTERVAL = s"Invalid update interval defined in "

  /** What is the app type to ask for when listing apps on YARN? */
  val SPARK_YARN_APPLICATION_TYPE = "SPARK"

  /**
   * Option for the interval for listing timeline refreshes. Bigger: less chatty.
   * Smaller: history more responsive.
   */
  val OPTION_MANUAL_REFRESH_INTERVAL = "spark.history.yarn.manual.refresh.interval"
  val DEFAULT_MANUAL_REFRESH_INTERVAL_SECONDS = 30

  /**
   * Interval for background refreshes.
   */
  val OPTION_BACKGROUND_REFRESH_INTERVAL = "spark.history.yarn.background.refresh.interval"
  val DEFAULT_BACKGROUND_REFRESH_INTERVAL_SECONDS = 60

  /**
   * Minimum allowed refresh interval in milliseconds.
   */
  val MIN_BACKGROUND_REFRESH_INTERVAL = 10000

  /**
   * Option for the number of events to retrieve.
   */
  val OPTION_EVENT_FETCH_LIMIT = "spark.history.yarn.event-fetch-limit"
  val DEFAULT_EVENT_FETCH_LIMIT = 1000

  /**
   * Expiry age window. This is only going to be turned from a constant
   * to an option if the default is found to be inadequate. It's needed
   * primarily because killing a YARN app can sometimes cause it to get
   * its state mixed up on a listing operation.
   */
  val OPTION_EXPIRY_AGE = "spark.history.yarn.liveness.window"
  val DEFAULT_LIVENESS_WINDOW_I = 60 * 60
  val DEFAULT_LIVENESS_WINDOW = s"${DEFAULT_LIVENESS_WINDOW_I}s"

  /**
   * Maximum timeline of the window when getting updates.
   * If set to zero, there's no limit
   */
  val OPTION_WINDOW_LIMIT = "spark.history.yarn.window.limit"
  val DEFAULT_WINDOW_LIMIT = "24h"

  /**
   * Option to enabled detailed/diagnostics view.
   */
  val OPTION_DETAILED_INFO = "spark.history.yarn.diagnostics"

  /**
   * Option to enable YARN probes for running applications.
   */
  val OPTION_YARN_LIVENESS_CHECKS = "spark.history.yarn.probe.running.applications"

  /** Current time. */
  val KEY_CURRENT_TIME = "Current Time"

  /** Entry to use in the 'enabled' status line. */
  val KEY_ENABLED = "Timeline Service"

  /** Key for the last operation failure entry. */
  val KEY_LAST_FAILURE = "Last Operation Failure"

  /** Key for reporting the time of the last operation failure. */
  val KEY_LAST_FAILURE_TIME = "Last Operation Failed"

  /** Key for the last updated entry. */
  val KEY_LAST_UPDATED = "Last Updated"

  /** Key for reporting update interval. */
  val KEY_LISTING_REFRESH_INTERVAL = "Update Interval"

  /** Key of the [[KEY_PROVIDER_NAME]] entry. */
  val PROVIDER_DESCRIPTION = "Apache Hadoop YARN Timeline Service"

  /** Key used to identify the history provider */
  val KEY_PROVIDER_NAME = "History Provider"

  /** Key used when listing the URL of the ATS instance. */
  val KEY_SERVICE_URL = "Timeline Service Location"

  /** Key for service start time entry. */
  val KEY_START_TIME = "Service Started"

  /** Key for username; shown in secure clusters. */
  val KEY_USERNAME = "User"

  /** key on background refresh interval (millis). */
  val KEY_X_BACKGROUND_REFRESH_INTERVAL = "x-Background refresh interval"

  /** Detailed-view URL to timeline entity listing path. */
  val KEY_X_ENTITY_LISTING = "x-Entity Listing URL"

  /** Detailed view of entity fetch limit . */
  val KEY_X_EVENT_FETCH_LIMIT = "x-" + OPTION_EVENT_FETCH_LIMIT

  /** Detailed view of internal state of history provider; the `toString()` value. */
  val KEY_X_INTERNAL_STATE = "x-Internal State"

  /** Detailed view of the duration of the last refresh operation. */
  val KEY_X_LAST_REFRESH_DURATION = "x-Last Refresh Duration"

  /** Detailed view of the minimum interval between manual refreshes. */
  val KEY_X_MIN_REFRESH_INTERVAL = "x-manual refresh interval"

  /** Detailed view: is a refresh in progress? */
  val KEY_X_REFRESH_IN_PROGRESS = "x-Refresh in Progress"

  /** Detailed view: has a security token ever been renewed? */
  val KEY_X_TOKEN_RENEWAL = "x-Token Renewed"

  /** Detailed view: how many timeline server tokens have been renewed? */
  val KEY_X_TOKEN_RENEWAL_COUNT = "x-Token Renewal Count"

}
