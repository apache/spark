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

package org.apache.spark.deploy.history.yarn

import java.io.InterruptedIOException
import java.net.{ConnectException, URI}
import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.codahale.metrics.{Metric, Counter, MetricRegistry, Timer}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}
import org.apache.hadoop.yarn.api.records.timeline.{TimelineDomain, TimelineEntity, TimelineEvent}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockUpdated, SparkListenerEvent, SparkListenerExecutorMetricsUpdate}
import org.apache.spark.scheduler.cluster.{SchedulerExtensionService, SchedulerExtensionServiceBinding}
import org.apache.spark.util.{SystemClock, Utils}

/**
 * A Yarn Extension Service to post lifecycle events to a registered YARN Timeline Server.
 *
 * Posting algorithm
 *
 * 1. The service subscribes to all events coming from the Spark Context.
 * 1. These events are serialized into JSON objects for publishing to the timeline service through
 * HTTP(S) posts.
 * 1. Events are buffered into `pendingEvents` until a batch is aggregated into a
 * [[TimelineEntity]] for posting.
 * 1. That aggregation happens when a lifecycle event (application start/stop) takes place,
 * or the number of pending events in a running application exceeds the limit set in
 * `spark.hadoop.yarn.timeline.batch.size`.
 * 1. Posting operations take place in a separate thread from the spark event listener.
 * 1. If an attempt to post to the timeline server fails, the service sleeps and then
 * it is re-attempted after the retry period defined by
 * `spark.hadoop.yarn.timeline.post.retry.interval`.
 * 1. If the number of events buffered in the history service exceed the limit set in
 * `spark.hadoop.yarn.timeline.post.limit`, then further events other than application start/stop
 * are dropped.
 * 1. When the service is stopped, it will make a best-effort attempt to post all queued events.
 * the call of [[stop()]] can block up to the duration of
 * `spark.hadoop.yarn.timeline.shutdown.waittime` for this to take place.
 * 1. No events are posted until the service receives a [[SparkListenerApplicationStart]] event.
 *
 * If the spark context has a metrics registry, then the internal counters of queued entities,
 * post failures and successes, and the performance of the posting operation are all registered
 * as metrics.
 *
 * The shutdown logic is somewhat convoluted, as the posting thread may be blocked on HTTP IO
 * when the shutdown process begins. In this situation, the thread continues to be blocked, and
 * will be interrupted once the wait time has expired. All time consumed during the ongoing
 * operation will be counted as part of the shutdown time period.
 */
private[spark] class YarnHistoryService extends SchedulerExtensionService with Logging {

  import org.apache.spark.deploy.history.yarn.YarnHistoryService._

  /** Simple state model implemented in an atomic integer. */
  private val _serviceState = new AtomicInteger(CreatedState)

  /** Get the current state. */
  def serviceState: Int = {
    _serviceState.get()
  }

  /**
   * Atomic operatin to enter a new state, returning the old one.
   * There are no checks on state model.
   * @param state new state
   * @return previous state
   */
  private def enterState(state: Int): Int = {
    logDebug(s"Entering state $state from $serviceState")
    _serviceState.getAndSet(state)
  }

  /** Spark context; valid once started. */
  private var sparkContext: SparkContext = _

  /** YARN configuration from the spark context. */
  private var config: YarnConfiguration = _

  /** Application ID. */
  private[yarn] var applicationId: ApplicationId = _

  /** Attempt ID -this will be null if the service is started in yarn-client mode. */
  private var attemptId: Option[ApplicationAttemptId] = None

  /** YARN timeline client. */
  private var _timelineClient: Option[TimelineClient] = None

  /** Registered event listener. */
  private var listener: Option[YarnEventListener] = None

  /** Application name  from the spark start event. */
  private var applicationName: String = _

  /** Application ID received from a [[SparkListenerApplicationStart]]. */
  private var sparkApplicationId: Option[String] = None

  /** Optional Attempt ID string from [[SparkListenerApplicationStart]]. */
  private var sparkApplicationAttemptId: Option[String] = None

  /** User name as derived from `SPARK_USER` env var or [[Utils]]. */
  private var userName = Utils.getCurrentUserName

  /** Clock for recording time */
  private val clock = new SystemClock

  /** Start time of the application, as received in the start event. */
  private var startTime: Long = _

  /** Start time of the application, as received in the end event. */
  private var endTime: Long = _

  /** Number of events to batch up before posting. */
  private[yarn] var batchSize = DEFAULT_BATCH_SIZE

  /** Queue of entities to asynchronously post, plus the number of events in each entry. */
  private val postingQueue = new LinkedBlockingDeque[PostQueueAction]()

  /** Number of events in the post queue. */
  private val postQueueEventSize = new AtomicLong

  /** Limit on the total number of events permitted. */
  private var postQueueLimit = DEFAULT_POST_EVENT_LIMIT

  /** List of events which will be pulled into a timeline entity when created. */
  private var pendingEvents = new mutable.LinkedList[TimelineEvent]()

  /** The received application started event; `None` if no event has been received. */
  private var applicationStartEvent: Option[SparkListenerApplicationStart] = None

  /** The received application end event; `None` if no event has been received. */
  private var applicationEndEvent: Option[SparkListenerApplicationEnd] = None

  /** Has a start event been processed? */
  private val appStartEventProcessed = new AtomicBoolean(false)

  /** Has the application event event been processed? */
  private val appEndEventProcessed = new AtomicBoolean(false)

  /** Event handler thread. */
  private var entityPostThread: Option[Thread] = None

  /** Flag to indicate the queue is stopped; events aren't being processed. */
  private val queueStopped = new AtomicBoolean(true)

  /** Boolean to track when the post thread is active; Set and reset in the thread itself. */
  private val postThreadActive = new AtomicBoolean(false)

  /** How long to wait in millseconds for shutdown before giving up? */
  private var shutdownWaitTime = 0L

  /** What is the initial and incrementing interval for POST retries? */
  private var retryInterval = 0L

  /** Domain ID for entities: may be null. */
  private var domainId: Option[String] = None

  /** URI to timeline web application -valid after [[start()]]. */
  private[yarn] var timelineWebappAddress: URI = _

  /** Metric fields. Used in tests as well as metrics infrastructure. */
  val metrics = new HistoryMetrics

  /**
   * Create a timeline client and start it. This does not update the
   * `timelineClient` field, though it does verify that the field
   * is unset.
   *
   * The method is private to the package so that tests can access it, which
   * some of the mock tests do to override the timeline client creation.
   * @return the timeline client
   */
  private[yarn] def createTimelineClient(): TimelineClient = {
    require(_timelineClient.isEmpty, "timeline client already set")
    YarnTimelineUtils.createTimelineClient(sparkContext)
  }

  /**
   * Get the timeline client.
   * @return the client
   * @throws Exception if the timeline client is not currently running
   */
  def timelineClient: TimelineClient = {
    synchronized { _timelineClient.get }
  }

  /**
   * Get the total number of events dropped due to the queue of
   * outstanding posts being too long.
   * @return counter of events processed
   */

  def eventsDropped: Long = metrics.eventsDropped.getCount

  /**
   * Get the total number of processed events, those handled in the back-end thread without
   * being rejected.
   *
   * @return counter of events processed
   */
  def eventsProcessed: Long = metrics.eventsProcessed.getCount

  /**
   * Get the total number of events queued.
   *
   * @return the total event count
   */
  def eventsQueued: Long = metrics.eventsQueued.getCount

  /**
    * Get the current size of the posting queue.
    *
    * @return the current queue length
    */
  def postingQueueSize: Int = postingQueue.size()

  /**
   * Query the counter of attempts to post entities to the timeline service.
   *
   * @return the current value
   */
  def postAttempts: Long = metrics.entityPostAttempts.getCount

  /**
   * Get the total number of failed post operations.
   *
   * @return counter of timeline post operations which failed
   */
  def postFailures: Long = metrics.entityPostFailures.getCount

  /**
   * Query the counter of successful post operations (this is not the same as the
   * number of events posted).
   *
   * @return the number of successful post operations.
   */
  def postSuccesses: Long = metrics.entityPostSuccesses.getCount

  /**
   * Is the asynchronous posting thread active?
   *
   * @return true if the post thread has started; false if it has not yet/ever started, or
   *         if it has finished.
   */
  def isPostThreadActive: Boolean = postThreadActive.get

  /**
   * Reset the timeline client. Idempotent.
   *
   * 1. Stop the timeline client service if running.
   * 2. set the `timelineClient` field to `None`
   */
  def stopTimelineClient(): Unit = {
    synchronized {
      _timelineClient.foreach(_.stop())
      _timelineClient = None
    }
  }

  /**
   * Create the timeline domain.
   *
   * A Timeline Domain is a uniquely identified 'namespace' for accessing parts of the timeline.
   * Security levels are are managed at the domain level, so one is created if the
   * spark acls are enabled. Full access is then granted to the current user,
   * all users in the configuration options `"spark.modify.acls"` and `"spark.admin.acls"`;
   * read access to those users and those listed in `"spark.ui.view.acls"`
   *
   * @return an optional domain string. If `None`, then no domain was created.
   */
  private def createTimelineDomain(): Option[String] = {
    val sparkConf = sparkContext.getConf
    val aclsOn = sparkConf.getBoolean("spark.ui.acls.enable",
        sparkConf.getBoolean("spark.acls.enable", false))
    if (!aclsOn) {
      logDebug("ACLs are disabled; not creating the timeline domain")
      return None
    }
    val predefDomain = sparkConf.getOption(TIMELINE_DOMAIN)
    if (predefDomain.isDefined) {
      logDebug(s"Using predefined domain $predefDomain")
      return predefDomain
    }
    val current = UserGroupInformation.getCurrentUser.getShortUserName
    val adminAcls = stringToSet(sparkConf.get("spark.admin.acls", ""))
    val viewAcls = stringToSet(sparkConf.get("spark.ui.view.acls", ""))
    val modifyAcls = stringToSet(sparkConf.get("spark.modify.acls", ""))

    val readers = (Seq(current) ++ adminAcls ++ modifyAcls ++ viewAcls).mkString(" ")
    val writers = (Seq(current) ++ adminAcls ++ modifyAcls).mkString(" ")
    val domain = DOMAIN_ID_PREFIX + applicationId
    logInfo(s"Creating domain $domain with readers: $readers and writers: $writers")

    // create the timeline domain with the reader and writer permissions
    val timelineDomain = new TimelineDomain()
    timelineDomain.setId(domain)
    timelineDomain.setReaders(readers)
    timelineDomain.setWriters(writers)
    try {
      timelineClient.putDomain(timelineDomain)
      Some(domain)
    } catch {
      case e: Exception =>
        logError(s"cannot create the domain $domain", e)
        // fallback to default
        None
    }
  }

  /**
   * Start the service.
   *
   * @param binding binding to the spark application and YARN
   */
  override def start(binding: SchedulerExtensionServiceBinding): Unit = {
    val oldstate = enterState(StartedState)
    if (oldstate != CreatedState) {
      // state model violation
      _serviceState.set(oldstate)
      throw new IllegalArgumentException(s"Cannot start the service from state $oldstate")
    }
    val context = binding.sparkContext
    val appId = binding.applicationId
    val attemptId = binding.attemptId
    require(context != null, "Null context parameter")
    bindToYarnApplication(appId, attemptId)
    this.sparkContext = context
    this.config = new YarnConfiguration(context.hadoopConfiguration)
    val sparkConf = sparkContext.conf

    // work out the attempt ID from the YARN attempt ID. No attempt, assume "1".
    val attempt1 = attemptId match {
      case Some(attempt) => attempt.getAttemptId.toString
      case None => CLIENT_BACKEND_ATTEMPT_ID
    }
    setContextAppAndAttemptInfo(Some(appId.toString), Some(attempt1))
    batchSize = sparkConf.getInt(BATCH_SIZE, batchSize)
    postQueueLimit = sparkConf.getInt(POST_EVENT_LIMIT, postQueueLimit)
    retryInterval = 1000 * sparkConf.getTimeAsSeconds(POST_RETRY_INTERVAL,
      DEFAULT_POST_RETRY_INTERVAL)
    shutdownWaitTime = 1000 * sparkConf.getTimeAsSeconds(SHUTDOWN_WAIT_TIME,
      DEFAULT_SHUTDOWN_WAIT_TIME)

    // the full metrics integration happens if the spark context has a metrics system
    val metricsSystem = sparkContext.metricsSystem
    if (metricsSystem != null) {
      metricsSystem.registerSource(metrics)
    }

    // set up the timeline service, unless it's been disabled for testing
    if (timelineServiceEnabled) {
      timelineWebappAddress = getTimelineEndpoint(config)

      logInfo(s"Starting $this")
      logInfo(s"Spark events will be published to the Timeline at $timelineWebappAddress")
      _timelineClient = Some(createTimelineClient())
      domainId = createTimelineDomain()
      // declare that the processing is started
      queueStopped.set(false)
      val thread = new Thread(new EntityPoster(), "EventPoster")
      entityPostThread = Some(thread)
      thread.setDaemon(true)
      thread.start()
    } else {
      logInfo("Timeline service is disabled")
    }
    if (registerListener()) {
      logInfo(s"History Service listening for events: $this")
    } else {
      logInfo(s"History Service is not listening for events: $this")
    }
  }

  /**
   * Check the service configuration to see if the timeline service is enabled.
   *
   * @return true if `YarnConfiguration.TIMELINE_SERVICE_ENABLED` is set.
   */
  def timelineServiceEnabled: Boolean = {
    YarnTimelineUtils.timelineServiceEnabled(config)
  }

  /**
   * Return a summary of the service state to help diagnose problems
   * during test runs, possibly even production.
   *
   * @return a summary of the current service state
   */
  override def toString(): String =
    s"""YarnHistoryService for application $applicationId attempt $attemptId;
       | state=$serviceState;
       | endpoint=$timelineWebappAddress;
       | bonded to ATS=$bondedToATS;
       | listening=$listening;
       | batchSize=$batchSize;
       | flush count=$getFlushCount;
       | total number queued=$eventsQueued, processed=$eventsProcessed;
       | attempted entity posts=$postAttempts
       | successful entity posts=$postSuccesses
       | failed entity posts=$postFailures;
       | events dropped=$eventsDropped;
       | app start event received=$appStartEventProcessed;
       | app end event received=$appEndEventProcessed;
     """.stripMargin

  /**
   * Is the service listening to events from the spark context?
   *
   * @return true if it has registered as a listener
   */
  def listening: Boolean = {
    listener.isDefined
  }

  /**
   * Is the service hooked up to an ATS server?
   *
   * This does not check the validity of the link, only whether or not the service
   * has been set up to talk to ATS.
   *
   * @return true if the service has a timeline client
   */
  def bondedToATS: Boolean = {
    _timelineClient.isDefined
  }

  /**
   * Set the YARN binding information.
   *
   * This is called during startup. It is private to the package so that tests
   * may update this data.
   * @param appId YARN application ID
   * @param maybeAttemptId optional attempt ID
   */
  private[yarn] def bindToYarnApplication(appId: ApplicationId,
      maybeAttemptId: Option[ApplicationAttemptId]): Unit = {
    require(appId != null, "Null appId parameter")
    applicationId = appId
    attemptId = maybeAttemptId
  }

  /**
   * Set the "spark" application and attempt information -the information
   * provided in the start event. The attempt ID here may be `None`; even
   * if set it may only be unique amongst the attempts of this application.
   * That is: not unique enough to be used as the entity ID
   *
   * @param appId application ID
   * @param attemptId attempt ID
   */
  private def setContextAppAndAttemptInfo(appId: Option[String],
      attemptId: Option[String]): Unit = {
    logDebug(s"Setting application ID to $appId; attempt ID to $attemptId")
    sparkApplicationId = appId
    sparkApplicationAttemptId = attemptId
  }

  /**
   * Add the listener if it is not disabled.
   * This is accessible in the same package purely for testing
   *
   * @return true if the register was enabled
   */
  private[yarn] def registerListener(): Boolean = {
    assert(sparkContext != null, "Null context")
    if (sparkContext.conf.getBoolean(REGISTER_LISTENER, true)) {
      logDebug("Registering listener to spark context")
      val l = new YarnEventListener(sparkContext, this)
      listener = Some(l)
      sparkContext.listenerBus.addListener(l)
      true
    } else {
      false
    }
  }

  /**
   * Queue an action, or, if the service's `stopped` flag is set, discard it.
   *
   * This is the method called by the event listener when forward events to the service.
   * @param event event to process
   * @return true if the event was queued
   */
  def enqueue(event: SparkListenerEvent): Boolean = {
    if (!queueStopped.get) {
      metrics.eventsQueued.inc()
      logDebug(s"Enqueue $event")
      handleEvent(event)
      true
    } else {
      // the service is stopped, so the event will not be processed.
      if (timelineServiceEnabled) {
        // if a timeline service was ever enabled, log the fact the event
        // is being discarded. Don't do this if it was not, as it will
        // only make the (test run) logs noisy.
        logInfo(s"History service stopped; ignoring queued event : $event")
      }
      false
    }
  }

  /**
   * Stop the service; this triggers flushing the queue and, if not already processed,
   * a pushing out of an application end event.
   *
   * This operation will block for up to `maxTimeToWaitOnShutdown` milliseconds
   * to await the asynchronous action queue completing.
   */
  override def stop(): Unit = {
    val oldState = enterState(StoppedState)
    if (oldState != StartedState) {
      // stopping from a different state
      logDebug(s"Ignoring stop() request from state $oldState")
      return
    }
    try {
      stopQueue()
    } finally {
      if (sparkContext.metricsSystem != null) {
        // unregister from metrics
        sparkContext.metricsSystem.removeSource(metrics)
      }
    }
  }

  /**
   * Stop the queue system.
   */
  private def stopQueue(): Unit = {
    // if the queue is live
    if (!queueStopped.get) {

      if (appStartEventProcessed.get && !appEndEventProcessed.get) {
        // push out an application stop event if none has been received
        logDebug("Generating a SparkListenerApplicationEnd during service stop()")
        enqueue(SparkListenerApplicationEnd(now()))
      }

      // flush out the events
      asyncFlush()

      // push out that queue stop event; this immediately sets the `queueStopped` flag
      pushQueueStop(now(), shutdownWaitTime)

      // Now await the halt of the posting thread.
      var shutdownPosted = false
      if (postThreadActive.get) {
        postThreadActive.synchronized {
          // check it hasn't switched state
          if (postThreadActive.get) {
            logDebug(s"Stopping posting thread and waiting $shutdownWaitTime mS")
            shutdownPosted = true
            postThreadActive.wait(shutdownWaitTime)
            // then interrupt the thread if it is still running
            if (postThreadActive.get) {
              logInfo("Interrupting posting thread after $shutdownWaitTime mS")
              entityPostThread.foreach(_.interrupt())
            }
          }
        }
      }
      if (!shutdownPosted) {
        // there was no running post thread, just stop the timeline client ourselves.
        // (if there is a thread running, it must be the one to stop it)
        stopTimelineClient()
        logInfo(s"Stopped: $this")
      }
    }
  }

  /**
   * Can an event be added?
   *
   * The policy is: only if the number of queued entities is below the limit, or the
   * event marks the end of the application.
   *
   * @param isLifecycleEvent is this operation triggered by an application start/end?
   * @return true if the event can be added to the queue
   */
  private def canAddEvent(isLifecycleEvent: Boolean): Boolean = {
    isLifecycleEvent || metrics.eventsQueued.getCount < postQueueLimit
  }

  /**
   * Add another event to the pending event list.
   *
   * Returns the size of the event list after the event was added
   * (thread safe).
   * @param event event to add
   * @return the event list size
   */
  private def addPendingEvent(event: TimelineEvent): Int = {
    pendingEvents.synchronized {
      pendingEvents :+= event
      pendingEvents.size
    }
  }

  /**
   * Publish next set of pending events if there are events to publish,
   * and the application has been recorded as started.
   *
   * Builds the next event to push onto [[postingQueue]]; resets
   * the current [[pendingEvents]] list and then adds a [[PostEntity]]
   * operation to the queue.
   *
   * @return true if another entity was queued
   */
  private def publishPendingEvents(): Boolean = {
    // verify that there are events to publish
    val size = pendingEvents.synchronized {
      pendingEvents.size
    }
    if (size > 0 && applicationStartEvent.isDefined) {
      // push if there are events *and* the app is recorded as having started.
      // -as the app name is needed for the the publishing.
      metrics.flushCount.inc()
      val timelineEntity = createTimelineEntity(
        applicationId,
        attemptId,
        sparkApplicationId,
        sparkApplicationAttemptId,
        applicationName,
        userName,
        startTime,
        endTime,
        now())

      // copy in pending events and then reset the list
      pendingEvents.synchronized {
        pendingEvents.foreach(timelineEntity.addEvent)
        pendingEvents = new mutable.LinkedList[TimelineEvent]()
      }
      queueForPosting(timelineEntity)
      true
    } else {
      false
    }
  }

  /**
    * Queue an asynchronous flush operation.
    * @return if the flush event was queued
    */
  def asyncFlush(): Boolean = {
    publishPendingEvents()
  }

  /**
   * A `StopQueueAction` action has a size of 0
   * @param currentTime time when action was queued.
   * @param waitTime time for shutdown to wait
   */
  private def pushQueueStop(currentTime: Long, waitTime: Long): Unit = {
    queueStopped.set(true)
    postingQueue.add(StopQueueAction(currentTime, waitTime))
  }

  /**
   * Queue an entity for posting; also increases
   * [[postQueueEventSize]] by the size of the entity.
   * @param timelineEntity entity to push
   */
  def queueForPosting(timelineEntity: TimelineEntity): Unit = {
    // queue the entity for posting
    preflightCheck(timelineEntity)
    val e = new PostEntity(timelineEntity)
    postQueueEventSize.addAndGet(e.size)
    metrics.postQueueEventSize.inc(e.size)
    postingQueue.add(e)
  }

  /**
   * Push a `PostQueueAction` to the start of the queue; also increments
   * [[postQueueEventSize]] by the size of the action.
   * @param action action to push
   */
  private def pushToFrontOfQueue(action: PostQueueAction): Unit = {
    postingQueue.push(action)
    postQueueEventSize.addAndGet(action.size)
    metrics.postQueueEventSize.inc(action.size)
  }

  /**
    * Take from the posting queue; decrements  [[postQueueEventSize]] by the size
    * of the action.
   * @return the action
    */
  private def takeFromPostingQueue(): PostQueueAction = {
    val taken = postingQueue.take()
    postQueueEventSize.addAndGet(-taken.size)
    metrics.postQueueEventSize.dec(taken.size)
    taken
  }

  /**
    * Poll from the posting queue; decrements  [[postQueueEventSize]] by the size
    * of the action.
   * @return
    */
  private def pollFromPostingQueue(mills: Long): Option[PostQueueAction] = {
    val taken = postingQueue.poll(mills, TimeUnit.MILLISECONDS)
    postQueueEventSize.addAndGet(-taken.size)
    metrics.postQueueEventSize.dec(taken.size)
    Option(taken)
  }

  /**
   * Perform any preflight checks.
   *
   * This is just a safety check to catch regressions in the code which
   * publish data that cannot be parsed at the far end.
   * @param entity timeline entity to review.
   */
  private def preflightCheck(entity: TimelineEntity): Unit = {
    require(entity.getStartTime != null,
      s"No start time in ${describeEntity(entity)}")
  }

  /** Actions in the post queue */
  private sealed trait PostQueueAction {
    /**
     * Number of events in this entry
     * @return a natural number
     */
    def size: Int
  }

  /**
   * A `StopQueueAction` action has a size of 0
   * @param currentTime time when action was queued.
   * @param waitTime time for shutdown to wait
   */
  private case class StopQueueAction(currentTime: Long, waitTime: Long) extends PostQueueAction {
    override def size: Int = 0
    def timeLimit: Long = currentTime + waitTime
  }

  /**
   *  A `PostEntity` action has a size of the number of listed events
   */
  private case class PostEntity(entity: TimelineEntity) extends PostQueueAction {
    override def size: Int = entity.getEvents.size()
  }

  /**
   * Post a single entity.
   *
   * Any network/connectivity errors will be caught and logged, and returned as the
   * exception field in the returned tuple.
   *
   * Any posting which generates a response will result in the timeline response being
   * returned. This response *may* contain errors; these are almost invariably going
   * to re-occur when resubmitted.
   *
   * @param entity entity to post
   * @return Any exception other than an interruption raised during the operation.
   * @throws InterruptedException if an [[InterruptedException]] or [[InterruptedIOException]] is
   * received. These exceptions may also get caught and wrapped in the ATS client library.
   */
  private def postOneEntity(entity: TimelineEntity): Option[Exception] = {
    domainId.foreach(entity.setDomainId)
    val entityDescription = describeEntity(entity)
    logInfo(s"About to POST entity ${entity.getEntityId} with ${entity.getEvents.size()} events" +
        s" to timeline service $timelineWebappAddress")
    logDebug(s"About to POST $entityDescription")
    val timeContext = metrics.postOperationTimer.time()
    metrics.entityPostAttempts.inc()
    try {
      val response = timelineClient.putEntities(entity)
      val errors = response.getErrors
      if (errors.isEmpty) {
        logDebug(s"entity successfully posted")
        metrics.entityPostSuccesses.inc()
      } else {
        // The ATS service rejected the request at the API level.
        // this is something we assume cannot be re-tried
        metrics.entityPostRejections.inc()
        logError(s"Failed to post $entityDescription")
        errors.asScala.foreach { err =>
          logError(describeError(err))
        }
      }
      // whether accepted or rejected, this request is not re-issued
      None
    } catch {

      case e: InterruptedException =>
        // interrupted; this will break out of IO/Sleep operations and
        // trigger a rescan of the stopped() event.
        throw e

      case e: ConnectException =>
        // connection failure: network, ATS down, config problems, ...
        metrics.entityPostFailures.inc()
        logDebug(s"Connection exception submitting $entityDescription", e)
        Some(e)

      case e: Exception =>
        val cause = e.getCause
        if (cause != null && cause.isInstanceOf[InterruptedException]) {
          // hadoop 2.7 retry logic wraps the interrupt
          throw cause
        }
        // something else has gone wrong.
        metrics.entityPostFailures.inc()
        logDebug(s"Could not handle history entity: $entityDescription", e)
        Some(e)

    } finally {
      timeContext.stop()
    }
  }

  /**
   * Wait for and then post entities until stopped.
   *
   * Algorithm.
   *
   * 1. The thread waits for events in the [[postingQueue]] until stopped or interrupted.
   * 1. Failures result in the entity being queued for resending, after a delay which grows
   * linearly on every retry.
   * 1. Successful posts reset the retry delay.
   * 1. If the process is interrupted, the loop continues with the `stopFlag` flag being checked.
   *
   * To stop this process then, first set the `stopFlag` flag, then interrupt the thread.
   *
   * @param retryInterval delay in milliseconds for the first retry delay; the delay increases
   *        by this value on every future failure. If zero, there is no delay, ever.
   * @return the [[StopQueueAction]] received to stop the process.
   */
  private def postEntities(retryInterval: Long): StopQueueAction = {
    var lastAttemptFailed = false
    var currentRetryDelay = retryInterval
    var result: StopQueueAction = null
    while (result == null) {
      takeFromPostingQueue() match {
        case PostEntity(entity) =>
          val ex = postOneEntity(entity)
          if (ex.isDefined && !queueStopped.get()) {
            // something went wrong
            if (!lastAttemptFailed) {
              // avoid filling up logs with repeated failures
              logWarning(s"Exception submitting entity to $timelineWebappAddress", ex.get)
            }
            // log failure and queue for posting again
            lastAttemptFailed = true
            currentRetryDelay += retryInterval
            if (!queueStopped.get()) {
              // push back to the head of the queue
              postingQueue.addFirst(PostEntity(entity))
              if (currentRetryDelay > 0) {
                Thread.sleep(currentRetryDelay)
              }
            }
          } else {
            // success; reset flags and retry delay
            lastAttemptFailed = false
            currentRetryDelay = retryInterval
          }
        case stop: StopQueueAction =>
          logDebug("Queue stopped")
          result = stop
      }
    }
    result
  }

  /**
   * Shutdown phase: continually post oustanding entities until the timeout has been exceeded.
   * The interval between failures is the retryInterval: there is no escalation, and if
   * is longer than the remaining time in the shutdown, the remaining time sets the limit.
   *
   * @param shutdown shutdown parameters.
   * @param retryInterval delay in milliseconds for every delay.
   */
  private def postEntitiesShutdownPhase(shutdown: StopQueueAction, retryInterval: Long): Unit = {
    val timeLimit = shutdown.timeLimit
    val timestamp = YarnTimelineUtils.timeShort(timeLimit, "")
    logDebug(s"Queue shutdown, time limit= $timestamp")
    while (now() < timeLimit && !postingQueue.isEmpty) {
      pollFromPostingQueue(timeLimit - now()) match {
          case Some(PostEntity(entity)) =>
            postOneEntity(entity).foreach { e =>
              if (!e.isInstanceOf[InterruptedException] &&
                  !e.isInstanceOf[InterruptedIOException]) {
                // failure, push back to try again
                pushToFrontOfQueue(PostEntity(entity))
                if (retryInterval > 0) {
                  Thread.sleep(retryInterval)
                } else {
                  // there's no retry interval, so fail immediately
                  throw e
                }
              } else {
                // this was an interruption. Throw it again
                throw e
              }
            }
          case Some(StopQueueAction(_, _)) =>
            // ignore these
            logDebug("Ignoring StopQueue action")

          case None =>
            // get here then the queue is empty; all is well
        }
      }
  }

  /**
   * If the event reaches the batch size or flush is true, push events to ATS.
   *
   * @param event event. If null, no event is queued, but the post-queue flush logic still applies
   */
  private def handleEvent(event: SparkListenerEvent): Unit = {
    // publish events unless stated otherwise
    var publish = true
    // don't trigger a push to the ATS
    var push = false
    // lifecycle events get special treatment: they are never discarded from the queues,
    // even if the queues are full.
    var isLifecycleEvent = false
    val timestamp = now()
    metrics.eventsProcessed.inc()
    if (metrics.eventsProcessed.getCount() % 1000 == 0) {
      logDebug(s"${metrics.eventsProcessed} events are processed")
    }
    event match {
      case start: SparkListenerApplicationStart =>
        // we already have all information,
        // flush it for old one to switch to new one
        logDebug(s"Handling application start event: $event")
        if (!appStartEventProcessed.getAndSet(true)) {
          applicationStartEvent = Some(start)
          applicationName = start.appName
          if (applicationName == null || applicationName.isEmpty) {
            logWarning("Application does not have a name")
            applicationName = applicationId.toString
          }
          userName = start.sparkUser
          startTime = start.time
          if (startTime == 0) {
            startTime = timestamp
          }
          setContextAppAndAttemptInfo(start.appId, start.appAttemptId)
          logDebug(s"Application started: $event")
          isLifecycleEvent = true
          push = true
        } else {
          logWarning(s"More than one application start event received -ignoring: $start")
          publish = false
        }

      case end: SparkListenerApplicationEnd =>
        if (!appStartEventProcessed.get()) {
          // app-end events being received before app-start events can be triggered in
          // tests, even if not seen in real applications.
          // react by ignoring the event altogether, as an un-started application
          // cannot be reported.
          logError(s"Received application end event without application start $event -ignoring.")
        } else if (!appEndEventProcessed.getAndSet(true)) {
          // the application has ended
          logDebug(s"Application end event: $event")
          applicationEndEvent = Some(end)
          // flush old entity
          endTime = if (end.time > 0) end.time else timestamp
          push = true
          isLifecycleEvent = true
        } else {
          // another test-time only situation: more than one application end event
          // received. Discard the later one.
          logInfo(s"Discarding duplicate application end event $end")
          publish = false
        }

      case update: SparkListenerBlockUpdated =>
        publish = false

      case update: SparkListenerExecutorMetricsUpdate =>
        publish = false

      case _ =>
    }

    if (publish) {
      val tlEvent = toTimelineEvent(event, timestamp)
      val eventCount = if (tlEvent.isDefined && canAddEvent(isLifecycleEvent)) {
        addPendingEvent(tlEvent.get)
      } else {
        // discarding the event
        logInfo(s"Discarding event $tlEvent")
        metrics.eventsDropped.inc()
        0
      }

      // trigger a push if the batch limit is reached
      // There's no need to check for the application having started, as that is done later.
      push |= eventCount >= batchSize

      logDebug(s"current event num: $eventCount")
      if (push) {
        logDebug("Push triggered")
        publishPendingEvents()
      }
    }
  }

  /**
   * Return the current time in milliseconds.
   * @return system time in milliseconds
   */
  private def now(): Long = {
    clock.getTimeMillis()
  }

  /**
   * Get the number of flush events that have taken place.
   *
   * This includes flushes triggered by the event list being bigger the batch size,
   * but excludes flush operations triggered when the action processor thread
   * is stopped, or if the timeline service binding is disabled.
   *
   * @return count of processed flush events.
   */
  def getFlushCount: Long = {
    metrics.flushCount.getCount
  }

  /**
   * Post events until told to stop.
   */
  private class EntityPoster extends Runnable {

    override def run(): Unit = {
      postThreadActive.set(true)
      try {
        val shutdown = postEntities(retryInterval)
        // getting here means the `stop` flag is true
        postEntitiesShutdownPhase(shutdown, retryInterval)
        logInfo(s"Stopping dequeue service, final queue size is ${postingQueue.size};" +
          s" outstanding events to post count: ${postQueueEventSize.get()}")
      } catch {
        // handle exceptions triggering thread exit. Interruptes are good; others less welcome.
        case ex: InterruptedException =>
          logInfo("Entity Posting thread interrupted")
          logDebug("Entity Posting thread interrupted", ex)

        case ex: InterruptedIOException =>
          logInfo("Entity Posting thread interrupted")
          logDebug("Entity Posting thread interrupted", ex)

        case ex: Exception =>
          logError("Entity Posting thread exiting after exception raised", ex)
      } finally {
        stopTimelineClient()
        postThreadActive synchronized {
          // declare that this thread is no longer active
          postThreadActive.set(false)
          // and notify all listeners of this fact
          postThreadActive.notifyAll()
        }
      }
    }
  }

}

/**
 * Metrics integration: the various counters of activity
 */
private[yarn] class HistoryMetrics extends ExtendedMetricsSource {

  /** Name for metrics: yarn_history */
  override val sourceName = YarnHistoryService.METRICS_NAME

  /** Metrics registry */
  override val metricRegistry = new MetricRegistry()

  /** Number of events in the post queue. */
  val postQueueEventSize = new Counter()

  /** Counter of events processed -that is have been through handleEvent() */
  val eventsProcessed = new Counter()

  /** Counter of events queued. */
  val eventsQueued = new Counter()

  /** Counter of number of attempts to post entities. */
  val entityPostAttempts = new Counter()

  /** Counter of number of successful entity post operations. */
  val entityPostSuccesses = new Counter()

  /** How many entity postings failed? */
  val entityPostFailures = new Counter()

  /** How many entity postings were rejected? */
  val entityPostRejections = new Counter()

  /** The number of events which were dropped as the backlog of pending posts was too big. */
  val eventsDropped = new Counter()

  /** How many flushes have taken place? */
  val flushCount = new Counter()

  /** Timer to build up statistics on post operation times */
  val postOperationTimer = new Timer()

  val metricsMap: Map[String, Metric] = Map(
    "eventsDropped" -> eventsDropped,
    "eventsProcessed" -> eventsProcessed,
    "eventsQueued" -> eventsQueued,
    "entityPostAttempts" -> entityPostAttempts,
    "entityPostFailures" -> entityPostFailures,
    "entityPostRejections" -> entityPostRejections,
    "entityPostSuccesses" -> entityPostSuccesses,
    "entityPostTimer" -> postOperationTimer,
    "flushCount" -> flushCount
  )

  init()

}

/**
 * Constants and defaults for the history service.
 */
private[spark] object YarnHistoryService {

  /**
   * Name of the entity type used to declare spark Applications.
   */
  val SPARK_EVENT_ENTITY_TYPE = "spark_event_v01"

  /**
   * Domain ID.
   */
  val DOMAIN_ID_PREFIX = "Spark_ATS_"

  /**
   * Time in millis to wait for shutdown on service stop.
   */
  val DEFAULT_SHUTDOWN_WAIT_TIME = "30s"

  /**
   * The maximum time in to wait for event posting to complete when the service stops.
   */
  val SHUTDOWN_WAIT_TIME = "spark.hadoop.yarn.timeline.shutdown.waittime"

  /**
   * Option to declare that the history service should register as a spark context
   * listener. (default: true; this option is here for testing)
   *
   * This is a spark option, though its use of name will cause it to propagate down to the Hadoop
   * Configuration.
   */
  val REGISTER_LISTENER = "spark.hadoop.yarn.timeline.listen"

  /**
   * Option for the size of the batch for timeline uploads. Bigger: less chatty.
   * Smaller: history more responsive.
   */
  val BATCH_SIZE = "spark.hadoop.yarn.timeline.batch.size"

  /**
   * The default size of a batch
   */
  val DEFAULT_BATCH_SIZE = 10

  /**
   * Name of a domain for the timeline
   */
  val TIMELINE_DOMAIN = "spark.hadoop.yarn.timeline.domain"

  /**
   * Limit on number of posts in the outbound queue -when exceeded
   * new events will be dropped
   */
  val POST_EVENT_LIMIT = "spark.hadoop.yarn.timeline.post.limit"

    /**
   * The default limit of events in the post queue
   */
  val DEFAULT_POST_EVENT_LIMIT = 1000

  /**
   * Interval in milliseconds between POST retries. Every
   * failure causes the interval to increase by this value.
   */
  val POST_RETRY_INTERVAL = "spark.hadoop.yarn.timeline.post.retry.interval"

  /**
   * The default retry interval in millis
   */
  val DEFAULT_POST_RETRY_INTERVAL = "1000ms"

  /**
   * Primary key used for events
   */
  val PRIMARY_KEY = "spark_application_entity"

  /**
   *  Entity `OTHER_INFO` field: start time
   */
  val FIELD_START_TIME = "startTime"

  /**
   * Entity `OTHER_INFO` field: last updated time.
   */
  val FIELD_LAST_UPDATED = "lastUpdated"

  /**
   * Entity `OTHER_INFO` field: end time. Not present if the app is running.
   */
  val FIELD_END_TIME = "endTime"

  /**
   * Entity `OTHER_INFO` field: application name from context.
   */
  val FIELD_APP_NAME = "appName"

  /**
   * Entity `OTHER_INFO` field: user.
   */
  val FIELD_APP_USER = "appUser"

  /**
   * Entity `OTHER_INFO` field: YARN application ID.
   */
  val FIELD_APPLICATION_ID = "applicationId"

  /**
   * Entity `OTHER_INFO` field: attempt ID from spark start event.
   */
  val FIELD_ATTEMPT_ID = "attemptId"

  /**
   * Entity `OTHER_INFO` field: a counter which is incremented whenever a new timeline entity
   * is created in this JVM (hence, attempt). It can be used to compare versions of the
   * current entity with any cached copy -it is less brittle than using timestamps.
   */
  val FIELD_ENTITY_VERSION = "entityVersion"

  /**
   * Entity `OTHER_INFO` field: Spark version.
   */
  val FIELD_SPARK_VERSION = "sparkVersion"

  /**
   * Entity filter field: to search for entities that have started.
   */
  val FILTER_APP_START = "startApp"

  /**
   * Value of the `startApp` filter field.
   */
  val FILTER_APP_START_VALUE = "SparkListenerApplicationStart"

  /**
   * Entity filter field: to search for entities that have ended.
   */
  val FILTER_APP_END = "endApp"

  /**
   * Value of the `endApp`filter field.
   */
  val FILTER_APP_END_VALUE = "SparkListenerApplicationEnd"

  /**
   * ID used in yarn-client attempts only.
   */
  val CLIENT_BACKEND_ATTEMPT_ID = "1"

  /**
   * The classname of the history service to instantiate in the YARN AM.
   */
  val CLASSNAME = "org.apache.spark.deploy.history.yarn.YarnHistoryService"

  /**
   * Name of metrics.
   */
  val METRICS_NAME = "yarn_history"

  /**
   * Enum value of application created state
   */
  val CreatedState = 0

  /**
   * Enum value of started state.
   */
  val StartedState = 1

  /**
   * Enum value of stopped state.
   */
  val StoppedState = 2
}
