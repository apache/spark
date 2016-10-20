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

package org.apache.spark.deploy.history.yarn.testtools

import java.io.IOException
import java.net.URL

import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.{YarnHistoryService, YarnTimelineUtils}
import org.apache.spark.scheduler.cluster.{StubApplicationAttemptId, StubApplicationId}
import org.apache.spark.scheduler.{JobFailed, JobSucceeded, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.util.Utils

object YarnTestUtils extends ExtraAssertions with FreePortFinder {

  val environmentUpdate = SparkListenerEnvironmentUpdate(Map[String, Seq[(String, String)]](
    "JVM Information" -> Seq(("GC speed", "9999 objects/s"), ("Java home", "Land of coffee")),
    "Spark Properties" -> Seq(("Job throughput", "80000 jobs/s, regardless of job type")),
    "System Properties" -> Seq(("Username", "guest"), ("Password", "guest")),
    "Classpath Entries" -> Seq(("Super library", "/tmp/super_library"))))

  /**
   * Application name used in the app start event and tests
   */
  val APP_NAME = "spark-demo"

  /**
   * User submitting the job
   */
  val APP_USER = "data-scientist"

  /**
   * application ID
   */
  val APP_ID = "application_id_0001"

  /**
   * Spark option to set for the history provider
   */
  val SPARK_HISTORY_PROVIDER = "spark.history.provider"

  /**
   * Constant used to define history port in Spark `HistoryServer` class
   */
  val SPARK_HISTORY_UI_PORT = "spark.history.ui.port"

  val completedJobsMarker = "Completed Jobs (1)"
  val activeJobsMarker = "Active Jobs (1)"


  /**
   * Time to wait for anything to start/state to be reached
   */
  val TEST_STARTUP_DELAY = 5000

  /** probes during service shutdown need to handle delayed posting */
  val SERVICE_SHUTDOWN_DELAY = 10000

  /**
   * Cancel a test if the network isn't there.
   *
   * If called during setup, this will abort the test
   */
  def cancelIfOffline(): Unit = {

    try {
      val hostname = Utils.localHostName()
      log.debug(s"local hostname is $hostname")
    } catch {
      case ex: IOException =>
        cancel(s"Localhost name not known: $ex", ex)
    }
  }

  /**
   * Return a time value
   *
   * @return the current time in milliseconds
   */
  def now(): Long = {
    System.currentTimeMillis()
  }

  /**
   * Get a time in the future
   *
   * @param millis future time in millis
   * @return now + the time offset
   */
  def future(millis: Long): Long = {
    now() + millis
  }

  /**
   * Log an entry with a line either side. This aids splitting up tests from the noisy logs
   *
   * @param text text to log
   */
  def describe(text: String): Unit = {
    logInfo(s"\nTest:\n  $text\n\n")
  }

  /**
   * Set a hadoop opt in the config.
   *
   * This adds the `"spark.hadoop."` prefix to all entries which do not already have it
   *
   * @param sparkConfig target configuration
   * @param key hadoop option key
   * @param value value
   */
  def hadoopOpt(sparkConfig: SparkConf, key: String, value: String): SparkConf = {
    if (key.startsWith("spark.hadoop.")) {
      sparkConfig.set(key, value)
    } else {
      sparkConfig.set("spark.hadoop." + key, value)
    }
  }

  /**
   * Bulk set of an entire map of Hadoop options
   *
   * @param sparkConfig target configuration
   * @param options option map
   */
  def applyHadoopOptions(sparkConfig: SparkConf, options: Map[String, String]): SparkConf = {
    options.foreach( e => hadoopOpt(sparkConfig, e._1, e._2))
    sparkConfig
  }

  /**
   * Apply the basic timeline options to the hadoop config
   *
   * @return the modified config
   */
  def addBasicTimelineOptions(sparkConf: SparkConf): SparkConf = {
    val ports = findUniquePorts(3)
    applyHadoopOptions(sparkConf,
      Map(YarnConfiguration.TIMELINE_SERVICE_ENABLED -> "true",
         YarnConfiguration.TIMELINE_SERVICE_ADDRESS -> localhostAndPort(ports.head),
         YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS -> localhostAndPort(ports(1)),
         YarnConfiguration.TIMELINE_SERVICE_STORE -> classOf[MemoryTimelineStore].getName,
         YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES -> "1",
         YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS -> "200"))

    // final port in the set
    sparkConf.set(SPARK_HISTORY_UI_PORT, ports(2).toString)

    // turn off the minimum refresh interval.
    // uses a string to it can be set from code that doesn't refer to any provider-side
    // classes
    sparkConf.set("spark.history.yarn.min-refresh-interval", "0")

    // shorter reset interval and shutdown time
    sparkConf.set(POST_RETRY_INTERVAL, "10ms")
    sparkConf.set(SHUTDOWN_WAIT_TIME, "5s")
  }

  /**
   * Convert the single timeline event in a timeline entity to a spark event
   *
   * @param entity entity to convert, which must contain exactly one event.
   * @return the converted event
   */
  def convertToSparkEvent(entity: TimelineEntity): SparkListenerEvent = {
    assertResult(1, "-wrong # of events in the timeline entry") {
      entity.getEvents().size()
    }
    YarnTimelineUtils.toSparkEvent(entity.getEvents().get(0))
  }

  /**
   * Create an app start event, using the fixed [[APP_NAME]] and [[APP_USER]] values
   * for appname and user; no attempt ID
   *
   * @param time application start time
   * @param appId event ID; default is [[APP_ID]]
   * @return the event
   */
  def appStartEventWithAttempt(time: Long = 1,
      appId: String,
      user: String,
      attemptId: ApplicationAttemptId): SparkListenerApplicationStart = {
    appStartEvent(time, appId, user, Some(attemptId.toString))
  }

  /**
   * Create an app start event
   *
   * @param time application start time
   * @param appId event ID; default is [[APP_ID]]
   * @param user the user; defaults is [[APP_USER]]
   * @param attempt attempt ID; default is `None`
   * @return the event
   */
  def appStartEvent(time: Long = 1434920400000L,
      appId: String = APP_ID,
      user: String = APP_USER,
      attempt: Option[String] = None,
      name: String = APP_NAME): SparkListenerApplicationStart = {
    require(name != null)
    require(appId != null)
    require(user != null)
    SparkListenerApplicationStart(name, Some(appId), time, user, attempt)
  }

  def appStartEvent(time: Long, ctx: SparkContext): SparkListenerApplicationStart = {
    appStartEvent(time, ctx.applicationId, ctx.sparkUser, ctx.applicationAttemptId)
  }

  def appStopEvent(time: Long = 1): SparkListenerApplicationEnd = {
    new SparkListenerApplicationEnd(time)
  }

  def jobStartEvent(time: Long, id: Int) : SparkListenerJobStart = {
    new SparkListenerJobStart(id, time, Nil, null)
  }

  def jobSuccessEvent(time: Long, id: Int) : SparkListenerJobEnd = {
    new SparkListenerJobEnd(id, time, JobSucceeded)
  }

  def jobFailureEvent(time: Long, id: Int, ex: Exception) : SparkListenerJobEnd = {
    new SparkListenerJobEnd(id, time, JobFailed(ex))
  }

  def newEntity(time: Long): TimelineEntity = {
    val entity = new TimelineEntity
    entity.setStartTime(time)
    entity.setEntityId("post")
    entity.setEntityType(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    entity
  }

  val applicationId: ApplicationId = new StubApplicationId(0, 1111L)
  val attemptId: ApplicationAttemptId = new StubApplicationAttemptId(applicationId, 1)
  val applicationStart = appStartEventWithAttempt(now(), applicationId.toString, "bob", attemptId)
  val applicationEnd = SparkListenerApplicationEnd(now() + 60000)

  /**
   * Outcomes of probes
   */
  sealed abstract class Outcome
  case class Fail() extends Outcome
  case class Retry() extends Outcome
  case class Success() extends Outcome
  case class TimedOut() extends Outcome

  /**
   * Spin and sleep awaiting an observable state.
   *
   * The scalatest `eventually` operator is similar, and even adds exponential backoff.
   * What this offers is:
   *
   * 1. The ability of the probe to offer more than just success/fail, but a "fail fast"
   * operation which stops spinning early.
   * 2. A detailed operation to invoke on failure, so provide more diagnostics than
   * just the assertion.
   *
   * @param interval sleep interval
   * @param timeout time to wait
   * @param probe probe to execute
   * @param failure closure invoked on timeout/probe failure
   */
  def spinForState(description: String,
      interval: Long,
      timeout: Long,
      probe: () => Outcome,
      failure: (Outcome, Int, Boolean) => Unit): Unit = {
    logInfo(description)
    val timelimit = now() + timeout
    var result: Outcome = Retry()
    var current = 0L
    var iterations = 0
    do {
      iterations += 1
      result = probe()
      if (result == Retry()) {
        // probe says retry
        current = now()
        if (current> timelimit) {
          // timeout, uprate to fail
          result = TimedOut()
        } else {
          Thread.sleep(interval)
        }
      }
    } while (result == Retry())
    result match {
      case Fail() => failure(result, iterations, false)
      case TimedOut() => failure(result, iterations, true)
      case _ =>
    }
  }

  /**
   * Convert a boolean into a success or retry outcome, that is:
   * false is considered "retry", not a failure
   *
   * @param value value to probe
   * @return
   */
  def outcomeFromBool(value: Boolean): Outcome = {
    if (value) Success() else Retry()
  }

  /**
   * From an increasing counter, compare the results and decide whether to succeed, fail or try
   * again. Requires that if actual is greater than expected, it is a failed state.
   *
   * @param expected expected outcome
   * @param actual actual value
   */
  def outcomeFromCounter(expected: Long, actual: Long): Outcome = {
    if (expected == actual) {
      Success()
    } else if (actual < expected) {
      Retry()
    } else {
      Fail()
    }
  }

  /**
   * From an increasing counter, compare the results and decide whether to succeed or try
   * again. Any value equal to or greater than expected is a success. Ideal for waiting for
   * asynchronous operations to complete
   * @param expected expected outcome
   * @param actual actual value
   */
  def outcomeAtLeast(expected: Long, actual: Long): Outcome = {
    if (actual >= expected) Success() else Retry()
  }

  /**
   * Curryable function to use for timeouts if something more specific is not needed
   *
   * @param text text mesage on timeouts
   * @param iterations number of iterations performed
   * @param timeout true if the event was a timeout (i.e. not a failure)
   */
  def timeout(text: String, iterations: Int, timeout: Boolean): Unit = {
    fail(text)
  }

  /**
   * a No-op on failure
   *
   * @param outcome outcome of the last operation
   * @param iterations number of iterations performed
   * @param timeout did the wait result in a timeout
   */
  def failure_noop(outcome: Outcome, iterations: Int, timeout: Boolean): Unit = {
  }

  /**
   * Spin for the number of processed events to exactly match the supplied value.
   *
   * Fails if the timeout is exceeded
   *
   * @param historyService history
   * @param expected exact number to await
   * @param timeout timeout in millis
   */
  def awaitEventsProcessed(historyService: YarnHistoryService,
    expected: Int, timeout: Long): Unit = {

    def eventsProcessedCheck(): Outcome = {
      outcomeFromCounter(expected, historyService.eventsProcessed)
    }

    def eventProcessFailure(outcome: Outcome, iterations: Int, timeout: Boolean): Unit = {
      val eventsCount = historyService.eventsProcessed
      val details = s"Expected $expected events" + s" actual=$eventsCount" +
        s" after $iterations iterations; in $historyService"
      if (timeout) {
        val message = s"Timeout: $details"
        logError(message)
        fail(message)
      } else if (expected != eventsCount) {
        val message = s"event count mismatch; $details;"
        logError(message)
        fail(message)
        fail(s"Expected $details")
        }
    }

    spinForState("awaitEventsProcessed",
                  interval = 50,
                  timeout = timeout,
                  probe = eventsProcessedCheck,
                  failure = eventProcessFailure)
  }

  /**
   * Spin awaiting a URL to be accessible. Useful to await a web application
   * going live before running the tests against it
   *
   * @param url URL to probe
   * @param timeout timeout in mils
   */
  def awaitURL(url: URL, timeout: Long): Unit = {
    def probe(): Outcome = {
      try {
        url.openStream().close()
        Success()
      } catch {
        case ioe: IOException => Retry()
      }
    }

    /*
     failure action is simply to attempt the connection without
     catching the exception raised
     */
    def failure(outcome: Outcome, iterations: Int, timeout: Boolean): Unit = {
      url.openStream().close()
    }

    spinForState(s"Awaiting a response from URL $url",
     interval = 50, timeout = timeout, probe = probe, failure = failure)
  }


  /**
   * Wait for a history service's queue to become empty
   *
   * @param historyService service
   * @param timeout timeout
   */
  def awaitEmptyQueue(historyService: YarnHistoryService, timeout: Long): Unit = {

    spinForState("awaiting empty queue",
      interval = 50,
      timeout = timeout,
      probe = () => outcomeFromBool(historyService.postingQueueSize == 0),
      failure = (_, _, _) => fail(s"queue never cleared after $timeout mS for $historyService"))
  }

  /**
   * Await for the count of flushes in the history service to match the expected value
   *
   * @param historyService service
   * @param count min number of flushes
   * @param timeout timeout
   */
  def awaitFlushCount(historyService: YarnHistoryService, count: Long, timeout: Long): Unit = {
    spinForState(s"awaiting flush count of $count",
      interval = 50,
      timeout = timeout,
      probe = () => outcomeFromBool(historyService.getFlushCount >= count),
      failure = (_, _, _) => fail(s"flush count not $count after $timeout mS in $historyService"))
  }

  /**
   * Await the number of post events
   * @param service service
   * @param posts attempt count.
   */
  def awaitPostAttemptCount(service: YarnHistoryService, posts: Long): Unit = {
    awaitCount(posts, TEST_STARTUP_DELAY,
      () => service.postAttempts,
      () => s"Post count in $service")
  }

  /**
   * Await the number of post events
   *
   * @param service service
   * @param posts attempt count.
   */
  def awaitPostSuccessCount(service: YarnHistoryService, posts: Long): Unit = {
    awaitCount(posts, TEST_STARTUP_DELAY,
      () => service.postSuccesses,
      () => s"Post count in $service")
  }

  /**
   * Await for the counter function to match the expected value
   *
   * @param expected desired count
   * @param timeout timeout
   * @param counter function to return an integer
   * @param diagnostics diagnostics string evaluated on timeout
   */
  def awaitCount(expected: Long, timeout: Long,
      counter: () => Long, diagnostics: () => String): Unit = {
    spinForState(s"awaiting probe count of $expected",
      50, timeout,
      () => outcomeFromCounter(expected, counter()),
      (_, _, _) =>
        fail(s"Expected $expected equalled ${counter()} after $timeout mS: ${diagnostics()}"))
  }

  /**
   * Await for the counter function to match the expected value
   *
   * @param expected desired count
   * @param timeout timeout
   * @param counter function to return an integer
   * @param diagnostics diagnostics string evaluated on timeout
   */
  def awaitAtLeast(expected: Long, timeout: Long,
      counter: () => Long, diagnostics: () => String): Unit = {
    spinForState(s"awaiting probe count of at least $expected",
      50, timeout,
      () => outcomeAtLeast(expected, counter()),
      (_, _, _) =>
        fail(s"Expected >= $expected got ${counter()} after $timeout mS: ${diagnostics()}"))
  }


  /**
   * Probe operation to wait for an empty queue
   *
   * @param historyService history service
   * @param timeout timeout in milliseconds
   * @param failOnTimeout flag -fail vs warn on timeout. Default: true
   */
  def awaitServiceThreadStopped(historyService: YarnHistoryService, timeout: Long,
      failOnTimeout: Boolean = true): Unit = {
    assertNotNull(historyService, "null historyService")
    spinForState("awaitServiceThreadStopped",
      interval = 50,
      timeout = timeout,
      probe = () => outcomeFromBool(!historyService.isPostThreadActive),
      failure = (_, _, _) => if (failOnTimeout) {
        fail(s"After $timeout mS, history service post thread did not finish:" +
            s" $historyService")
      } else {
        logWarning(s"After $timeout mS, history service post thread did not finish:" +
            s" $historyService")
      })
  }

  /**
   * Wait for a specified operation to return a list of the desired size
   *
   * @param expectedSize expected size of list
   * @param message message on failure
   * @param timeout timeout
   * @param operation operation to create the list
   * @tparam T list type
   * @return the list created in the last successful operation
   */
  def awaitListSize[T](expectedSize: Int, message: String, timeout: Long,
      operation: () => List[T]): List[T] = {
    // last list fetched
    var list: List[T] = Nil
    def probeOperation(): Outcome = {
      list = operation()
      outcomeFromBool(list.size == expectedSize)
    }
    def failOperation(o: Outcome, i: Int, b: Boolean) = {
      assertListSize(list, expectedSize, message)
    }
    spinForState(message, 50, timeout, probeOperation, failOperation)
    list
  }

  /**
   * Show a Spark context in a string form
   * @param ctx context
   * @return a string value for assertions and other messages
   */
  def asString(ctx: SparkContext): String = {
    s"Spark Context ${ctx.appName} ID ${ctx.applicationId} attempts ${ctx.applicationAttemptId}"
  }

}



