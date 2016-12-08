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

import java.util.{Date, NoSuchElementException}
import javax.servlet.Filter
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.codahale.metrics.Counter
import com.google.common.cache.LoadingCache
import com.google.common.util.concurrent.UncheckedExecutionException
import org.eclipse.jetty.servlet.ServletContextHandler
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo => AttemptInfo, ApplicationInfo}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{Clock, ManualClock, Utils}

class ApplicationCacheSuite extends SparkFunSuite with Logging with MockitoSugar with Matchers {

  /**
   * subclass with access to the cache internals
   * @param retainedApplications number of retained applications
   */
  class TestApplicationCache(
      operations: ApplicationCacheOperations = new StubCacheOperations(),
      retainedApplications: Int,
      clock: Clock = new ManualClock(0))
      extends ApplicationCache(operations, retainedApplications, clock) {

    def cache(): LoadingCache[CacheKey, CacheEntry] = appCache
  }

  /**
   * Stub cache operations.
   * The state is kept in a map of [[CacheKey]] to [[CacheEntry]],
   * the `probeTime` field in the cache entry setting the timestamp of the entry
   */
  class StubCacheOperations extends ApplicationCacheOperations with Logging {

    /** map to UI instances, including timestamps, which are used in update probes */
    val instances = mutable.HashMap.empty[CacheKey, CacheEntry]

    /** Map of attached spark UIs */
    val attached = mutable.HashMap.empty[CacheKey, SparkUI]

    var getAppUICount = 0L
    var attachCount = 0L
    var detachCount = 0L
    var updateProbeCount = 0L

    override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
      logDebug(s"getAppUI($appId, $attemptId)")
      getAppUICount += 1
      instances.get(CacheKey(appId, attemptId)).map( e =>
        LoadedAppUI(e.ui, updateProbe(appId, attemptId, e.probeTime)))
    }

    override def attachSparkUI(
        appId: String,
        attemptId: Option[String],
        ui: SparkUI,
        completed: Boolean): Unit = {
      logDebug(s"attachSparkUI($appId, $attemptId, $ui)")
      attachCount += 1
      attached += (CacheKey(appId, attemptId) -> ui)
    }

    def putAndAttach(
        appId: String,
        attemptId: Option[String],
        completed: Boolean,
        started: Long,
        ended: Long,
        timestamp: Long): SparkUI = {
      val ui = putAppUI(appId, attemptId, completed, started, ended, timestamp)
      attachSparkUI(appId, attemptId, ui, completed)
      ui
    }

    def putAppUI(
        appId: String,
        attemptId: Option[String],
        completed: Boolean,
        started: Long,
        ended: Long,
        timestamp: Long): SparkUI = {
      val ui = newUI(appId, attemptId, completed, started, ended)
      putInstance(appId, attemptId, ui, completed, timestamp)
      ui
    }

    def putInstance(
        appId: String,
        attemptId: Option[String],
        ui: SparkUI,
        completed: Boolean,
        timestamp: Long): Unit = {
      instances += (CacheKey(appId, attemptId) ->
          new CacheEntry(ui, completed, updateProbe(appId, attemptId, timestamp), timestamp))
    }

    /**
     * Detach a reconstructed UI
     *
     * @param ui Spark UI
     */
    override def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit = {
      logDebug(s"detachSparkUI($appId, $attemptId, $ui)")
      detachCount += 1
      var name = ui.getAppName
      val key = CacheKey(appId, attemptId)
      attached.getOrElse(key, { throw new java.util.NoSuchElementException() })
      attached -= key
    }

    /**
     * Lookup from the internal cache of attached UIs
     */
    def getAttached(appId: String, attemptId: Option[String]): Option[SparkUI] = {
      attached.get(CacheKey(appId, attemptId))
    }

    /**
     * The update probe.
     * @param appId application to probe
     * @param attemptId attempt to probe
     * @param updateTime timestamp of this UI load
     */
    private[history] def updateProbe(
        appId: String,
        attemptId: Option[String],
        updateTime: Long)(): Boolean = {
      updateProbeCount += 1
      logDebug(s"isUpdated($appId, $attemptId, ${updateTime})")
      val entry = instances.get(CacheKey(appId, attemptId)).get
      val updated = entry.probeTime > updateTime
      logDebug(s"entry = $entry; updated = $updated")
      updated
    }
  }

  /**
   * Create a new UI. The info/attempt info classes here are from the package
   * `org.apache.spark.status.api.v1`, not the near-equivalents from the history package
   */
  def newUI(
      name: String,
      attemptId: Option[String],
      completed: Boolean,
      started: Long,
      ended: Long): SparkUI = {
    val info = new ApplicationInfo(name, name, Some(1), Some(1), Some(1), Some(64),
      Seq(new AttemptInfo(attemptId, new Date(started), new Date(ended),
        new Date(ended), ended - started, "user", completed)))
    val ui = mock[SparkUI]
    when(ui.getApplicationInfoList).thenReturn(List(info).iterator)
    when(ui.getAppName).thenReturn(name)
    when(ui.appName).thenReturn(name)
    val handler = new ServletContextHandler()
    when(ui.getHandlers).thenReturn(Seq(handler))
    ui
  }

  /**
   * Test operations on completed UIs: they are loaded on demand, entries
   * are removed on overload.
   *
   * This effectively tests the original behavior of the history server's cache.
   */
  test("Completed UI get") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(1)
    implicit val cache = new ApplicationCache(operations, 2, clock)
    val metrics = cache.metrics
    // cache misses
    val app1 = "app-1"
    assertNotFound(app1, None)
    assertMetric("lookupCount", metrics.lookupCount, 1)
    assertMetric("lookupFailureCount", metrics.lookupFailureCount, 1)
    assert(1 === operations.getAppUICount, "getAppUICount")
    assertNotFound(app1, None)
    assert(2 === operations.getAppUICount, "getAppUICount")
    assert(0 === operations.attachCount, "attachCount")

    val now = clock.getTimeMillis()
    // add the entry
    operations.putAppUI(app1, None, true, now, now, now)

    // make sure its local
    operations.getAppUI(app1, None).get
    operations.getAppUICount = 0
    // now expect it to be found
    val cacheEntry = cache.lookupCacheEntry(app1, None)
    assert(1 === cacheEntry.probeTime)
    assert(cacheEntry.completed)
    // assert about queries made of the operations
    assert(1 === operations.getAppUICount, "getAppUICount")
    assert(1 === operations.attachCount, "attachCount")

    // and in the map of attached
    assert(operations.getAttached(app1, None).isDefined, s"attached entry '1' from $cache")

    // go forward in time
    clock.setTime(10)
    val time2 = clock.getTimeMillis()
    val cacheEntry2 = cache.get(app1)
    // no more refresh as this is a completed app
    assert(1 === operations.getAppUICount, "getAppUICount")
    assert(0 === operations.updateProbeCount, "updateProbeCount")
    assert(0 === operations.detachCount, "attachCount")

    // evict the entry
    operations.putAndAttach("2", None, true, time2, time2, time2)
    operations.putAndAttach("3", None, true, time2, time2, time2)
    cache.get("2")
    cache.get("3")

    // there should have been a detachment here
    assert(1 === operations.detachCount, s"detach count from $cache")
    // and entry app1 no longer attached
    assert(operations.getAttached(app1, None).isEmpty, s"get($app1) in $cache")
    val appId = "app1"
    val attemptId = Some("_01")
    val time3 = clock.getTimeMillis()
    operations.putAppUI(appId, attemptId, false, time3, 0, time3)
    // expect an error here
    assertNotFound(appId, None)
  }

  test("Test that if an attempt ID is is set, it must be used in lookups") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(1)
    implicit val cache = new ApplicationCache(operations, retainedApplications = 10, clock = clock)
    val appId = "app1"
    val attemptId = Some("_01")
    operations.putAppUI(appId, attemptId, false, clock.getTimeMillis(), 0, 0)
    assertNotFound(appId, None)
  }

  /**
   * Test that incomplete apps are not probed for updates during the time window,
   * but that they are checked if that window has expired and they are not completed.
   * Then, if they have changed, the old entry is replaced by a new one.
   */
  test("Incomplete apps refreshed") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(50)
    val window = 500
    implicit val cache = new ApplicationCache(operations, retainedApplications = 5, clock = clock)
    val metrics = cache.metrics
    // add the incomplete app
    // add the entry
    val started = clock.getTimeMillis()
    val appId = "app1"
    val attemptId = Some("001")
    operations.putAppUI(appId, attemptId, false, started, 0, started)
    val firstEntry = cache.lookupCacheEntry(appId, attemptId)
    assert(started === firstEntry.probeTime, s"timestamp in $firstEntry")
    assert(!firstEntry.completed, s"entry is complete: $firstEntry")
    assertMetric("lookupCount", metrics.lookupCount, 1)

    assert(0 === operations.updateProbeCount, "expected no update probe on that first get")

    val checkTime = window * 2
    clock.setTime(checkTime)
    val entry3 = cache.lookupCacheEntry(appId, attemptId)
    assert(firstEntry !== entry3, s"updated entry test from $cache")
    assertMetric("lookupCount", metrics.lookupCount, 2)
    assertMetric("updateProbeCount", metrics.updateProbeCount, 1)
    assertMetric("updateTriggeredCount", metrics.updateTriggeredCount, 0)
    assert(1 === operations.updateProbeCount, s"refresh count in $cache")
    assert(0 === operations.detachCount, s"detach count")
    assert(entry3.probeTime === checkTime)

    val updateTime = window * 3
    // update the cached value
    val updatedApp = operations.putAppUI(appId, attemptId, true, started, updateTime, updateTime)
    val endTime = window * 10
    clock.setTime(endTime)
    logDebug(s"Before operation = $cache")
    val entry5 = cache.lookupCacheEntry(appId, attemptId)
    assertMetric("lookupCount", metrics.lookupCount, 3)
    assertMetric("updateProbeCount", metrics.updateProbeCount, 2)
    // the update was triggered
    assertMetric("updateTriggeredCount", metrics.updateTriggeredCount, 1)
    assert(updatedApp === entry5.ui, s"UI {$updatedApp} did not match entry {$entry5} in $cache")

    // at which point, the refreshes stop
    clock.setTime(window * 20)
    assertCacheEntryEquals(appId, attemptId, entry5)
    assertMetric("updateProbeCount", metrics.updateProbeCount, 2)
  }

  /**
   * Assert that a metric counter has a specific value; failure raises an exception
   * including the cache's toString value
   * @param name counter name (for exceptions)
   * @param counter counter
   * @param expected expected value.
   * @param cache cache
   */
  def assertMetric(
      name: String,
      counter: Counter,
      expected: Long)
      (implicit cache: ApplicationCache): Unit = {
    val actual = counter.getCount
    if (actual != expected) {
      // this is here because Scalatest loses stack depth
      throw new Exception(s"Wrong $name value - expected $expected but got $actual in $cache")
    }
  }

  /**
   * Look up the cache entry and assert that it matches in the expected value.
   * This assertion works if the two CacheEntries are different -it looks at the fields.
   * UI are compared on object equality; the timestamp and completed flags directly.
   * @param appId application ID
   * @param attemptId attempt ID
   * @param expected expected value
   * @param cache app cache
   */
  def assertCacheEntryEquals(
      appId: String,
      attemptId: Option[String],
      expected: CacheEntry)
      (implicit cache: ApplicationCache): Unit = {
    val actual = cache.lookupCacheEntry(appId, attemptId)
    val errorText = s"Expected get($appId, $attemptId) -> $expected, but got $actual from $cache"
    assert(expected.ui === actual.ui, errorText + " SparkUI reference")
    assert(expected.completed === actual.completed, errorText + " -completed flag")
    assert(expected.probeTime === actual.probeTime, errorText + " -timestamp")
  }

  /**
   * Assert that a key wasn't found in cache or loaded.
   *
   * Looks for the specific nested exception raised by [[ApplicationCache]]
   * @param appId application ID
   * @param attemptId attempt ID
   * @param cache app cache
   */
  def assertNotFound(
      appId: String,
      attemptId: Option[String])
      (implicit cache: ApplicationCache): Unit = {
    val ex = intercept[UncheckedExecutionException] {
      cache.get(appId, attemptId)
    }
    var cause = ex.getCause
    assert(cause !== null)
    if (!cause.isInstanceOf[NoSuchElementException]) {
      throw cause
    }
  }

  test("Large Scale Application Eviction") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(0)
    val size = 5
    // only two entries are retained, so we expect evictions to occur on lookups
    implicit val cache: ApplicationCache = new TestApplicationCache(operations,
      retainedApplications = size, clock = clock)

    val attempt1 = Some("01")

    val ids = new ListBuffer[String]()
    // build a list of applications
    val count = 100
    for (i <- 1 to count ) {
      val appId = f"app-$i%04d"
      ids += appId
      clock.advance(10)
      val t = clock.getTimeMillis()
      operations.putAppUI(appId, attempt1, true, t, t, t)
    }
    // now go through them in sequence reading them, expect evictions
    ids.foreach { id =>
      cache.get(id, attempt1)
    }
    logInfo(cache.toString)
    val metrics = cache.metrics

    assertMetric("loadCount", metrics.loadCount, count)
    assertMetric("evictionCount", metrics.evictionCount, count - size)
}

  test("Attempts are Evicted") {
    val operations = new StubCacheOperations()
    implicit val cache: ApplicationCache = new TestApplicationCache(operations,
      retainedApplications = 4)
    val metrics = cache.metrics
    val appId = "app1"
    val attempt1 = Some("01")
    val attempt2 = Some("02")
    val attempt3 = Some("03")
    operations.putAppUI(appId, attempt1, true, 100, 110, 110)
    operations.putAppUI(appId, attempt2, true, 200, 210, 210)
    operations.putAppUI(appId, attempt3, true, 300, 310, 310)
    val attempt4 = Some("04")
    operations.putAppUI(appId, attempt4, true, 400, 410, 410)
    val attempt5 = Some("05")
    operations.putAppUI(appId, attempt5, true, 500, 510, 510)

    def expectLoadAndEvictionCounts(expectedLoad: Int, expectedEvictionCount: Int): Unit = {
      assertMetric("loadCount", metrics.loadCount, expectedLoad)
      assertMetric("evictionCount", metrics.evictionCount, expectedEvictionCount)
    }

    // first entry
    cache.get(appId, attempt1)
    expectLoadAndEvictionCounts(1, 0)

    // second
    cache.get(appId, attempt2)
    expectLoadAndEvictionCounts(2, 0)

    // no change
    cache.get(appId, attempt2)
    expectLoadAndEvictionCounts(2, 0)

    // eviction time
    cache.get(appId, attempt3)
    cache.size() should be(3)
    cache.get(appId, attempt4)
    expectLoadAndEvictionCounts(4, 0)
    cache.get(appId, attempt5)
    expectLoadAndEvictionCounts(5, 1)
    cache.get(appId, attempt5)
    expectLoadAndEvictionCounts(5, 1)

  }

  test("Instantiate Filter") {
    // this is a regression test on the filter being constructable
    val clazz = Utils.classForName(ApplicationCacheCheckFilterRelay.FILTER_NAME)
    val instance = clazz.newInstance()
    instance shouldBe a [Filter]
  }

  test("redirect includes query params") {
    val clazz = Utils.classForName(ApplicationCacheCheckFilterRelay.FILTER_NAME)
    val filter = clazz.newInstance().asInstanceOf[ApplicationCacheCheckFilter]
    filter.appId = "local-123"
    val cache = mock[ApplicationCache]
    when(cache.checkForUpdates(any(), any())).thenReturn(true)
    ApplicationCacheCheckFilterRelay.setApplicationCache(cache)
    val request = mock[HttpServletRequest]
    when(request.getMethod()).thenReturn("GET")
    when(request.getRequestURI()).thenReturn("http://localhost:18080/history/local-123/jobs/job/")
    when(request.getQueryString()).thenReturn("id=2")
    val resp = mock[HttpServletResponse]
    when(resp.encodeRedirectURL(any())).thenAnswer(new Answer[String]() {
      override def answer(invocationOnMock: InvocationOnMock): String = {
        invocationOnMock.getArguments()(0).asInstanceOf[String]
      }
    })
    filter.doFilter(request, resp, null)
    verify(resp).sendRedirect("http://localhost:18080/history/local-123/jobs/job/?id=2")
  }

}
