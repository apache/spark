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

import scala.collection.mutable

import com.codahale.metrics.Counter
import com.google.common.util.concurrent.UncheckedExecutionException
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import org.apache.spark.status.api.v1.{ApplicationAttemptInfo => AttemptInfo, ApplicationInfo}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.ManualClock
import org.apache.spark.{Logging, SparkFunSuite}

class ApplicationCacheSuite extends SparkFunSuite with Logging with MockitoSugar {

  /**
   * cache operations
   */
  class StubCacheOperations extends ApplicationCacheOperations {

    // map to UI instances, including timestamps, which are used in update probes
    val instances = mutable.HashMap.empty[CacheKey, CacheEntry]

    val attached = mutable.HashMap.empty[CacheKey, SparkUI]

    var getAppUICount = 0L
    var attachCount = 0L
    var detachCount = 0L
    var updateProbeCount = 0L

    /**
     * Get the application UI
     * @param appId application ID
     * @param attemptId attempt ID
     * @return (the Spark UI, completed flag)
     */
    override def getAppUI(appId: String, attemptId: Option[String]): Option[SparkUI] = {
      getAppUICount +=1
      instances.get(CacheKey(appId, attemptId)).map(_.ui)
    }

    override def attachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI,
        completed: Boolean): Unit = {
      attachCount += 1
      attached += (CacheKey(appId, attemptId) -> ui)
    }

    def putAndAttach(appId: String, attemptId: Option[String], completed: Boolean, started: Long, ended: Long): SparkUI = {
      val ui = putAppUI(appId, attemptId, completed, started, ended)
      attachSparkUI(appId, attemptId, ui, completed)
      ui
    }
    
    def putAppUI(appId: String, attemptId: Option[String], completed: Boolean, started: Long, ended: Long, timestamp: Long = 0): SparkUI = {
      val ui = newUI(appId, attemptId, completed, started, ended)
      putInstance(appId, attemptId, ui, completed, timestamp)
      ui
    }

    def putInstance(appId: String, attemptId: Option[String], ui: SparkUI, completed: Boolean, timestamp: Long): Unit = {
      instances += (CacheKey(appId, attemptId) -> new CacheEntry(ui, completed, timestamp))
    }

    /**
     * Detach a reconstructed UI
     *
     * @param ui Spark UI
     */
    override def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit = {
      detachCount += 1
      var name = ui.getAppName
      val key = CacheKey(appId, attemptId)
      attached.getOrElse(key , { throw new scala.NoSuchElementException() })
      attached -= key
    }

    /**
     * @return true if the timestamp on a cached instance is greater than `updateTimeMillis`.
     */
    override def isUpdated(appId: String, attemptId: Option[String],
        updateTimeMillis: Long): Boolean = {
      updateProbeCount += 1
      instances.get(CacheKey(appId, attemptId)).get.timestamp > updateTimeMillis
    }

    /**
     * Lookup from the internal cache of attached UIs
     */
    def getAttached(appId: String, attemptId: Option[String]): Option[SparkUI] = {
      attached.get(CacheKey(appId, attemptId ))
    }

  }

  /**
   * Create a new UI. The info/attempt info classes here are from the package
   * `org.apache.spark.status.api.v1`, not the near-equivalents from the history package
   */
  def newUI(name: String, attemptId: Option[String], completed: Boolean, started: Long, ended: Long): SparkUI =  {
    val info = new ApplicationInfo(name, name, Some(1), Some(1), Some(1), Some(64),
      Seq(new AttemptInfo(attemptId, new Date(started), new Date(ended), "user", completed)))
    val ui = mock[SparkUI]
    when(ui.getApplicationInfoList).thenReturn(List(info).iterator)
    when(ui.getAppName).thenReturn(name)
    when(ui.appName).thenReturn(name)
    ui
  }

  /**
   * Assert that a key wasn't found in cache or loaded.
   *
   * Looks for the specific nested exception raised by [[ApplicationCache]]
   * @param cache
   * @param appId
   */
  def assertNotFound(cache: ApplicationCache, appId: String, attemptId: Option[String]) {
    val ex = intercept[UncheckedExecutionException] {
      cache.get(appId, attemptId)
    }
    var cause = ex.getCause
    assert(cause !== null)
    if (!cause.isInstanceOf[NoSuchElementException]) {
      throw cause;
    }
  }

  /**
   * Now verify that incomplete applications are refreshed
   */
  def assertLookupFails(cache: ApplicationCache, appId: String, attemptId: Option[String]): Unit = {
    val ex = intercept[UncheckedExecutionException] {
      cache.get(appId, attemptId)
    }
    if (ex.getCause == null || !ex.getCause.isInstanceOf[scala.NoSuchElementException]) {
      throw ex;
    }
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
    val cache = new ApplicationCache(operations, 5, 2, clock)
    // cache misses
    val app1 = "app-1"
    assertNotFound(cache, app1, None)
    assert(1 === cache.lookupCount.getCount)
    assert(1 === cache.lookupFailureCount.getCount)
    assert(1 === operations.getAppUICount, "getAppUICount")
    assertNotFound(cache, app1, None)
    assert(2 === operations.getAppUICount, "getAppUICount")
    assert(0 === operations.attachCount, "attachCount")

    val now = clock.getTimeMillis()
    // add the entry
    operations.putAppUI(app1, None, true, now, now)

    // make sure its local
    operations.getAppUI(app1, None).get
    operations.getAppUICount = 0
    // now expect it to be found
    val cacheEntry = cache.lookupCacheEntry(app1, None)
    assert(1 === cacheEntry.timestamp)
    assert(cacheEntry.completed)
    // assert about queries made of the opereations
    assert(1 === operations.getAppUICount, "getAppUICount")
    assert(1 === operations.attachCount, "attachCount")
    // and the metrics of the cache



    // and in the map of attached
    assert(operations.getAttached(app1, None).isDefined, s"attached entry '1' from $cache")

    // go forward in time
    clock.setTime(10)
    val time2 = clock.getTimeMillis()
    val cacheEntry2 = cache.get(app1)
    // no more refresh as this is a completed app
    assert(1 === operations.getAppUICount, "getAppUICount")
    assert(0 === operations.updateProbeCount, "probeCount")
    assert(0 === operations.detachCount, "attachCount")

    // evict the entry
    operations.putAndAttach("2", None, true, time2, time2)
    operations.putAndAttach("3", None, true, time2, time2)
    cache.get("2")
    cache.get("3")

    // there should have been a detachment here
    assert(1 === operations.detachCount, s"detach count from $cache")
    // and entry app1 no longer attached
    assert(operations.getAttached(app1, None).isEmpty, s"get($app1) in $cache")
    val appId = "app1"
    val attemptId = Some("_01")
    operations.putAppUI(appId, attemptId, false, clock.getTimeMillis(), 0)
    // expect an error here
    assertNotFound(cache, appId, None)
  }

  test("Naming") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(1)
    val cache = new ApplicationCache(operations, 5, 10, clock)
    val appId = "app1"
    val attemptId = Some("_01")
    operations.putAppUI(appId, attemptId, false,  clock.getTimeMillis(), 0)
    assertLookupFails(cache, appId, None)
  }

  test("Incomplete apps refreshed") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(1)
    val cache = new ApplicationCache(operations, 5, 10, clock)
    // add the incomplete app
    // add the entry
    val started = clock.getTimeMillis()
    val appId = "app1"
    val attemptId = Some("001")
    operations.putAppUI(appId, attemptId, false, started, 0)
    val entry = cache.lookupCacheEntry(appId, attemptId)
    assert(1 === entry.timestamp)
    assert(!entry.completed)
    assertCount(cache, "lookupCount", cache.lookupCount, 1)

    assert(0 === operations.updateProbeCount, "expected no update probe on that first get")

    // a get within the refresh window returns the same value
    clock.setTime(3)
    assertCacheEntryEquals(cache, appId, attemptId, entry)
    assertCount(cache, "updateProbeCount", cache.updateProbeCount, 0)
    assertCount(cache, "lookupCount", cache.lookupCount, 2)
    assert(0 === operations.updateProbeCount, "expected no updated probe within the time window")

    // but now move the ticker past that refresh
    clock.setTime(15)
    assert((clock.getTimeMillis() - entry.timestamp) > cache.refreshInterval)
    val entry3 = cache.lookupCacheEntry(appId, attemptId)
    assert(entry !== entry3, s"updated entry test from $cache")
    assertCount(cache, "updateProbeCount", cache.updateProbeCount, 1)
    assertCount(cache, "updateTriggeredCount", cache.updateTriggeredCount, 0)
    assert(1 === operations.updateProbeCount, s"refresh count in $cache")
    assert(0 === operations.detachCount, s"detach count")
    assert(entry3.timestamp === 15)

    val updateTime = clock.getTimeMillis()
    // update the cached value. This won't get picked up on until after the refresh interval
    val completedRunUI = newUI(appId, attemptId, true, started, updateTime)
        operations.putInstance(appId, attemptId, completedRunUI, true, updateTime )

    // go a little bit forward again and the refresh window means no new probe
    clock.setTime(17)
    assertCacheEntryEquals(cache, appId, attemptId, entry3)
    assertCount(cache, "updateProbeCount", cache.updateProbeCount, 1)

    // and a short time later, and again the entry can be updated.
    // here with a now complete entry
    clock.setTime(30)
    val ended = clock.getTimeMillis()

    operations.putAppUI(appId, attemptId , true, started, ended)
    val entry5 = cache.lookupCacheEntry(appId, attemptId)

    assertCount(cache, "updateProbeCount", cache.updateProbeCount, 2)
    assertCount(cache, "updateTriggeredCount", cache.updateTriggeredCount, 1)
    assert(entry5.completed, s"$entry5")
    assert(completedRunUI === entry5.ui, s"$entry5")

    // at which point, the refreshes stop
    clock.setTime(40)
    assertCacheEntryEquals(cache, appId, attemptId, entry5)

  }

  def assertCount(cache: ApplicationCache, name: String, c: Counter, expected: Long): Unit = {
    assertResult(expected, s"Wrong counter value for $name in $cache") { c.getCount }
  }

  /**
   * Look up the cache entry and assert that it maches in the expected value.
   * This assertion works if the two CacheEntries are different -it looks at the fields.
   * UI are compared on object equality; the timestamp and completed flags directly.
   */
  def assertCacheEntryEquals(cache: ApplicationCache, appId: String, attemptId: Option[String],
      expected: CacheEntry): Unit = {
    val actual = cache.lookupCacheEntry(appId, attemptId)
    assert(expected.ui === actual.ui,
      s"Expected get($appId, $attemptId) -> $expected, but got $actual from $cache")
    assert(expected.completed === actual.completed,
      s"Expected get($appId, $attemptId) -> $expected, but got $actual from $cache")
    assert(expected.timestamp === actual.timestamp,
      s"Expected get($appId, $attemptId) -> $expected, but got $actual from $cache")
  }
}
