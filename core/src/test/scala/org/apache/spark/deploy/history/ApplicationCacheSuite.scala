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

    val instances = mutable.HashMap.empty[String, SparkUI]

    val attached = mutable.HashMap.empty[String, SparkUI]

    var getCount = 0L
    var attachCount = 0L
    var detachCount = 0L
    var probeCount = 0L
    // set this for the update probes to start signalling attempts as out of date
    var lastUpdatedTime = 0L

    /**
     * Get the application UI
     * @param appId application ID
     * @param attemptId attempt ID
     * @return (the Spark UI, completed flag)
     */
    override def getAppUI(appId: String,
        attemptId: Option[String]): Option[SparkUI] = {
      getCount +=1
      instances.get(appId)
    }

    /** Attach a reconstructed UI  */
    override def attachSparkUI(ui: SparkUI, completed: Boolean): Unit = {
      attachCount += 1
      val name = ui.getAppName
      assert(name != null, s"no name for spark UI $ui")
      attached += (name -> ui)
    }

    def create(name: String, completed: Boolean, started: Long, ended: Long): SparkUI = {
      val ui = newUI(name, completed, started, ended)
      instances += (name -> ui)
      ui
    }

    /**
     * Detach a reconstructed UI
     *
     * @param ui Spark UI
     */
    override def detachSparkUI(ui: SparkUI): Unit = {
      detachCount += 1
      var name = ui.getAppName
      attached.getOrElse(name, { throw new scala.NoSuchElementException() })
      attached -= name
    }

    /**
     * @return true if `lastUpdatedTime` is greater than `updateTimeMillis`
     */
    override def isUpdated(appId: String, attemptId: Option[String],
        updateTimeMillis: Long): Boolean = {
      probeCount += 1
      lastUpdatedTime > updateTimeMillis
    }
  }

  def newUI(name: String, completed: Boolean, started: Long, ended: Long): SparkUI =  {
    val info = new ApplicationInfo(name, name, Some(1), Some(1), Some(1), Some(64),
      Seq(new AttemptInfo(None, new Date(started), new Date(ended), "user", completed)))
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
  def assertNotLoaded(cache: ApplicationCache, appId: String, attemptId: Option[String]) {
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
    assertNotLoaded(cache, "1", None)
    assert(1 == operations.getCount)
    assertNotLoaded(cache, "1", None)
    assert(2 == operations.getCount)
    assert(0 == operations.attachCount)

    val now = clock.getTimeMillis()
    // add the entry
    operations.instances += ("1" -> (newUI("1", true, now, now)))
    operations.create("1", true, now, now)

    // now expect it to be found
    val cacheEntry = cache.get("1")
    assert(1 === cacheEntry.timestamp)
    assert(cacheEntry.completed)
    assert(3 == operations.getCount)
    assert(1 == operations.attachCount)

    // and in the map of attached
    assert(None !== operations.attached.get("1"), s"attached entry '1' from $cache")

    // go forward in time
    clock.setTime(10)
    val time2 = clock.getTimeMillis()
    val cacheEntry2 = cache.get("1")
    // no more refresh as this is a completed app
    assert(3 == operations.getCount)
    assert(0 == operations.detachCount)

    // evict the entry
    operations.create("2", true, time2, time2)
    operations.create("3", true, time2, time2)
    cache.get("2")
    cache.get("3")

    // there should have been a detachment here
    assert(1 === operations.detachCount, s"detach count from $cache")
    // and entry "1" no longer attached
    assert(None === operations.attached.get("1"), s"get(1) in $cache")

  }

  /**
   * Now verify that incomplete applications are refreshed
   */
  test("Incomplete apps refreshed") {
    val operations = new StubCacheOperations()
    val clock = new ManualClock(1)
    val cache = new ApplicationCache(operations, 5, 10, clock)
    // add the incomplete app
    // add the entry
    val started = clock.getTimeMillis()
    operations.instances += ("inc" ->(newUI("inc", false, started, 0)))
    val entry = cache.get("inc")
    assert(1 === entry.timestamp)
    assert(!entry.completed)

    // a get within the refresh window returns the same value
    clock.setTime(3)
    val entry2 = cache.get("inc")
    assert (entry === entry2)

    // but now move the ticker past that refresh
    clock.setTime(15)
    assert((clock.getTimeMillis() - entry.timestamp) > cache.refreshInterval)
    val entry3 = cache.get("inc")
    assert(entry !== entry3, s"updated entry test from $cache")
    assert(1 === operations.probeCount, s"refresh count in $cache")
    assert(1 === operations.detachCount, s"detach count in $cache")
    assert(None !== operations.attached.get("inc"), s"attached['inc'] in $cache")
    assert(entry3.timestamp === 15)

    // go a little bit forward again
    clock.setTime(17)
    val entry4 = cache.get("inc")
    assert(entry3 === entry4)

    // and a short time later, and again the entry can be updated.
    // here with a now complete entry
    clock.setTime(30)
    val ended = clock.getTimeMillis()

    operations.instances += ("inc" ->(newUI("inc", true, started, ended)))
    val entry5 = cache.get("inc")
    assert(entry5.completed)

    // at which point, the refreshes stop
    clock.setTime(40)
    val entry6 = cache.get("inc")
    assert(entry5 === entry6)

  }

}
