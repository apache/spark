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

import com.google.common.base.Ticker
import com.google.common.util.concurrent.UncheckedExecutionException
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import org.apache.spark.status.api.v1.{ApplicationAttemptInfo => AttemptInfo, ApplicationInfo}
import org.apache.spark.ui.SparkUI
import org.apache.spark.{Logging, SparkFunSuite}

class ApplicationCacheSuite extends SparkFunSuite with Logging with MockitoSugar {

  /**
   * cache operations
   */
  class StubCacheOperations extends ApplicationCacheOperations {

    var instances = scala.collection.mutable.HashMap.empty[String, SparkUI]

    var attached = scala.collection.mutable.HashMap.empty[String, SparkUI]

    var getCount = 0L
    var attachCount = 0L
    var detachCount = 0L
    var refreshCount = 0L

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

    def create(name: String, completed: Boolean): SparkUI = {
      val ui = newUI(name, completed)
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
      attached -= ui.getAppName
    }

    /**
     * Notification of a refresh. This will be followed by the normal
     * detach/attach operations
     * @param key
     * @param ui
     */
    override def refreshTriggered(key: String, ui: SparkUI): Unit = {
      refreshCount += 1
    }
  }

  /**
   * Ticker class for testing
   * @param now initial time
   */
  class FakeTicker(var now: Long) extends Ticker {

    override def read(): Long =  now

    def tick(): Unit = {
      now += 1
    }

    def set(l: Long): Unit = {
      now = l;
    }
  }

  def newUI(name: String, completed: Boolean): SparkUI =  {
    val date = new Date(0)
    val info = new ApplicationInfo(name, name,
      Some(1), Some(1), Some(1), Some(64),
      Seq(new AttemptInfo(None, date, date, "user", completed)))
    val ui = mock[SparkUI]
    when(ui.getApplicationInfoList).thenReturn(List(info).iterator)
    when(ui.getAppName).thenReturn(name)
    when(ui.appName).thenReturn(name)
    ui
  }

  /**
   * Assert that a key wasn't found in cache or loaded.
   * <p>
   * Looks for the specific nested exception raised by [[ApplicationCache]]
   * @param cache
   * @param key
   */
  def assertNotLoaded(cache: ApplicationCache, key: String) {
    val ex = intercept[UncheckedExecutionException] {
      cache.get("key")
    }
    var cause = ex.getCause
    assert(cause !== null)
    if(!cause.isInstanceOf[NoSuchElementException]) {
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
    val ticker = new FakeTicker(1)
    val cache = new ApplicationCache(operations, 5, 2, ticker)
    // cache misses
    assertNotLoaded(cache, "1")
    assert(1 == operations.getCount)
    assertNotLoaded(cache, "1")
    assert(2 == operations.getCount)
    assert(0 == operations.attachCount)

    // add the entry
    operations.instances += ("1" -> (newUI("1", true)))
    operations.create("1", true)

    // now expect it to be found
    val cacheEntry = cache.get("1")
    assert(1 === cacheEntry.timestamp)
    assert(cacheEntry.completed)
    assert(3 == operations.getCount)
    assert(1 == operations.attachCount)

    // and in the map of attached
    assert(None !== operations.attached.get("1"), s"attached entry '1' from $cache")

    // go forward in time
    ticker.set(10)
    val cacheEntry2 = cache.get("1")
    // no more refresh as this is a completed app
    assert(3 == operations.getCount)
    assert(0 == operations.detachCount)

    // evict the entry
    operations.create("2", true)
    operations.create("3", true)
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
  test("Incomplete") {
    val operations = new StubCacheOperations()
    val ticker = new FakeTicker(1)
    val cache = new ApplicationCache(operations, 5, 10, ticker)
    // add the incomplete app
    operations.instances += ("inc" ->(newUI("inc", false)))
    val entry = cache.get("inc")
    assert(1 === entry.timestamp)
    assert(!entry.completed)

    // a get within the refresh window returns the same value
    ticker.set(3)
    val entry2 = cache.get("inc")
    assert (entry === entry2)

    // but now move the ticker past that refresh
    ticker.set(15)
    assert((ticker.read() - entry.timestamp) > cache.refreshInterval)
    val entry3 = cache.get("inc")
    assert(entry !== entry3, s"updated entry test from $cache")
    assert(1 === operations.refreshCount, s"refresh count in $cache")
    assert(1 === operations.detachCount, s"detach count in $cache")
    assert(None !== operations.attached.get("inc"), s"attached['inc'] in $cache")
    assert(entry3.timestamp === 15)

    // go a little bit forward again
    ticker.set(17)
    val entry4 = cache.get("inc")
    assert(entry3 === entry4)

    // and a short time later, and again the entry can be updated.
    // here with a now complete entry
    ticker.set(30)
    operations.instances += ("inc" ->(newUI("inc", true)))
    val entry5 = cache.get("inc")
    assert(entry5.completed)

    // at which point, the refreshes stop
    ticker.set(40)
    val entry6 = cache.get("inc")
    assert(entry5 === entry6)

  }

}
