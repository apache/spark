/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.history

import java.util.NoSuchElementException

import com.google.common.base.Ticker
import com.google.common.util.concurrent.UncheckedExecutionException

import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.ui.SparkUI
import org.apache.spark.{SparkConf, Logging, SparkFunSuite}

class ApplicationCacheSuite extends SparkFunSuite with Logging {

  /**
   * cache operations
   */
  class StubCacheOperations extends ApplicationCacheOperations {

    var instances = scala.collection.mutable.HashMap.empty[String, (SparkUI, Boolean)]

    var attached = scala.collection.mutable.HashMap.empty[String, (SparkUI, Boolean)]

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
        attemptId: Option[String]): Option[(SparkUI, Boolean)] = {
      getCount +=1
      instances.get(appId)
    }

    /** Attach a reconstructed UI  */
    override def attachSparkUI(ui: SparkUI, completed: Boolean): Unit  = {
      attachCount += 1
      attached += (ui.appName -> (ui, completed))
    }


    def create(name: String, completed: Boolean): SparkUI  = {
      val ui = newUI(name)
      instances += (name -> (ui, completed))
      ui
    }

    /**
     * Detach a reconstructed UI
     *
     * @param ui Spark UI
     * @param refreshInProgress flag to indicate this was triggered by a refresh of an
     *                          incomplete application
     */
    override def detachSparkUI(ui: SparkUI, refreshInProgress: Boolean): Unit = {
      detachCount += 1
      if (refreshInProgress) {
        refreshCount += 1
      }
      attached -= ui.appName
    }
  }

  /**
   * Ticker class for testing
   * @param now initial time
   */
  class FakeTicker(var now: Long) extends Ticker {

    override def read(): Long = {
      now
    }

    def tick(): Unit =  {
      now += 1
    }

    def set(l: Long): Unit = {
      now = l;
    }
  }


  def newUI(name: String) : SparkUI =  {
    SparkUI.createHistoryUI(new SparkConf(),
      new ReplayListenerBus(),null, name, "", 0)
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
    assert(ex.getCause !== null)
    ex.getCause.asInstanceOf[NoSuchElementException]
  }

  /**
   * Test operations on completed UIs: they are loaded on demand, entries
   * are removed on overload
   */
  test("Completed UI get") {
    val operations = new StubCacheOperations()
    val ticker = new FakeTicker(0)
    val cache = new ApplicationCache(operations, 5, 2, ticker)
    // cache misses
    assertNotLoaded(cache, "1")
    assert(1 == operations.getCount)
    assertNotLoaded(cache, "1")
    assert(2 == operations.getCount)
    assert(0 == operations.attachCount)

    // add the entry
    operations.instances += ("1" -> (newUI("1"), true))
    operations.create("1", true)

    // now expect it to be found
    val cacheEntry = cache.get("1")
    assert(3 == operations.getCount)
    assert(1 == operations.attachCount)

    // and in the cache of attached
    assert(None !== operations.attached.get("1"))

    //go forward in time
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
    assert(1 === operations.detachCount)
    /// and entry "1" no longer attached
    assert(None === operations.attached.get("1"))

  }

}
