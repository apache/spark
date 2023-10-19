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

package org.apache.spark.util;

import java.util.concurrent.ExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicReference

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader

import org.apache.spark.SparkFunSuite

object NonFateSharingCacheSuite {
  private val TEST_KEY = "key"
  private val FAIL_MESSAGE = "loading failed"
  private val THREAD2_HOLDER = new AtomicReference[Thread](null)

  trait InternalTestStatus {
    var intentionalFail: ThreadLocal[Boolean] = ThreadLocal.withInitial(() => false)
    var startLoading = new Semaphore(0)

    def waitUntilThread2Waiting(): Unit = {
      while (true) {
        Thread.sleep(100)
        if (Option(THREAD2_HOLDER.get()).exists(_.getState.equals(Thread.State.WAITING))) {
          return
        }
      }
    }
  }


  class TestCacheLoader extends CacheLoader[String, String] with InternalTestStatus {
    override def load(key: String): String = {
      startLoading.release()
      if (Thread.currentThread().getName.contains("test-executor1")) {
        waitUntilThread2Waiting()
      }
      if (intentionalFail.get) throw new RuntimeException(FAIL_MESSAGE)
      key
    }
  }

  class TestLoaderFunc extends InternalTestStatus {
    def func: String => String = key => {
      startLoading.release()
      if (Thread.currentThread().getName.contains("test-executor1")) {
        waitUntilThread2Waiting()
      }
      if (intentionalFail.get) throw new RuntimeException(FAIL_MESSAGE)
      key
    }
  }

}

/**
 * Test non-fate-sharing behavior
 */
class NonFateSharingCacheSuite extends SparkFunSuite {

  type WorkerFunc = () => Unit

  import NonFateSharingCacheSuite._

  test("loading cache loading failure should not affect concurrent query on same key") {
    val loader = new TestCacheLoader
    val loadingCache0: NonFateSharingLoadingCache[String, String] =
      NonFateSharingCache(CacheBuilder.newBuilder.build(loader))
    val loaderFunc = new TestLoaderFunc
    val loadingCache1: NonFateSharingLoadingCache[String, String] =
      NonFateSharingCache(loaderFunc.func)
    Seq((loadingCache0, loader), (loadingCache1, loaderFunc)).foreach {
      case (loadingCache, status) =>
        val thread1Task: WorkerFunc = () => {
          status.intentionalFail.set(true)
          loadingCache.get(TEST_KEY)
        }
        val thread2Task: WorkerFunc = () => {
          loadingCache.get(TEST_KEY)
        }
        testImpl(loadingCache, status, thread1Task, thread2Task)
    }
  }

  test("loading cache mix usage of default loader and provided loader") {
    // Intentionally mix usage of default loader and provided value loader.
    val loader = new TestCacheLoader
    val loadingCache: NonFateSharingLoadingCache[String, String] =
      NonFateSharingCache(CacheBuilder.newBuilder.build(loader))
    val thread1Task: WorkerFunc = () => {
      loader.intentionalFail.set(true)
      loadingCache.get(TEST_KEY, () => loader.load(TEST_KEY)
      )
    }
    val thread2Task: WorkerFunc = () => {
      loadingCache.get(TEST_KEY)
    }
    testImpl(loadingCache, loader, thread1Task, thread2Task)
  }

  test("cache loading failure should not affect concurrent query on same key") {
    val loader = new TestCacheLoader
    val cache = NonFateSharingCache(CacheBuilder.newBuilder.build[String, String])
    val thread1Task: WorkerFunc = () => {
      loader.intentionalFail.set(true)
      cache.get(
        TEST_KEY,
        () => loader.load(TEST_KEY)
      )
    }
    val thread2Task: WorkerFunc = () => {
      cache.get(
        TEST_KEY,
        () => loader.load(TEST_KEY)
      )
    }
    testImpl(cache, loader, thread1Task, thread2Task)
  }

  def testImpl(
      cache: NonFateSharingCache[String, String],
      internalTestStatus: InternalTestStatus,
      thread1Task: WorkerFunc,
      thread2Task: WorkerFunc): Unit = {
    val executor1 = ThreadUtils.newDaemonSingleThreadExecutor("test-executor1")
    val executor2 = ThreadUtils.newDaemonSingleThreadExecutor("test-executor2")
    val r1: Runnable = () => thread1Task()
    val r2: Runnable = () => {
      internalTestStatus.startLoading.acquire() // wait until thread1 start loading
      THREAD2_HOLDER.set(Thread.currentThread())
      thread2Task()
    }
    val f1 = executor1.submit(r1)
    val f2 = executor2.submit(r2)
    // thread1 should fail intentionally
    val e = intercept[ExecutionException] {
      f1.get
    }
    assert(e.getMessage.contains(FAIL_MESSAGE))

    f2.get // thread 2 should not be affected by thread 1 failure
    assert(cache.getIfPresent(TEST_KEY) != null)
  }
}
