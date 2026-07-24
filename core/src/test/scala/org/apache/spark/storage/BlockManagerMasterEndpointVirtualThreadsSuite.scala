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

package org.apache.spark.storage

import java.util.concurrent.{ExecutorService, ThreadPoolExecutor, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.STORAGE_BLOCK_MANAGER_MASTER_VIRTUAL_THREADS
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Unit tests for the virtual-thread-backed ask thread pool in
 * [[BlockManagerMasterEndpoint]] (see SPARK-49807).
 */
class BlockManagerMasterEndpointVirtualThreadsSuite extends SparkFunSuite {

  /**
   * Returns whether the given Thread is a virtual thread, using reflection so
   * this source compiles on Java 17 (the Thread.isVirtual method was added in
   * Java 21).
   */
  private def isVirtualThread(t: Thread): Boolean = {
    try {
      t.getClass.getMethod("isVirtual").invoke(t).asInstanceOf[Boolean]
    } catch {
      case _: NoSuchMethodException => false
    }
  }

  /** Submit a task to the pool and return whether it ran on a virtual thread. */
  private def runOnPool(pool: ExecutorService): Boolean = {
    val ec = ExecutionContext.fromExecutor(pool)
    ThreadUtils.awaitResult(Future { isVirtualThread(Thread.currentThread()) }(ec), 5.seconds)
  }

  /** Submit a task to the pool and return the executing thread's name. */
  private def threadNameFromPool(pool: ExecutorService): String = {
    val ec = ExecutionContext.fromExecutor(pool)
    ThreadUtils.awaitResult(Future { Thread.currentThread().getName }(ec), 5.seconds)
  }

  private def shutdownQuietly(pool: ExecutorService): Unit = {
    pool.shutdownNow()
    pool.awaitTermination(5, TimeUnit.SECONDS)
  }

  test("shouldUseVirtualThreads requires both Java 21+ and config enabled") {
    val confOff = new SparkConf()
    assert(!BlockManagerMasterEndpoint.shouldUseVirtualThreads(confOff),
      "Default config should not enable virtual threads")

    val confOn = new SparkConf().set(STORAGE_BLOCK_MANAGER_MASTER_VIRTUAL_THREADS, true)
    assert(
      BlockManagerMasterEndpoint.shouldUseVirtualThreads(confOn) ===
        Utils.isJavaVersionAtLeast21,
      "When config is enabled, virtual threads should be used iff Java is 21+")
  }

  test("default config returns a ThreadPoolExecutor fallback") {
    val conf = new SparkConf()
    val pool = BlockManagerMasterEndpoint.createAskThreadPool(conf)
    try {
      assert(pool.isInstanceOf[ThreadPoolExecutor],
        s"Default config should produce a ThreadPoolExecutor, got ${pool.getClass.getName}")
      assert(!runOnPool(pool),
        "Default config should run tasks on a platform (non-virtual) thread")
    } finally {
      shutdownQuietly(pool)
    }
  }

  test("config enabled on Java 21+ returns a virtual-thread executor") {
    assume(Utils.isJavaVersionAtLeast21,
      "Requires Java 21+ to use virtual threads")
    val conf = new SparkConf().set(STORAGE_BLOCK_MANAGER_MASTER_VIRTUAL_THREADS, true)
    val pool = BlockManagerMasterEndpoint.createAskThreadPool(conf)
    try {
      assert(!pool.isInstanceOf[ThreadPoolExecutor],
        s"Virtual-thread executor should not be a ThreadPoolExecutor, " +
          s"got ${pool.getClass.getName}")
      assert(runOnPool(pool),
        "Enabled config on Java 21+ should run tasks on a virtual thread")
      val name = threadNameFromPool(pool)
      assert(name.startsWith("block-manager-ask-"),
        s"Virtual thread name should start with 'block-manager-ask-', got '$name'")
    } finally {
      shutdownQuietly(pool)
    }
  }

  test("config enabled on Java < 21 falls back to ThreadPoolExecutor") {
    assume(!Utils.isJavaVersionAtLeast21,
      "Only meaningful on Java < 21")
    val conf = new SparkConf().set(STORAGE_BLOCK_MANAGER_MASTER_VIRTUAL_THREADS, true)
    val pool = BlockManagerMasterEndpoint.createAskThreadPool(conf)
    try {
      assert(pool.isInstanceOf[ThreadPoolExecutor],
        s"Java < 21 should always fall back to ThreadPoolExecutor, " +
          s"got ${pool.getClass.getName}")
      assert(!runOnPool(pool),
        "Java < 21 fallback should run tasks on a platform thread")
    } finally {
      shutdownQuietly(pool)
    }
  }

  test("pool supports concurrent submissions and shutdown") {
    val conf = new SparkConf().set(STORAGE_BLOCK_MANAGER_MASTER_VIRTUAL_THREADS, true)
    val pool = BlockManagerMasterEndpoint.createAskThreadPool(conf)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    try {
      // Submit a modest fan-out; each task blocks briefly to exercise concurrency.
      val futures = (1 to 50).map { i =>
        Future {
          Thread.sleep(20)
          i
        }
      }
      val sum = ThreadUtils.awaitResult(Future.sequence(futures), 30.seconds).sum
      assert(sum === (1 to 50).sum)
    } finally {
      shutdownQuietly(pool)
      assert(pool.isShutdown, "Pool should be shut down")
    }
  }
}
