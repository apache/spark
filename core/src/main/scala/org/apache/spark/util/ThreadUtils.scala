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


package org.apache.spark.util

import java.util.concurrent._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}

private[spark] object ThreadUtils {

  private val sameThreadExecutionContext =
    ExecutionContext.fromExecutorService(MoreExecutors.sameThreadExecutor())

  /**
   * An `ExecutionContextExecutor` that runs each task in the thread that invokes `execute/submit`.
   * The caller should make sure the tasks running in this `ExecutionContextExecutor` are short and
   * never block.
   */
  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  /**
   * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
   */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  /**
   * Wrapper over newCachedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
   * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(prefix: String, maxThreadNumber: Int): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    new ThreadPoolExecutor(
      0, maxThreadNumber, 60L, TimeUnit.SECONDS, new SynchronousQueue[Runnable], threadFactory)
  }

  /**
   * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Wrapper over newSingleThreadExecutor.
   */
  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadExecutor(threadFactory)
  }

  /**
   * Wrapper over newSingleThreadScheduledExecutor.
   */
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadScheduledExecutor(threadFactory)
  }
}
