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

import java.util
import java.util.concurrent._

import com.google.common.util.concurrent.Futures

// scalastyle:off
/**
 * This thread pool executor throttles the submission of new tasks by using a semaphore.
 * Task submissions require permits, task completions release permits.
 * <p>
 * NOTE: [[invoke*]] methods are not supported, you should either use the [[submit]] methods
 * or the [[execute]] method.
 * <p>
 * This is inspired by
 * <a href="https://github.com/apache/incubator-retired-s4/blob/0.6.0-Final/subprojects/s4-comm/src/main/java/org/apache/s4/comm/staging/BlockingThreadPoolExecutorService.java">
 * Apache S4 BlockingThreadPoolExecutorService</a>
 */
// scalastyle:on
private[spark] class BlockingThreadPoolExecutorService(
    nThreads: Int, workQueueSize: Int, threadFactory: ThreadFactory)
  extends ExecutorService {

  private val permits = new Semaphore(nThreads + workQueueSize)

  private val workQuque = new LinkedBlockingQueue[Runnable](nThreads + workQueueSize)

  private val delegate = new ThreadPoolExecutor(
    nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, workQuque, threadFactory)

  override def shutdown(): Unit = delegate.shutdown()

  override def shutdownNow(): util.List[Runnable] = delegate.shutdownNow()

  override def isShutdown: Boolean = delegate.isShutdown

  override def isTerminated: Boolean = delegate.isTerminated

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
    delegate.awaitTermination(timeout, unit)

  override def submit[T](task: Callable[T]): Future[T] = {
    try permits.acquire() catch {
      case e: InterruptedException =>
        Thread.currentThread.interrupt()
        return Futures.immediateFailedFuture(e)
    }
    delegate.submit(new CallableWithPermitRelease(task))
  }

  override def submit[T](task: Runnable, result: T): Future[T] = {
    try permits.acquire() catch {
      case e: InterruptedException =>
        Thread.currentThread.interrupt()
        return Futures.immediateFailedFuture(e)
    }
    delegate.submit(new RunnableWithPermitRelease(task), result)
  }

  override def submit(task: Runnable): Future[_] = {
    try permits.acquire() catch {
      case e: InterruptedException =>
        Thread.currentThread.interrupt()
        return Futures.immediateFailedFuture(e)
    }
    delegate.submit(new RunnableWithPermitRelease(task))
  }

  override def execute(command: Runnable): Unit = {
    try permits.acquire() catch {
      case _: InterruptedException =>
        Thread.currentThread.interrupt()
    }
    delegate.execute(new RunnableWithPermitRelease(command))
  }

  override def invokeAll[T](
      tasks: util.Collection[_ <: Callable[T]]): util.List[Future[T]] =
    throw new UnsupportedOperationException("Not implemented")

  override def invokeAll[T](
      tasks: util.Collection[_ <: Callable[T]],
      timeout: Long, unit: TimeUnit): util.List[Future[T]] =
    throw new UnsupportedOperationException("Not implemented")

  override def invokeAny[T](tasks: util.Collection[_ <: Callable[T]]): T =
    throw new UnsupportedOperationException("Not implemented")

  override def invokeAny[T](
      tasks: util.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit): T =
    throw new UnsupportedOperationException("Not implemented")

  /**
   * Releases a permit after the task is executed.
   */
  private class RunnableWithPermitRelease(delegate: Runnable) extends Runnable {
    override def run(): Unit = try delegate.run() finally permits.release()
  }

  /**
   * Releases a permit after the task is completed.
   */
  private class CallableWithPermitRelease[T](delegate: Callable[T]) extends Callable[T] {
    override def call(): T = try delegate.call() finally permits.release()
  }
}
