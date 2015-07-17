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

package org.apache.spark.scheduler

import scala.concurrent.duration.Duration
import scala.concurrent._
import scala.util.Try

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 */
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler,
    val jobId: Int,
    totalTasks: Int,
    resultHandler: (Int, T) => Unit)
  extends JobListener with Future[Unit] {

  private[this] val promise: Promise[Unit] = {
    if (totalTasks == 0) {
      Promise.successful[Unit]()
    } else {
      Promise[Unit]()
    }
  }
  private[this] val promiseFuture: Future[Unit] = promise.future
  private[this] var finishedTasks = 0

  override def onComplete[U](func: (Try[Unit]) => U)(implicit executor: ExecutionContext): Unit = {
    promiseFuture.onComplete(func)
  }

  override def isCompleted: Boolean = promiseFuture.isCompleted

  override def value: Option[Try[Unit]] = promiseFuture.value

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): Unit = {
    promiseFuture.result(atMost)(permit)
  }

  @throws(classOf[InterruptedException])
  @throws(classOf[TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    promiseFuture.ready(atMost)(permit)
    this
  }

  /**
   * Sends a signal to the DAGScheduler to cancel the job. The cancellation itself is handled
   * asynchronously. After the low level scheduler cancels all the tasks belonging to this job, it
   * will fail this job with a SparkException.
   */
  def cancel() {
    dagScheduler.cancelJob(jobId)
  }

  override def taskSucceeded(index: Int, result: Any): Unit = synchronized {
    if (isCompleted) {
      throw new UnsupportedOperationException("taskSucceeded() called on a finished JobWaiter")
    }
    resultHandler(index, result.asInstanceOf[T])
    finishedTasks += 1
    if (finishedTasks == totalTasks) {
      promise.success()
    }
  }

  override def jobFailed(exception: Exception): Unit = synchronized {
    // There are certain situations where jobFailed can be called multiple times for the same
    // job. We guard against this by making this method idempotent.
    if (!isCompleted) {
      promise.failure(exception)
    } else {
      assert(promiseFuture.value.get.isFailure)
    }
  }
}
