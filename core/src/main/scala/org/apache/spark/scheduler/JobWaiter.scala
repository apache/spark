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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Future, Promise}

import org.apache.spark.internal.Logging

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 */
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler,
    val jobId: Int,
    totalTasks: Int,
    resultHandler: (Int, T) => Unit)
  extends JobListener with Logging {

  private val finishedTasks = new AtomicInteger(0)
  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  private val jobPromise: Promise[Unit] =
    if (totalTasks == 0) Promise.successful(()) else Promise()

  def jobFinished: Boolean = jobPromise.isCompleted

  def completionFuture: Future[Unit] = jobPromise.future

  /**
   * Sends a signal to the DAGScheduler to cancel the job. The cancellation itself is handled
   * asynchronously. After the low level scheduler cancels all the tasks belonging to this job, it
   * will fail this job with a SparkException.
   */
  def cancel() {
    dagScheduler.cancelJob(jobId, None)
  }

  override def taskSucceeded(index: Int, result: Any): Unit = {
    // resultHandler call must be synchronized in case resultHandler itself is not thread safe.
    synchronized {
      resultHandler(index, result.asInstanceOf[T])
    }
    if (finishedTasks.incrementAndGet() == totalTasks) {
      jobPromise.success(())
    }
  }

  override def jobFailed(exception: Exception): Unit = {
    if (!jobPromise.tryFailure(exception)) {
      logWarning("Ignore failure", exception)
    }
  }

}
