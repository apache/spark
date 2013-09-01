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

import scala.collection.mutable.ArrayBuffer

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 */
private[spark] class JobWaiter[T](totalTasks: Int, resultHandler: (Int, T) => Unit)
  extends JobListener {

  private var finishedTasks = 0

  private var jobFinished = false          // Is the job as a whole finished (succeeded or failed)?
  private var jobResult: JobResult = null  // If the job is finished, this will be its result

  override def taskSucceeded(index: Int, result: Any) {
    synchronized {
      if (jobFinished) {
        throw new UnsupportedOperationException("taskSucceeded() called on a finished JobWaiter")
      }
      resultHandler(index, result.asInstanceOf[T])
      finishedTasks += 1
      if (finishedTasks == totalTasks) {
        jobFinished = true
        jobResult = JobSucceeded
        this.notifyAll()
      }
    }
  }

  override def jobFailed(exception: Exception) {
    synchronized {
      if (jobFinished) {
        throw new UnsupportedOperationException("jobFailed() called on a finished JobWaiter")
      }
      jobFinished = true
      jobResult = JobFailed(exception, None)
      this.notifyAll()
    }
  }

  def awaitResult(): JobResult = synchronized {
    while (!jobFinished) {
      this.wait()
    }
    return jobResult
  }
}
