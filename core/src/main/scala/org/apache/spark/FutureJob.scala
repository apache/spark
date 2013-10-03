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

package org.apache.spark

import java.util.concurrent.TimeoutException

import scala.concurrent.{CanAwait, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.spark.scheduler.{JobFailed, JobSucceeded, JobWaiter}

class FutureJob[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: () => T)
  extends Future[T] {

  /**
   * Cancels the execution of this job.
   */
  def cancel() {
    jobWaiter.kill()
  }

  /**
   * Blocks until this job completes.
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
   * @return this FutureJob
   */
  override def ready(atMost: Duration)(implicit permit: CanAwait): FutureJob.this.type = {
    if (atMost.isFinite()) {
      awaitResult()
    } else {
      val finishTime = System.currentTimeMillis() + atMost.toMillis
      while (!isCompleted) {
        val time = System.currentTimeMillis()
        if (time >= finishTime) {
          throw new TimeoutException
        } else {
          jobWaiter.wait(finishTime - time)
        }
      }
    }
    this
  }

  /**
   * Await and return the result (of type T) of this job.
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
   * @throws Exception exception during job execution
   * @return the result value if the job is completed within the specific maximum wait time
   */
  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    ready(atMost)(permit)
    awaitResult() match {
      case scala.util.Success(res) => res
      case scala.util.Failure(e) => throw e
    }
  }

  /**
   * When this job is completed, either through an exception, or a value, apply the provided
   * function.
   */
  def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext) {
    executor.execute(new Runnable {
      override def run() {
        func(awaitResult())
      }
    })
  }

  /**
   * Returns whether the job has already been completed with a value or an exception.
   */
  def isCompleted: Boolean = jobWaiter.jobFinished

  /**
   * The value of this Future.
   *
   * If the future is not completed the returned value will be None. If the future is completed
   * the value will be Some(Success(t)) if it contains a valid result, or Some(Failure(error)) if
   * it contains an exception.
   */
  def value: Option[Try[T]] = {
    if (jobWaiter.jobFinished) {
      Some(awaitResult())
    } else {
      None
    }
  }

  /**
   * Block and return the result of this job.
   */
  @throws(classOf[Exception])
  def get(): T = {
    awaitResult() match {
      case scala.util.Success(res) => res
      case scala.util.Failure(e) => throw e
    }
  }

  private def awaitResult(): Try[T] = {
    jobWaiter.awaitResult() match {
      case JobSucceeded => scala.util.Success(resultFunc())
      case JobFailed(e: Exception, _) => scala.util.Failure(e)
    }
  }
}
