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

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.spark.scheduler.{JobSucceeded, JobWaiter}
import org.apache.spark.scheduler.JobFailed
import org.apache.spark.rdd.RDD


/**
 * A future for the result of an action. This is an extension of the Scala Future interface to
 * support cancellation.
 */
trait FutureAction[T] extends Future[T] {

  /**
   * Cancels the execution of this action.
   */
  def cancel()

  /**
   * Blocks until this action completes.
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
   * @return this FutureAction
   */
  override def ready(atMost: Duration)(implicit permit: CanAwait): FutureAction.this.type

  /**
   * Await and return the result (of type T) of this action.
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
   * @throws Exception exception during action execution
   * @return the result value if the action is completed within the specific maximum wait time
   */
  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T

  /**
   * When this action is completed, either through an exception, or a value, apply the provided
   * function.
   */
  def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext)

  /**
   * Returns whether the action has already been completed with a value or an exception.
   */
  override def isCompleted: Boolean

  /**
   * The value of this Future.
   *
   * If the future is not completed the returned value will be None. If the future is completed
   * the value will be Some(Success(t)) if it contains a valid result, or Some(Failure(error)) if
   * it contains an exception.
   */
  override def value: Option[Try[T]]

  /**
   * Block and return the result of this job.
   */
  @throws(classOf[Exception])
  def get(): T = Await.result(this, Duration.Inf)
}


/**
 * The future holding the result of an action that triggers a single job. Examples include
 * count, collect, reduce.
 */
class FutureJob[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: => T)
  extends FutureAction[T] {

  override def cancel() {
    jobWaiter.kill()
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): FutureJob.this.type = {
    if (!atMost.isFinite()) {
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

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    ready(atMost)(permit)
    awaitResult() match {
      case scala.util.Success(res) => res
      case scala.util.Failure(e) => throw e
    }
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext) {
    executor.execute(new Runnable {
      override def run() {
        func(awaitResult())
      }
    })
  }

  override def isCompleted: Boolean = jobWaiter.jobFinished

  override def value: Option[Try[T]] = {
    if (jobWaiter.jobFinished) {
      Some(awaitResult())
    } else {
      None
    }
  }

  private def awaitResult(): Try[T] = {
    jobWaiter.awaitResult() match {
      case JobSucceeded => scala.util.Success(resultFunc)
      case JobFailed(e: Exception, _) => scala.util.Failure(e)
    }
  }
}


/**
 * A FutureAction for actions that could trigger multiple Spark jobs. Examples include take,
 * takeSample.
 *
 * This is implemented as a Scala Promise that can be cancelled. Note that the promise itself is
 * also its own Future (i.e. this.future returns this). See the implementation of takeAsync for
 * usage.
 */
class CancellablePromise[T] extends FutureAction[T] with Promise[T] {
  // Cancellation works by setting the cancelled flag to true and interrupt the action thread
  // if it is in progress. Before executing the action, the execution thread needs to check the
  // cancelled flag in case cancel() is called before the thread even starts to execute. Because
  // this and the execution thread is synchronized on the same promise object (this), the actual
  // cancellation/interrupt event can only be triggered when the execution thread is waiting for
  // the result of a job.

  override def cancel(): Unit = this.synchronized {
    _cancelled = true
    if (thread != null) {
      thread.interrupt()
    }
  }

  /**
   * Executes some action enclosed in the closure. To properly enable cancellation, the closure
   * should use runJob implementation in this promise. See takeAsync for example.
   */
  def run(func: => T)(implicit executor: ExecutionContext): Unit = scala.concurrent.future {
    thread = Thread.currentThread
    try {
      this.success(func)
    } catch {
      case e: Exception => this.failure(e)
    } finally {
      thread = null
    }
  }

  /**
   * Runs a Spark job. This is a wrapper around the same functionality provided by SparkContext
   * to enable cancellation.
   */
  def runJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      partitionResultHandler: (Int, U) => Unit,
      resultFunc: => R) {
    // If the action hasn't been cancelled yet, submit the job. The check and the submitJob
    // command need to be in an atomic block.
    val job = this.synchronized {
      if (!cancelled) {
        rdd.context.submitJob(rdd, processPartition, partitions, partitionResultHandler, resultFunc)
      } else {
        throw new SparkException("action has been cancelled")
      }
    }

    // Wait for the job to complete. If the action is cancelled (with an interrupt),
    // cancel the job and stop the execution. This is not in a synchronized block because
    // Await.ready eventually waits on the monitor in FutureJob.jobWaiter.
    try {
      Await.ready(job, Duration.Inf)
    } catch {
      case e: InterruptedException =>
        job.cancel()
        throw new SparkException("action has been cancelled")
    }
  }

  /**
   * Returns whether the promise has been cancelled.
   */
  def cancelled: Boolean = _cancelled

  // Pointer to the thread that is executing the action. It is set when the action is run.
  @volatile private var thread: Thread = _

  // A flag indicating whether the future has been cancelled. This is used in case the future
  // is cancelled before the action was even run (and thus we have no thread to interrupt).
  @volatile private var _cancelled: Boolean = false

  // Internally, we delegate most functionality to this promise.
  private val p = promise[T]()

  override def future: this.type = this

  override def tryComplete(result: Try[T]): Boolean = p.tryComplete(result)

  @scala.throws(classOf[InterruptedException])
  @scala.throws(classOf[scala.concurrent.TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    p.future.ready(atMost)(permit)
    this
  }

  @scala.throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    p.future.result(atMost)(permit)
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    p.future.onComplete(func)(executor)
  }

  override def isCompleted: Boolean = p.isCompleted

  override def value: Option[Try[T]] = p.future.value
}
