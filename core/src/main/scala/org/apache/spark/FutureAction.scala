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

import java.util.Collections
import java.util.concurrent.TimeUnit

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.JavaFutureAction
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.JobWaiter
import org.apache.spark.util.ThreadUtils


/**
 * A future for the result of an action to support cancellation. This is an extension of the
 * Scala Future interface to support cancellation.
 */
trait FutureAction[T] extends Future[T] {
  // Note that we redefine methods of the Future trait here explicitly so we can specify a different
  // documentation (with reference to the word "action").

  /**
   * Cancels the execution of this action.
   */
  def cancel(): Unit

  /**
   * Blocks until this action completes.
   *
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
   * @return this FutureAction
   */
  override def ready(atMost: Duration)(implicit permit: CanAwait): FutureAction.this.type

  /**
   * Awaits and returns the result (of type T) of this action.
   *
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
   * @throws Exception exception during action execution
   * @return the result value if the action is completed within the specific maximum wait time
   */
  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T

  /**
   * When this action is completed, either through an exception, or a value, applies the provided
   * function.
   */
  def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit

  /**
   * Returns whether the action has already been completed with a value or an exception.
   */
  override def isCompleted: Boolean

  /**
   * Returns whether the action has been cancelled.
   */
  def isCancelled: Boolean

  /**
   * The value of this Future.
   *
   * If the future is not completed the returned value will be None. If the future is completed
   * the value will be Some(Success(t)) if it contains a valid result, or Some(Failure(error)) if
   * it contains an exception.
   */
  override def value: Option[Try[T]]

  /**
   * Blocks and returns the result of this job.
   */
  @throws(classOf[SparkException])
  def get(): T = ThreadUtils.awaitResult(this, Duration.Inf)

  /**
   * Returns the job IDs run by the underlying async operation.
   *
   * This returns the current snapshot of the job list. Certain operations may run multiple
   * jobs, so multiple calls to this method may return different lists.
   */
  def jobIds: Seq[Int]

}


/**
 * A [[FutureAction]] holding the result of an action that triggers a single job. Examples include
 * count, collect, reduce.
 */
@DeveloperApi
class SimpleFutureAction[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: => T)
  extends FutureAction[T] {

  @volatile private var _cancelled: Boolean = false

  override def cancel() {
    _cancelled = true
    jobWaiter.cancel()
  }

  private[spark] def cancelWithoutFailing(): Unit = {
    _cancelled = true
    jobWaiter.cancel(failJob = false)
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): SimpleFutureAction.this.type = {
    jobWaiter.completionFuture.ready(atMost)
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    jobWaiter.completionFuture.ready(atMost)
    assert(value.isDefined, "Future has not completed properly")
    value.get.get
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext) {
    jobWaiter.completionFuture onComplete {_ => func(value.get)}
  }

  override def isCompleted: Boolean = jobWaiter.jobFinished

  override def isCancelled: Boolean = _cancelled

  override def value: Option[Try[T]] =
    jobWaiter.completionFuture.value.map {res => res.map(_ => resultFunc)}

  def jobIds: Seq[Int] = Seq(jobWaiter.jobId)
}


/**
 * Handle via which a "run" function passed to a [[ComplexFutureAction]]
 * can submit jobs for execution.
 */
@DeveloperApi
trait JobSubmitter {
  /**
   * Submit a job for execution and return a FutureAction holding the result.
   * This is a wrapper around the same functionality provided by SparkContext
   * to enable cancellation.
   */
  def submitJob[T, U, R](
    rdd: RDD[T],
    processPartition: Iterator[T] => U,
    partitions: Seq[Int],
    resultHandler: (Int, U) => Unit,
    resultFunc: => R): FutureAction[R]
}


/**
 * A [[FutureAction]] for actions that could trigger multiple Spark jobs. Examples include take,
 * takeSample. Cancellation works by setting the cancelled flag to true and cancelling any pending
 * jobs.
 */
@DeveloperApi
class ComplexFutureAction[T](run : JobSubmitter => Future[T])
  extends FutureAction[T] { self =>

  @volatile private var _cancelled = false

  @volatile private var subActions: List[FutureAction[_]] = Nil

  // A promise used to signal the future.
  private val p = Promise[T]().tryCompleteWith(run(jobSubmitter))

  override def cancel(): Unit = synchronized {
    _cancelled = true
    p.tryFailure(new SparkException("Action has been cancelled"))
    subActions.foreach(_.cancel())
  }

  private def jobSubmitter = new JobSubmitter {
    def submitJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R): FutureAction[R] = self.synchronized {
      // If the action hasn't been cancelled yet, submit the job. The check and the submitJob
      // command need to be in an atomic block.
      if (!isCancelled) {
        val job = rdd.context.submitJob(
          rdd,
          processPartition,
          partitions,
          resultHandler,
          resultFunc)
        subActions = job :: subActions
        job
      } else {
        throw new SparkException("Action has been cancelled")
      }
    }
  }

  override def isCancelled: Boolean = _cancelled

  @throws(classOf[InterruptedException])
  @throws(classOf[scala.concurrent.TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    p.future.ready(atMost)(permit)
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    p.future.result(atMost)(permit)
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    p.future.onComplete(func)(executor)
  }

  override def isCompleted: Boolean = p.isCompleted

  override def value: Option[Try[T]] = p.future.value

  def jobIds: Seq[Int] = subActions.flatMap(_.jobIds)

}


private[spark]
class JavaFutureActionWrapper[S, T](futureAction: FutureAction[S], converter: S => T)
  extends JavaFutureAction[T] {

  import scala.collection.JavaConverters._

  override def isCancelled: Boolean = futureAction.isCancelled

  override def isDone: Boolean = {
    // According to java.util.Future's Javadoc, this returns True if the task was completed,
    // whether that completion was due to successful execution, an exception, or a cancellation.
    futureAction.isCancelled || futureAction.isCompleted
  }

  override def jobIds(): java.util.List[java.lang.Integer] = {
    Collections.unmodifiableList(futureAction.jobIds.map(Integer.valueOf).asJava)
  }

  private def getImpl(timeout: Duration): T = {
    // This will throw TimeoutException on timeout:
    Await.ready(futureAction, timeout)
    futureAction.value.get match {
      case scala.util.Success(value) => converter(value)
      case scala.util.Failure(exception) =>
        if (isCancelled) {
          throw new CancellationException("Job cancelled").initCause(exception)
        } else {
          // java.util.Future.get() wraps exceptions in ExecutionException
          throw new ExecutionException("Exception thrown by job", exception)
        }
    }
  }

  override def get(): T = getImpl(Duration.Inf)

  override def get(timeout: Long, unit: TimeUnit): T =
    getImpl(Duration.fromNanos(unit.toNanos(timeout)))

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = synchronized {
    if (isDone) {
      // According to java.util.Future's Javadoc, this should return false if the task is completed.
      false
    } else {
      // We're limited in terms of the semantics we can provide here; our cancellation is
      // asynchronous and doesn't provide a mechanism to not cancel if the job is running.
      futureAction.cancel()
      true
    }
  }

}
